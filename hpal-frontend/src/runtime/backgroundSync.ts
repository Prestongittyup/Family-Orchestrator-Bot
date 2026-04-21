/**
 * Background sync and offline queue management for HPAL.
 *
 * Handles:
 * - Queuing actions/messages when offline
 * - Replaying queued items when connection restored
 * - IndexedDB persistence for offline queue
 * - Deterministic queue ordering
 * - Sync status monitoring
 */

export interface QueuedAction {
  id: string;
  timestamp: number;
  action: unknown;
  headers: Record<string, string>;
  retryCount: number;
  nextAttemptAt?: number;
}

export interface QueuedMessage {
  id: string;
  timestamp: number;
  message: string;
  headers: Record<string, string>;
  retryCount: number;
  nextAttemptAt?: number;
}

export interface SyncStatus {
  isOnline: boolean;
  pending: number;
  syncing: boolean;
  lastSyncTime: number | null;
  lastError: string | null;
}

/**
 * Manager for offline queue and background sync
 */
export class BackgroundSyncManager {
  private db: IDBDatabase | null = null;
  private syncStatus: SyncStatus = {
    isOnline: navigator.onLine,
    pending: 0,
    syncing: false,
    lastSyncTime: null,
    lastError: null,
  };
  private listeners: Set<(status: SyncStatus) => void> = new Set();
  private maxRetries = 3;
  private maxBackoffMs = 10000;

  constructor() {
    // Monitor online/offline status
    window.addEventListener("online", () => this.handleOnline());
    window.addEventListener("offline", () => this.handleOffline());

    // Initialize IndexedDB
    this.initDB();
  }

  /**
   * Initialize IndexedDB for offline storage
   */
  private async initDB(): Promise<void> {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open("hpal-offline", 1);

      request.onerror = () => reject(request.error);
      request.onsuccess = () => {
        this.db = request.result;
        this.updatePendingCount();
        resolve();
      };

      request.onupgradeneeded = (event) => {
        const db = (event.target as IDBOpenDBRequest).result;

        if (!db.objectStoreNames.contains("pending-actions")) {
          const actionStore = db.createObjectStore("pending-actions", {
            keyPath: "id",
          });
          actionStore.createIndex("timestamp", "timestamp");
        }

        if (!db.objectStoreNames.contains("pending-messages")) {
          const messageStore = db.createObjectStore("pending-messages", {
            keyPath: "id",
          });
          messageStore.createIndex("timestamp", "timestamp");
        }
      };
    });
  }

  /**
   * Queue an action for later replay
   */
  async queueAction(
    action: unknown,
    headers: Record<string, string>
  ): Promise<string> {
    const id = this.generateId();
    const queuedAction: QueuedAction = {
      id,
      timestamp: Date.now(),
      action,
      headers,
      retryCount: 0,
      nextAttemptAt: Date.now(),
    };

    await this.putInDB("pending-actions", queuedAction);
    this.updatePendingCount();
    this.notifyListeners();

    return id;
  }

  /**
   * Queue a message for later replay
   */
  async queueMessage(
    message: string,
    headers: Record<string, string>
  ): Promise<string> {
    const id = this.generateId();
    const queuedMessage: QueuedMessage = {
      id,
      timestamp: Date.now(),
      message,
      headers,
      retryCount: 0,
      nextAttemptAt: Date.now(),
    };

    await this.putInDB("pending-messages", queuedMessage);
    this.updatePendingCount();
    this.notifyListeners();

    return id;
  }

  /**
   * Get all queued actions (deterministic order by timestamp)
   */
  async getPendingActions(): Promise<QueuedAction[]> {
    return this.getAllFromDB("pending-actions");
  }

  /**
   * Get all queued messages (deterministic order by timestamp)
   */
  async getPendingMessages(): Promise<QueuedMessage[]> {
    return this.getAllFromDB("pending-messages");
  }

  /**
   * Remove queued action after successful replay
   */
  async removeAction(id: string): Promise<void> {
    await this.deleteFromDB("pending-actions", id);
    this.updatePendingCount();
    this.notifyListeners();
  }

  /**
   * Remove queued message after successful replay
   */
  async removeMessage(id: string): Promise<void> {
    await this.deleteFromDB("pending-messages", id);
    this.updatePendingCount();
    this.notifyListeners();
  }

  /**
   * Trigger manual sync (if online)
   */
  async triggerSync(): Promise<void> {
    if (!navigator.onLine) {
      console.log("Cannot sync while offline");
      return;
    }

    await this.sync();
  }

  /**
   * Get current sync status
   */
  getStatus(): SyncStatus {
    return { ...this.syncStatus };
  }

  /**
   * Subscribe to status changes
   */
  subscribe(listener: (status: SyncStatus) => void): () => void {
    this.listeners.add(listener);

    // Immediate callback with current status
    listener({ ...this.syncStatus });

    // Return unsubscribe function
    return () => {
      this.listeners.delete(listener);
    };
  }

  /**
   * Handle online event
   */
  private async handleOnline(): Promise<void> {
    console.log("App came online, triggering sync");
    this.syncStatus.isOnline = true;
    this.notifyListeners();

    // Request background sync if available
    if ("serviceWorker" in navigator && "SyncManager" in window) {
      try {
        const registration = await navigator.serviceWorker.ready;
        await (registration as any).sync.register("sync-pending-actions");
        await (registration as any).sync.register("sync-pending-messages");
      } catch (err) {
        console.error("Failed to register background sync:", err);
      }
    }

    // Also trigger immediate sync
    await this.sync();
  }

  /**
   * Handle offline event
   */
  private handleOffline(): void {
    console.log("App went offline, queuing enabled");
    this.syncStatus.isOnline = false;
    this.notifyListeners();
  }

  /**
   * Perform sync of pending actions and messages
   */
  private async sync(): Promise<void> {
    if (this.syncStatus.syncing) {
      return; // Sync already in progress
    }

    this.syncStatus.syncing = true;
    this.notifyListeners();

    try {
      const actions = await this.getPendingActions();
      const messages = await this.getPendingMessages();
      const now = Date.now();

      // Replay in order (deterministic by timestamp)
      for (const action of actions) {
        if ((action.nextAttemptAt ?? 0) > now) {
          continue;
        }
        await this.replayAction(action);
      }

      for (const message of messages) {
        if ((message.nextAttemptAt ?? 0) > now) {
          continue;
        }
        await this.replayMessage(message);
      }

      this.syncStatus.lastSyncTime = Date.now();
      this.syncStatus.lastError = null;
    } catch (err) {
      console.error("Sync failed:", err);
      this.syncStatus.lastError =
        err instanceof Error ? err.message : "Unknown error";
    } finally {
      this.syncStatus.syncing = false;
      this.notifyListeners();
    }
  }

  /**
   * Replay a queued action
   */
  private async replayAction(action: QueuedAction): Promise<void> {
    try {
      const response = await fetch("/api/v1/ui/action", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...action.headers,
        },
        body: JSON.stringify(action.action),
      });

      if (response.ok) {
        await this.removeAction(action.id);
        console.log("Replayed action:", action.id);
      } else if ([401, 403].includes(response.status)) {
        // Auth errors are permanent, remove permanently
        await this.removeAction(action.id);
        throw new Error(`Auth error: ${response.status}`);
      } else {
        throw new Error(`HTTP ${response.status}`);
      }
    } catch (err) {
      console.error("Failed to replay action:", err);

      if (action.retryCount < this.maxRetries) {
        action.retryCount++;
        action.nextAttemptAt = Date.now() + this.getBackoffMs(action.retryCount);
        await this.putInDB("pending-actions", action);
      } else {
        console.error("Max retries exceeded for action:", action.id);
        // Could implement dead-letter queue here
      }
    }
  }

  /**
   * Replay a queued message
   */
  private async replayMessage(message: QueuedMessage): Promise<void> {
    try {
      const response = await fetch("/api/v1/ui/message", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...message.headers,
        },
        body: JSON.stringify({ message: message.message }),
      });

      if (response.ok) {
        await this.removeMessage(message.id);
        console.log("Replayed message:", message.id);
      } else if ([401, 403].includes(response.status)) {
        // Auth errors are permanent
        await this.removeMessage(message.id);
        throw new Error(`Auth error: ${response.status}`);
      } else {
        throw new Error(`HTTP ${response.status}`);
      }
    } catch (err) {
      console.error("Failed to replay message:", err);

      if (message.retryCount < this.maxRetries) {
        message.retryCount++;
        message.nextAttemptAt = Date.now() + this.getBackoffMs(message.retryCount);
        await this.putInDB("pending-messages", message);
      } else {
        console.error("Max retries exceeded for message:", message.id);
      }
    }
  }

  private getBackoffMs(retryCount: number): number {
    const safeRetryCount = Math.max(0, retryCount);
    const baseDelay = Math.min(this.maxBackoffMs, 500 * 2 ** safeRetryCount);
    const jitter = Math.random() * baseDelay * 0.25;
    return Math.min(this.maxBackoffMs, baseDelay + jitter);
  }

  /**
   * Update pending queue count
   */
  private async updatePendingCount(): Promise<void> {
    try {
      const actions = await this.getAllFromDB("pending-actions");
      const messages = await this.getAllFromDB("pending-messages");
      this.syncStatus.pending = actions.length + messages.length;
    } catch (err) {
      console.error("Failed to update pending count:", err);
    }
  }

  /**
   * Notify all listeners of status change
   */
  private notifyListeners(): void {
    const status = { ...this.syncStatus };
    this.listeners.forEach((listener) => listener(status));
  }

  /**
   * Generate deterministic queue item ID
   */
  private generateId(): string {
    return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }

  // ===== IndexedDB Helpers =====

  private async putInDB(storeName: string, item: unknown): Promise<void> {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    return new Promise((resolve, reject) => {
      const transaction = this.db!.transaction(storeName, "readwrite");
      const store = transaction.objectStore(storeName);
      const request = store.put(item);

      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve();
    });
  }

  private async getAllFromDB(storeName: string): Promise<any[]> {
    if (!this.db) {
      return [];
    }

    return new Promise((resolve, reject) => {
      const transaction = this.db!.transaction(storeName, "readonly");
      const store = transaction.objectStore(storeName);
      const index = store.index("timestamp");
      const request = index.getAll();

      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve(request.result);
    });
  }

  private async deleteFromDB(storeName: string, key: string): Promise<void> {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    return new Promise((resolve, reject) => {
      const transaction = this.db!.transaction(storeName, "readwrite");
      const store = transaction.objectStore(storeName);
      const request = store.delete(key);

      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve();
    });
  }
}

/**
 * Global singleton instance
 */
export const backgroundSyncManager = new BackgroundSyncManager();
