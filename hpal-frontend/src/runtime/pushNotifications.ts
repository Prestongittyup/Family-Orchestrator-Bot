/**
 * Push notification infrastructure for HPAL.
 *
 * Handles:
 * - Notification permission lifecycle
 * - Push subscription management
 * - Device registration with backend
 * - Notification routing by household/user
 * - Deterministic subscription persistence
 */

import type { RequestIdentityContext } from "../api/contracts";

export interface PushSubscription {
  endpoint: string;
  expirationTime: number | null;
  keys: {
    p256dh: string;
    auth: string;
  };
}

export interface NotificationPermissionState {
  status: NotificationPermission;
  timestamp: number;
  householdId?: string;
  userId?: string;
}

/**
 * Notification payload structure (deterministic)
 */
export interface NotificationPayload {
  title: string;
  body: string;
  icon?: string;
  badge?: string;
  tag?: string;
  url?: string;
  data?: Record<string, unknown>;
  householdId: string;
  userId: string;
}

/**
 * Manager for push notification lifecycle
 */
export class PushNotificationManager {
  private permissionCache: NotificationPermissionState | null = null;
  private subscriptionCache: PushSubscription | null = null;

  async requestPermission(
    householdId: string,
    userId: string
  ): Promise<NotificationPermission> {
    // Check feature availability
    if (!("Notification" in window) || !("serviceWorker" in navigator)) {
      console.warn("Push notifications not supported on this device");
      return "denied";
    }

    // Check current permission
    const currentPermission = Notification.permission;
    if (currentPermission === "granted" || currentPermission === "denied") {
      this.cachePermission(currentPermission, householdId, userId);
      return currentPermission;
    }

    // Request permission
    const permission = await Notification.requestPermission();

    if (permission === "granted") {
      this.cachePermission(permission, householdId, userId);
      // Subscribe to push immediately after permission granted
      await this.subscribeAndRegister(householdId, userId);
    } else {
      this.cachePermission(permission, householdId, userId);
    }

    return permission;
  }

  /**
   * Subscribe to push and register device with backend
   */
  async subscribeAndRegister(
    householdId: string,
    userId: string
  ): Promise<boolean> {
    try {
      // Check notification permission
      if (Notification.permission !== "granted") {
        console.warn("Notification permission not granted");
        return false;
      }

      // Get service worker registration
      const registration = await navigator.serviceWorker.ready;

      // Get or create push subscription
      let subscription = await registration.pushManager.getSubscription();

      if (!subscription) {
        // Create new subscription
        subscription = await registration.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey: this.urlBase64ToUint8Array(
            this.getApplicationServerKey()
          ),
        });
      }

      // Cache subscription
      this.cacheSubscription(subscription);

      // Register device with backend
      return await this.registerDeviceWithBackend(
        householdId,
        userId,
        subscription
      );
    } catch (err) {
      console.error("Failed to subscribe to push notifications:", err);
      return false;
    }
  }

  /**
   * Register device push subscription with backend
   */
  async registerDeviceWithBackend(
    householdId: string,
    userId: string,
    subscription: PushSubscription | PushManager
  ): Promise<boolean> {
    try {
      // Extract subscription data
      const endpoint =
        subscription instanceof PushManager
          ? (subscription as any).endpoint
          : subscription.endpoint;
      const keys =
        subscription instanceof PushManager
          ? (subscription as any).keys
          : subscription.keys;

      // Get device info (should be available from identity context)
      const deviceNameFromStorage =
        localStorage.getItem("hpal-device-name") ||
        `Device (${new Date().toLocaleDateString()})`;

      const response = await fetch("/api/v1/identity/device/register", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          household_id: householdId,
          user_id: userId,
          device_name: deviceNameFromStorage,
          platform: this.getPlatform(),
          user_agent: this.getUserAgentHash(),
          push_subscription: {
            endpoint,
            keys: {
              p256dh: this.arrayBufferToBase64(keys.p256dh),
              auth: this.arrayBufferToBase64(keys.auth),
            },
          },
        }),
      });

      if (response.ok) {
        console.log("Device registered with push subscription");
        return true;
      }

      console.error("Device registration failed:", await response.text());
      return false;
    } catch (err) {
      console.error("Failed to register device:", err);
      return false;
    }
  }

  /**
   * Request permission to send notifications
   */
  getPermissionStatus(): NotificationPermission {
    if (!("Notification" in window)) {
      return "denied";
    }
    return Notification.permission;
  }

  /**
   * Unsubscribe from push notifications
   */
  async unsubscribe(): Promise<boolean> {
    try {
      const registration = await navigator.serviceWorker.ready;
      const subscription = await registration.pushManager.getSubscription();

      if (subscription) {
        await subscription.unsubscribe();
        this.subscriptionCache = null;
        console.log("Unsubscribed from push notifications");
        return true;
      }

      return false;
    } catch (err) {
      console.error("Failed to unsubscribe from push:", err);
      return false;
    }
  }

  /**
   * Check if device is currently subscribed to push
   */
  async isSubscribed(): Promise<boolean> {
    try {
      const registration = await navigator.serviceWorker.ready;
      const subscription = await registration.pushManager.getSubscription();
      return !!subscription;
    } catch (err) {
      console.error("Failed to check push subscription status:", err);
      return false;
    }
  }

  /**
   * Store permission in localStorage with timestamp
   */
  private cachePermission(
    permission: NotificationPermission,
    householdId: string,
    userId: string
  ): void {
    const cached: NotificationPermissionState = {
      status: permission,
      timestamp: Date.now(),
      householdId,
      userId,
    };
    localStorage.setItem("hpal-notification-permission", JSON.stringify(cached));
    this.permissionCache = cached;
  }

  /**
   * Get cached permission state
   */
  getCachedPermission(): NotificationPermissionState | null {
    if (this.permissionCache) {
      return this.permissionCache;
    }

    try {
      const cached = localStorage.getItem("hpal-notification-permission");
      if (cached) {
        this.permissionCache = JSON.parse(cached);
        return this.permissionCache;
      }
    } catch (err) {
      console.error("Failed to parse cached permission:", err);
    }

    return null;
  }

  /**
   * Cache push subscription for offline reference
   */
  private cacheSubscription(subscription: any): void {
    const cached: PushSubscription = {
      endpoint: subscription.endpoint,
      expirationTime: subscription.expirationTime,
      keys: {
        p256dh: this.arrayBufferToBase64(subscription.getKey("p256dh")),
        auth: this.arrayBufferToBase64(subscription.getKey("auth")),
      },
    };
    localStorage.setItem("hpal-push-subscription", JSON.stringify(cached));
    this.subscriptionCache = cached;
  }

  /**
   * Get cached subscription
   */
  getCachedSubscription(): PushSubscription | null {
    if (this.subscriptionCache) {
      return this.subscriptionCache;
    }

    try {
      const cached = localStorage.getItem("hpal-push-subscription");
      if (cached) {
        this.subscriptionCache = JSON.parse(cached);
        return this.subscriptionCache;
      }
    } catch (err) {
      console.error("Failed to parse cached subscription:", err);
    }

    return null;
  }

  /**
   * Deterministic platform detection
   */
  private getPlatform(): "iOS" | "Android" | "Web" {
    const ua = navigator.userAgent.toLowerCase();

    if (/iphone|ipad|ipod/.test(ua)) {
      return "iOS";
    }

    if (/android/.test(ua)) {
      return "Android";
    }

    return "Web";
  }

  /**
   * Deterministic user agent hash
   */
  private getUserAgentHash(): string {
    return this.simpleHash(navigator.userAgent);
  }

  /**
   * Simple deterministic hash function
   */
  private simpleHash(str: string): string {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = (hash << 5) - hash + char;
      hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash).toString(16);
  }

  /**
   * Get VAPID public key (should be from env or config)
   */
  private getApplicationServerKey(): string {
    // Placeholder - in production, this should come from environment
    return (
      import.meta.env.VITE_VAPID_PUBLIC_KEY ||
      "DDQSm_KeKrEZ9LS5f0A0j0V41uW-i8n-aAZP7vb0n6w"
    );
  }

  /**
   * Convert VAPID key to Uint8Array
   */
  private urlBase64ToUint8Array(base64String: string): Uint8Array {
    const padding = "=".repeat((4 - (base64String.length % 4)) % 4);
    const base64 = (base64String + padding)
      .replace(/\-/g, "+")
      .replace(/_/g, "/");

    const rawData = window.atob(base64);
    const outputArray = new Uint8Array(rawData.length);

    for (let i = 0; i < rawData.length; ++i) {
      outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
  }

  /**
   * Convert ArrayBuffer to Base64
   */
  private arrayBufferToBase64(buffer: any): string {
    const bytes = new Uint8Array(buffer);
    let binary = "";
    for (let i = 0; i < bytes.byteLength; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return window.btoa(binary);
  }
}

/**
 * Global singleton instance
 */
export const pushNotificationManager = new PushNotificationManager();
