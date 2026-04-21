/**
 * Service Worker for HPAL - Offline support, caching, and background sync
 *
 * Handles:
 * - Static asset caching (app shell)
 * - API response caching with fallback
 * - Offline fallback page
 * - Background sync for pending actions
 * - Push notifications
 */

const CACHE_VERSION = 'hpal-v1';
const STATIC_CACHE = `${CACHE_VERSION}-static`;
const API_CACHE = `${CACHE_VERSION}-api`;
const OFFLINE_PAGE = '/offline.html';

const STATIC_ASSETS = [
  '/',
  '/index.html',
  '/manifest.json',
  '/offline.html',
  '/src/main.tsx',
  '/api/contracts',
];

const API_PATTERNS = [
  /^https?:\/\/.*\/v1\/(ui|chat|action)/,
];

/**
 * Install event - cache static assets
 */
self.addEventListener('install', (event) => {
  console.log('Service Worker: install event');
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) => {
      return cache.addAll(STATIC_ASSETS).catch((err) => {
        console.warn('Could not cache some static assets:', err);
      });
    })
  );
  self.skipWaiting();
});

/**
 * Activate event - clean up old caches
 */
self.addEventListener('activate', (event) => {
  console.log('Service Worker: activate event');
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames
          .filter((name) => name.startsWith('hpal-') && name !== STATIC_CACHE && name !== API_CACHE)
          .map((name) => caches.delete(name))
      );
    })
  );
  self.clients.claim();
});

/**
 * Fetch event - network-first for APIs, cache-first for static assets
 */
self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);

  // Skip non-GET requests and non-HTTP(S) URLs
  if (request.method !== 'GET' || !url.protocol.startsWith('http')) {
    return;
  }

  // Handle API requests
  if (API_PATTERNS.some((pattern) => pattern.test(request.url))) {
    event.respondWith(networkFirstStrategy(request));
    return;
  }

  // Handle static assets
  event.respondWith(cacheFirstStrategy(request));
});

/**
 * Network-first strategy for API calls
 * Try network, fallback to cache, then offline page
 */
async function networkFirstStrategy(request) {
  try {
    const response = await fetch(request.clone());

    if (response.ok) {
      // Cache successful responses
      const cache = await caches.open(API_CACHE);
      cache.put(request, response.clone());
    }

    return response;
  } catch (error) {
    console.log('Network request failed, trying cache:', request.url);

    // Try cache
    const cachedResponse = await caches.match(request);
    if (cachedResponse) {
      return cachedResponse;
    }

    // Return offline page as fallback
    return caches.match(OFFLINE_PAGE);
  }
}

/**
 * Cache-first strategy for static assets
 * Try cache, fallback to network, then offline page
 */
async function cacheFirstStrategy(request) {
  const cached = await caches.match(request);
  if (cached) {
    return cached;
  }

  try {
    const response = await fetch(request.clone());

    if (response.ok) {
      const cache = await caches.open(STATIC_CACHE);
      cache.put(request, response.clone());
    }

    return response;
  } catch (error) {
    console.log('Cache miss and network unavailable:', request.url);
    return caches.match(OFFLINE_PAGE);
  }
}

/**
 * Background sync for pending actions
 * Triggered when connection is restored
 */
self.addEventListener('sync', (event) => {
  console.log('Service Worker: background sync event', event.tag);

  if (event.tag === 'sync-pending-actions') {
    event.waitUntil(syncPendingActions());
  }

  if (event.tag === 'sync-pending-messages') {
    event.waitUntil(syncPendingMessages());
  }
});

async function syncPendingActions() {
  try {
    // Retrieve pending actions from IndexedDB
    const db = await openIndexedDB();
    const pendingActions = await getFromDB(db, 'pending-actions');

    if (!pendingActions || pendingActions.length === 0) {
      return;
    }

    // Replay each pending action
    for (const action of pendingActions) {
      try {
        const response = await fetch('/api/v1/ui/action', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            ...action.headers,
          },
          body: JSON.stringify(action.body),
        });

        if (response.ok) {
          // Remove from pending
          await deleteFromDB(db, 'pending-actions', action.id);
          console.log('Replayed pending action:', action.id);
        }
      } catch (err) {
        console.error('Failed to replay action:', err);
      }
    }

    // Notify all clients that sync is complete
    const clients = await self.clients.matchAll();
    clients.forEach((client) => {
      client.postMessage({ type: 'sync-complete', category: 'actions' });
    });
  } catch (err) {
    console.error('Background sync failed:', err);
  }
}

async function syncPendingMessages() {
  try {
    const db = await openIndexedDB();
    const pendingMessages = await getFromDB(db, 'pending-messages');

    if (!pendingMessages || pendingMessages.length === 0) {
      return;
    }

    for (const message of pendingMessages) {
      try {
        const response = await fetch('/api/v1/ui/message', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            ...message.headers,
          },
          body: JSON.stringify(message.body),
        });

        if (response.ok) {
          await deleteFromDB(db, 'pending-messages', message.id);
          console.log('Replayed pending message:', message.id);
        }
      } catch (err) {
        console.error('Failed to replay message:', err);
      }
    }

    const clients = await self.clients.matchAll();
    clients.forEach((client) => {
      client.postMessage({ type: 'sync-complete', category: 'messages' });
    });
  } catch (err) {
    console.error('Message sync failed:', err);
  }
}

/**
 * Push notifications
 */
self.addEventListener('push', (event) => {
  console.log('Service Worker: push event');

  let notificationData = {
    title: 'HPAL',
    body: 'New message from family',
    icon: '/images/icon-192.png',
    badge: '/images/icon-192.png',
    tag: 'hpal-notification',
  };

  if (event.data) {
    try {
      notificationData = event.data.json();
    } catch (err) {
      notificationData.body = event.data.text();
    }
  }

  event.waitUntil(
    self.registration.showNotification(notificationData.title, {
      body: notificationData.body,
      icon: notificationData.icon,
      badge: notificationData.badge,
      tag: notificationData.tag,
      data: notificationData.data || {},
      actions: [
        { action: 'open', title: 'Open' },
        { action: 'dismiss', title: 'Dismiss' },
      ],
    })
  );
});

/**
 * Notification click handler
 */
self.addEventListener('notificationclick', (event) => {
  console.log('Service Worker: notification click', event.action);

  event.notification.close();

  if (event.action === 'dismiss') {
    return;
  }

  // Open or focus the app
  event.waitUntil(
    self.clients.matchAll({ type: 'window' }).then((clients) => {
      // Check if app is already open
      for (const client of clients) {
        if (client.url === '/' && 'focus' in client) {
          return client.focus();
        }
      }
      // Open new window if not already open
      if (self.clients.openWindow) {
        return self.clients.openWindow(event.notification.data.url || '/');
      }
    })
  );
});

/**
 * Simple IndexedDB helpers for background sync persistence
 */
function openIndexedDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open('hpal-offline-db', 1);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);

    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains('pending-actions')) {
        db.createObjectStore('pending-actions', { keyPath: 'id' });
      }
      if (!db.objectStoreNames.contains('pending-messages')) {
        db.createObjectStore('pending-messages', { keyPath: 'id' });
      }
    };
  });
}

function getFromDB(db, storeName) {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(storeName, 'readonly');
    const store = transaction.objectStore(storeName);
    const request = store.getAll();

    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);
  });
}

function deleteFromDB(db, storeName, key) {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(storeName, 'readwrite');
    const store = transaction.objectStore(storeName);
    const request = store.delete(key);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve();
  });
}
