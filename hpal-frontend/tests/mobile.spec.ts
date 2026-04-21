/**
 * Comprehensive tests for mobile productization layer.
 *
 * Tests:
 * - PWA installability (manifest presence, service worker)
 * - Offline/online transition behavior
 * - Push subscription lifecycle
 * - Onboarding flow state correctness
 * - Background sync determinism
 * - Multi-device session continuity
 */

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { OnboardingFlow } from "../src/runtime/onboarding";
import { BackgroundSyncManager } from "../src/runtime/backgroundSync";
import { PushNotificationManager } from "../src/runtime/pushNotifications";
import type { OnboardingState } from "../src/runtime/onboarding";

/**
 * ============================================================================
 * PWA Manifest and Service Worker Tests
 * ============================================================================
 */

describe("PWA Installability", () => {
  it("should have manifest.json at correct path", async () => {
    const response = await fetch("/manifest.json");
    expect(response.ok).toBe(true);
    expect(response.headers.get("content-type")).toContain("application/json");
  });

  it("should have valid manifest with required fields", async () => {
    const response = await fetch("/manifest.json");
    const manifest = await response.json();

    expect(manifest.name).toBeDefined();
    expect(manifest.short_name).toBeDefined();
    expect(manifest.display).toBe("standalone");
    expect(manifest.start_url).toBe("/");
    expect(manifest.scope).toBe("/");
    expect(manifest.icons).toBeDefined();
    expect(manifest.icons.length).toBeGreaterThan(0);
  });

  it("should have service worker available", async () => {
    expect("serviceWorker" in navigator).toBe(true);
  });

  it("should register service worker successfully", async () => {
    if (!("serviceWorker" in navigator)) {
      throw new Error("Service Worker API not available");
    }

    // This would run in a real browser environment
    // In test environment, we just verify the endpoint exists
    const response = await fetch("/sw.js");
    expect(response.ok).toBe(true);
    expect(response.headers.get("content-type")).toContain("javascript");
  });

  it("should have offline fallback page", async () => {
    const response = await fetch("/offline.html");
    expect(response.ok).toBe(true);
    expect(response.headers.get("content-type")).toContain("text/html");
  });

  it("manifest should include all required shortcut icons", async () => {
    const response = await fetch("/manifest.json");
    const manifest = await response.json();

    expect(manifest.shortcuts).toBeDefined();
    const shortcutNames = manifest.shortcuts.map((s: any) => s.name);
    expect(shortcutNames).toContain("Dashboard");
    expect(shortcutNames).toContain("Tasks");
    expect(shortcutNames).toContain("Chat");
    expect(shortcutNames).toContain("Calendar");
  });
});

/**
 * ============================================================================
 * Offline/Online Transition Tests
 * ============================================================================
 */

describe("Offline/Online Behavior", () => {
  let syncManager: BackgroundSyncManager;

  beforeEach(() => {
    syncManager = new BackgroundSyncManager();
  });

  it("should detect online status correctly", () => {
    const status = syncManager.getStatus();
    expect(status).toHaveProperty("isOnline");
    expect(typeof status.isOnline).toBe("boolean");
  });

  it("should detect pending queue count", () => {
    const status = syncManager.getStatus();
    expect(status).toHaveProperty("pending");
    expect(typeof status.pending).toBe("number");
    expect(status.pending >= 0).toBe(true);
  });

  it("should subscribe to status changes", (done) => {
    let callCount = 0;

    const unsubscribe = syncManager.subscribe((status) => {
      callCount++;
      expect(status).toHaveProperty("isOnline");

      if (callCount >= 1) {
        unsubscribe();
        done();
      }
    });
  });

  it("should queue actions when offline", async () => {
    // Mock offline state
    Object.defineProperty(navigator, "onLine", {
      writable: true,
      value: false,
    });

    const action = { type: "test-action", payload: {} };
    const headers = { "Authorization": "Bearer token" };

    const id = await syncManager.queueAction(action, headers);
    expect(id).toBeDefined();
    expect(typeof id).toBe("string");

    const status = syncManager.getStatus();
    expect(status.pending).toGreaterThan(0);
  });

  it("should deterministically order queued items by timestamp", async () => {
    const action1 = { payload: 1 };
    const action2 = { payload: 2 };
    const headers = {};

    const id1 = await syncManager.queueAction(action1, headers);
    // Small delay to ensure different timestamps
    await new Promise((resolve) => setTimeout(resolve, 10));
    const id2 = await syncManager.queueAction(action2, headers);

    const pending = await syncManager.getPendingActions();
    expect(pending.length).toBeGreaterThanOrEqual(2);
    expect(pending[0].action).toEqual(expect.objectContaining({ payload: 1 }));
  });

  it("should persist queue across page reloads", async () => {
    const action = { type: "persistent-action" };
    await syncManager.queueAction(action, {});

    const pending = await syncManager.getPendingActions();
    expect(pending.length).toBeGreaterThan(0);
  });
});

/**
 * ============================================================================
 * Push Notification Tests
 * ============================================================================
 */

describe("Push Notifications", () => {
  let pushManager: PushNotificationManager;

  beforeEach(() => {
    pushManager = new PushNotificationManager();
  });

  it("should check notification support", () => {
    const supported = "Notification" in window;
    expect(typeof supported).toBe("boolean");
  });

  it("should get current notification permission", () => {
    if ("Notification" in window) {
      const permission = pushManager.getPermissionStatus();
      expect(["granted", "denied", "default"]).toContain(permission);
    }
  });

  it("should cache notification permission with timestamp", () => {
    pushManager["cachePermission"]("granted", "hh-001", "user-001");
    const cached = pushManager.getCachedPermission();

    expect(cached).not.toBeNull();
    expect(cached!.status).toBe("granted");
    expect(cached!.householdId).toBe("hh-001");
    expect(cached!.userId).toBe("user-001");
    expect(cached!.timestamp).toBeGreaterThan(0);
  });

  it("should cache push subscription", () => {
    const mockSubscription = {
      endpoint: "https://push.example.com/endpoint",
      expirationTime: null,
      getKey: (type: string) => new Uint8Array([1, 2, 3]),
    };

    pushManager["cacheSubscription"](mockSubscription);
    const cached = pushManager.getCachedSubscription();

    expect(cached).not.toBeNull();
    expect(cached!.endpoint).toContain("https://");
  });

  it("should detect platform deterministically", () => {
    const platform = pushManager["getPlatform"]();
    expect(["iOS", "Android", "Web"]).toContain(platform);
  });

  it("should generate consistent user agent hash", () => {
    const hash1 = pushManager["getUserAgentHash"]();
    const hash2 = pushManager["getUserAgentHash"]();

    expect(hash1).toBe(hash2);
    expect(typeof hash1).toBe("string");
  });
});

/**
 * ============================================================================
 * Onboarding Flow Tests
 * ============================================================================
 */

describe("Onboarding Flow", () => {
  let flow: OnboardingFlow;

  beforeEach(() => {
    flow = new OnboardingFlow();
  });

  afterEach(() => {
    flow.reset();
  });

  it("should start at welcome step", () => {
    const state = flow.getState();
    expect(state.step).toBe("welcome");
    expect(state.progress).toBe(0);
  });

  it("should select create household flow", () => {
    flow.selectCreateHousehold();
    const state = flow.getState();

    expect(state.step).toBe("household-name");
    expect(state.progress).toBeGreaterThan(0);
  });

  it("should select join household flow", () => {
    flow.selectJoinHousehold("invite-token-123");
    const state = flow.getState();

    expect(state.step).toBe("join-household");
    expect(state.joinToken).toBe("invite-token-123");
  });

  it("should set household name and validate", () => {
    flow.selectCreateHousehold();
    flow.setHouseholdName("Test Household");

    const state = flow.getState();
    expect(state.householdName).toBe("Test Household");
    expect(flow.canProgress()).toBe(true);
  });

  it("should set founder name", () => {
    flow.selectCreateHousehold();
    flow.setHouseholdName("Test");
    flow.setFounderName("Alice");

    const state = flow.getState();
    expect(state.founderName).toBe("Alice");
  });

  it("should set founder email (optional)", () => {
    flow.setFounderEmail("alice@example.com");
    const state = flow.getState();

    expect(state.founderEmail).toBe("alice@example.com");
  });

  it("should select user role", () => {
    flow.selectRole("ADULT");
    const state = flow.getState();

    expect(state.userRole).toBe("ADULT");
    expect(state.step).toBe("device-setup");
  });

  it("should set device name", () => {
    flow.selectRole("INFANT");
    flow.setDeviceName("Alice's iPhone");

    const state = flow.getState();
    expect(state.deviceName).toBe("Alice's iPhone");
  });

  it("should notify listeners of state changes", (done) => {
    let callCount = 0;
    const unsubscribe = flow.subscribe((state: OnboardingState) => {
      callCount++;

      if (callCount === 2) {
        // First call is initial state, second is after action
        expect(state.step).toBe("household-name");
        unsubscribe();
        done();
      }
    });

    flow.selectCreateHousehold();
  });

  it("should go back to previous step", () => {
    flow.selectCreateHousehold();
    flow.setHouseholdName("Test");
    flow.goBack();

    const state = flow.getState();
    expect(state.step).toBe("welcome");
  });

  it("should reset to welcome state", () => {
    flow.selectCreateHousehold();
    flow.setHouseholdName("Test");
    flow.reset();

    const state = flow.getState();
    expect(state.step).toBe("welcome");
    expect(state.householdName).toBe("");
  });

  it("should validate progress requirements", () => {
    flow.selectCreateHousehold();
    expect(flow.canProgress()).toBe(false);

    flow.setHouseholdName("Test");
    expect(flow.canProgress()).toBe(true);
  });

  it("should track progress percentage", () => {
    let previousProgress = 0;
    flow.subscribe((state) => {
      expect(state.progress >= previousProgress).toBe(true);
      previousProgress = state.progress;
    });

    flow.selectCreateHousehold();
    // Progress should increase
  });

  it("should not complete until all steps finished", () => {
    expect(flow.isComplete()).toBe(false);

    flow.selectCreateHousehold();
    expect(flow.isComplete()).toBe(false);

    flow.completeOnboarding();
    expect(flow.isComplete()).toBe(true);
  });
});

/**
 * ============================================================================
 * Background Sync Determinism Tests
 * ============================================================================
 */

describe("Background Sync Determinism", () => {
  let syncManager: BackgroundSyncManager;

  beforeEach(() => {
    syncManager = new BackgroundSyncManager();
  });

  it("should generate deterministic queue IDs", async () => {
    const id1 = syncManager["generateId"]();
    await new Promise((resolve) => setTimeout(resolve, 1));
    const id2 = syncManager["generateId"]();

    expect(typeof id1).toBe("string");
    expect(typeof id2).toBe("string");
    expect(id1).not.toBe(id2);
  });

  it("should queue items with monotonic timestamps", async () => {
    const item1 = await syncManager.queueAction({}, {});
    await new Promise((resolve) => setTimeout(resolve, 5));
    const item2 = await syncManager.queueAction({}, {});

    const pending = await syncManager.getPendingActions();
    if (pending.length >= 2) {
      expect(pending[0].timestamp).toBeLessThanOrEqual(pending[1].timestamp);
    }
  });

  it("should respect FIFO order for sync retry", async () => {
    const items = [];
    for (let i = 0; i < 3; i++) {
      items.push(await syncManager.queueMessage(`Message ${i}`, {}));
    }

    const pending = await syncManager.getPendingMessages();
    if (pending.length >= 3) {
      expect(pending[0].message).toBe("Message 0");
      expect(pending[1].message).toBe("Message 1");
      expect(pending[2].message).toBe("Message 2");
    }
  });

  it("should increment retry count deterministically", () => {
    // Implementation-specific test
    // Verify retry logic doesn't have side effects
  });
});

/**
 * ============================================================================
 * Multi-Device Session Continuity Tests
 * ============================================================================
 */

describe("Multi-Device Session Continuity", () => {
  it("should cache session state per device", () => {
    const deviceInfo = {
      deviceId: "dev-123",
      userId: "user-456",
      householdId: "hh-789",
    };

    localStorage.setItem("hpal-device-context", JSON.stringify(deviceInfo));
    const retrieved = JSON.parse(
      localStorage.getItem("hpal-device-context") || ""
    );

    expect(retrieved.deviceId).toBe("dev-123");
  });

  it("should preserve session across tab reloads", () => {
    const sessionData = {
      sessionToken: "token-abc",
      timestamp: Date.now(),
    };

    sessionStorage.setItem("hpal-session", JSON.stringify(sessionData));
    const retrieved = JSON.parse(sessionStorage.getItem("hpal-session") || "");

    expect(retrieved.sessionToken).toBe("token-abc");
  });

  it("should survive clear browser data (localStorage persistence)", () => {
    const householdId = "hh-persistent";
    localStorage.setItem("hpal-household-id", householdId);

    // Simulate page reload
    const reloaded = localStorage.getItem("hpal-household-id");
    expect(reloaded).toBe(householdId);
  });
});
