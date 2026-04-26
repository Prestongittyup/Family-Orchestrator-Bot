import { create } from "zustand";
import type {
  ActionCard,
  ChatResponse,
  CreateCalendarEventRequest,
  RequestIdentityContext,
  UIPatch,
  UpdateCalendarEventRequest,
} from "../api/contracts";
import { productSurfaceClient } from "../api/productSurfaceClient";
import { DeterministicActionExecutionBinder } from "./actionExecution";
import {
  EMPTY_COL_SIGNALS,
  InteractionContractEngine,
  type ActiveWorkContext,
  type COLSignals,
  type UIBehaviorMode,
  InteractionState,
} from "./interactionContract";
import type { Device, Household, PermissionFlags, UserPerson } from "./identity";
import { HouseholdRole, resolveIdentity } from "./identity";
import { authProvider } from "./authProvider";
import {
  applyChatResponse,
  applyPatches,
  clearSessionOnDesync,
  hydrateSnapshot,
  initializeFrontendState,
  markDesynced,
  markLagging,
  optimisticActionNotification,
} from "./reducer";
import { pollingInterval } from "./sync";
import type { FrontendState } from "./types";

interface RuntimeStore {
  familyId: string;
  runtimeState: FrontendState | null;
  interactionState: InteractionState;
  activeWorkContext: ActiveWorkContext | null;
  uiBehavior: UIBehaviorMode;
  colSignals: COLSignals;
  active_user: UserPerson | null;
  active_household: Household | null;
  device_context: Device | null;
  permission_flags: PermissionFlags;
  activeRole: HouseholdRole;
  sessionToken: string;
  syncTimer: number | null;
  realtimeStream: EventSource | null;
  realtimeConnected: boolean;
  realtimeLastWatermark: number | null;
  isLoading: boolean;
  error: string | null;

  initialize: (familyId: string) => Promise<void>;
  sendMessage: (sessionId: string, message: string) => Promise<void>;
  executeAction: (sessionId: string, action: ActionCard) => Promise<void>;
  ingestPatches: (patches: UIPatch[]) => void;
  setLagging: () => void;
  setDesynced: (sessionId?: string) => void;
  forceReconcile: () => Promise<void>;
  startSyncLoop: () => void;
  stopSyncLoop: () => void;
  startRealtimeStream: () => void;
  stopRealtimeStream: () => void;
  setCOLSignals: (signals: Partial<COLSignals>) => void;
  syncInteraction: () => void;
  hydrateSession: () => Promise<void>;
  createCalendarEvent: (request: CreateCalendarEventRequest) => Promise<void>;
  updateCalendarEvent: (eventId: string, request: UpdateCalendarEventRequest) => Promise<void>;
  deleteCalendarEvent: (eventId: string) => Promise<void>;
}

const actionBinder = new DeterministicActionExecutionBinder();
const interactionEngine = new InteractionContractEngine();
const initialUiBehavior = interactionEngine.uiBehaviorFor(InteractionState.IDLE);
const defaultPermissions: PermissionFlags = {
  can_chat: false,
  can_execute_actions: false,
  can_override_conflicts: false,
  can_view_sensitive_cards: false,
};

export const useRuntimeStore = create<RuntimeStore>((set, get) => ({
  familyId: "family-1",
  runtimeState: null,
  interactionState: InteractionState.IDLE,
  activeWorkContext: null,
  uiBehavior: initialUiBehavior,
  colSignals: EMPTY_COL_SIGNALS,
  active_user: null,
  active_household: null,
  device_context: null,
  permission_flags: defaultPermissions,
  activeRole: HouseholdRole.VIEW_ONLY,
  sessionToken: "",
  syncTimer: null,
  realtimeStream: null,
  realtimeConnected: false,
  realtimeLastWatermark: null,
  isLoading: false,
  error: null,

  initialize: async (familyId: string) => {
    set({ isLoading: true, error: null, familyId });
    await get().hydrateSession();
    get().syncInteraction();

    const identity = currentRequestIdentity(get());
    const targetHouseholdId = identity.household_id || familyId;

    try {
      const snapshot = await productSurfaceClient.fetchBootstrap(targetHouseholdId, identity);
      const next = initializeFrontendState({
        ...snapshot,
        identity_context: {
          household_id: identity.household_id,
          user_id: identity.user_id,
          device_id: identity.device_id,
          role: get().activeRole,
        },
      });

      set({
        runtimeState: next,
        isLoading: false,
        familyId: targetHouseholdId,
      });
      get().setCOLSignals({
        last_updated_watermark: snapshot.source_watermark,
        recoverable_error: false,
        terminal_error: false,
        conflict_detected: false,
      });
      get().startSyncLoop();
      get().startRealtimeStream();
      get().syncInteraction();
    } catch (error) {
      set({ isLoading: false, error: toMessage(error) });
      get().setCOLSignals({ recoverable_error: true });
      get().syncInteraction();
    }

    createCalendarEvent: async (request: CreateCalendarEventRequest) => {
      const identity = currentRequestIdentity(get());
      set({ isLoading: true, error: null });
      try {
        await productSurfaceClient.createCalendarEvent(identity.household_id, request, identity);
        await get().forceReconcile();
        set({ isLoading: false });
      } catch (error) {
        set({ isLoading: false, error: toMessage(error) });
      }
    },

    updateCalendarEvent: async (eventId: string, request: UpdateCalendarEventRequest) => {
      const identity = currentRequestIdentity(get());
      set({ isLoading: true, error: null });
      try {
        await productSurfaceClient.updateCalendarEvent(identity.household_id, eventId, request, identity);
        await get().forceReconcile();
        set({ isLoading: false });
      } catch (error) {
        set({ isLoading: false, error: toMessage(error) });
      }
    },

    deleteCalendarEvent: async (eventId: string) => {
      const identity = currentRequestIdentity(get());
      set({ isLoading: true, error: null });
      try {
        await productSurfaceClient.deleteCalendarEvent(identity.household_id, eventId, identity);
        await get().forceReconcile();
        set({ isLoading: false });
      } catch (error) {
        set({ isLoading: false, error: toMessage(error) });
      }
    },
  },

  sendMessage: async (sessionId: string, message: string) => {
    const runtimeState = get().runtimeState;
    if (!runtimeState) {
      return;
    }

    if (!get().permission_flags.can_chat) {
      set({ error: "permission_denied:chat" });
      get().setCOLSignals({ recoverable_error: true });
      get().syncInteraction();
      return;
    }

    set({ isLoading: true, error: null });
    get().setCOLSignals({
      has_clarification_request: false,
      proposed_actions_count: 0,
      has_pending_confirmation: false,
    });
    get().syncInteraction();
    try {
      const identity = currentRequestIdentity(get());
      const response: ChatResponse = await productSurfaceClient.sendMessage({
        family_id: identity.household_id,
        message,
        session_id: `${identity.user_id}:${sessionId}`,
      }, identity);
      const next = applyChatResponse(runtimeState, sessionId, response);
      set({ runtimeState: next, isLoading: false });
      get().setCOLSignals({
        proposed_actions_count: response.action_cards.length,
        has_pending_confirmation: response.requires_confirmation,
        recoverable_error: false,
        terminal_error: false,
      });
      get().syncInteraction();
    } catch (error) {
      set({ isLoading: false, error: toMessage(error) });
      get().setCOLSignals({ recoverable_error: true });
      get().setDesynced(sessionId);
      get().syncInteraction();
    }
  },

  executeAction: async (sessionId: string, action: ActionCard) => {
    const runtimeState = get().runtimeState;
    if (!runtimeState) {
      return;
    }

    if (!get().permission_flags.can_execute_actions) {
      set({ error: "permission_denied:execute_action" });
      get().setCOLSignals({ recoverable_error: true });
      get().syncInteraction();
      return;
    }

    if (get().interactionState === InteractionState.RESOLVING_CONFLICT && !get().permission_flags.can_override_conflicts) {
      set({ error: "permission_denied:override_conflict" });
      get().setCOLSignals({ recoverable_error: true });
      get().syncInteraction();
      return;
    }

    const optimisticVersion = nextVersion(runtimeState);
    const optimisticPatch = optimisticActionNotification(action, optimisticVersion);
    const optimisticState = applyPatches(runtimeState, [optimisticPatch]);
    set({ runtimeState: optimisticState, isLoading: true, error: null });
    get().setCOLSignals({
      execution_in_flight: true,
      proposed_actions_count: Math.max(1, get().runtimeState?.pending_actions.length ?? 0),
      has_pending_confirmation: false,
      recoverable_error: false,
      terminal_error: false,
      focus_candidates: [
        {
          focus_type: inferFocusFromEntity(action.related_entity),
          entity_id: action.related_entity,
          summary_text: action.title,
          confidence_score: 0.95,
        },
      ],
    });
    get().syncInteraction();

    try {
      const identity = currentRequestIdentity(get());
      const request = actionBinder.buildRequest({
        familyId: identity.household_id,
        sessionId: `${identity.user_id}:${sessionId}`,
        actionCard: action,
        endpoint: "/v1/ui/action",
      });

      const result = await actionBinder.execute({
        request,
        send: (input) => productSurfaceClient.executeAction(input, currentRequestIdentity(get())),
      });

      if (result.status === "succeeded" && result.response) {
        const next = applyChatResponse(optimisticState, sessionId, result.response);
        set({ runtimeState: next, isLoading: false });
        get().setCOLSignals({
          execution_in_flight: false,
          proposed_actions_count: result.response.action_cards.length,
          has_pending_confirmation: result.response.requires_confirmation,
          recoverable_error: false,
        });
        get().syncInteraction();
      } else {
        set({ isLoading: false, error: result.error ?? "action_failed" });
        get().setCOLSignals({
          execution_in_flight: false,
          recoverable_error: true,
        });
        await get().forceReconcile();
        get().syncInteraction();
      }
    } catch (error) {
      set({ isLoading: false, error: toMessage(error) });
      get().setCOLSignals({
        execution_in_flight: false,
        recoverable_error: true,
      });
      await get().forceReconcile();
      get().syncInteraction();
    }
  },

  ingestPatches: (patches: UIPatch[]) => {
    const runtimeState = get().runtimeState;
    if (!runtimeState) {
      return;
    }
    const next = applyPatches(runtimeState, patches);
    set({ runtimeState: next });
    get().syncInteraction();
  },

  setLagging: () => {
    const runtimeState = get().runtimeState;
    if (!runtimeState) {
      return;
    }
    set({ runtimeState: markLagging(runtimeState) });
    get().setCOLSignals({ conflict_detected: true });
    get().syncInteraction();
  },

  setDesynced: (sessionId?: string) => {
    const runtimeState = get().runtimeState;
    if (!runtimeState) {
      return;
    }

    if (sessionId) {
      set({ runtimeState: clearSessionOnDesync(runtimeState, sessionId) });
      get().setCOLSignals({
        conflict_detected: true,
        recoverable_error: true,
      });
      get().syncInteraction();
      return;
    }

    set({ runtimeState: markDesynced(runtimeState) });
    get().setCOLSignals({
      conflict_detected: true,
      recoverable_error: true,
    });
    get().syncInteraction();
  },

  forceReconcile: async () => {
    const runtimeState = get().runtimeState;
    if (!runtimeState) {
      return;
    }

    try {
      const snapshot = await productSurfaceClient.fetchBootstrap(get().familyId, currentRequestIdentity(get()));
      const next = hydrateSnapshot(runtimeState, snapshot);
      set({ runtimeState: next, error: null });
      get().setCOLSignals({
        conflict_detected: false,
        recoverable_error: false,
        terminal_error: false,
        last_updated_watermark: snapshot.source_watermark,
      });
      get().syncInteraction();
    } catch (error) {
      set({ error: toMessage(error) });
      get().setCOLSignals({ recoverable_error: true });
      get().setDesynced();
      get().syncInteraction();
    }
  },

  startSyncLoop: () => {
    const existing = get().syncTimer;
    if (existing) {
      clearTimeout(existing);
    }

    const tick = async () => {
      const runtimeState = get().runtimeState;
      if (!runtimeState) {
        return;
      }

      try {
        const snapshot = await productSurfaceClient.fetchBootstrap(get().familyId, currentRequestIdentity(get()));
        if (snapshot.source_watermark !== runtimeState.last_sync_watermark) {
          const next = hydrateSnapshot(runtimeState, snapshot);
          set({ runtimeState: next });
          get().setCOLSignals({
            conflict_detected: false,
            recoverable_error: false,
            last_updated_watermark: snapshot.source_watermark,
          });
        } else if (snapshot.system_health.stale_projection && runtimeState.sync_status === "synced") {
          set({ runtimeState: markLagging(runtimeState) });
          get().setCOLSignals({ conflict_detected: true });
        }
        get().syncInteraction();
      } catch (_error) {
        get().setDesynced();
        get().syncInteraction();
      }

      const current = get().runtimeState;
      if (!current) {
        return;
      }
      const delay = pollingInterval(current);
      const timerId = window.setTimeout(tick, delay);
      set({ syncTimer: timerId });
    };

    const runtimeState = get().runtimeState;
    if (!runtimeState) {
      return;
    }
    const firstDelay = pollingInterval(runtimeState);
    const timerId = window.setTimeout(tick, firstDelay);
    set({ syncTimer: timerId });
  },

  stopSyncLoop: () => {
    const existing = get().syncTimer;
    if (existing) {
      clearTimeout(existing);
      set({ syncTimer: null });
    }
    get().stopRealtimeStream();
  },

  startRealtimeStream: () => {
    if (typeof window === "undefined") {
      return;
    }
    const existing = get().realtimeStream;
    if (existing) {
      existing.close();
    }
    const householdId = get().familyId || get().active_household?.household_id;
    if (!householdId) {
      return;
    }

    const base = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "http://localhost:8000").replace(/\/$/, "");
    const lastWatermark = get().realtimeLastWatermark;
    const watermarkQuery = typeof lastWatermark === "number" ? `&last_watermark=${lastWatermark}` : "";
    const url = `${base}/v1/realtime/stream?household_id=${encodeURIComponent(householdId)}${watermarkQuery}`;
    const source = new EventSource(url);

    source.onopen = () => {
      set({ realtimeConnected: true });
    };

    source.onerror = () => {
      set({ realtimeConnected: false });
    };

    source.addEventListener("update", async (evt: MessageEvent) => {
      let parsed: {
        event_id?: string;
        event_type?: string;
        watermark?: number;
        payload?: unknown;
      } | null = null;
      try {
        parsed = JSON.parse(evt.data) as {
          event_id?: string;
          event_type?: string;
          watermark?: number;
          payload?: unknown;
        };
      } catch {
        return;
      }

      const nextWatermark = typeof parsed?.watermark === "number" ? parsed.watermark : null;
      if (nextWatermark === null || !parsed?.event_id || !parsed?.event_type) {
        return;
      }

      const currentWatermark = get().realtimeLastWatermark;
      if (typeof currentWatermark === "number" && nextWatermark <= currentWatermark) {
        return;
      }

      set({ realtimeLastWatermark: nextWatermark });
      // Source-of-truth remains backend snapshot; live event triggers fast reconcile.
      await get().forceReconcile();
    });

    source.addEventListener("resync_required", async () => {
      set({ realtimeLastWatermark: null });
      await get().forceReconcile();
    });

    set({ realtimeStream: source });
  },

  stopRealtimeStream: () => {
    const source = get().realtimeStream;
    if (source) {
      source.close();
      set({ realtimeStream: null, realtimeConnected: false });
    }
  },

  setCOLSignals: (signals: Partial<COLSignals>) => {
    const current = get().colSignals;
    set({
      colSignals: {
        ...current,
        ...signals,
      },
    });
  },

  syncInteraction: () => {
    const current = get();
    const backendSnapshot = current.runtimeState?.snapshot ?? null;
    const identity = currentRequestIdentity(current);
    const output = interactionEngine.derive({
      runtime_state: current.runtimeState,
      backend_snapshot: backendSnapshot,
      col_signals: current.colSignals,
      identity: {
        household_id: identity.household_id,
        user_id: identity.user_id,
        device_id: identity.device_id,
      },
      is_loading: current.isLoading,
      error: current.error,
      previous_state: current.interactionState,
      strict_transitions: false,
    });

    const gatedBehavior = applyPermissionGates(output.ui_behavior, current.permission_flags);

    set({
      interactionState: output.interaction_state,
      activeWorkContext: output.active_work_context,
      uiBehavior: gatedBehavior,
    });
  },

  hydrateSession: async () => {
    const session = await authProvider.ensureAuthenticated();
    set({
      active_household: session.household,
      active_user: session.user,
      device_context: session.device,
      permission_flags: session.permission_flags,
      activeRole: session.membership.role,
      familyId: session.household.household_id,
      sessionToken: session.session_token,
    });
  },
}));

function nextVersion(state: FrontendState): number {
  if (state.applied_patches.length === 0) {
    return state.snapshot.snapshot_version + 1;
  }
  return Math.max(...state.applied_patches.map((patch) => patch.version)) + 1;
}

function toMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function inferFocusFromEntity(relatedEntity: string): "PLAN" | "TASK" | "EVENT" | "CHAT" {
  const normalized = relatedEntity.toLowerCase();
  if (normalized.startsWith("plan")) {
    return "PLAN";
  }
  if (normalized.startsWith("task")) {
    return "TASK";
  }
  if (normalized.startsWith("event")) {
    return "EVENT";
  }
  return "CHAT";
}

function currentRequestIdentity(state: {
  active_household: Household | null;
  active_user: UserPerson | null;
  device_context: Device | null;
  sessionToken: string;
}): RequestIdentityContext {
  const identity = resolveIdentity({
    household: state.active_household ?? {
      household_id: "family-1",
      name: "Fallback Household",
      timezone: "UTC",
    },
    user: state.active_user ?? {
      user_id: "user-view",
      display_name: "user-view",
    },
    device: state.device_context ?? {
      device_id: "dev-fallback",
      platform: "web",
      label: "fallback-device",
    },
    membership: {
      household_id: state.active_household?.household_id ?? "family-1",
      user_id: state.active_user?.user_id ?? "user-view",
      role: HouseholdRole.VIEW_ONLY,
      is_active: true,
    },
    permission_flags: defaultPermissions,
    session_token: state.sessionToken || "mock.%7B%22household_id%22%3A%22family-1%22%2C%22user_id%22%3A%22user-view%22%2C%22role%22%3A%22VIEW_ONLY%22%2C%22issued_at_epoch_ms%22%3A0%7D",
  });

  return {
    household_id: identity.household_id,
    user_id: identity.user_id,
    device_id: identity.device_id,
    session_token:
      state.sessionToken ||
      "mock.%7B%22household_id%22%3A%22family-1%22%2C%22user_id%22%3A%22user-view%22%2C%22role%22%3A%22VIEW_ONLY%22%2C%22issued_at_epoch_ms%22%3A0%7D",
  };
}

function applyPermissionGates(behavior: UIBehaviorMode, permissions: PermissionFlags): UIBehaviorMode {
  const requiresOverride = behavior.layout_mode === "conflict_resolution";
  const canUseActionCards = permissions.can_execute_actions && (!requiresOverride || permissions.can_override_conflicts);

  return {
    ...behavior,
    panels: {
      ...behavior.panels,
      chat: {
        visible: behavior.panels.chat.visible,
        enabled: behavior.panels.chat.enabled && permissions.can_chat,
      },
      action_cards: {
        visible: behavior.panels.action_cards.visible && canUseActionCards,
        enabled: behavior.panels.action_cards.enabled && canUseActionCards,
      },
    },
    chat: {
      ...behavior.chat,
      input_enabled: behavior.chat.input_enabled && permissions.can_chat,
      send_enabled: behavior.chat.send_enabled && permissions.can_chat,
      action_cards_visible: behavior.chat.action_cards_visible && canUseActionCards,
    },
  };
}
