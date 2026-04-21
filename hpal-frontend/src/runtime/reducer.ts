import type { ActionCard, ChatResponse, UIBootstrapState, UIPatch } from "../api/contracts";
import type { ChatSessionState, FrontendState, SyncStatus } from "./types";

const DEFAULT_SYNC_STATUS: SyncStatus = "synced";

export function initializeFrontendState(snapshot: UIBootstrapState): FrontendState {
  return {
    snapshot,
    applied_patches: [],
    pending_actions: [],
    chat_sessions: {},
    last_sync_watermark: snapshot.source_watermark,
    sync_status: DEFAULT_SYNC_STATUS,
    materialized_index: reconstructMaterialized(snapshot, []),
  };
}

export function hydrateSnapshot(state: FrontendState, snapshot: UIBootstrapState): FrontendState {
  return {
    ...state,
    snapshot,
    applied_patches: [],
    last_sync_watermark: snapshot.source_watermark,
    sync_status: snapshot.system_health.stale_projection ? "lagging" : "synced",
    materialized_index: reconstructMaterialized(snapshot, []),
  };
}

export function applyPatches(state: FrontendState, patches: UIPatch[]): FrontendState {
  const ordered = sortAndDedupePatches(patches);
  if (ordered.length === 0) {
    return state;
  }

  const alreadyApplied = new Set(state.applied_patches.map((patch) => patchIdentity(patch)));
  const toApply = ordered.filter((patch) => !alreadyApplied.has(patchIdentity(patch)));

  if (toApply.length === 0) {
    return state;
  }

  const baselineVersion =
    state.applied_patches.length > 0
      ? Math.max(...state.applied_patches.map((patch) => patch.version))
      : state.snapshot.snapshot_version;

  let expectedVersion = baselineVersion + 1;
  for (const patch of toApply) {
    if (patch.version !== expectedVersion) {
      return {
        ...state,
        sync_status: "desynced",
      };
    }
    expectedVersion += 1;
  }

  const nextApplied = [...state.applied_patches, ...toApply];

  return {
    ...state,
    applied_patches: nextApplied,
    sync_status: "synced",
    last_sync_watermark: `${state.snapshot.source_watermark}:${nextApplied[nextApplied.length - 1].version}`,
    materialized_index: reconstructMaterialized(state.snapshot, nextApplied),
  };
}

export function applyChatResponse(
  state: FrontendState,
  sessionId: string,
  response: ChatResponse,
): FrontendState {
  const existingSession = state.chat_sessions[sessionId] ?? {
    session_id: sessionId,
    message_history: [],
    pending_action_cards: [],
    last_ui_patch: [],
    awaiting_confirmation: false,
  };

  const fingerprint = chatFingerprint(response);
  const shouldAppendMessage = existingSession.last_response_fingerprint !== fingerprint;
  const nextHistory = shouldAppendMessage
    ? [...existingSession.message_history, response.assistant_message]
    : existingSession.message_history;

  const nextSession: ChatSessionState = {
    session_id: sessionId,
    message_history: nextHistory,
    pending_action_cards: response.action_cards,
    last_ui_patch: response.ui_patch,
    awaiting_confirmation: response.requires_confirmation,
    last_response_fingerprint: fingerprint,
  };

  const interimState: FrontendState = {
    ...state,
    pending_actions: [...response.action_cards].sort((a, b) => a.id.localeCompare(b.id)),
    chat_sessions: {
      ...state.chat_sessions,
      [sessionId]: nextSession,
    },
  };

  return applyPatches(interimState, response.ui_patch);
}

export function sortAndDedupePatches(patches: UIPatch[]): UIPatch[] {
  const sorted = [...patches].sort((a, b) => {
    if (a.version !== b.version) {
      return a.version - b.version;
    }

    const tA = Date.parse(a.source_timestamp);
    const tB = Date.parse(b.source_timestamp);
    if (tA !== tB) {
      return tA - tB;
    }

    const cmpEntity = a.entity_type.localeCompare(b.entity_type);
    if (cmpEntity !== 0) {
      return cmpEntity;
    }

    const cmpId = a.entity_id.localeCompare(b.entity_id);
    if (cmpId !== 0) {
      return cmpId;
    }

    return a.change_type.localeCompare(b.change_type);
  });

  const seen = new Set<string>();
  const unique: UIPatch[] = [];

  for (const patch of sorted) {
    const key = `${patch.entity_id}:${patch.version}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    unique.push(patch);
  }

  return unique;
}

export function reconstructMaterialized(
  snapshot: UIBootstrapState,
  patches: UIPatch[],
): Record<string, Record<string, unknown>> {
  const index: Record<string, Record<string, unknown>> = snapshotIndex(snapshot);

  for (const patch of sortAndDedupePatches(patches)) {
    const key = entityKey(patch.entity_type, patch.entity_id);
    if (patch.change_type === "delete") {
      delete index[key];
      continue;
    }
    index[key] = { ...patch.payload };
  }

  return Object.fromEntries(Object.entries(index).sort(([a], [b]) => a.localeCompare(b)));
}

export function markLagging(state: FrontendState): FrontendState {
  return { ...state, sync_status: "lagging" };
}

export function markDesynced(state: FrontendState): FrontendState {
  return { ...state, sync_status: "desynced" };
}

export function clearSessionOnDesync(state: FrontendState, sessionId: string): FrontendState {
  const current = state.chat_sessions[sessionId];
  if (!current) {
    return markDesynced(state);
  }

  const nextSession: ChatSessionState = {
    ...current,
    pending_action_cards: [],
    last_ui_patch: [],
    awaiting_confirmation: false,
  };

  return {
    ...markDesynced(state),
    chat_sessions: {
      ...state.chat_sessions,
      [sessionId]: nextSession,
    },
  };
}

function snapshotIndex(snapshot: UIBootstrapState): Record<string, Record<string, unknown>> {
  const index: Record<string, Record<string, unknown>> = {};

  index[entityKey("family", snapshot.family.family_id)] = snapshot.family as unknown as Record<string, unknown>;

  for (const plan of snapshot.active_plans) {
    index[entityKey("plan", plan.plan_id)] = plan as unknown as Record<string, unknown>;
  }

  const taskGroups = [
    snapshot.task_board.pending,
    snapshot.task_board.in_progress,
    snapshot.task_board.completed,
    snapshot.task_board.failed,
  ];

  for (const group of taskGroups) {
    for (const task of group) {
      index[entityKey("task", task.task_id)] = task as unknown as Record<string, unknown>;
    }
  }

  for (const event of snapshot.calendar.events) {
    index[entityKey("event", event.event_id)] = event as unknown as Record<string, unknown>;
  }

  for (const notification of snapshot.notifications) {
    index[entityKey("notification", notification.notification_id)] =
      notification as unknown as Record<string, unknown>;
  }

  return index;
}

function entityKey(entityType: UIPatch["entity_type"], entityId: string): string {
  return `${entityType}:${entityId}`;
}

function patchIdentity(patch: UIPatch): string {
  return `${patch.entity_type}:${patch.entity_id}:${patch.version}:${patch.change_type}`;
}

function chatFingerprint(response: ChatResponse): string {
  const payload = JSON.stringify(
    {
      assistant_message: response.assistant_message,
      requires_confirmation: response.requires_confirmation,
      action_cards: response.action_cards,
      ui_patch: response.ui_patch,
    },
    Object.keys,
  );

  let hash = 0;
  for (let i = 0; i < payload.length; i += 1) {
    hash = (hash << 5) - hash + payload.charCodeAt(i);
    hash |= 0;
  }
  return String(hash);
}

export function optimisticActionNotification(action: ActionCard, version: number): UIPatch {
  return {
    entity_type: "notification",
    entity_id: `optimistic:${action.id}`,
    change_type: "create",
    payload: {
      notification_id: `optimistic:${action.id}`,
      title: "Applying action",
      message: `Applying ${action.title}...`,
      level: "info",
      related_entity: action.related_entity,
    },
    version,
    source_timestamp: new Date().toISOString(),
  };
}
