import type { UIBootstrapState } from "../api/contracts";
import type { FrontendState } from "./types";
import {
  EMPTY_COL_SIGNALS,
  InteractionContractEngine,
  InteractionState,
  INTERACTION_TRANSITIONS,
} from "./interactionContract";

function assert(condition: boolean, message: string): void {
  if (!condition) {
    throw new Error(`assertion_failed:${message}`);
  }
}

function assertEqual<T>(actual: T, expected: T, message: string): void {
  if (actual !== expected) {
    throw new Error(`assertion_failed:${message}; actual=${String(actual)} expected=${String(expected)}`);
  }
}

function testDeterministicOutputForIdenticalInputs(): void {
  const engine = new InteractionContractEngine();
  const runtime = createRuntimeState();
  const col = {
    ...EMPTY_COL_SIGNALS,
    proposed_actions_count: 1,
    focus_candidates: [
      {
        focus_type: "TASK" as const,
        entity_id: "task-42",
        summary_text: "Complete household maintenance task",
        confidence_score: 0.8,
      },
    ],
  };

  const input = {
    runtime_state: runtime,
    backend_snapshot: runtime.snapshot,
    col_signals: col,
    identity: {
      household_id: "family-1",
      user_id: "user-1",
      device_id: "dev-1",
    },
    is_loading: false,
    error: null,
    previous_state: InteractionState.IDLE,
  };

  const first = engine.derive(input);
  const second = engine.derive(input);

  assertEqual(first.interaction_state, second.interaction_state, "state should be deterministic");
  assertEqual(
    JSON.stringify(first.active_work_context),
    JSON.stringify(second.active_work_context),
    "active context should be deterministic",
  );
  assertEqual(JSON.stringify(first.ui_behavior), JSON.stringify(second.ui_behavior), "ui behavior should be deterministic");
}

function testTransitionCoverageAndInvalidNormalization(): void {
  const engine = new InteractionContractEngine();

  for (const [from, allowed] of Object.entries(INTERACTION_TRANSITIONS)) {
    assert(allowed.length > 0, `allowed transitions must exist for ${from}`);
  }

  const normalized = engine.resolveTransition({
    from: InteractionState.IDLE,
    to: InteractionState.RESOLVING_CONFLICT,
    strict: false,
  });
  assertEqual(normalized.accepted, false, "invalid transition should not be accepted");
  assertEqual(normalized.normalized, true, "invalid transition should normalize");
  assertEqual(normalized.next_state, InteractionState.FAILED_RECOVERABLE, "idle invalid transition normalizes deterministically");

  let strictFailed = false;
  try {
    engine.resolveTransition({
      from: InteractionState.IDLE,
      to: InteractionState.RESOLVING_CONFLICT,
      strict: true,
    });
  } catch {
    strictFailed = true;
  }
  assert(strictFailed, "strict invalid transition should throw");
}

function testFailureRecoveryPath(): void {
  const engine = new InteractionContractEngine();
  const runtime = createRuntimeState();

  const failed = engine.derive({
    runtime_state: runtime,
    backend_snapshot: runtime.snapshot,
    col_signals: {
      ...EMPTY_COL_SIGNALS,
      recoverable_error: true,
    },
    identity: {
      household_id: "family-1",
      user_id: "user-1",
      device_id: "dev-1",
    },
    is_loading: false,
    error: "message_failed:503",
    previous_state: InteractionState.EXECUTING,
  });

  assertEqual(failed.interaction_state, InteractionState.FAILED_RECOVERABLE, "recoverable error must route to recoverable failure");

  const recovered = engine.derive({
    runtime_state: runtime,
    backend_snapshot: runtime.snapshot,
    col_signals: EMPTY_COL_SIGNALS,
    identity: {
      household_id: "family-1",
      user_id: "user-1",
      device_id: "dev-1",
    },
    is_loading: false,
    error: null,
    previous_state: failed.interaction_state,
  });

  assertEqual(recovered.interaction_state, InteractionState.IDLE, "recovery should return to idle deterministically");
}

function testConflictingContextResolution(): void {
  const engine = new InteractionContractEngine();
  const runtime = createRuntimeState();

  const output = engine.derive({
    runtime_state: runtime,
    backend_snapshot: runtime.snapshot,
    col_signals: {
      ...EMPTY_COL_SIGNALS,
      focus_candidates: [
        {
          focus_type: "EVENT",
          entity_id: "event-b",
          summary_text: "Review event",
          confidence_score: 0.9,
        },
        {
          focus_type: "PLAN",
          entity_id: "plan-a",
          summary_text: "Review plan",
          confidence_score: 0.9,
        },
      ],
    },
    identity: {
      household_id: "family-1",
      user_id: "user-1",
      device_id: "dev-1",
    },
    is_loading: false,
    error: null,
    previous_state: InteractionState.IDLE,
  });

  assertEqual(output.active_work_context.current_focus_type, "PLAN", "equal confidence context should resolve by focus rank");
  assertEqual(output.active_work_context.focus_entity_id, "plan-a", "focus entity should be deterministic");
  assertEqual(output.active_work_context.household_id, "family-1", "household identity must flow into context");
  assertEqual(output.active_work_context.user_id, "user-1", "user identity must flow into context");
  assertEqual(output.active_work_context.device_id, "dev-1", "device identity must flow into context");
}

export function runInteractionContractTests(): void {
  testDeterministicOutputForIdenticalInputs();
  testTransitionCoverageAndInvalidNormalization();
  testFailureRecoveryPath();
  testConflictingContextResolution();
}

function createRuntimeState(): FrontendState {
  const snapshot = createSnapshot();

  return {
    snapshot,
    applied_patches: [],
    pending_actions: [],
    chat_sessions: {},
    last_sync_watermark: snapshot.source_watermark,
    sync_status: "synced",
    materialized_index: {},
  };
}

function createSnapshot(): UIBootstrapState {
  return {
    snapshot_version: 7,
    source_watermark: "wm-7",
    family: {
      family_id: "family-1",
      member_count: 3,
      member_names: ["A", "B", "C"],
      default_time_zone: "UTC",
    },
    today_overview: {
      date: "2026-04-20",
      open_task_count: 2,
      scheduled_event_count: 1,
      active_plan_count: 1,
      notification_count: 0,
    },
    active_plans: [
      {
        plan_id: "plan-a",
        title: "Plan A",
        status: "active",
        revision: 2,
        linked_task_count: 2,
      },
    ],
    task_board: {
      pending: [
        {
          task_id: "task-1",
          title: "Task 1",
          plan_id: "plan-a",
          assigned_to: "A",
          status: "pending",
          priority: "high",
          due_time: null,
        },
      ],
      in_progress: [],
      completed: [],
      failed: [],
    },
    calendar: {
      window_start: "2026-04-20T00:00:00Z",
      window_end: "2026-04-21T00:00:00Z",
      events: [
        {
          event_id: "event-b",
          title: "Event B",
          start: "2026-04-20T08:00:00Z",
          end: "2026-04-20T09:00:00Z",
          participants: ["A"],
        },
      ],
    },
    notifications: [],
    explanation_digest: [],
    system_health: {
      status: "healthy",
      pending_actions: 0,
      stale_projection: false,
      state_version: 7,
      last_updated: "2026-04-20T08:00:00Z",
    },
  };
}
