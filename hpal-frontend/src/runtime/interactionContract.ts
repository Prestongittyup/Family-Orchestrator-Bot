import type { UIBootstrapState } from "../api/contracts";
import type { FrontendState } from "./types";

export enum InteractionState {
  IDLE = "IDLE",
  ASSISTING = "ASSISTING",
  CLARIFYING = "CLARIFYING",
  PROPOSING_ACTIONS = "PROPOSING_ACTIONS",
  AWAITING_CONFIRMATION = "AWAITING_CONFIRMATION",
  EXECUTING = "EXECUTING",
  RESOLVING_CONFLICT = "RESOLVING_CONFLICT",
  FAILED_RECOVERABLE = "FAILED_RECOVERABLE",
  FAILED_TERMINAL = "FAILED_TERMINAL",
}

export type FocusType = "PLAN" | "TASK" | "EVENT" | "CHAT";

export interface ActiveWorkContext {
  household_id: string;
  user_id: string;
  device_id: string;
  current_focus_type: FocusType;
  focus_entity_id: string;
  summary_text: string;
  confidence_score: number;
  last_updated_watermark: string;
}

export interface COLFocusCandidate {
  focus_type: FocusType;
  entity_id: string;
  summary_text: string;
  confidence_score: number;
}

// UI-safe COL projection only; no backend orchestration/internal types.
export interface COLSignals {
  has_clarification_request: boolean;
  proposed_actions_count: number;
  has_pending_confirmation: boolean;
  execution_in_flight: boolean;
  conflict_detected: boolean;
  recoverable_error: boolean;
  terminal_error: boolean;
  last_updated_watermark: string;
  focus_candidates: COLFocusCandidate[];
}

export interface PanelMode {
  visible: boolean;
  enabled: boolean;
}

export interface ChatMode {
  input_enabled: boolean;
  send_enabled: boolean;
  action_cards_visible: boolean;
  requires_confirmation_banner: boolean;
}

export interface UIBehaviorMode {
  layout_mode:
    | "dashboard_ready"
    | "assistant_busy"
    | "clarification_required"
    | "action_proposal"
    | "confirmation_gate"
    | "execution_busy"
    | "conflict_resolution"
    | "recoverable_error"
    | "terminal_error";
  panels: {
    dashboard: PanelMode;
    tasks: PanelMode;
    calendar: PanelMode;
    chat: PanelMode;
    action_cards: PanelMode;
    system_status: PanelMode;
  };
  chat: ChatMode;
}

export interface InteractionContractInput {
  runtime_state: FrontendState | null;
  backend_snapshot: UIBootstrapState | null;
  col_signals: COLSignals;
  identity: {
    household_id: string;
    user_id: string;
    device_id: string;
  };
  is_loading: boolean;
  error: string | null;
  previous_state?: InteractionState;
  strict_transitions?: boolean;
}

export interface TransitionResult {
  accepted: boolean;
  normalized: boolean;
  next_state: InteractionState;
}

export interface InteractionContractOutput {
  interaction_state: InteractionState;
  active_work_context: ActiveWorkContext;
  ui_behavior: UIBehaviorMode;
  transition: TransitionResult;
}

export const EMPTY_COL_SIGNALS: COLSignals = {
  has_clarification_request: false,
  proposed_actions_count: 0,
  has_pending_confirmation: false,
  execution_in_flight: false,
  conflict_detected: false,
  recoverable_error: false,
  terminal_error: false,
  last_updated_watermark: "",
  focus_candidates: [],
};

const FALLBACK_CONTEXT: Omit<ActiveWorkContext, "household_id" | "user_id" | "device_id" | "last_updated_watermark"> = {
  current_focus_type: "CHAT",
  focus_entity_id: "global",
  summary_text: "Waiting for the next instruction.",
  confidence_score: 1,
};

const FOCUS_TYPE_RANK: Record<FocusType, number> = {
  PLAN: 0,
  TASK: 1,
  EVENT: 2,
  CHAT: 3,
};

const STATE_PRIORITY: InteractionState[] = [
  InteractionState.FAILED_TERMINAL,
  InteractionState.FAILED_RECOVERABLE,
  InteractionState.RESOLVING_CONFLICT,
  InteractionState.EXECUTING,
  InteractionState.AWAITING_CONFIRMATION,
  InteractionState.PROPOSING_ACTIONS,
  InteractionState.CLARIFYING,
  InteractionState.ASSISTING,
  InteractionState.IDLE,
];

export const INTERACTION_TRANSITIONS: Record<InteractionState, InteractionState[]> = {
  [InteractionState.IDLE]: [
    InteractionState.IDLE,
    InteractionState.ASSISTING,
    InteractionState.CLARIFYING,
    InteractionState.PROPOSING_ACTIONS,
    InteractionState.FAILED_RECOVERABLE,
    InteractionState.FAILED_TERMINAL,
  ],
  [InteractionState.ASSISTING]: [
    InteractionState.ASSISTING,
    InteractionState.IDLE,
    InteractionState.CLARIFYING,
    InteractionState.PROPOSING_ACTIONS,
    InteractionState.AWAITING_CONFIRMATION,
    InteractionState.FAILED_RECOVERABLE,
    InteractionState.FAILED_TERMINAL,
  ],
  [InteractionState.CLARIFYING]: [
    InteractionState.CLARIFYING,
    InteractionState.ASSISTING,
    InteractionState.PROPOSING_ACTIONS,
    InteractionState.IDLE,
    InteractionState.FAILED_RECOVERABLE,
  ],
  [InteractionState.PROPOSING_ACTIONS]: [
    InteractionState.PROPOSING_ACTIONS,
    InteractionState.AWAITING_CONFIRMATION,
    InteractionState.EXECUTING,
    InteractionState.CLARIFYING,
    InteractionState.IDLE,
    InteractionState.FAILED_RECOVERABLE,
  ],
  [InteractionState.AWAITING_CONFIRMATION]: [
    InteractionState.AWAITING_CONFIRMATION,
    InteractionState.EXECUTING,
    InteractionState.PROPOSING_ACTIONS,
    InteractionState.IDLE,
    InteractionState.FAILED_RECOVERABLE,
  ],
  [InteractionState.EXECUTING]: [
    InteractionState.EXECUTING,
    InteractionState.PROPOSING_ACTIONS,
    InteractionState.IDLE,
    InteractionState.RESOLVING_CONFLICT,
    InteractionState.FAILED_RECOVERABLE,
    InteractionState.FAILED_TERMINAL,
  ],
  [InteractionState.RESOLVING_CONFLICT]: [
    InteractionState.RESOLVING_CONFLICT,
    InteractionState.ASSISTING,
    InteractionState.PROPOSING_ACTIONS,
    InteractionState.IDLE,
    InteractionState.FAILED_RECOVERABLE,
  ],
  [InteractionState.FAILED_RECOVERABLE]: [
    InteractionState.FAILED_RECOVERABLE,
    InteractionState.ASSISTING,
    InteractionState.RESOLVING_CONFLICT,
    InteractionState.IDLE,
    InteractionState.FAILED_TERMINAL,
  ],
  [InteractionState.FAILED_TERMINAL]: [
    InteractionState.FAILED_TERMINAL,
  ],
};

const UI_BEHAVIOR_BY_STATE: Record<InteractionState, UIBehaviorMode> = {
  [InteractionState.IDLE]: {
    layout_mode: "dashboard_ready",
    panels: {
      dashboard: { visible: true, enabled: true },
      tasks: { visible: true, enabled: true },
      calendar: { visible: true, enabled: true },
      chat: { visible: true, enabled: true },
      action_cards: { visible: false, enabled: false },
      system_status: { visible: true, enabled: true },
    },
    chat: {
      input_enabled: true,
      send_enabled: true,
      action_cards_visible: false,
      requires_confirmation_banner: false,
    },
  },
  [InteractionState.ASSISTING]: {
    layout_mode: "assistant_busy",
    panels: {
      dashboard: { visible: true, enabled: false },
      tasks: { visible: true, enabled: false },
      calendar: { visible: true, enabled: false },
      chat: { visible: true, enabled: true },
      action_cards: { visible: false, enabled: false },
      system_status: { visible: true, enabled: true },
    },
    chat: {
      input_enabled: false,
      send_enabled: false,
      action_cards_visible: false,
      requires_confirmation_banner: false,
    },
  },
  [InteractionState.CLARIFYING]: {
    layout_mode: "clarification_required",
    panels: {
      dashboard: { visible: true, enabled: false },
      tasks: { visible: false, enabled: false },
      calendar: { visible: false, enabled: false },
      chat: { visible: true, enabled: true },
      action_cards: { visible: false, enabled: false },
      system_status: { visible: true, enabled: true },
    },
    chat: {
      input_enabled: true,
      send_enabled: true,
      action_cards_visible: false,
      requires_confirmation_banner: false,
    },
  },
  [InteractionState.PROPOSING_ACTIONS]: {
    layout_mode: "action_proposal",
    panels: {
      dashboard: { visible: true, enabled: true },
      tasks: { visible: true, enabled: true },
      calendar: { visible: true, enabled: true },
      chat: { visible: true, enabled: true },
      action_cards: { visible: true, enabled: true },
      system_status: { visible: true, enabled: true },
    },
    chat: {
      input_enabled: true,
      send_enabled: true,
      action_cards_visible: true,
      requires_confirmation_banner: false,
    },
  },
  [InteractionState.AWAITING_CONFIRMATION]: {
    layout_mode: "confirmation_gate",
    panels: {
      dashboard: { visible: true, enabled: false },
      tasks: { visible: true, enabled: false },
      calendar: { visible: true, enabled: false },
      chat: { visible: true, enabled: true },
      action_cards: { visible: true, enabled: true },
      system_status: { visible: true, enabled: true },
    },
    chat: {
      input_enabled: true,
      send_enabled: true,
      action_cards_visible: true,
      requires_confirmation_banner: true,
    },
  },
  [InteractionState.EXECUTING]: {
    layout_mode: "execution_busy",
    panels: {
      dashboard: { visible: true, enabled: false },
      tasks: { visible: true, enabled: false },
      calendar: { visible: true, enabled: false },
      chat: { visible: true, enabled: false },
      action_cards: { visible: true, enabled: false },
      system_status: { visible: true, enabled: true },
    },
    chat: {
      input_enabled: false,
      send_enabled: false,
      action_cards_visible: true,
      requires_confirmation_banner: false,
    },
  },
  [InteractionState.RESOLVING_CONFLICT]: {
    layout_mode: "conflict_resolution",
    panels: {
      dashboard: { visible: true, enabled: false },
      tasks: { visible: true, enabled: false },
      calendar: { visible: true, enabled: false },
      chat: { visible: true, enabled: true },
      action_cards: { visible: true, enabled: false },
      system_status: { visible: true, enabled: true },
    },
    chat: {
      input_enabled: true,
      send_enabled: true,
      action_cards_visible: false,
      requires_confirmation_banner: false,
    },
  },
  [InteractionState.FAILED_RECOVERABLE]: {
    layout_mode: "recoverable_error",
    panels: {
      dashboard: { visible: true, enabled: false },
      tasks: { visible: false, enabled: false },
      calendar: { visible: false, enabled: false },
      chat: { visible: true, enabled: true },
      action_cards: { visible: false, enabled: false },
      system_status: { visible: true, enabled: true },
    },
    chat: {
      input_enabled: true,
      send_enabled: true,
      action_cards_visible: false,
      requires_confirmation_banner: false,
    },
  },
  [InteractionState.FAILED_TERMINAL]: {
    layout_mode: "terminal_error",
    panels: {
      dashboard: { visible: true, enabled: false },
      tasks: { visible: false, enabled: false },
      calendar: { visible: false, enabled: false },
      chat: { visible: true, enabled: false },
      action_cards: { visible: false, enabled: false },
      system_status: { visible: true, enabled: true },
    },
    chat: {
      input_enabled: false,
      send_enabled: false,
      action_cards_visible: false,
      requires_confirmation_banner: false,
    },
  },
};

export class InteractionContractEngine {
  derive(input: InteractionContractInput): InteractionContractOutput {
    const derivedState = this.deriveInteractionState(input);
    const transition = this.resolveTransition({
      from: input.previous_state ?? derivedState,
      to: derivedState,
      strict: input.strict_transitions ?? false,
    });

    const stateForOutput = transition.next_state;

    return {
      interaction_state: stateForOutput,
      active_work_context: this.deriveActiveWorkContext({
        runtime_state: input.runtime_state,
        backend_snapshot: input.backend_snapshot,
        col_signals: input.col_signals,
        identity: input.identity,
        interaction_state: stateForOutput,
      }),
      ui_behavior: UI_BEHAVIOR_BY_STATE[stateForOutput],
      transition,
    };
  }

  deriveInteractionState(input: InteractionContractInput): InteractionState {
    const runtimeState = input.runtime_state;
    const backendSnapshot = input.backend_snapshot;
    const col = input.col_signals;
    const hasTerminalError = col.terminal_error || this.isTerminalError(input.error);
    const hasRecoverableError = col.recoverable_error || (!!input.error && !hasTerminalError);

    const hasPendingConfirmation =
      col.has_pending_confirmation ||
      !!runtimeState && Object.values(runtimeState.chat_sessions).some((session) => session.awaiting_confirmation);

    const proposedActionsCount = col.proposed_actions_count + (runtimeState?.pending_actions.length ?? 0);

    const hasConflict =
      col.conflict_detected ||
      runtimeState?.sync_status === "lagging" ||
      runtimeState?.sync_status === "desynced" ||
      backendSnapshot?.system_health.stale_projection === true;

    const isExecuting = col.execution_in_flight || (input.is_loading && proposedActionsCount > 0);

    const isAssisting = input.is_loading;

    const checks: Array<{ state: InteractionState; enabled: boolean }> = [
      { state: InteractionState.FAILED_TERMINAL, enabled: hasTerminalError },
      { state: InteractionState.FAILED_RECOVERABLE, enabled: hasRecoverableError },
      { state: InteractionState.RESOLVING_CONFLICT, enabled: hasConflict },
      { state: InteractionState.EXECUTING, enabled: isExecuting },
      { state: InteractionState.AWAITING_CONFIRMATION, enabled: hasPendingConfirmation },
      { state: InteractionState.PROPOSING_ACTIONS, enabled: proposedActionsCount > 0 },
      { state: InteractionState.CLARIFYING, enabled: col.has_clarification_request },
      { state: InteractionState.ASSISTING, enabled: isAssisting },
      { state: InteractionState.IDLE, enabled: true },
    ];

    for (const check of checks) {
      if (check.enabled) {
        return check.state;
      }
    }
    return InteractionState.IDLE;
  }

  deriveActiveWorkContext(input: {
    runtime_state: FrontendState | null;
    backend_snapshot: UIBootstrapState | null;
    col_signals: COLSignals;
    identity: {
      household_id: string;
      user_id: string;
      device_id: string;
    };
    interaction_state: InteractionState;
  }): ActiveWorkContext {
    const householdId =
      input.identity.household_id ||
      input.backend_snapshot?.family.family_id ||
      input.runtime_state?.snapshot.family.family_id ||
      "unknown-household";

    const watermark =
      input.backend_snapshot?.source_watermark ??
      input.runtime_state?.last_sync_watermark ??
      input.col_signals.last_updated_watermark ??
      "";

    const fromCandidates = this.resolveFocusCandidate(input.col_signals.focus_candidates);
    if (fromCandidates) {
      return {
        household_id: householdId,
        user_id: input.identity.user_id,
        device_id: input.identity.device_id,
        current_focus_type: fromCandidates.focus_type,
        focus_entity_id: fromCandidates.entity_id,
        summary_text: fromCandidates.summary_text,
        confidence_score: normalizeConfidence(fromCandidates.confidence_score),
        last_updated_watermark: watermark,
      };
    }

    const fromPendingAction = input.runtime_state?.pending_actions[0];
    if (fromPendingAction) {
      const focus = inferFocusType(fromPendingAction.related_entity);
      return {
        household_id: householdId,
        user_id: input.identity.user_id,
        device_id: input.identity.device_id,
        current_focus_type: focus,
        focus_entity_id: fromPendingAction.related_entity,
        summary_text: fromPendingAction.title,
        confidence_score: 0.9,
        last_updated_watermark: watermark,
      };
    }

    return {
      household_id: householdId,
      user_id: input.identity.user_id,
      device_id: input.identity.device_id,
      current_focus_type: FALLBACK_CONTEXT.current_focus_type,
      focus_entity_id: `${FALLBACK_CONTEXT.focus_entity_id}:${input.interaction_state}`,
      summary_text: describeStateSummary(input.interaction_state),
      confidence_score: FALLBACK_CONTEXT.confidence_score,
      last_updated_watermark: watermark,
    };
  }

  resolveTransition(input: {
    from: InteractionState;
    to: InteractionState;
    strict: boolean;
  }): TransitionResult {
    const allowed = INTERACTION_TRANSITIONS[input.from];
    if (allowed.includes(input.to)) {
      return {
        accepted: true,
        normalized: false,
        next_state: input.to,
      };
    }

    if (input.strict) {
      throw new Error(`invalid_interaction_transition:${input.from}->${input.to}`);
    }

    const nextState = this.normalizeInvalidTransition(input.from, input.to);
    return {
      accepted: false,
      normalized: true,
      next_state: nextState,
    };
  }

  normalizeInvalidTransition(from: InteractionState, to: InteractionState): InteractionState {
    if (from === InteractionState.FAILED_TERMINAL) {
      return InteractionState.FAILED_TERMINAL;
    }

    const allowed = INTERACTION_TRANSITIONS[from];
    const rankedAllowed = STATE_PRIORITY.filter((state) => allowed.includes(state));
    if (rankedAllowed.includes(to)) {
      return to;
    }

    return rankedAllowed[0] ?? from;
  }

  uiBehaviorFor(state: InteractionState): UIBehaviorMode {
    return UI_BEHAVIOR_BY_STATE[state];
  }

  private resolveFocusCandidate(candidates: COLFocusCandidate[]): COLFocusCandidate | null {
    if (candidates.length === 0) {
      return null;
    }

    const ordered = [...candidates].sort((a, b) => {
      if (a.confidence_score !== b.confidence_score) {
        return b.confidence_score - a.confidence_score;
      }

      const typeRank = FOCUS_TYPE_RANK[a.focus_type] - FOCUS_TYPE_RANK[b.focus_type];
      if (typeRank !== 0) {
        return typeRank;
      }

      const entityCmp = a.entity_id.localeCompare(b.entity_id);
      if (entityCmp !== 0) {
        return entityCmp;
      }

      return a.summary_text.localeCompare(b.summary_text);
    });

    return ordered[0];
  }

  private isTerminalError(error: string | null): boolean {
    if (!error) {
      return false;
    }

    const normalized = error.toLowerCase();
    return normalized.includes("terminal") || normalized.includes("schema") || normalized.includes("forbidden");
  }
}

export function inferFocusType(relatedEntity: string): FocusType {
  const lower = relatedEntity.toLowerCase();
  if (lower.startsWith("plan")) {
    return "PLAN";
  }
  if (lower.startsWith("task")) {
    return "TASK";
  }
  if (lower.startsWith("event")) {
    return "EVENT";
  }
  return "CHAT";
}

export function describeStateSummary(state: InteractionState): string {
  switch (state) {
    case InteractionState.ASSISTING:
      return "Assistant is processing your request.";
    case InteractionState.CLARIFYING:
      return "Clarification is required to continue.";
    case InteractionState.PROPOSING_ACTIONS:
      return "Action proposals are ready for review.";
    case InteractionState.AWAITING_CONFIRMATION:
      return "Waiting for action confirmation.";
    case InteractionState.EXECUTING:
      return "Executing approved action.";
    case InteractionState.RESOLVING_CONFLICT:
      return "Resolving synchronization or state conflict.";
    case InteractionState.FAILED_RECOVERABLE:
      return "A recoverable failure occurred.";
    case InteractionState.FAILED_TERMINAL:
      return "A terminal failure blocked interaction.";
    case InteractionState.IDLE:
    default:
      return "Waiting for the next instruction.";
  }
}

function normalizeConfidence(value: number): number {
  if (Number.isNaN(value) || !Number.isFinite(value)) {
    return 0;
  }
  if (value < 0) {
    return 0;
  }
  if (value > 1) {
    return 1;
  }
  return Math.round(value * 1000) / 1000;
}
