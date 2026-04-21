import type { ActionCard, ChatResponse, UIBootstrapState, UIPatch } from "../api/contracts";

export type SyncStatus = "synced" | "lagging" | "desynced";

export interface ChatSessionState {
  session_id: string;
  message_history: string[];
  pending_action_cards: ActionCard[];
  last_ui_patch: UIPatch[];
  awaiting_confirmation: boolean;
  last_response_fingerprint?: string;
}

export interface FrontendState {
  snapshot: UIBootstrapState;
  applied_patches: UIPatch[];
  pending_actions: ActionCard[];
  chat_sessions: Record<string, ChatSessionState>;
  last_sync_watermark: string;
  sync_status: SyncStatus;
  materialized_index: Record<string, Record<string, unknown>>;
}

export interface SyncLoopConfig {
  syncedMs: number;
  laggingMs: number;
  desyncedMs: number;
}

export interface ActionExecutionRequest {
  family_id: string;
  session_id: string;
  action_card: ActionCard;
  endpoint: string;
  payload: Record<string, unknown>;
  idempotency_key: string;
  retry_count: number;
}

export interface ActionExecutionResult {
  status: "succeeded" | "failed";
  response?: ChatResponse;
  error?: string;
}

export interface ActionExecutionBinder {
  buildRequest(input: {
    familyId: string;
    sessionId: string;
    actionCard: ActionCard;
    endpoint: string;
    payload?: Record<string, unknown>;
  }): ActionExecutionRequest;
  execute(input: {
    request: ActionExecutionRequest;
    send: (request: ActionExecutionRequest) => Promise<ActionExecutionResult>;
  }): Promise<ActionExecutionResult>;
}
