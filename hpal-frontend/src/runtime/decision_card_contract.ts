export const DECISION_CARD_CONTRACT_VERSION = "v1" as const;

export type DecisionCardContractVersion = typeof DECISION_CARD_CONTRACT_VERSION;

export type DecisionCardState =
  | "generated"
  | "surfaced"
  | "acknowledged"
  | "resolved"
  | "applied";

export type DecisionCardEventType =
  | "DecisionCardGenerated"
  | "DecisionCardSurfaced"
  | "DecisionCardAcknowledged"
  | "DecisionCardResolved"
  | "DecisionCardApplied"
  | "DecisionDeferred"
  | "DecisionIgnored"
  | "DecisionCompleted";

export interface DecisionCardRecord {
  decision_card_id: string;
  household_id: string;
  title: string;
  root_cause_key: string;
  contract_version: DecisionCardContractVersion;
  origin_api: string;
  dedupe_key: string;
  state: DecisionCardState;
  created_at: string;
  updated_at: string;
  actor_id: string;
  last_event_type: DecisionCardEventType;
  metadata?: Record<string, unknown>;
  resolved_at?: string;
  applied_at?: string;
  defer_to_date?: string;
}
