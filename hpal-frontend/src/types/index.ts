/**
 * HPAL Frontend Type Definitions
 *
 * This file defines all product-domain types consumed from HPAL backend.
 * Internal orchestration concepts (DAG, leases, outbox, invariants) are
 * intentionally omitted from the frontend domain model.
 */

export enum PlanStatus {
  ACTIVE = "active",
  PAUSED = "paused",
  COMPLETED = "completed",
  FAILED = "failed",
}

export enum PlanStability {
  STABLE = "stable",
  ADJUSTING = "adjusting",
  BLOCKED = "blocked",
}

export enum TaskStatus {
  PENDING = "pending",
  IN_PROGRESS = "in_progress",
  COMPLETED = "completed",
  FAILED = "failed",
  STALE_PROJECTION = "stale_projection",
}

export enum EventSource {
  MANUAL = "manual",
  SYSTEM_GENERATED = "system_generated",
}

export type TaskStatusType = TaskStatus | "pending" | "in_progress" | "completed" | "failed" | "stale_projection";

export interface TimeWindow {
  start: string;
  end: string;
}

export interface Person {
  person_id: string;
  name: string;
  role: string;
  availability_constraints: string[];
  preferences: Record<string, any>;
  assigned_tasks: string[];
  schedule_overlay: Array<{ [key: string]: string }>;
}

export interface Plan {
  plan_id: string;
  family_id: string;
  title: string;
  intent_origin: string;
  status: PlanStatus;
  linked_tasks: string[];
  schedule_window: TimeWindow;
  last_recomputed_at: string | null;
  revision: number;
  stability_state: PlanStability;
  plan_type?: string;
  participants?: string[];
}

export interface Task {
  task_id: string;
  plan_id: string;
  assigned_to: string;
  status: TaskStatusType;
  due_time: string | null;
  auto_generated: boolean;
  priority: string;
  title: string;
}

export interface Event {
  event_id: string;
  family_id: string;
  title: string;
  time_window: TimeWindow;
  participants: string[];
  linked_plans: string[];
  source: EventSource;
  linked_plan_revisions?: string[];
}

export interface Family {
  family_id: string;
  members: Person[];
  shared_calendar_ref: string;
  default_time_zone: string;
  household_preferences: Record<string, any>;
  active_plans: string[];
  system_state_summary: SystemStateSummary;
}

export interface SystemStateSummary {
  state_version: number;
  pending_actions: number;
  projection_epoch: number;
  last_projection_at: string;
  stale_projection: boolean;
}

export interface HouseholdOverview {
  family: Family;
  today_events: Event[];
  active_plan_count: number;
  pending_task_count: number;
  completed_task_count: number;
}

export interface ProjectionWatermark {
  projection_epoch: number;
  transition_count: number;
  event_count: number;
  source_state_version: number;
  snapshot_hash: string;
  last_projection_at: string;
}

/**
 * Frontend State Shape
 * Represents the complete client-side projection of HPAL state.
 */
export interface HPALFrontendState {
  // Core projections
  family: Family | null;
  plans: Plan[];
  tasks: Task[];
  events: Event[];

  // Watermark for preventing regressions
  projection_watermark: ProjectionWatermark | null;

  // UI state
  selected_plan_id: string | null;
  selected_person_id: string | null;
  error: string | null;
  loading: boolean;
  last_sync_at: string | null;

  // Change tracking for System Explain panel
  last_change: ChangeEvent | null;
}

export interface ChangeEvent {
  entity_type: "plan" | "task" | "event";
  entity_id: string;
  reason_code: string;
  initiated_by: "user" | "system";
  recompute_trigger?: string;
  watermark: ProjectionWatermark | null;
  timestamp: string;
}

export interface CommandResult {
  command_id: string;
  status: "accepted" | "replayed";
  submitted_at: string;
}

export interface APIError {
  code: string;
  message: string;
  status: number;
}
