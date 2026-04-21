export interface FamilySummary {
  family_id: string;
  member_count: number;
  member_names: string[];
  default_time_zone: string;
}

export interface TodayOverview {
  date: string;
  open_task_count: number;
  scheduled_event_count: number;
  active_plan_count: number;
  notification_count: number;
}

export interface PlanSummary {
  plan_id: string;
  title: string;
  status: string;
  revision: number;
  linked_task_count: number;
}

export interface TaskSummary {
  task_id: string;
  title: string;
  plan_id: string;
  assigned_to: string;
  status: string;
  priority: string;
  due_time?: string | null;
}

export interface TaskBoardState {
  pending: TaskSummary[];
  in_progress: TaskSummary[];
  completed: TaskSummary[];
  failed: TaskSummary[];
}

export interface CalendarEventSummary {
  event_id: string;
  title: string;
  start: string;
  end: string;
  participants: string[];
}

export interface CalendarState {
  window_start: string;
  window_end: string;
  events: CalendarEventSummary[];
}

export interface Notification {
  notification_id: string;
  title: string;
  message: string;
  level: "info" | "warning" | "critical";
  related_entity?: string | null;
}

export interface XAIExplanationSummary {
  explanation_id: string;
  entity_type: string;
  entity_id: string;
  summary: string;
  timestamp: string;
}

export interface SystemHealthSnapshot {
  status: "healthy" | "degraded";
  pending_actions: number;
  stale_projection: boolean;
  state_version: number;
  last_updated: string;
}

export interface UIIdentityContext {
  household_id: string;
  user_id: string;
  device_id: string;
  role: "ADMIN" | "ADULT" | "CHILD" | "VIEW_ONLY";
}

export interface UIBootstrapState {
  snapshot_version: number;
  source_watermark: string;
  family: FamilySummary;
  today_overview: TodayOverview;
  active_plans: PlanSummary[];
  task_board: TaskBoardState;
  calendar: CalendarState;
  notifications: Notification[];
  explanation_digest: XAIExplanationSummary[];
  system_health: SystemHealthSnapshot;
  identity_context?: UIIdentityContext;
}

export interface UIPatch {
  entity_type: "task" | "plan" | "event" | "family" | "notification";
  entity_id: string;
  change_type: "create" | "update" | "delete" | "replace";
  payload: Record<string, unknown>;
  version: number;
  source_timestamp: string;
}

export interface ActionCard {
  id: string;
  type: "confirm" | "reschedule" | "approve" | "reject" | "edit";
  title: string;
  description: string;
  related_entity: string;
  required_action_payload: Record<string, unknown>;
  risk_level: "low" | "medium" | "high";
}

export interface RequestIdentityContext {
  household_id: string;
  user_id: string;
  device_id: string;
  session_token: string;
}

export interface ChatMessageRequest {
  family_id: string;
  message: string;
  session_id: string;
}

export interface ChatResponse {
  assistant_message: string;
  action_cards: ActionCard[];
  ui_patch: UIPatch[];
  requires_confirmation: boolean;
  explanation_summary: XAIExplanationSummary[];
}

export type CalendarRecurrence = "none" | "daily" | "weekly" | "monthly";

export interface CreateCalendarEventRequest {
  user_id: string;
  title: string;
  description?: string | null;
  start_time?: string | null;
  duration_minutes?: number;
  recurrence?: CalendarRecurrence;
}

export interface UpdateCalendarEventRequest {
  title?: string;
  start_time?: string;
  end_time?: string;
  description?: string;
}

export interface CalendarEventRecord {
  event_id: string;
  household_id: string;
  title: string;
  start_time: string;
  end_time: string;
  priority: number;
  metadata: Record<string, unknown>;
  created_at: string;
}
