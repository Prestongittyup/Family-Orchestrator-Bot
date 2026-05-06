import type {
  ChatMessageRequest,
  ChatResponse,
  CreateCalendarEventRequest,
  CalendarEventRecord,
  DLSAnalyticsSnapshot,
  EmailDetail,
  HomeV0Contract,
  RequestIdentityContext,
  UIBootstrapState,
  UpdateCalendarEventRequest,
} from "./contracts";
import type { ActionExecutionRequest, ActionExecutionResult } from "../runtime/types";
import { buildApiUrl, fetchWithApiFallback } from "./network";

const GOOGLE_CONNECTED_USER_STORAGE_KEY = "hpal-google-user-id";
const GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY = "hpal-google-household-id";
const SHADOW_ENDPOINTS_ENABLED = false;

const resolveBootstrapUserId = (identity: RequestIdentityContext): string => {
  const identityUserId = (identity.user_id || "").trim();
  const identityHouseholdId = (identity.household_id || "").trim();

  try {
    const linkedUserId = (localStorage.getItem(GOOGLE_CONNECTED_USER_STORAGE_KEY) || "").trim();
    const linkedHouseholdId = (localStorage.getItem(GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY) || "").trim();
    if (linkedUserId && linkedHouseholdId && linkedHouseholdId === identityHouseholdId) {
      return linkedUserId;
    }
  } catch {
    // Storage is best-effort only.
  }

  return identityUserId;
};

export type GoogleEmailSyncResponse = {
  status: string;
  provider: string;
  count: number;
  processed_count: number;
  ignored_count: number;
  failed_count: number;
};

export type PantryInventoryDelta = {
  item: string;
  delta: number;
  unit?: string;
};

export type PantryAdjustResponse = {
  status: string;
  family_id: string;
  applied: Array<Record<string, unknown>>;
  inventory: Record<string, number>;
};

export type PantryReceiptDetectedItem = {
  item: string;
  delta: number;
  unit?: string;
};

export type PantryReceiptDryRunResponse = {
  status: "dry_run";
  family_id: string;
  detected_items: PantryReceiptDetectedItem[];
};

export type PantryReceiptAppliedResponse = {
  status: "applied";
  family_id: string;
  applied: Array<Record<string, unknown>>;
  inventory: Record<string, number>;
};

export type PantryReceiptIngestResponse = PantryReceiptDryRunResponse | PantryReceiptAppliedResponse;

type CanonicalCommandResult = {
  status?: string;
  request_id?: string;
  response?: Record<string, unknown>;
  code?: string;
  reason?: string;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const toRiskLevel = (value: unknown): "low" | "medium" | "high" => {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "high") {
    return "high";
  }
  if (normalized === "medium") {
    return "medium";
  }
  return "low";
};

const toIso = (value: Date): string => value.toISOString().replace("+00:00", "Z");

const defaultWindowEnd = (start: Date): Date => {
  const end = new Date(start.getTime());
  end.setDate(end.getDate() + 7);
  return end;
};

export class ProductSurfaceClient {
  supportsShadowEndpoints(): boolean {
    return SHADOW_ENDPOINTS_ENABLED;
  }

  supportsAnalytics(): boolean {
    return false;
  }

  async fetchHomeV0(
    householdId: string,
    identity?: RequestIdentityContext,
    requestedDate?: string,
  ): Promise<HomeV0Contract> {
    const normalizedHouseholdId = householdId.trim();
    if (!normalizedHouseholdId) {
      throw new Error("home_fetch_failed:missing_household_id");
    }

    const params = new URLSearchParams({ household_id: normalizedHouseholdId });
    const normalizedDate = (requestedDate || "").trim();
    if (normalizedDate) {
      params.set("date", normalizedDate);
    }

    const response = await fetchWithApiFallback(`/home?${params.toString()}`, {
      method: "GET",
      headers: identity ? this.identityHeaders(identity) : undefined,
    });

    if (!response.ok) {
      throw new Error(`home_fetch_failed:${response.status}`);
    }

    return (await response.json()) as HomeV0Contract;
  }

  async fetchBootstrap(familyId: string, identity: RequestIdentityContext): Promise<UIBootstrapState> {
    const householdId = familyId.trim();
    if (!householdId) {
      throw new Error("bootstrap_failed:missing_household_id");
    }

    const [tasksPayload, schedulesPayload, notificationsPayload] = await Promise.all([
      this.fetchCanonicalRead("/tasks", householdId, identity),
      this.fetchCanonicalRead("/schedule", householdId, identity),
      this.fetchCanonicalRead("/notifications", householdId, identity),
    ]);

    const taskRows = Array.isArray(tasksPayload.tasks) ? tasksPayload.tasks : [];
    const scheduleRows = Array.isArray(schedulesPayload.schedules) ? schedulesPayload.schedules : [];
    const notificationRows = Array.isArray(notificationsPayload.notifications)
      ? notificationsPayload.notifications
      : [];

    const pendingTasks = taskRows
      .filter((row) => String((row as Record<string, unknown>).status || "") !== "completed")
      .map((row) => this.toTaskSummary(row, identity.user_id));
    const completedTasks = taskRows
      .filter((row) => String((row as Record<string, unknown>).status || "") === "completed")
      .map((row) => this.toTaskSummary(row, identity.user_id));

    const calendarEvents = scheduleRows
      .filter((row) => String((row as Record<string, unknown>).status || "") !== "cancelled")
      .map((row) => this.toCalendarSummary(row));

    const notifications = notificationRows.map((row) => this.toNotification(row));

    const now = new Date();
    const nowIso = toIso(now);
    const watermark = `canonical:${householdId}:${taskRows.length}:${scheduleRows.length}:${notificationRows.length}:${Date.now()}`;
    const bootstrapUserId = resolveBootstrapUserId(identity) || identity.user_id;
    const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";

    return {
      snapshot_version: 1,
      source_watermark: watermark,
      family: {
        family_id: householdId,
        member_count: 1,
        member_names: [bootstrapUserId || "member"],
        default_time_zone: timeZone,
      },
      today_overview: {
        date: nowIso.slice(0, 10),
        open_task_count: pendingTasks.length,
        scheduled_event_count: calendarEvents.length,
        active_plan_count: 0,
        notification_count: notifications.length,
      },
      active_plans: [],
      task_board: {
        pending: pendingTasks,
        in_progress: [],
        completed: completedTasks,
        failed: [],
      },
      calendar: {
        window_start: nowIso,
        window_end: toIso(defaultWindowEnd(now)),
        events: calendarEvents,
      },
      notifications,
      explanation_digest: [],
      system_health: {
        status: "healthy",
        pending_actions: 0,
        stale_projection: false,
        state_version: 1,
        last_updated: nowIso,
      },
      identity_context: {
        household_id: householdId,
        user_id: bootstrapUserId,
        device_id: identity.device_id,
        role: identity.user_id === bootstrapUserId ? "ADULT" : "VIEW_ONLY",
      },
    };
  }

  async sendMessage(payload: ChatMessageRequest, identity: RequestIdentityContext): Promise<ChatResponse> {
    const result = await this.postCanonicalCommand({
      commandType: "assistant.query",
      householdId: payload.family_id,
      payload: {
        household_id: payload.family_id,
        query: payload.message,
        message: payload.message,
        request_id: payload.session_id,
      },
      identity,
    });
    return this.toChatResponse(result);
  }

  async executeAction(request: ActionExecutionRequest, identity: RequestIdentityContext): Promise<ActionExecutionResult> {
    const actionPayload = isRecord(request.action_card.required_action_payload)
      ? request.action_card.required_action_payload
      : {};
    const rawActionId = String(
      actionPayload.action_id || actionPayload.target_action_id || request.action_card.id || "",
    ).trim();
    const resolvedActionId = rawActionId.replace(/:reject$/i, "");
    const resolvedRequestId = String(actionPayload.request_id || request.session_id || "").trim();
    const commandType = request.action_card.type === "reject" ? "assistant.reject" : "assistant.approve";

    try {
      const result = await this.postCanonicalCommand({
        commandType,
        householdId: request.family_id,
        payload: {
          household_id: request.family_id,
          action_id: resolvedActionId,
          request_id: resolvedRequestId,
        },
        idempotencyKey: request.idempotency_key,
        identity,
      });

      return {
        status: "succeeded",
        response: this.toChatResponse(result),
      };
    } catch (error) {
      return {
        status: "failed",
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  async createCalendarEvent(
    householdId: string,
    request: CreateCalendarEventRequest,
    identity: RequestIdentityContext,
  ): Promise<CalendarEventRecord> {
    const start = request.start_time ? new Date(request.start_time) : new Date();
    const durationMinutes = Math.max(1, Number(request.duration_minutes || 30));
    const end = new Date(start.getTime() + durationMinutes * 60_000);

    const result = await this.postCanonicalCommand({
      commandType: "schedule.create",
      householdId,
      payload: {
        household_id: householdId,
        title: request.title,
        start_at: toIso(start),
        end_at: toIso(end),
      },
      identity,
    });

    const response = isRecord(result.response) ? result.response : {};
    const scheduleId = String(response.schedule_id || response.event_id || result.request_id || `schedule-${Date.now()}`);

    return {
      event_id: scheduleId,
      household_id: householdId,
      title: request.title,
      start_time: toIso(start),
      end_time: toIso(end),
      priority: 1,
      metadata: {
        source: "canonical.schedule.create",
      },
      created_at: new Date().toISOString(),
    };
  }

  async updateCalendarEvent(
    householdId: string,
    eventId: string,
    request: UpdateCalendarEventRequest,
    identity: RequestIdentityContext,
  ): Promise<CalendarEventRecord> {
    const start = request.start_time ? new Date(request.start_time) : null;
    const end = request.end_time ? new Date(request.end_time) : null;
    if (!start || !end || Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
      throw new Error("calendar_update_failed:canonical_requires_start_and_end");
    }

    await this.postCanonicalCommand({
      commandType: "schedule.cancel",
      householdId,
      payload: {
        household_id: householdId,
        schedule_id: eventId,
      },
      identity,
    });

    const created = await this.createCalendarEvent(
      householdId,
      {
        user_id: identity.user_id,
        title: request.title || "Updated calendar event",
        start_time: toIso(start),
        duration_minutes: Math.max(1, Math.round((end.getTime() - start.getTime()) / 60_000)),
        recurrence: "none",
        description: request.description || null,
      },
      identity,
    );

    return {
      ...created,
      metadata: {
        ...created.metadata,
        supersedes_event_id: eventId,
      },
    };
  }

  async deleteCalendarEvent(
    householdId: string,
    eventId: string,
    identity: RequestIdentityContext,
  ): Promise<{ deleted: boolean; event_id: string }> {
    await this.postCanonicalCommand({
      commandType: "schedule.cancel",
      householdId,
      payload: {
        household_id: householdId,
        schedule_id: eventId,
      },
      identity,
    });

    return { deleted: true, event_id: eventId };
  }

  async completeDecision(householdId: string, decisionId: string, identity?: RequestIdentityContext): Promise<void> {
    const response = await fetchWithApiFallback("/decision/complete", {
      method: "POST",
      headers: identity ? this.identityHeaders(identity) : { "Content-Type": "application/json" },
      body: JSON.stringify({
        household_id: householdId,
        decision_id: decisionId,
      }),
    });
    if (!response.ok) {
      throw new Error(`decision_complete_failed:${response.status}`);
    }
  }

  async deferDecision(
    householdId: string,
    decisionId: string,
    deferToDate: string,
    identity?: RequestIdentityContext,
  ): Promise<void> {
    const response = await fetchWithApiFallback("/decision/defer", {
      method: "POST",
      headers: identity ? this.identityHeaders(identity) : { "Content-Type": "application/json" },
      body: JSON.stringify({
        household_id: householdId,
        decision_id: decisionId,
        defer_to_date: deferToDate,
      }),
    });
    if (!response.ok) {
      throw new Error(`decision_defer_failed:${response.status}`);
    }
  }

  async ignoreDecision(householdId: string, decisionId: string, identity?: RequestIdentityContext): Promise<void> {
    const response = await fetchWithApiFallback("/decision/ignore", {
      method: "POST",
      headers: identity ? this.identityHeaders(identity) : { "Content-Type": "application/json" },
      body: JSON.stringify({
        household_id: householdId,
        decision_id: decisionId,
      }),
    });
    if (!response.ok) {
      throw new Error(`decision_ignore_failed:${response.status}`);
    }
  }

  async fetchEmailDetail(
    familyId: string,
    emailId: string,
    identity?: RequestIdentityContext,
  ): Promise<EmailDetail> {
    // TODO: REMOVE_SHADOW_ENDPOINT
    if (!SHADOW_ENDPOINTS_ENABLED) {
      throw new Error("email_detail_failed:shadow_endpoint_disabled");
    }

    const params = new URLSearchParams({
      family_id: familyId,
      email_id: emailId,
    });

    const headers = identity ? this.identityHeaders(identity) : undefined;
    let response: Response | null = null;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      response = await fetchWithApiFallback(`/v1/ui/email/detail?${params.toString()}`, {
        method: "GET",
        headers,
      });

      if (response.status === 429 && attempt < 2) {
        await new Promise((resolve) => window.setTimeout(resolve, 200 * (attempt + 1)));
        continue;
      }
      break;
    }

    if (!response) {
      throw new Error("email_detail_failed:unknown");
    }

    if (!response.ok) {
      throw new Error(`email_detail_failed:${response.status}`);
    }

    return (await response.json()) as EmailDetail;
  }

  async syncGoogleInbox(
    userId: string,
    householdId: string,
    identity?: RequestIdentityContext,
    maxResults = 100,
  ): Promise<GoogleEmailSyncResponse> {
    // TODO: REMOVE_SHADOW_ENDPOINT
    if (!SHADOW_ENDPOINTS_ENABLED) {
      throw new Error("email_sync_failed:shadow_endpoint_disabled");
    }

    const params = new URLSearchParams({
      household_id: householdId,
      max_results: String(maxResults),
    });

    const headers = identity ? this.identityHeaders(identity) : undefined;

    const response = await fetchWithApiFallback(`/integrations/google-email/sync/${encodeURIComponent(userId)}?${params.toString()}`, {
      method: "POST",
      headers,
    });

    if (!response.ok) {
      let detail = "";
      try {
        const payload = (await response.json()) as {
          detail?: { message?: string } | string;
        };
        const candidate = payload?.detail;
        if (typeof candidate === "string") {
          detail = candidate;
        } else if (candidate && typeof candidate.message === "string") {
          detail = candidate.message;
        }
      } catch {
        detail = "";
      }

      const suffix = detail ? `:${detail}` : "";
      throw new Error(`email_sync_failed:${response.status}${suffix}`);
    }

    return (await response.json()) as GoogleEmailSyncResponse;
  }

  async adjustPantryInventory(
    householdId: string,
    updates: PantryInventoryDelta[],
    identity: RequestIdentityContext,
    note?: string,
  ): Promise<PantryAdjustResponse> {
    // TODO: REMOVE_SHADOW_ENDPOINT
    if (!SHADOW_ENDPOINTS_ENABLED) {
      throw new Error("pantry_adjust_failed:shadow_endpoint_disabled");
    }

    void householdId;
    void updates;
    void identity;
    void note;
    throw new Error("pantry_adjust_failed:shadow_endpoint_disabled");
  }

  async ingestPantryReceipt(
    householdId: string,
    file: File,
    identity: RequestIdentityContext,
    dryRun = false,
  ): Promise<PantryReceiptIngestResponse> {
    // TODO: REMOVE_SHADOW_ENDPOINT
    if (!SHADOW_ENDPOINTS_ENABLED) {
      throw new Error("pantry_receipt_failed:shadow_endpoint_disabled");
    }

    void householdId;
    void file;
    void identity;
    void dryRun;
    throw new Error("pantry_receipt_failed:shadow_endpoint_disabled");
  }

  async fetchDLSAnalytics(userId?: string, limit = 500): Promise<DLSAnalyticsSnapshot> {
    // TODO: REMOVE_SHADOW_ENDPOINT
    if (!SHADOW_ENDPOINTS_ENABLED) {
      throw new Error("analytics_dls_failed:shadow_endpoint_disabled");
    }

    void userId;
    void limit;
    throw new Error("analytics_dls_failed:shadow_endpoint_disabled");
  }

  createDLSAnalyticsSocket(userId?: string, limit = 500, intervalSeconds = 2): WebSocket {
    // TODO: REMOVE_SHADOW_ENDPOINT
    void userId;
    void limit;
    void intervalSeconds;
    throw new Error("analytics_ws_failed:shadow_endpoint_disabled");
  }

  private async fetchCanonicalRead(
    path: "/tasks" | "/schedule" | "/notifications",
    householdId: string,
    identity: RequestIdentityContext,
  ): Promise<Record<string, unknown>> {
    const params = new URLSearchParams({
      household_id: householdId,
      limit: "200",
      offset: "0",
    });

    const response = await fetchWithApiFallback(`${path}?${params.toString()}`, {
      method: "GET",
      headers: this.identityHeaders(identity),
    });

    if (!response.ok) {
      throw new Error(`bootstrap_failed:${path}:${response.status}`);
    }

    const data = (await response.json()) as unknown;
    if (!isRecord(data)) {
      throw new Error(`bootstrap_failed:${path}:invalid_payload`);
    }
    return data;
  }

  private toTaskSummary(taskRow: unknown, fallbackUserId: string): {
    task_id: string;
    title: string;
    plan_id: string;
    assigned_to: string;
    status: string;
    priority: string;
    due_time?: string | null;
  } {
    const row = isRecord(taskRow) ? taskRow : {};
    return {
      task_id: String(row.task_id || ""),
      title: String(row.title || "Untitled task"),
      plan_id: "",
      assigned_to: fallbackUserId,
      status: String(row.status || "pending"),
      priority: String(row.priority || "medium"),
      due_time: row.due_at ? String(row.due_at) : null,
    };
  }

  private toCalendarSummary(rowValue: unknown): {
    event_id: string;
    title: string;
    start: string;
    end: string;
    participants: string[];
  } {
    const row = isRecord(rowValue) ? rowValue : {};
    const start = String(row.start_at || new Date().toISOString());
    const end = String(row.end_at || start);
    return {
      event_id: String(row.schedule_id || row.event_id || `schedule-${Date.now()}`),
      title: String(row.title || "Scheduled event"),
      start,
      end,
      participants: [],
    };
  }

  private toNotification(rowValue: unknown): {
    notification_id: string;
    title: string;
    message: string;
    level: "info" | "warning" | "critical";
    related_entity?: string | null;
  } {
    const row = isRecord(rowValue) ? rowValue : {};
    const deliveryStatus = String(row.delivery_status || "pending").toLowerCase();
    const sourceType = String(row.source_type || "notification");
    const level: "info" | "warning" | "critical" =
      deliveryStatus === "pending" ? "warning" : "info";

    return {
      notification_id: String(row.notification_id || `notification-${Date.now()}`),
      title: sourceType || "Notification",
      message: String(row.message || "Notification update available."),
      level,
      related_entity: row.source_id ? String(row.source_id) : null,
    };
  }

  private async postCanonicalCommand(options: {
    commandType: string;
    householdId: string;
    payload: Record<string, unknown>;
    idempotencyKey?: string;
    identity?: RequestIdentityContext;
  }): Promise<CanonicalCommandResult> {
    const response = await fetchWithApiFallback("/command", {
      method: "POST",
      headers: options.identity
        ? this.identityHeaders(options.identity)
        : { "Content-Type": "application/json" },
      body: JSON.stringify({
        command_type: options.commandType,
        household_id: options.householdId,
        payload: options.payload,
        idempotency_key: options.idempotencyKey,
      }),
    });

    if (!response.ok) {
      throw new Error(`command_failed:${options.commandType}:${response.status}`);
    }

    const data = (await response.json()) as CanonicalCommandResult;
    const status = String(data.status || "").toLowerCase();
    if (status === "rejected") {
      const responsePayload = isRecord(data.response) ? data.response : {};
      const code = String(responsePayload.code || data.code || "rejected");
      throw new Error(`command_rejected:${options.commandType}:${code}`);
    }

    return data;
  }

  private toChatResponse(result: CanonicalCommandResult): ChatResponse {
    const responsePayload = isRecord(result.response) ? result.response : {};
    const intent = isRecord(responsePayload.intent_interpretation)
      ? responsePayload.intent_interpretation
      : {};
    const recommended = isRecord(responsePayload.recommended_action)
      ? responsePayload.recommended_action
      : {};
    const trace = Array.isArray(responsePayload.reasoning_trace)
      ? responsePayload.reasoning_trace.map((item) => String(item))
      : [];
    const requestId = String(responsePayload.request_id || result.request_id || "").trim();
    const actionId = String(recommended.action_id || `${requestId || "assistant"}-action`).trim();
    const recommendationTitle = String(recommended.title || "Review assistant recommendation").trim();
    const recommendationDescription = String(
      recommended.description || "Review and approve this assistant recommendation.",
    ).trim();
    const assistantMessageParts = [
      String(intent.summary || "Household coordination update ready.").trim(),
      recommendationTitle ? `Recommended action: ${recommendationTitle}.` : "",
      trace[0] ? `Reasoning: ${trace[0]}` : "",
    ].filter((part) => part.length > 0);

    return {
      assistant_message: assistantMessageParts.join(" "),
      action_cards: [
        {
          id: actionId,
          type: "confirm",
          title: recommendationTitle,
          description: recommendationDescription,
          related_entity: actionId,
          required_action_payload: {
            request_id: requestId,
            action_id: actionId,
          },
          risk_level: toRiskLevel(recommended.urgency),
        },
        {
          id: `${actionId}:reject`,
          type: "reject",
          title: `Reject: ${recommendationTitle}`,
          description: "Reject this recommendation and keep current state.",
          related_entity: actionId,
          required_action_payload: {
            request_id: requestId,
            action_id: actionId,
          },
          risk_level: "low",
        },
      ],
      ui_patch: [],
      requires_confirmation: Boolean(recommended.approval_required ?? true),
      explanation_summary: [],
    };
  }

  private identityHeaders(identity: RequestIdentityContext): HeadersInit {
    return {
      "Content-Type": "application/json",
      ...this.identityAuthHeaders(identity),
    };
  }

  private identityAuthHeaders(identity: RequestIdentityContext): HeadersInit {
    return {
      "x-hpal-household-id": identity.household_id,
      "x-hpal-user-id": identity.user_id,
      "x-hpal-device-id": identity.device_id,
      Authorization: `Bearer ${identity.session_token}`,
    };
  }
}

export const productSurfaceClient = new ProductSurfaceClient();
