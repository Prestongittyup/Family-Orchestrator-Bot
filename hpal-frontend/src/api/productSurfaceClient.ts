import type {
  ChatMessageRequest,
  ChatResponse,
  CreateCalendarEventRequest,
  CalendarEventRecord,
  EmailDetail,
  RequestIdentityContext,
  UIBootstrapState,
  UpdateCalendarEventRequest,
} from "./contracts";
import type { ActionExecutionRequest, ActionExecutionResult } from "../runtime/types";
import { fetchWithApiFallback } from "./network";

const GOOGLE_CONNECTED_USER_STORAGE_KEY = "hpal-google-user-id";
const GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY = "hpal-google-household-id";

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

export class ProductSurfaceClient {
  async fetchBootstrap(familyId: string, identity: RequestIdentityContext): Promise<UIBootstrapState> {
    const bootstrapUserId = resolveBootstrapUserId(identity);
    const params = new URLSearchParams({
      family_id: familyId,
      user_id: bootstrapUserId,
      device_id: identity.device_id,
    });
    let response: Response | null = null;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      response = await fetchWithApiFallback(`/v1/ui/bootstrap?${params.toString()}`, {
        method: "GET",
        headers: this.identityHeaders(identity),
      });

      if (response.status === 429 && attempt < 2) {
        await new Promise((resolve) => window.setTimeout(resolve, 150 * (attempt + 1)));
        continue;
      }
      break;
    }

    if (!response) {
      throw new Error("bootstrap_failed:unknown");
    }

    if (!response.ok) {
      throw new Error(`bootstrap_failed:${response.status}`);
    }
    return (await response.json()) as UIBootstrapState;
  }

  async sendMessage(payload: ChatMessageRequest, identity: RequestIdentityContext): Promise<ChatResponse> {
    const params = new URLSearchParams({
      user_id: identity.user_id,
      device_id: identity.device_id,
    });

    const response = await fetchWithApiFallback(`/v1/ui/message?${params.toString()}`, {
      method: "POST",
      headers: this.identityHeaders(identity),
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      throw new Error(`message_failed:${response.status}`);
    }
    return (await response.json()) as ChatResponse;
  }

  async executeAction(request: ActionExecutionRequest, identity: RequestIdentityContext): Promise<ActionExecutionResult> {
    // Contract-level abstraction: if no dedicated backend action endpoint exists,
    // return a deterministic failed result so caller can reconcile from bootstrap.
    const params = new URLSearchParams({
      user_id: identity.user_id,
      device_id: identity.device_id,
    });

    const response = await fetchWithApiFallback(`${request.endpoint}?${params.toString()}`, {
      method: "POST",
      headers: {
        ...this.identityHeaders(identity),
        "x-idempotency-key": request.idempotency_key,
      },
      body: JSON.stringify({
        family_id: request.family_id,
        session_id: request.session_id,
        action_card_id: request.action_card.id,
        payload: request.payload,
      }),
    });

    if (!response.ok) {
      return {
        status: "failed",
        error: `action_failed:${response.status}`,
      };
    }

    const data = (await response.json()) as ChatResponse;
    return {
      status: "succeeded",
      response: data,
    };
  }

  async createCalendarEvent(
    householdId: string,
    request: CreateCalendarEventRequest,
    identity: RequestIdentityContext,
  ): Promise<CalendarEventRecord> {
    const response = await fetchWithApiFallback(`/v1/calendar/${householdId}/events`, {
      method: "POST",
      headers: this.identityHeaders(identity),
      body: JSON.stringify(request),
    });
    if (!response.ok) {
      throw new Error(`calendar_create_failed:${response.status}`);
    }
    return (await response.json()) as CalendarEventRecord;
  }

  async updateCalendarEvent(
    householdId: string,
    eventId: string,
    request: UpdateCalendarEventRequest,
    identity: RequestIdentityContext,
  ): Promise<CalendarEventRecord> {
    const response = await fetchWithApiFallback(`/v1/calendar/${householdId}/events/${eventId}`, {
      method: "PATCH",
      headers: this.identityHeaders(identity),
      body: JSON.stringify(request),
    });
    if (!response.ok) {
      throw new Error(`calendar_update_failed:${response.status}`);
    }
    return (await response.json()) as CalendarEventRecord;
  }

  async deleteCalendarEvent(
    householdId: string,
    eventId: string,
    identity: RequestIdentityContext,
  ): Promise<{ deleted: boolean; event_id: string }> {
    const response = await fetchWithApiFallback(`/v1/calendar/${householdId}/events/${eventId}`, {
      method: "DELETE",
      headers: this.identityHeaders(identity),
    });
    if (!response.ok) {
      throw new Error(`calendar_delete_failed:${response.status}`);
    }
    return (await response.json()) as { deleted: boolean; event_id: string };
  }

  async fetchEmailDetail(
    familyId: string,
    emailId: string,
    identity?: RequestIdentityContext,
  ): Promise<EmailDetail> {
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
    const response = await fetchWithApiFallback(
      `/v1/pantry/${encodeURIComponent(householdId)}/adjust`,
      {
        method: "POST",
        headers: this.identityHeaders(identity),
        body: JSON.stringify({
          updates,
          note,
        }),
      },
    );

    if (!response.ok) {
      let detail = "";
      try {
        const payload = (await response.json()) as { detail?: unknown };
        if (typeof payload.detail === "string") {
          detail = payload.detail;
        }
      } catch {
        detail = "";
      }

      const suffix = detail ? `:${detail}` : "";
      throw new Error(`pantry_adjust_failed:${response.status}${suffix}`);
    }

    return (await response.json()) as PantryAdjustResponse;
  }

  async ingestPantryReceipt(
    householdId: string,
    file: File,
    identity: RequestIdentityContext,
    dryRun = false,
  ): Promise<PantryReceiptIngestResponse> {
    const formData = new FormData();
    formData.append("file", file);
    formData.append("dry_run", dryRun ? "true" : "false");

    const response = await fetchWithApiFallback(
      `/v1/pantry/${encodeURIComponent(householdId)}/ingest-receipt`,
      {
        method: "POST",
        headers: this.identityAuthHeaders(identity),
        body: formData,
      },
    );

    if (!response.ok) {
      let detail = "";
      try {
        const payload = (await response.json()) as { detail?: unknown };
        if (typeof payload.detail === "string") {
          detail = payload.detail;
        }
      } catch {
        detail = "";
      }

      const suffix = detail ? `:${detail}` : "";
      throw new Error(`pantry_receipt_failed:${response.status}${suffix}`);
    }

    return (await response.json()) as PantryReceiptIngestResponse;
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
