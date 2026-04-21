import type {
  ChatMessageRequest,
  ChatResponse,
  CreateCalendarEventRequest,
  CalendarEventRecord,
  RequestIdentityContext,
  UIBootstrapState,
  UpdateCalendarEventRequest,
} from "./contracts";
import type { ActionExecutionRequest, ActionExecutionResult } from "../runtime/types";

const BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "http://localhost:8000";

export class ProductSurfaceClient {
  async fetchBootstrap(familyId: string, identity: RequestIdentityContext): Promise<UIBootstrapState> {
    const params = new URLSearchParams({
      family_id: familyId,
      user_id: identity.user_id,
      device_id: identity.device_id,
    });
    const url = `${BASE_URL}/v1/ui/bootstrap?${params.toString()}`;
    const response = await fetch(url, {
      method: "GET",
      headers: this.identityHeaders(identity),
    });
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

    const response = await fetch(`${BASE_URL}/v1/ui/message?${params.toString()}`, {
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

    const response = await fetch(`${BASE_URL}${request.endpoint}?${params.toString()}`, {
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
    const response = await fetch(`${BASE_URL}/v1/calendar/${householdId}/events`, {
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
    const response = await fetch(`${BASE_URL}/v1/calendar/${householdId}/events/${eventId}`, {
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
    const response = await fetch(`${BASE_URL}/v1/calendar/${householdId}/events/${eventId}`, {
      method: "DELETE",
      headers: this.identityHeaders(identity),
    });
    if (!response.ok) {
      throw new Error(`calendar_delete_failed:${response.status}`);
    }
    return (await response.json()) as { deleted: boolean; event_id: string };
  }

  private identityHeaders(identity: RequestIdentityContext): HeadersInit {
    return {
      "Content-Type": "application/json",
      "x-hpal-household-id": identity.household_id,
      "x-hpal-user-id": identity.user_id,
      "x-hpal-device-id": identity.device_id,
      Authorization: `Bearer ${identity.session_token}`,
    };
  }
}

export const productSurfaceClient = new ProductSurfaceClient();
