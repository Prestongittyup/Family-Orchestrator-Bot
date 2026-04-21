/**
 * HPAL API Client
 *
 * This is the ONLY surface for backend communication.
 * All requests route through HPAL command gateway and read endpoints.
 * No direct orchestration or internal system access is permitted.
 */

import {
  Family,
  HouseholdOverview,
  Plan,
  Task,
  Event,
  CommandResult,
  CreatePlanRequest,
} from "../types/index";

const API_BASE = process.env.REACT_APP_HPAL_API_URL || "http://localhost:8000/v1";

class HPALClient {
  private baseURL: string;

  constructor(baseURL: string = API_BASE) {
    this.baseURL = baseURL;
  }

  /**
   * Get family state and system health summary
   * SAFE: Read-only, returns current projection with watermark
   */
  async getFamilyState(familyId: string): Promise<Family> {
    const response = await fetch(`${this.baseURL}/families/${familyId}`, {
      method: "GET",
      headers: this.headers(),
    });
    if (!response.ok) {
      throw this.handleError(response);
    }
    return response.json();
  }

  /**
   * Get complete household overview (family + today's events + task/plan summaries)
   * SAFE: Read-only, includes watermark for UI-level regression prevention
   */
  async getHouseholdOverview(familyId: string): Promise<HouseholdOverview> {
    const response = await fetch(`${this.baseURL}/families/${familyId}/overview`, {
      method: "GET",
      headers: this.headers(),
    });
    if (!response.ok) {
      throw this.handleError(response);
    }
    return response.json();
  }

  /**
   * Get all active plans for a family
   * SAFE: Read-only
   */
  async getPlansByFamily(familyId: string): Promise<Plan[]> {
    const response = await fetch(`${this.baseURL}/families/${familyId}/plans`, {
      method: "GET",
      headers: this.headers(),
    });
    if (!response.ok) {
      throw this.handleError(response);
    }
    const data = await response.json();
    return data.plans || [];
  }

  /**
   * Get a specific plan by ID
   * SAFE: Read-only
   */
  async getPlanById(familyId: string, planId: string): Promise<Plan> {
    const response = await fetch(`${this.baseURL}/families/${familyId}/plans/${planId}`, {
      method: "GET",
      headers: this.headers(),
    });
    if (!response.ok) {
      throw this.handleError(response);
    }
    return response.json();
  }

  /**
   * Get all tasks for a family
   * SAFE: Read-only
   */
  async getTasksByFamily(familyId: string): Promise<Task[]> {
    const response = await fetch(`${this.baseURL}/families/${familyId}/tasks`, {
      method: "GET",
      headers: this.headers(),
    });
    if (!response.ok) {
      throw this.handleError(response);
    }
    const data = await response.json();
    return data.tasks || [];
  }

  /**
   * Get tasks for a specific person
   * SAFE: Read-only
   */
  async getTasksByPerson(familyId: string, personId: string): Promise<Task[]> {
    const response = await fetch(
      `${this.baseURL}/families/${familyId}/people/${personId}/tasks`,
      {
        method: "GET",
        headers: this.headers(),
      }
    );
    if (!response.ok) {
      throw this.handleError(response);
    }
    const data = await response.json();
    return data.tasks || [];
  }

  /**
   * Get calendar events for a family
   * SAFE: Read-only
   */
  async getEventsByFamily(familyId: string): Promise<Event[]> {
    const response = await fetch(`${this.baseURL}/families/${familyId}/calendar`, {
      method: "GET",
      headers: this.headers(),
    });
    if (!response.ok) {
      throw this.handleError(response);
    }
    const data = await response.json();
    return data.events || [];
  }

  /**
   * Create a new plan via HPAL command gateway
   * SAFE: Only mutation path is through HPAL gateway with idempotency
   */
  async createPlan(
    familyId: string,
    request: {
      title: string;
      intent_origin: string;
      schedule_window: { start: string; end: string };
      participants?: string[];
      idempotency_key: string;
    }
  ): Promise<{ plan: Plan; command: CommandResult }> {
    const response = await fetch(`${this.baseURL}/families/${familyId}/plans`, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify(request),
    });
    if (!response.ok) {
      throw this.handleError(response);
    }
    return response.json();
  }

  /**
   * Update a plan via HPAL command gateway
   * SAFE: Only through HPAL with idempotency
   */
  async updatePlan(
    familyId: string,
    planId: string,
    request: {
      title?: string;
      schedule_window?: { start: string; end: string };
      status?: "active" | "paused";
      idempotency_key: string;
    }
  ): Promise<{ plan: Plan; command: CommandResult }> {
    const response = await fetch(`${this.baseURL}/families/${familyId}/plans/${planId}`, {
      method: "PATCH",
      headers: this.headers(),
      body: JSON.stringify(request),
    });
    if (!response.ok) {
      throw this.handleError(response);
    }
    return response.json();
  }

  /**
   * Recompute a plan
   * SAFE: Only through HPAL with idempotency
   */
  async recomputePlan(
    familyId: string,
    planId: string,
    request: {
      reason: string;
      idempotency_key: string;
    }
  ): Promise<CommandResult> {
    const response = await fetch(
      `${this.baseURL}/families/${familyId}/plans/${planId}/recompute`,
      {
        method: "POST",
        headers: this.headers(),
        body: JSON.stringify(request),
      }
    );
    if (!response.ok) {
      throw this.handleError(response);
    }
    return response.json();
  }

  /**
   * Create an event via HPAL
   * SAFE: Only through HPAL with idempotency
   */
  async createEvent(
    familyId: string,
    request: {
      title: string;
      time_window: { start: string; end: string };
      participants?: string[];
      idempotency_key: string;
    }
  ): Promise<{ event: Event; command: CommandResult }> {
    const response = await fetch(`${this.baseURL}/families/${familyId}/events`, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify(request),
    });
    if (!response.ok) {
      throw this.handleError(response);
    }
    return response.json();
  }

  /**
   * Link event to plan
   * SAFE: Only through HPAL with idempotency
   */
  async linkEventToPlan(
    familyId: string,
    eventId: string,
    request: {
      plan_id: string;
      idempotency_key: string;
    }
  ): Promise<{ event: Event; command: CommandResult }> {
    const response = await fetch(
      `${this.baseURL}/families/${familyId}/events/${eventId}/link-plan`,
      {
        method: "POST",
        headers: this.headers(),
        body: JSON.stringify(request),
      }
    );
    if (!response.ok) {
      throw this.handleError(response);
    }
    return response.json();
  }

  /**
   * INTERNAL ONLY: Update task status (system override)
   * This endpoint requires HPAL_INTERNAL_TOKEN and must be called
   * only by trusted reconciliation logic, never from user input.
   */
  async systemUpdateTaskStatus(
    familyId: string,
    taskId: string,
    request: {
      target_status: string;
      reason_code: string;
    },
    internalToken: string
  ): Promise<Task> {
    const response = await fetch(
      `${this.baseURL}/internal/families/${familyId}/tasks/${taskId}/status`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-hpal-system-token": internalToken,
        },
        body: JSON.stringify(request),
      }
    );
    if (!response.ok) {
      throw this.handleError(response);
    }
    return response.json();
  }

  private headers(): HeadersInit {
    return {
      "Content-Type": "application/json",
    };
  }

  private handleError(response: Response): Error {
    const message = `API Error: ${response.status} ${response.statusText}`;
    const error = new Error(message);
    (error as any).status = response.status;
    return error;
  }
}

export const hpalClient = new HPALClient();
