import type { ActionCard } from "../api/contracts";
import type {
  ActionExecutionBinder,
  ActionExecutionRequest,
  ActionExecutionResult,
} from "./types";

export class DeterministicActionExecutionBinder implements ActionExecutionBinder {
  private readonly executionLog = new Map<string, ActionExecutionResult>();

  buildRequest(input: {
    familyId: string;
    sessionId: string;
    actionCard: ActionCard;
    endpoint: string;
    payload?: Record<string, unknown>;
  }): ActionExecutionRequest {
    const payload = input.payload ?? input.actionCard.required_action_payload;
    const canonical = JSON.stringify({
      family_id: input.familyId,
      session_id: input.sessionId,
      action_card_id: input.actionCard.id,
      endpoint: input.endpoint,
      payload,
    });

    return {
      family_id: input.familyId,
      session_id: input.sessionId,
      action_card: input.actionCard,
      endpoint: input.endpoint,
      payload,
      idempotency_key: `ui-action-${simpleHash(canonical)}`,
      retry_count: 0,
    };
  }

  async execute(input: {
    request: ActionExecutionRequest;
    send: (request: ActionExecutionRequest) => Promise<ActionExecutionResult>;
  }): Promise<ActionExecutionResult> {
    const existing = this.executionLog.get(input.request.idempotency_key);
    if (existing) {
      return existing;
    }

    const result = await input.send(input.request);
    this.executionLog.set(input.request.idempotency_key, result);
    return result;
  }
}

function simpleHash(value: string): string {
  let hash = 0;
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash).toString(16);
}
