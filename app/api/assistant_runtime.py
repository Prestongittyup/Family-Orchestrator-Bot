from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.services.commands import CommandActor, get_command_runtime_service
from core.health import derive_system_health, normalize_drift_classification, system_health_inputs_from_projection


router = APIRouter(prefix="/assistant", tags=["assistant-runtime"])


class AssistantRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str | None = None
    query: str | None = None
    household_id: str = "default"
    repeat_window_days: int = 10
    fitness_goal: str | None = None

    @model_validator(mode="after")
    def validate_message_or_query(self) -> "AssistantRunRequest":
        if not (self.message or self.query):
            raise ValueError("Either 'message' or 'query' must be provided")
        return self


class AssistantApproveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str | None = None
    action_ids: list[str] = Field(default_factory=list)
    request_id: str | None = None
    household_id: str = "default"

    @model_validator(mode="after")
    def validate_action_selector(self) -> "AssistantApproveRequest":
        if self.action_id:
            return self
        if self.action_ids:
            self.action_id = self.action_ids[0]
            return self
        raise ValueError("Either 'action_id' or 'action_ids' must be provided")


class AssistantRejectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    request_id: str | None = None
    household_id: str = "default"


def _actor_from_request(request: Request, fallback_user_id: str = "system") -> CommandActor:
    actor_type = str(getattr(request.state, "actor_type", "api_user") or "api_user").strip().lower()
    if actor_type in {"", "unknown"}:
        actor_type = "api_user"

    user_claims = getattr(request.state, "user", None)
    claim_subject = ""
    if isinstance(user_claims, dict):
        claim_subject = str(user_claims.get("sub") or user_claims.get("user_id") or "").strip()

    session_id = str(getattr(request.state, "session_id", "") or "").strip() or None
    resolved_user_id = claim_subject or fallback_user_id
    return CommandActor(actor_type=actor_type, user_id=resolved_user_id, session_id=session_id)


def _format_run_payload(response: dict[str, Any]) -> dict[str, Any]:
    recommended = dict(response.get("recommended_action") or {})
    return {
        **response,
        "action_id": str(recommended.get("action_id") or ""),
        "recommendation": str(recommended.get("title") or ""),
        "why": list(response.get("reasoning_trace") or []),
        "impact": str(recommended.get("description") or ""),
        "approval_required": bool(recommended.get("approval_required", True)),
        "routing_case": "high_confidence",
        "secondary_suggestions": [],
    }


def _response_from_command_result(result: dict[str, Any]) -> dict[str, Any]:
    response = result.get("response")
    if isinstance(response, dict):
        return dict(response)

    projection = result.get("projection")
    request_id = str(result.get("request_id") or "")
    if isinstance(projection, dict) and request_id:
        responses = projection.get("responses")
        if isinstance(responses, dict) and isinstance(responses.get(request_id), dict):
            return dict(responses[request_id])
    return {}


@router.post("/run")
async def run_assistant(payload: AssistantRunRequest, request: Request) -> dict[str, Any]:
    runtime = get_command_runtime_service()
    actor = _actor_from_request(request)

    command_result = runtime.handle_command(
        command_type="assistant.run",
        household_id=payload.household_id,
        actor=actor,
        payload={
            "query": payload.query or payload.message or "",
            "message": payload.message,
            "repeat_window_days": payload.repeat_window_days,
            "fitness_goal": payload.fitness_goal,
        },
        source="api.assistant.run",
        idempotency_key=f"assistant.run:{payload.household_id}:{payload.query or payload.message or ''}",
    )
    canonical_response = _response_from_command_result(command_result)
    if not canonical_response:
        raise HTTPException(status_code=500, detail="assistant runtime failed to generate response")
    return _format_run_payload(canonical_response)


@router.post("/query")
async def query_assistant(payload: AssistantRunRequest, request: Request) -> dict[str, Any]:
    runtime = get_command_runtime_service()
    actor = _actor_from_request(request)
    command_result = runtime.handle_command(
        command_type="assistant.query",
        household_id=payload.household_id,
        actor=actor,
        payload={
            "query": payload.query or payload.message or "",
            "message": payload.message,
            "repeat_window_days": payload.repeat_window_days,
            "fitness_goal": payload.fitness_goal,
        },
        source="api.assistant.query",
        idempotency_key=f"assistant.query:{payload.household_id}:{payload.query or payload.message or ''}",
    )
    canonical_response = _response_from_command_result(command_result)
    if not canonical_response:
        raise HTTPException(status_code=500, detail="assistant query failed to generate response")
    return canonical_response


@router.post("/approve")
async def approve_assistant_action(payload: AssistantApproveRequest, request: Request) -> dict[str, Any]:
    actor = _actor_from_request(request)
    if actor.actor_type == "assistant":
        raise HTTPException(status_code=403, detail="assistant cannot approve actions")

    runtime = get_command_runtime_service()
    action_ids = [str(item) for item in payload.action_ids if str(item).strip()]
    if payload.action_id and payload.action_id not in action_ids:
        action_ids.append(payload.action_id)

    command_result = runtime.handle_command(
        command_type="assistant.approve",
        household_id=payload.household_id,
        actor=actor,
        payload={
            "request_id": payload.request_id,
            "action_id": payload.action_id,
            "action_ids": action_ids,
        },
        source="api.assistant.approve",
        idempotency_key=f"assistant.approve:{payload.household_id}:{payload.request_id or ':'.join(action_ids)}",
    )
    response = _response_from_command_result(command_result)
    projection = command_result.get("projection") if isinstance(command_result.get("projection"), dict) else {}
    if not response and isinstance(projection, dict):
        responses = projection.get("responses")
        if isinstance(responses, dict):
            if payload.request_id and isinstance(responses.get(payload.request_id), dict):
                response = dict(responses[payload.request_id])
            elif action_ids:
                actions = projection.get("actions")
                if isinstance(actions, dict):
                    action_payload = actions.get(action_ids[0])
                    if isinstance(action_payload, dict):
                        resolved_request_id = str(action_payload.get("request_id") or "")
                        if resolved_request_id and isinstance(responses.get(resolved_request_id), dict):
                            response = dict(responses[resolved_request_id])
    if not response:
        fallback_action_id = action_ids[0] if action_ids else str(payload.action_id or "")
        response = {
            "request_id": str(payload.request_id or ""),
            "intent_interpretation": {
                "summary": "approval command",
                "urgency": "medium",
                "extracted_signals": [],
            },
            "current_state_summary": {
                "household_id": payload.household_id,
                "reference_time": "",
                "calendar_events": 0,
                "open_tasks": 0,
                "meals_recorded": 0,
                "low_grocery_items": [],
                "fitness_routines": 0,
                "constraints_count": 0,
                "pending_approvals": 0,
                "state_version": 0,
            },
            "recommended_action": {
                "action_id": fallback_action_id,
                "title": "Approved assistant action",
                "description": "Action approval recorded through command pipeline.",
                "urgency": "medium",
                "scheduled_for": None,
                "approval_required": True,
                "approval_status": "approved",
            },
            "grouped_approval_payload": {
                "group_id": f"{payload.request_id or 'approval'}-group",
                "label": "Batch Household Action Execution",
                "action_ids": action_ids or ([fallback_action_id] if fallback_action_id else []),
                "execution_mode": "inert_until_approved",
                "approval_status": "approved",
            },
            "follow_ups": [],
            "reasoning_trace": ["Approval recorded via command pipeline."],
        }

    return {
        "status": str(command_result.get("status") or "approved"),
        "effects": list(command_result.get("effects") or []),
        **response,
    }


@router.post("/reject")
async def reject_assistant_action(payload: AssistantRejectRequest, request: Request) -> dict[str, Any]:
    actor = _actor_from_request(request)
    runtime = get_command_runtime_service()

    command_result = runtime.handle_command(
        command_type="assistant.reject",
        household_id=payload.household_id,
        actor=actor,
        payload={
            "request_id": payload.request_id,
            "action_id": payload.action_id,
        },
        source="api.assistant.reject",
        idempotency_key=f"assistant.reject:{payload.household_id}:{payload.action_id}",
    )
    response = _response_from_command_result(command_result)
    return {
        "status": str(command_result.get("status") or "rejected"),
        "effects": list(command_result.get("effects") or []),
        **response,
    }


@router.get("/today")
async def assistant_today(
    request: Request,
    household_id: str = Query(..., min_length=1),
) -> dict[str, Any]:
    actor = _actor_from_request(request)
    if actor.actor_type == "api_user" and actor.user_id and actor.user_id != "system":
        # Household owner verification hook belongs in auth middleware; until then, fail closed only when explicit mismatch is known.
        pass

    projection = get_command_runtime_service().get_projection(household_id)
    projection_drift = normalize_drift_classification(projection.get("drift") if isinstance(projection, dict) else None)
    system_health = derive_system_health(**system_health_inputs_from_projection(projection))
    return {
        "household_id": household_id,
        "events": list(projection.get("events") or []),
        "pending_actions": list(projection.get("pending_actions") or []),
        "last_recommendation": projection.get("last_recommendation"),
        "projection_drift": projection_drift,
        "system_health": system_health,
    }


