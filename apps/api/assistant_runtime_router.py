from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field, model_validator

from assistant.governance.output_governor import OutputGovernor
from apps.assistant_core.planning_engine import _fallback_household_state, _request_id
from household_os.core import HouseholdOSDecisionEngine, HouseholdOSRunResponse
from household_os.presentation.humanizer import RecommendationHumanizer
from household_os.presentation.recommendation_builder import RecommendationBuilder
from household_os.runtime.orchestrator import HouseholdOSOrchestrator


router = APIRouter()
runtime_orchestrator = HouseholdOSOrchestrator()
recommendation_builder = RecommendationBuilder()
recommendation_humanizer = RecommendationHumanizer()
output_governor = OutputGovernor()


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


class AssistantRunResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    recommendation: str
    why: list[str] = Field(default_factory=list)
    impact: str
    approval_required: bool
    routing_case: str = "high_confidence"  # high_confidence | medium_confidence | low_confidence
    secondary_suggestions: list[dict[str, Any]] = Field(default_factory=list)


class ClarificationResponse(BaseModel):
    """Returned when confidence is too low to produce an action."""
    model_config = ConfigDict(extra="forbid")

    clarification: str
    routing_case: str = "low_confidence"


class AssistantApproveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    household_id: str = "default"


class AssistantApproveResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    effects: list[dict[str, Any]] = Field(default_factory=list)


class AssistantRejectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    household_id: str = "default"


class AssistantRejectResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str


class AssistantTodayResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    household_id: str
    events: list[dict[str, Any]] = Field(default_factory=list)
    pending_actions: list[dict[str, Any]] = Field(default_factory=list)
    last_recommendation: dict[str, Any] | None = None


RunAssistantResponse = AssistantRunResponse | ClarificationResponse | HouseholdOSRunResponse


@router.post("/run", response_model=RunAssistantResponse)
def run_assistant(request: AssistantRunRequest) -> RunAssistantResponse:
    if request.query and not request.message:
        return _run_legacy_household_os(request)

    state = _fallback_household_state(request.household_id)
    message = request.message or request.query or ""
    result = runtime_orchestrator.tick(
        household_id=request.household_id,
        state=state,
        user_input=message,
        fitness_goal=request.fitness_goal,
    )

    # Case C — low confidence: no action produced, return clarification
    if result.clarification_text:
        return ClarificationResponse(
            clarification=result.clarification_text,
            routing_case="low_confidence",
        )

    response = result.response
    action = result.action_record
    if response is None or action is None:
        raise HTTPException(status_code=500, detail="Orchestrator did not emit an action")

    graph = runtime_orchestrator.state_store.load_graph(request.household_id)
    enriched = recommendation_builder.build(response=response, graph=graph)
    _apply_recommendation_adjustments(
        household_id=request.household_id,
        request_id=response.request_id,
        action_id=action.action_id,
        recommendation=enriched,
    )
    graph = runtime_orchestrator.state_store.load_graph(request.household_id)
    humanized = recommendation_humanizer.humanize(
        enriched.as_dict(),
        reference_time=graph.get("reference_time"),
    )
    governed = output_governor.govern(
        user_message=message,
        payload=humanized.as_dict(),
        decision_response=response,
    )

    return AssistantRunResponse(
        action_id=governed.action_id,
        recommendation=governed.recommendation,
        why=governed.why,
        impact=governed.impact,
        approval_required=governed.approval_required,
        routing_case=result.routing_case or "high_confidence",
        secondary_suggestions=result.secondary_suggestions,
    )


@router.post("/approve", response_model=AssistantApproveResponse)
def approve_assistant_action(request: AssistantApproveRequest) -> AssistantApproveResponse:
    graph = runtime_orchestrator.state_store.load_graph(request.household_id)
    action_payload = graph.get("action_lifecycle", {}).get("actions", {}).get(request.action_id)
    if action_payload is None:
        raise HTTPException(status_code=404, detail="Action not found")

    request_id = str(action_payload.get("request_id", ""))
    if not request_id:
        raise HTTPException(status_code=400, detail="Action is missing request association")

    approval_result = runtime_orchestrator.approve_and_execute(
        household_id=request.household_id,
        request_id=request_id,
        action_ids=[request.action_id],
    )
    effects = [
        {
            "action_id": action.action_id,
            "handler": action.execution_result.get("handler") if action.execution_result else None,
            "result": action.execution_result or {},
        }
        for action in approval_result.executed_actions
    ]
    return AssistantApproveResponse(status="executed", effects=effects)


@router.get("/today", response_model=AssistantTodayResponse)
def assistant_today(household_id: str = "default") -> AssistantTodayResponse:
    graph = runtime_orchestrator.state_store.load_graph(household_id)

    actions = graph.get("action_lifecycle", {}).get("actions", {})
    pending_actions = []
    for action in actions.values():
        state = str(action.get("current_state", ""))
        if state in {"proposed", "pending_approval", "approved"}:
            pending_actions.append(
                {
                    "action_id": action.get("action_id"),
                    "title": action.get("title"),
                    "state": state,
                    "approval_required": bool(action.get("approval_required", True)),
                }
            )
    pending_actions.sort(key=lambda item: str(item.get("action_id", "")))

    last_recommendation = None
    responses = graph.get("responses", {})
    if responses:
        latest_key = sorted(responses.keys())[-1]
        payload = responses.get(latest_key, {})
        recommendation = payload.get("recommended_action", {})
        last_recommendation = {
            "action_id": recommendation.get("action_id"),
            "recommendation": recommendation.get("title"),
            "approval_status": recommendation.get("approval_status"),
        }

    return AssistantTodayResponse(
        household_id=household_id,
        events=list(graph.get("calendar_events", [])),
        pending_actions=pending_actions,
        last_recommendation=last_recommendation,
    )


@router.post("/reject", response_model=AssistantRejectResponse)
def reject_assistant_action(request: AssistantRejectRequest) -> AssistantRejectResponse:
    graph = runtime_orchestrator.state_store.load_graph(request.household_id)
    action_payload = graph.get("action_lifecycle", {}).get("actions", {}).get(request.action_id)
    if action_payload is None:
        raise HTTPException(status_code=404, detail="Action not found")

    request_id = str(action_payload.get("request_id", ""))
    if not request_id:
        raise HTTPException(status_code=400, detail="Action is missing request association")

    rejected = runtime_orchestrator.action_pipeline.reject_actions(
        graph=graph,
        request_id=request_id,
        action_ids=[request.action_id],
        now=graph.get("reference_time"),
    )
    runtime_orchestrator.state_store.save_graph(graph)
    if not rejected:
        raise HTTPException(status_code=409, detail="Action could not be rejected")
    return AssistantRejectResponse(status="rejected")


def _apply_recommendation_adjustments(*, household_id: str, request_id: str, action_id: str, recommendation: Any) -> None:
    graph = runtime_orchestrator.state_store.load_graph(household_id)
    action_map = graph.get("action_lifecycle", {}).get("actions", {})
    action_payload = action_map.get(action_id)
    if action_payload is not None and getattr(recommendation, "scheduled_for", None):
        action_payload["scheduled_for"] = recommendation.scheduled_for
        action_map[action_id] = action_payload

    response_payload = graph.get("responses", {}).get(request_id)
    if response_payload is not None and getattr(recommendation, "scheduled_for", None):
        recommended_action = dict(response_payload.get("recommended_action", {}))
        recommended_action["scheduled_for"] = recommendation.scheduled_for
        response_payload["recommended_action"] = recommended_action
        graph["responses"][request_id] = response_payload

    runtime_orchestrator.state_store.save_graph(graph)


def _run_legacy_household_os(request: AssistantRunRequest) -> HouseholdOSRunResponse:
    query = request.query or request.message or ""
    state = _fallback_household_state(request.household_id)
    graph = runtime_orchestrator.state_store.refresh_graph(
        household_id=request.household_id,
        state=state,
        query=query,
        fitness_goal=request.fitness_goal,
    )
    request_id = _request_id(query, request.household_id, request.repeat_window_days, request.fitness_goal)
    response = HouseholdOSDecisionEngine().run(
        household_id=request.household_id,
        query=query,
        graph=graph,
        request_id=request_id,
    )
    runtime_orchestrator.state_store.store_response(request.household_id, response.model_dump())
    return response