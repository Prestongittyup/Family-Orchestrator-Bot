from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator

from assistant.daily_loop.contracts import DailyPlan
from assistant.daily_loop.daily_loop_engine import DEFAULT_DAILY_LOOP_QUERY, DailyLoopEngine
from assistant.governance.output_governor import OutputGovernor
from archive.apps.assistant_core.planning_engine import _fallback_household_state, _request_id
from household_os.core import HouseholdOSRunResponse
from household_os.core.lifecycle_state import LifecycleState, enforce_boundary_state
from household_os.presentation.lifecycle_presentation_mapper import LifecyclePresentationMapper
from household_os.presentation.humanizer import RecommendationHumanizer
from household_os.presentation.recommendation_builder import RecommendationBuilder
from household_os.runtime.orchestrator import HouseholdOSOrchestrator
from household_os.runtime.orchestrator import OrchestratorRequest, RequestActionType
from archive.apps.api.observability.eil.tracer import trace_function


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


class AssistantApproveResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str
    effects: list[dict[str, Any]] = Field(default_factory=list)


class AssistantRejectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    household_id: str = "default"


class AssistantRejectResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str


class AssistantExecuteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    household_id: str = "default"


class AssistantExecuteResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    effects: list[dict[str, Any]] = Field(default_factory=list)


class AssistantTodayResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    household_id: str
    events: list[dict[str, Any]] = Field(default_factory=list)
    pending_actions: list[dict[str, Any]] = Field(default_factory=list)
    last_recommendation: dict[str, Any] | None = None


RunAssistantResponse = AssistantRunResponse | ClarificationResponse | HouseholdOSRunResponse


@router.post("/run", response_model=RunAssistantResponse)
@trace_function(entrypoint="assistant_runtime.run_assistant", actor_type="dynamic", source="api")
def run_assistant(payload: AssistantRunRequest, request: Request) -> RunAssistantResponse:
    raw_actor = _actor_from_request(request, allow_public_fallback=True)

    if payload.query and not payload.message:
        return _run_legacy_household_os(payload, raw_actor)

    state = _fallback_household_state(payload.household_id)
    message = payload.message or payload.query or ""
    result = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.RUN,
            household_id=payload.household_id,
            actor=raw_actor,
            state=state,
            user_input=message,
            fitness_goal=payload.fitness_goal,
            context=_request_context_from_actor(raw_actor),
        )
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

    graph = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.READ_SENSITIVE_STATE,
            household_id=payload.household_id,
            actor=raw_actor,
            resource_type="recommendation_enrichment",
            context=_request_context_from_actor(raw_actor),
        )
    )
    enriched = recommendation_builder.build(response=response, graph=graph)
    graph = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.READ_SENSITIVE_STATE,
            household_id=payload.household_id,
            actor=raw_actor,
            resource_type="recommendation_humanization",
            context=_request_context_from_actor(raw_actor),
        )
    )
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


@router.post("/query", response_model=HouseholdOSRunResponse)
@trace_function(entrypoint="assistant_runtime.query", actor_type="dynamic", source="api")
def query_assistant(payload: AssistantRunRequest, request: Request) -> HouseholdOSRunResponse:
    raw_actor = _actor_from_request(request, allow_public_fallback=True)
    if payload.query:
        normalized = payload
    else:
        normalized = payload.model_copy(update={"query": payload.message, "message": None})
    return _run_legacy_household_os(normalized, raw_actor)


@router.post("/approve", response_model=AssistantApproveResponse)
@trace_function(entrypoint="assistant_runtime.approve", actor_type="dynamic", source="api")
def approve_assistant_action(payload: AssistantApproveRequest, request: Request) -> AssistantApproveResponse:
    raw_actor = _actor_from_request(request, allow_public_fallback=True)
    resolved_action_id = str(payload.action_id or "")
    resolved_household_id = _infer_household_id_for_action(
        action_id=resolved_action_id,
        request_id=payload.request_id,
        household_id=payload.household_id,
    )
    try:
        graph = runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.READ_SENSITIVE_STATE,
                household_id=resolved_household_id,
                actor=raw_actor,
                resource_type="action_lifecycle",
                context=_request_context_from_actor(raw_actor),
            )
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    action_payload = graph.get("action_lifecycle", {}).get("actions", {}).get(resolved_action_id)
    request_id = str(payload.request_id or (action_payload or {}).get("request_id", ""))
    if not request_id:
        raise HTTPException(status_code=400, detail="Action is missing request association")

    if not resolved_action_id:
        response_graph = runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.READ_SENSITIVE_STATE,
                household_id=resolved_household_id,
                actor=raw_actor,
                resource_type="responses",
                context=_request_context_from_actor(raw_actor),
            )
        )
        response_payload = dict(response_graph.get("responses", {}).get(request_id, {}))
        resolved_action_id = str(response_payload.get("recommended_action", {}).get("action_id") or "")

    if not resolved_action_id:
        raise HTTPException(status_code=404, detail="Action not found")

    try:
        approval_result = runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.APPROVE,
                household_id=resolved_household_id,
                actor=raw_actor,
                request_id=request_id,
                action_ids=[resolved_action_id],
                context=_request_context_from_actor(raw_actor),
            )
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    effects = [
        {
            "action_id": action.action_id,
            "handler": action.execution_result.get("handler") if action.execution_result else None,
            "result": action.execution_result or {},
        }
        for action in approval_result.executed_actions
    ]

    response_graph = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.READ_SENSITIVE_STATE,
            household_id=resolved_household_id,
            actor=raw_actor,
            resource_type="responses",
            context=_request_context_from_actor(raw_actor),
        )
    )
    response_payload = dict(response_graph.get("responses", {}).get(request_id, {}))

    recommended = dict(response_payload.get("recommended_action", {}))
    if not recommended:
        recommended = {
            "action_id": resolved_action_id,
            "title": "Approved assistant action",
            "description": "Action approval recorded through command pipeline.",
            "urgency": "medium",
            "scheduled_for": None,
            "approval_required": True,
        }
    recommended["approval_status"] = LifecyclePresentationMapper.to_api_state(LifecycleState.APPROVED)
    response_payload["recommended_action"] = recommended

    grouped = dict(response_payload.get("grouped_approval_payload", {}))
    if not grouped:
        grouped = {
            "group_id": f"{request_id or 'approval'}-group",
            "label": "Batch Household Action Execution",
            "action_ids": [resolved_action_id] if resolved_action_id else [],
            "execution_mode": "inert_until_approved",
        }
    grouped["approval_status"] = LifecyclePresentationMapper.to_api_state(LifecycleState.APPROVED)
    response_payload["grouped_approval_payload"] = grouped

    response_payload.setdefault("request_id", request_id)
    reasoning_trace = list(response_payload.get("reasoning_trace") or [])
    if not reasoning_trace:
        reasoning_trace = ["Approval recorded via command pipeline."]
    response_payload["reasoning_trace"] = reasoning_trace

    return AssistantApproveResponse(
        status=LifecyclePresentationMapper.to_api_state(LifecycleState.COMMITTED),
        effects=effects,
        **response_payload,
    )


@router.get("/today", response_model=AssistantTodayResponse)
@trace_function(entrypoint="assistant_runtime.today", actor_type="dynamic", source="api")
def assistant_today(request: Request, household_id: str = "default") -> AssistantTodayResponse:
    raw_actor = _actor_from_request(request, allow_public_fallback=True)
    graph = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.READ_SENSITIVE_STATE,
            household_id=household_id,
            actor=raw_actor,
            resource_type="assistant_today",
            context=_request_context_from_actor(raw_actor),
        )
    )

    actions = graph.get("action_lifecycle", {}).get("actions", {})
    pending_actions = []
    for action in actions.values():
        state = enforce_boundary_state(action.get("current_state"))
        if state in {
            LifecycleState.PROPOSED,
            LifecycleState.PENDING_APPROVAL,
            LifecycleState.APPROVED,
        }:
            pending_actions.append(
                {
                    "action_id": action.get("action_id"),
                    "title": action.get("title"),
                    "state": LifecyclePresentationMapper.to_api_state(state),
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


@router.get("/daily", response_model=DailyPlan)
def get_daily_loop_plan(
    query: str = DEFAULT_DAILY_LOOP_QUERY,
    household_id: str = "household-001",
    repeat_window_days: int = 10,
    fitness_goal: str | None = None,
) -> DailyPlan:
    state = _fallback_household_state(household_id)
    result = DailyLoopEngine().generate(
        query=query,
        household_id=household_id,
        repeat_window_days=repeat_window_days,
        fitness_goal=fitness_goal,
        state=state,
        persisted=False,
    )
    return result.plan


@router.post("/daily/regenerate", response_model=DailyPlan)
def regenerate_daily_loop_plan(payload: AssistantRunRequest) -> DailyPlan:
    query = payload.query or payload.message or DEFAULT_DAILY_LOOP_QUERY
    state = _fallback_household_state(payload.household_id)
    result = DailyLoopEngine().generate(
        query=query,
        household_id=payload.household_id,
        repeat_window_days=payload.repeat_window_days,
        fitness_goal=payload.fitness_goal,
        state=state,
        persisted=True,
    )
    return result.plan


@router.post("/reject", response_model=AssistantRejectResponse)
@trace_function(entrypoint="assistant_runtime.reject", actor_type="dynamic", source="api")
def reject_assistant_action(payload: AssistantRejectRequest, request: Request) -> AssistantRejectResponse:
    raw_actor = _actor_from_request(request, allow_public_fallback=True)
    try:
        graph = runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.READ_SENSITIVE_STATE,
                household_id=payload.household_id,
                actor=raw_actor,
                resource_type="action_lifecycle",
                context=_request_context_from_actor(raw_actor),
            )
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    action_payload = graph.get("action_lifecycle", {}).get("actions", {}).get(payload.action_id)
    if action_payload is None:
        raise HTTPException(status_code=404, detail="Action not found")

    request_id = str(action_payload.get("request_id", ""))
    if not request_id:
        raise HTTPException(status_code=400, detail="Action is missing request association")

    try:
        rejected = runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.REJECT,
                household_id=payload.household_id,
                actor=raw_actor,
                request_id=request_id,
                action_ids=[payload.action_id],
                now=graph.get("reference_time"),
                context=_request_context_from_actor(raw_actor),
            )
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    if not rejected:
        raise HTTPException(status_code=409, detail="Action could not be rejected")
    return AssistantRejectResponse(
        status=LifecyclePresentationMapper.to_api_state(LifecycleState.REJECTED)
    )


@router.post("/execute", response_model=AssistantExecuteResponse)
@trace_function(entrypoint="assistant_runtime.execute", actor_type="dynamic", source="api")
def execute_assistant_action(payload: AssistantExecuteRequest, request: Request) -> AssistantExecuteResponse:
    raw_actor = _actor_from_request(request, allow_public_fallback=True)
    graph = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.READ_SENSITIVE_STATE,
            household_id=payload.household_id,
            actor=raw_actor,
            resource_type="action_lifecycle",
            context=_request_context_from_actor(raw_actor),
        )
    )
    action_payload = graph.get("action_lifecycle", {}).get("actions", {}).get(payload.action_id)
    if action_payload is None:
        raise HTTPException(status_code=404, detail="Action not found")

    executed = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.EXECUTE,
            household_id=payload.household_id,
            actor=raw_actor,
            now=graph.get("reference_time"),
            context=_request_context_from_actor(raw_actor),
        )
    )
    effects = [
        {
            "action_id": action.action_id,
            "handler": action.execution_result.get("handler") if action.execution_result else None,
            "result": action.execution_result or {},
        }
        for action in executed
    ]
    return AssistantExecuteResponse(
        status=LifecyclePresentationMapper.to_api_state(LifecycleState.COMMITTED),
        effects=effects,
    )


def _apply_recommendation_adjustments(*, household_id: str, request_id: str, action_id: str, recommendation: Any) -> None:
    graph = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.READ_SENSITIVE_STATE,
            household_id=household_id,
            actor={
                "actor_type": "system_worker",
                "subject_id": "assistant-runtime",
                "session_id": None,
                "verified": True,
            },
            resource_type="recommendation_adjustment",
            context={"system_worker_verified": True},
        )
    )
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

    runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.WRITE_SENSITIVE_STATE,
            household_id=household_id,
            actor={
                "actor_type": "system_worker",
                "subject_id": "assistant-runtime",
                "session_id": None,
                "verified": True,
            },
            graph=graph,
            context={"system_worker_verified": True},
        )
    )


def _run_legacy_household_os(request: AssistantRunRequest, raw_actor: dict[str, Any]) -> HouseholdOSRunResponse:
    query = request.query or request.message or ""
    state = _fallback_household_state(request.household_id)
    result = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.LEGACY_EXECUTION,
            household_id=request.household_id,
            actor=raw_actor,
            state=state,
            user_input=query,
            fitness_goal=request.fitness_goal,
            context={"legacy_execution": True, **_request_context_from_actor(raw_actor)},
        )
    )
    if result.response is None:
        raise HTTPException(status_code=500, detail="Legacy execution did not produce a response")
    return result.response


def _actor_from_request(request: Request, *, allow_public_fallback: bool = False) -> dict[str, Any]:
    actor_type = getattr(request.state, "actor_type", None)
    claims = getattr(request.state, "auth_claims", None)
    user_claims = getattr(request.state, "user", None)
    if claims is None and isinstance(user_claims, dict):
        claims = user_claims

    subject_id = ""
    if isinstance(claims, dict):
        subject_id = str(claims.get("sub") or claims.get("user_id") or "")
    elif getattr(request.state, "user", None) is not None:
        subject_id = str(getattr(request.state.user, "sub", ""))

    if actor_type is None:
        if allow_public_fallback:
            return {
                "actor_type": "system_worker",
                "subject_id": "assistant-runtime",
                "session_id": None,
                "verified": True,
                "auth_scope": "system",
            }
        raise HTTPException(status_code=401, detail="actor identity missing")

    resolved_actor_type = str(actor_type).strip().lower()
    is_system_worker = resolved_actor_type in {"system_worker", "scheduler"}

    return {
        "actor_type": resolved_actor_type,
        "subject_id": subject_id,
        "session_id": str(claims.get("sid")) if isinstance(claims, dict) and claims.get("sid") else None,
        "verified": bool(claims is not None) or is_system_worker,
        "auth_scope": "system" if is_system_worker else "household",
    }


def _request_context_from_actor(raw_actor: dict[str, Any]) -> dict[str, Any]:
    actor_type = str(raw_actor.get("actor_type") or "").strip().lower()
    return {
        "system_worker_verified": actor_type in {"system_worker", "scheduler"},
        "auth_scope": str(raw_actor.get("auth_scope") or "household"),
    }


def _infer_household_id_for_action(*, action_id: str, request_id: str | None, household_id: str) -> str:
    if household_id and household_id != "default":
        return household_id

    if request_id:
        try:
            resolved = runtime_orchestrator.state_store.find_household_id_for_request(str(request_id))
        except Exception:
            resolved = None
        if resolved:
            return str(resolved)

    if action_id:
        known_households = sorted(getattr(runtime_orchestrator.state_store, "_known_households", set()))
        for candidate_household_id in known_households:
            try:
                graph = runtime_orchestrator.state_store.load_graph(str(candidate_household_id))
            except Exception:
                continue
            actions = graph.get("action_lifecycle", {}).get("actions", {})
            if action_id in actions:
                return str(candidate_household_id)

    return household_id