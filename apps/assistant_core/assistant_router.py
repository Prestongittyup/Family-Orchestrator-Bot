from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from apps.api.endpoints.integrations_router import get_credential_store, get_http_client
from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore
from apps.api.integration_core.decision_engine import DecisionEngine
from apps.api.integration_core.models.household_state import HouseholdState
from apps.api.integration_core.orchestrator import create_orchestrator
from assistant.contracts.assistant_plan import AssistantPlan as UnifiedAssistantPlan
from assistant.daily_loop.contracts import DailyPlan
from assistant.daily_loop.daily_loop_engine import DEFAULT_DAILY_LOOP_QUERY, DailyLoopEngine
from assistant.runtime.assistant_runtime import AssistantRuntimeEngine
from apps.assistant_core.contracts import AssistantApprovalRequest, AssistantQueryRequest, AssistantResponse
from apps.assistant_core.planning_engine import _fallback_household_state
from apps.assistant_core.request_store import request_store
from household_os.core import HouseholdOSRunResponse
from household_os.runtime.orchestrator import HouseholdOSOrchestrator, OrchestratorRequest, RequestActionType
from household_state.contracts import HouseholdDecisionResponse
from household_state.household_state_manager import HouseholdStateManager


log = logging.getLogger(__name__)

router = APIRouter(prefix="/assistant", tags=["assistant"])
state_manager = HouseholdStateManager()
runtime_orchestrator = HouseholdOSOrchestrator()


def _load_household_state(
    household_id: str,
    credential_store: InMemoryOAuthCredentialStore,
    http_client: Any,
) -> HouseholdState:
    try:
        orchestrator = create_orchestrator(
            credential_store=credential_store,
            http_client=http_client,
            max_results=50,
            decision_engine=DecisionEngine(),
        )
        result = orchestrator.build_household_state(household_id)
        if isinstance(result, tuple):
            state, _decision_context = result
        else:
            state = result
        if state.calendar_events:
            return state
    except Exception as exc:  # pragma: no cover - defensive fallback path
        log.warning("assistant_state_fallback", extra={"household_id": household_id, "error": str(exc)})
    return _fallback_household_state(household_id)


@router.post("/query", response_model=HouseholdOSRunResponse)
def submit_assistant_query(
    request: AssistantQueryRequest,
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
) -> HouseholdOSRunResponse:
    household_id = request.household_id
    state = _load_household_state(household_id, credential_store, http_client)

    result = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.RUN,
            household_id=household_id,
            actor={
                "actor_type": "api_user",
                "subject_id": household_id,
                "session_id": None,
                "verified": True,
            },
            state=state,
            user_input=request.query,
            fitness_goal=request.fitness_goal,
            context={"system_worker_verified": False},
        )
    )
    if result.response is None:
        raise HTTPException(status_code=500, detail="Orchestrator did not emit a response")

    response = result.response
    log.info("household_os_query", extra={
        "request_id": response.request_id,
        "intent": response.intent_interpretation.summary,
    })

    return response


@router.post("/os/run", response_model=HouseholdOSRunResponse)
def run_assistant_os(
    request: AssistantQueryRequest,
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
) -> HouseholdOSRunResponse:
    household_id = request.household_id
    state = _load_household_state(household_id, credential_store, http_client)

    result = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.RUN,
            household_id=household_id,
            actor={
                "actor_type": "api_user",
                "subject_id": household_id,
                "session_id": None,
                "verified": True,
            },
            state=state,
            user_input=request.query,
            fitness_goal=request.fitness_goal,
            context={"system_worker_verified": False},
        )
    )
    if result.response is None:
        raise HTTPException(status_code=500, detail="Orchestrator did not emit a response")

    response = result.response
    log.info("household_os_decision", extra={
        "request_id": response.request_id,
        "intent": response.intent_interpretation.summary,
        "action_domain": response.recommended_action.title[:30],
    })

    return response


@router.get("/daily", response_model=DailyPlan)
def get_daily_loop_plan(
    query: str = DEFAULT_DAILY_LOOP_QUERY,
    household_id: str = "household-001",
    repeat_window_days: int = 10,
    fitness_goal: str | None = None,
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
) -> DailyPlan:
    state = _load_household_state(household_id, credential_store, http_client)
    result = DailyLoopEngine().generate(
        query=query,
        household_id=household_id,
        repeat_window_days=repeat_window_days,
        fitness_goal=fitness_goal,
        state=state,
        persisted=False,
    )
    log.info("assistant_daily_preview", extra={"request_id": result.plan.approval_state.request_id, "date": result.plan.date})
    return result.plan


@router.post("/daily/regenerate", response_model=DailyPlan)
def regenerate_daily_loop_plan(
    request: AssistantQueryRequest,
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
) -> DailyPlan:
    state = _load_household_state(request.household_id, credential_store, http_client)
    result = DailyLoopEngine().generate(
        query=request.query,
        household_id=request.household_id,
        repeat_window_days=request.repeat_window_days,
        fitness_goal=request.fitness_goal,
        state=state,
        persisted=True,
    )
    if result.approval_response is not None:
        request_store.save(result.approval_response)
    log.info("assistant_daily_regenerate", extra={"request_id": result.plan.approval_state.request_id, "date": result.plan.date})
    return result.plan


@router.get("/suggestions/{request_id}", response_model=HouseholdOSRunResponse)
def get_assistant_suggestions(request_id: str, household_id: str = "household-001") -> HouseholdOSRunResponse:
    graph = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.READ_SENSITIVE_STATE,
            household_id=household_id,
            actor={
                "actor_type": "api_user",
                "subject_id": household_id,
                "session_id": None,
                "verified": True,
            },
            resource_type="assistant_suggestions",
            context={"system_worker_verified": False},
        )
    )
    payload = graph.get("responses", {}).get(request_id)
    response = None if payload is None else HouseholdOSRunResponse.model_validate(payload)
    if response is None:
        raise HTTPException(status_code=404, detail="Household OS request not found")
    return response


@router.post("/os/approve", response_model=HouseholdOSRunResponse)
def approve_household_os_action(request: AssistantApprovalRequest, household_id: str = "household-001") -> HouseholdOSRunResponse:
    result = runtime_orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.APPROVE,
            household_id=household_id,
            actor={
                "actor_type": "api_user",
                "subject_id": household_id,
                "session_id": None,
                "verified": True,
            },
            request_id=request.request_id,
            action_ids=request.action_ids,
            context={"system_worker_verified": False},
        )
    )
    if result.response is None:
        raise HTTPException(status_code=404, detail="Household OS request not found")
    log.info("household_os_approval", extra={"request_id": request.request_id, "action_ids": request.action_ids})
    return result.response