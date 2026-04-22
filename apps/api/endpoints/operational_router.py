from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from apps.api.endpoints.integrations_router import get_credential_store, get_http_client
from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore
from apps.api.integration_core.decision_engine import DecisionEngine
from apps.api.integration_core.orchestrator import create_orchestrator
from apps.api.observability.execution_trace import trace_function
from apps.api.operational.contracts import OperationalResponse
from apps.api.operational.service import build_operational_response

router = APIRouter(prefix="/operational", tags=["operational"])


@trace_function(entrypoint="operational.pipeline", actor_type="api_user", source="api")
def _run_pipeline(
    household_id: str,
    credential_store: InMemoryOAuthCredentialStore,
    http_client: Any,
):
    orchestrator = create_orchestrator(
        credential_store=credential_store,
        http_client=http_client,
        max_results=50,
        decision_engine=DecisionEngine(),
    )
    result = orchestrator.build_household_state(household_id)
    if isinstance(result, tuple):
        state, decision_context = result
    else:
        state = result
        decision_context = None
    return state, decision_context


@router.get("/run", response_model=OperationalResponse)
@trace_function(entrypoint="operational.run", actor_type="api_user", source="api")
def run_operational_mode(
    household_id: str = Query(default="household-001"),
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
) -> OperationalResponse:
    state, decision_context = _run_pipeline(household_id, credential_store, http_client)
    return build_operational_response(
        household_id=household_id,
        state=state,
        decision_context=decision_context,
        mode="run",
    )


@router.get("/context", response_model=OperationalResponse)
@trace_function(entrypoint="operational.context", actor_type="api_user", source="api")
def get_operational_context(
    household_id: str = Query(default="household-001"),
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
) -> OperationalResponse:
    state, decision_context = _run_pipeline(household_id, credential_store, http_client)
    return build_operational_response(
        household_id=household_id,
        state=state,
        decision_context=decision_context,
        mode="context",
    )


@router.get("/brief", response_model=OperationalResponse)
@trace_function(entrypoint="operational.brief", actor_type="api_user", source="api")
def get_operational_brief(
    household_id: str = Query(default="household-001"),
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
) -> OperationalResponse:
    state, decision_context = _run_pipeline(household_id, credential_store, http_client)
    return build_operational_response(
        household_id=household_id,
        state=state,
        decision_context=decision_context,
        mode="brief",
    )
