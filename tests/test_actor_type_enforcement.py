from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from apps.api.core.state_machine import ActionState, StateMachine, TransitionError
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.runtime.orchestrator import HouseholdOSOrchestrator


def _build_test_app(
    *,
    orchestrator: HouseholdOSOrchestrator,
    actor_type: str,
    user_id: str | None,
) -> FastAPI:
    import apps.api.assistant_runtime_router as assistant_runtime_router

    assistant_runtime_router.runtime_orchestrator = orchestrator
    app = FastAPI()

    @app.middleware("http")
    async def inject_auth_context(request: Request, call_next):
        request.state.actor_type = actor_type
        request.state.user = {"sub": user_id} if user_id is not None else None
        return await call_next(request)

    app.include_router(assistant_runtime_router.router, prefix="/assistant")
    return app


def _build_client(
    tmp_path: Path,
    *,
    actor_type: str,
    user_id: str | None,
    verify_owner,
) -> tuple[TestClient, HouseholdOSOrchestrator]:
    store = HouseholdStateGraphStore(graph_path=Path(tmp_path) / f"actor-enforcement-{actor_type}.json")
    store.verify_household_owner = verify_owner
    orchestrator = HouseholdOSOrchestrator(state_store=store)
    app = _build_test_app(orchestrator=orchestrator, actor_type=actor_type, user_id=user_id)
    return TestClient(app), orchestrator


def _create_action_via_run(client: TestClient, household_id: str) -> str:
    nonce = uuid4().hex
    run_response = client.post(
        "/assistant/run",
        json={
            "message": f"I need to start working out {nonce}",
            "household_id": household_id,
        },
    )
    assert run_response.status_code == 200, run_response.text
    action_id = run_response.json().get("action_id")
    assert action_id, "Expected run endpoint to return action_id"
    return str(action_id)


def _normalize_state(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw).split(".")[-1].lower()


def test_api_user_can_approve_action(tmp_path: Path) -> None:
    client, orchestrator = _build_client(
        tmp_path,
        actor_type="api_user",
        user_id="user-123",
        verify_owner=lambda household_id, user_id: household_id == "household-a" and user_id == "user-123",
    )
    action_id = _create_action_via_run(client, "household-a")

    approve_response = client.post(
        "/assistant/approve",
        json={
            "action_id": action_id,
            "household_id": "household-a",
        },
    )

    assert approve_response.status_code == 200, approve_response.text

    graph = orchestrator.state_store.load_graph("household-a")
    action = graph["action_lifecycle"]["actions"][action_id]
    to_states = [_normalize_state(item.get("to_state")) for item in action.get("transitions", [])]
    assert "approved" in to_states, f"Expected approved transition in lifecycle transitions, got: {to_states}"


def test_assistant_cannot_approve(tmp_path: Path) -> None:
    # First create an action as a system worker in the same store.
    store = HouseholdStateGraphStore(graph_path=Path(tmp_path) / "assistant-block.json")
    store.verify_household_owner = lambda household_id, user_id: True
    orchestrator = HouseholdOSOrchestrator(state_store=store)

    worker_client = TestClient(
        _build_test_app(
            orchestrator=orchestrator,
            actor_type="system_worker",
            user_id=None,
        )
    )
    action_id = _create_action_via_run(worker_client, "household-a")

    assistant_client = TestClient(
        _build_test_app(
            orchestrator=orchestrator,
            actor_type="assistant",
            user_id="assistant-bot",
        )
    )
    approve_response = assistant_client.post(
        "/assistant/approve",
        json={
            "action_id": action_id,
            "household_id": "household-a",
        },
    )

    assert approve_response.status_code == 403, approve_response.text
    detail = str(approve_response.json().get("detail", "")).lower()
    assert "cannot approve" in detail


def test_fsm_blocks_assistant_transition_directly() -> None:
    fsm = StateMachine(action_id="fsm-assistant-block", state=ActionState.PENDING_APPROVAL)

    with pytest.raises(TransitionError):
        fsm.transition_to(
            ActionState.APPROVED,
            context={"actor_type": "assistant"},
        )


def test_household_ownership_enforced(tmp_path: Path) -> None:
    store = HouseholdStateGraphStore(graph_path=Path(tmp_path) / "ownership-enforcement.json")
    store.verify_household_owner = lambda household_id, user_id: household_id == "household-a" and user_id == "user-123"
    orchestrator = HouseholdOSOrchestrator(state_store=store)

    worker_client = TestClient(
        _build_test_app(
            orchestrator=orchestrator,
            actor_type="system_worker",
            user_id=None,
        )
    )
    action_id = _create_action_via_run(worker_client, "household-b")

    api_client = TestClient(
        _build_test_app(
            orchestrator=orchestrator,
            actor_type="api_user",
            user_id="user-123",
        )
    )
    approve_response = api_client.post(
        "/assistant/approve",
        json={
            "action_id": action_id,
            "household_id": "household-b",
        },
    )

    assert approve_response.status_code == 403, approve_response.text


def test_system_worker_allowed_fsm_transition() -> None:
    fsm = StateMachine(action_id="fsm-system-worker-allow", state=ActionState.PENDING_APPROVAL)

    event = fsm.transition_to(
        ActionState.APPROVED,
        context={"actor_type": "system_worker"},
    )

    assert event.to_state == ActionState.APPROVED
    assert fsm.state == ActionState.APPROVED


def test_actor_type_propagates_api_to_approve_actions(tmp_path: Path) -> None:
    client, orchestrator = _build_client(
        tmp_path,
        actor_type="api_user",
        user_id="user-123",
        verify_owner=lambda household_id, user_id: household_id == "household-a" and user_id == "user-123",
    )
    action_id = _create_action_via_run(client, "household-a")

    seen: dict[str, Any] = {}
    original_approve_actions = orchestrator.action_pipeline.approve_actions

    def approve_actions_spy(*, graph, request_id, action_ids, now, actor_type=None):
        seen["actor_type"] = actor_type
        return original_approve_actions(
            graph=graph,
            request_id=request_id,
            action_ids=action_ids,
            now=now,
            actor_type=actor_type,
        )

    orchestrator.action_pipeline.approve_actions = approve_actions_spy

    approve_response = client.post(
        "/assistant/approve",
        json={
            "action_id": action_id,
            "household_id": "household-a",
        },
    )

    assert approve_response.status_code == 200, approve_response.text
    assert seen.get("actor_type") == "api_user", "Expected actor_type to be propagated from API request to action pipeline"
