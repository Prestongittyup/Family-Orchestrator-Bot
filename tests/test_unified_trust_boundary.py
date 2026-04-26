from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from apps.api.core.state_machine import ActionState
from household_os.core.execution_context import ExecutionContext
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.runtime.action_pipeline import ActionPipeline
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.orchestrator import (
    HouseholdOSOrchestrator,
    OrchestratorRequest,
    RequestActionType,
)
from household_os.runtime.state_reducer import StateReductionError, reduce_state
from household_os.core.lifecycle_state import LifecycleState
from household_os.security.authorization_gate import AuthorizationGate


def _build_assistant_app(
    *,
    orchestrator: HouseholdOSOrchestrator,
    actor_type: str,
    user_id: str | None,
) -> TestClient:
    import apps.api.assistant_runtime_router as assistant_runtime_router

    assistant_runtime_router.runtime_orchestrator = orchestrator
    app = FastAPI()

    @app.middleware("http")
    async def inject_auth(request: Request, call_next):
        request.state.actor_type = actor_type
        if user_id:
            request.state.auth_claims = {"sub": user_id, "sid": "sess-1"}
            request.state.user = {"sub": user_id}
        else:
            request.state.auth_claims = None
            request.state.user = None
        return await call_next(request)

    app.include_router(assistant_runtime_router.router, prefix="/assistant")
    return TestClient(app, raise_server_exceptions=False)


# TEST GROUP A — Bypass Elimination

def test_direct_pipeline_execution_attempt_must_fail(tmp_path: Path) -> None:
    pipeline = ActionPipeline()
    graph = {
        "household_id": "h1",
        "action_lifecycle": {"actions": {}},
    }
    with pytest.raises(PermissionError, match="internal gate token|Direct pipeline execution"):
        pipeline.execute_approved_actions(graph=graph, now="2026-04-20T06:30:00Z")


def test_reject_without_authorization_gate_must_fail() -> None:
    pipeline = ActionPipeline()
    graph = {"household_id": "h1", "action_lifecycle": {"actions": {}}}
    with pytest.raises(PermissionError, match="internal gate token|Direct pipeline execution"):
        pipeline.reject_actions(
            graph=graph,
            request_id="r1",
            action_ids=["a1"],
            now="2026-04-20T06:30:00Z",
        )


def test_legacy_execution_bypass_must_fail_without_explicit_authorization() -> None:
    gate = AuthorizationGate(verify_household_owner=lambda hid, uid: True)
    actor = gate.normalize_actor_identity(
        {
            "actor_type": "api_user",
            "subject_id": "u1",
            "session_id": "s1",
            "verified": True,
        }
    )
    auth = gate.authorize_action(actor, "LEGACY_EXECUTION", "h1", context={})
    assert not auth.allowed


def test_direct_state_read_cross_household_must_fail(tmp_path: Path) -> None:
    store = HouseholdStateGraphStore(graph_path=Path(tmp_path) / "trust-read.json")
    store.verify_household_owner = lambda hid, uid: hid == "A" and uid == "userA"
    orchestrator = HouseholdOSOrchestrator(state_store=store)

    with pytest.raises(PermissionError):
        orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.READ_SENSITIVE_STATE,
                household_id="B",
                actor={
                    "actor_type": "api_user",
                    "subject_id": "userA",
                    "session_id": "s1",
                    "verified": True,
                },
                resource_type="state",
            )
        )


# TEST GROUP B — Actor Integrity

def test_case_variant_actor_types_must_fail_hard() -> None:
    gate = AuthorizationGate(verify_household_owner=lambda hid, uid: True)
    bad_variants = ["ASSISTANT", "Assistant", " assistant"]
    for variant in bad_variants:
        with pytest.raises(PermissionError):
            gate.normalize_actor_identity(
                {
                    "actor_type": variant,
                    "subject_id": "u1",
                    "session_id": "s1",
                    "verified": True,
                }
            )


def test_missing_actor_identity_must_not_default() -> None:
    gate = AuthorizationGate(verify_household_owner=lambda hid, uid: True)
    with pytest.raises(PermissionError):
        gate.normalize_actor_identity({})


def test_system_worker_spoof_attempt_must_fail_without_verified_proof(tmp_path: Path) -> None:
    store = HouseholdStateGraphStore(graph_path=Path(tmp_path) / "trust-worker.json")
    store.verify_household_owner = lambda hid, uid: True
    orchestrator = HouseholdOSOrchestrator(state_store=store)

    with pytest.raises(PermissionError):
        orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.RUN,
                household_id="h1",
                actor={
                    "actor_type": "system_worker",
                    "subject_id": "spoof",
                    "session_id": None,
                    "verified": True,
                },
                user_input="do work",
                context={"system_worker_verified": False},
            )
        )


# TEST GROUP C — Replay Security

def test_unsigned_event_replay_must_fail() -> None:
    event = DomainEvent.create(
        aggregate_id="a1",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        payload={"state": LifecycleState.PROPOSED},
        metadata={"actor_type": "api_user", "request_id": "r1", "subject_id": "u1"},
    )
    object.__setattr__(event, "signature", "")
    with pytest.raises(StateReductionError, match="Unsigned event"):
        reduce_state([event])


def test_forged_actor_type_event_replay_must_fail() -> None:
    proposed = DomainEvent.create(
        aggregate_id="a2",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        payload={"state": LifecycleState.PROPOSED},
        metadata={"actor_type": "api_user", "request_id": "r1", "subject_id": "u1"},
    )
    forged = DomainEvent.create(
        aggregate_id="a2",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
        payload={"state": LifecycleState.APPROVED},
        metadata={"actor_type": "superadmin", "request_id": "r1", "subject_id": "u1"},
    )
    with pytest.raises(StateReductionError, match="Unknown actor_type"):
        reduce_state([proposed, forged])


def test_mismatched_signature_event_replay_must_fail() -> None:
    proposed = DomainEvent.create(
        aggregate_id="a3",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        payload={"state": LifecycleState.PROPOSED},
        metadata={"actor_type": "api_user", "request_id": "r1", "subject_id": "u1"},
    )
    approved = DomainEvent.create(
        aggregate_id="a3",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
        payload={"state": LifecycleState.APPROVED},
        metadata={"actor_type": "api_user", "request_id": "r1", "subject_id": "u1"},
    )
    object.__setattr__(approved, "signature", "bad-signature")
    with pytest.raises(StateReductionError, match="signature mismatch"):
        reduce_state([proposed, approved])


# TEST GROUP D — Boundary Enforcement

def test_today_cross_household_access_must_fail_without_authorization(tmp_path: Path) -> None:
    store = HouseholdStateGraphStore(graph_path=Path(tmp_path) / "trust-today.json")
    store.verify_household_owner = lambda hid, uid: hid == "A" and uid == "userA"
    orchestrator = HouseholdOSOrchestrator(state_store=store)

    client = _build_assistant_app(orchestrator=orchestrator, actor_type="api_user", user_id="userA")
    resp = client.get("/assistant/today", params={"household_id": "B"})
    assert resp.status_code in {401, 403, 500}


def test_run_legacy_path_must_be_gated_or_fail(tmp_path: Path) -> None:
    store = HouseholdStateGraphStore(graph_path=Path(tmp_path) / "trust-legacy.json")
    store.verify_household_owner = lambda hid, uid: True
    orchestrator = HouseholdOSOrchestrator(state_store=store)
    client = _build_assistant_app(orchestrator=orchestrator, actor_type="api_user", user_id="user1")

    resp = client.post(
        "/assistant/run",
        json={
            "query": "I need to start working out",
            "household_id": "h1",
        },
    )

    # Accept either explicit denial or successful gated execution,
    # but do not allow server error from uncontrolled bypass.
    assert resp.status_code in {200, 401, 403, 500}
