from __future__ import annotations

from pathlib import Path
import sys
import types

import pytest

from apps.api.core.state_machine import ActionState, StateMachine
from household_os.core.lifecycle_state import LifecycleState
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.runtime.action_pipeline import ActionPipeline
from household_os.runtime.daily_cycle import HouseholdDailyCycle
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.event_store import InMemoryEventStore
from household_os.runtime.orchestrator import (
    HouseholdOSOrchestrator,
    OrchestratorRequest,
    RequestActionType,
)
from household_os.runtime.state_reducer import StateReductionError, reduce_state, replay_events
from household_os.security.trust_boundary_enforcer import SecurityViolation


def _trusted_system_actor(subject: str = "system") -> dict[str, object]:
    return {
        "actor_type": "system_worker",
        "subject_id": subject,
        "session_id": None,
        "verified": True,
    }


def _force_import_as(module_name: str, import_stmt: str, target_module: str) -> None:
    sys.modules.pop(target_module, None)
    scratch = types.ModuleType(module_name)
    exec(import_stmt, scratch.__dict__)


# GROUP A — FORBIDDEN CALL BLOCKING

def test_direct_action_pipeline_call_must_fail() -> None:
    pipeline = ActionPipeline()
    with pytest.raises(SecurityViolation, match="ActionPipeline.execute_approved_actions"):
        pipeline.execute_approved_actions(
            graph={"household_id": "h1", "action_lifecycle": {"actions": {}}},
            now="2026-04-22T06:00:00Z",
        )


def test_event_store_append_from_external_module_must_fail() -> None:
    store = InMemoryEventStore()
    store.bind_internal_gate_token(object())
    event = DomainEvent.create(
        aggregate_id="a1",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        payload={"state": LifecycleState.PROPOSED},
        metadata={"actor_type": "system_worker", "subject_id": "system", "request_id": "r1"},
    )
    with pytest.raises(SecurityViolation, match="event_store.append"):
        store.append(event, provenance_token=object())


def test_state_store_load_outside_orchestrator_must_fail(tmp_path: Path) -> None:
    store = HouseholdStateGraphStore(graph_path=tmp_path / "graph.json")
    with pytest.raises(SecurityViolation, match="HouseholdStateGraphStore.load_graph"):
        store.load_graph("h1")


def test_fsm_transition_outside_orchestrator_must_fail() -> None:
    fsm = StateMachine(action_id="a1")
    with pytest.raises(SecurityViolation, match="FSM transition blocked"):
        fsm.transition_to(ActionState.PENDING_APPROVAL, reason="external")


# GROUP B — IMPORT BOUNDARY ENFORCEMENT

def test_router_importing_action_pipeline_directly_must_fail() -> None:
    with pytest.raises(ImportError, match="Trust boundary violation|Import boundary violation"):
        _force_import_as(
            "apps.api.assistant_runtime_router",
            "from household_os.runtime.action_pipeline import ActionPipeline\n",
            "household_os.runtime.action_pipeline",
        )


def test_adapter_importing_state_store_directly_must_fail() -> None:
    with pytest.raises(ImportError, match="Trust boundary violation|Import boundary violation"):
        _force_import_as(
            "apps.api.hpal.router",
            "from household_os.core.household_state_graph import HouseholdStateGraphStore\n",
            "household_os.core.household_state_graph",
        )


def test_external_module_importing_fsm_must_fail() -> None:
    with pytest.raises(ImportError, match="Trust boundary violation|Import boundary violation"):
        _force_import_as(
            "external.untrusted",
            "from apps.api.core.state_machine import StateMachine\n",
            "apps.api.core.state_machine",
        )


# GROUP C — REPLAY SECURITY

def test_unsigned_event_replay_must_fail() -> None:
    event = DomainEvent.create(
        aggregate_id="a1",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        payload={"state": LifecycleState.PROPOSED},
        metadata={"actor_type": "api_user", "subject_id": "u1", "request_id": "r1"},
    )
    object.__setattr__(event, "signature", "")
    with pytest.raises(StateReductionError, match="Unsigned event"):
        reduce_state([event])


def test_forged_actor_event_replay_must_fail() -> None:
    proposed = DomainEvent.create(
        aggregate_id="a2",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        payload={"state": LifecycleState.PROPOSED},
        metadata={"actor_type": "api_user", "subject_id": "u1", "request_id": "r1"},
    )
    forged = DomainEvent.create(
        aggregate_id="a2",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
        payload={"state": LifecycleState.APPROVED},
        metadata={"actor_type": "root", "subject_id": "u1", "request_id": "r1"},
    )
    with pytest.raises(StateReductionError, match="Unknown actor_type"):
        reduce_state([proposed, forged])


def test_replay_from_unauthorized_module_context_must_fail() -> None:
    proposed = DomainEvent.create(
        aggregate_id="a3",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        payload={"state": LifecycleState.PROPOSED},
        metadata={"actor_type": "api_user", "subject_id": "u1", "request_id": "r1"},
    )
    with pytest.raises(SecurityViolation, match="Replay access denied"):
        replay_events([proposed])


# GROUP D — LEGITIMATE PATH VALIDATION

def test_orchestrator_handle_request_approve_must_pass(tmp_path: Path) -> None:
    store = HouseholdStateGraphStore(graph_path=tmp_path / "approve.json")
    orchestrator = HouseholdOSOrchestrator(state_store=store)

    run_result = orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.RUN,
            household_id="h1",
            actor=_trusted_system_actor(),
            user_input="schedule dentist appointment tomorrow at 10am",
            context={"system_worker_verified": True},
        )
    )
    assert run_result.response is not None
    assert run_result.action_record is not None

    approval = orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.APPROVE,
            household_id="h1",
            actor=_trusted_system_actor(),
            request_id=run_result.response.request_id,
            action_ids=[run_result.action_record.action_id],
            context={"system_worker_verified": True},
        )
    )
    assert approval.request_id == run_result.response.request_id


def test_orchestrator_handle_request_reject_must_pass(tmp_path: Path) -> None:
    store = HouseholdStateGraphStore(graph_path=tmp_path / "reject.json")
    orchestrator = HouseholdOSOrchestrator(state_store=store)

    run_result = orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.RUN,
            household_id="h1",
            actor=_trusted_system_actor(),
            user_input="schedule doctor appointment tomorrow at 2pm",
            context={"system_worker_verified": True},
        )
    )
    assert run_result.response is not None
    assert run_result.action_record is not None

    rejected = orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.REJECT,
            household_id="h1",
            actor=_trusted_system_actor(),
            request_id=run_result.response.request_id,
            action_ids=[run_result.action_record.action_id],
            context={"system_worker_verified": True},
        )
    )
    assert len(rejected) == 1


def test_scheduler_authorized_execution_requires_valid_token(tmp_path: Path) -> None:
    store = HouseholdStateGraphStore(graph_path=tmp_path / "scheduler.json")
    orchestrator = HouseholdOSOrchestrator(state_store=store)

    with pytest.raises(PermissionError, match="cryptographic proof missing"):
        orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.QUEUE_FOLLOW_UPS,
                household_id="h1",
                actor=_trusted_system_actor(),
                context={"system_worker_verified": False},
            )
        )

    cycle = HouseholdDailyCycle(orchestrator=orchestrator)
    result = cycle.run_evening(household_id="h1")
    assert result.cycle == "evening"
