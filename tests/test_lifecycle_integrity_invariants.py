from __future__ import annotations

from pathlib import Path

from apps.api.core.state_machine import ActionState, validate_transition
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.core.lifecycle_state import LifecycleState
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.state_reducer import reduce_state


def _fsm_validate(events: list[DomainEvent]) -> LifecycleState:
    if not events:
        raise ValueError("events required")

    current = ActionState.PROPOSED
    event_to_state = {
        LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"]: ActionState.PROPOSED,
        LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"]: ActionState.APPROVED,
        LIFECYCLE_EVENT_TYPES["ACTION_REJECTED"]: ActionState.REJECTED,
        LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"]: ActionState.COMMITTED,
        LIFECYCLE_EVENT_TYPES["ACTION_FAILED"]: ActionState.FAILED,
    }

    first = events[0]
    if first.event_type != LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"]:
        raise ValueError("first event must be action_proposed")

    for event in events[1:]:
        target = event_to_state[event.event_type]
        validate_transition(current, target)
        current = target

    return LifecycleState(current.value)


def test_db_roundtrip_state(tmp_path):
    store = HouseholdStateGraphStore(graph_path=Path(tmp_path) / "state_graph.json")
    graph = store.load_graph("db-roundtrip-household")
    action_id = "action-roundtrip-001"
    graph.setdefault("action_lifecycle", {}).setdefault("actions", {})[action_id] = {
        "action_id": action_id,
        "request_id": "req-roundtrip-001",
        "title": "Roundtrip",
        "current_state": LifecycleState.COMMITTED,
    }

    saved = store.save_graph(graph)
    store._cache.pop(saved["household_id"], None)
    loaded = store.load_graph(saved["household_id"])

    assert loaded["action_lifecycle"]["actions"][action_id]["current_state"] == LifecycleState.COMMITTED.value
    assert loaded["_lifecycle_hydration"]["action_lifecycle"]["actions"][action_id]["current_state"] == LifecycleState.COMMITTED.value


def test_fsm_never_overrides_reducer():
    events = [
        DomainEvent.create(
            aggregate_id="fsm-invariant-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            payload={"state": LifecycleState.PROPOSED},
        ),
        DomainEvent.create(
            aggregate_id="fsm-invariant-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            payload={"state": LifecycleState.APPROVED},
        ),
        DomainEvent.create(
            aggregate_id="fsm-invariant-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],
            payload={"state": LifecycleState.COMMITTED},
        ),
    ]

    derived = reduce_state(events)
    validated = _fsm_validate(events)

    assert derived == validated
