from __future__ import annotations

import pytest

from household_os.core.lifecycle_state import LifecycleState
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.event_store import EventStoreError, InMemoryEventStore
from household_os.runtime.state_reducer import StateReductionError, replay_events
from household_os.security.trust_boundary_enforcer import SecurityViolation, validate_replay_call


def _signed_event(*, aggregate_id: str, event_type: str, actor_type: str = "system_worker") -> DomainEvent:
    return DomainEvent.create(
        aggregate_id=aggregate_id,
        event_type=event_type,
        payload={"state": LifecycleState.PROPOSED},
        metadata={"actor_type": actor_type, "request_id": "req-1", "subject_id": "system"},
    )


def test_forbidden_append_caller_is_blocked() -> None:
    store = InMemoryEventStore()
    event = _signed_event(aggregate_id="agg-1", event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"])

    from household_os.runtime import event_store as event_store_module

    original = event_store_module._resolve_append_caller
    try:
        event_store_module._resolve_append_caller = lambda: "external.untrusted.module"
        with pytest.raises(EventStoreError, match="Forbidden append caller"):
            store.append(event)
    finally:
        event_store_module._resolve_append_caller = original


def test_duplicate_event_id_rejected() -> None:
    store = InMemoryEventStore()
    event = _signed_event(aggregate_id="agg-2", event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"])

    store.append(event)
    with pytest.raises(EventStoreError, match="Duplicate event_id"):
        store.append(event)


def test_invalid_replay_access_denied_for_user_actor() -> None:
    with pytest.raises(SecurityViolation, match="Replay access denied for user actor"):
        validate_replay_call(caller_module="household_os.runtime.orchestrator", actor_type="user")


def test_state_reduction_fails_for_unknown_actor_type() -> None:
    proposed = DomainEvent.create(
        aggregate_id="agg-3",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        payload={"state": LifecycleState.PROPOSED},
        metadata={"actor_type": "root", "request_id": "req-1", "subject_id": "u1"},
    )
    with pytest.raises(StateReductionError, match="Unknown actor_type"):
        replay_events([proposed])
