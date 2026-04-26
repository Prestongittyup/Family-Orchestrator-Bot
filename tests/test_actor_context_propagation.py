from __future__ import annotations

import pytest

from household_os.core.execution_context import ExecutionContext
from household_os.core.lifecycle_state import LifecycleState
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.state_reducer import reduce_state


def test_actor_type_is_mapped_from_api_user_to_user() -> None:
    ctx = ExecutionContext.from_api_request(
        household_id="hh-1",
        actor_type="api_user",
        user_id="user-1",
        request_id="req-1",
    )

    actor = ctx.to_actor_context()
    assert actor.actor_type == "user"
    assert actor.actor_id == "user-1"
    assert actor.household_id == "hh-1"
    assert actor.auth_scope == "household"


def test_system_worker_context_propagates_auth_scope_system() -> None:
    ctx = ExecutionContext.system_context("hh-2")
    actor = ctx.to_actor_context()
    assert actor.actor_type == "system_worker"
    assert actor.auth_scope == "system"


def test_event_metadata_contains_actor_context_payload() -> None:
    ctx = ExecutionContext.from_api_request(
        household_id="hh-3",
        actor_type="assistant",
        user_id="assistant-1",
        request_id="req-2",
    )
    meta = ctx.to_event_metadata()
    assert meta["actor_type"] == "assistant"
    assert meta["subject_id"] == "assistant-1"
    assert isinstance(meta.get("actor_context"), dict)
    assert meta["actor_context"]["actor_type"] == "assistant"


def test_reducer_normalizes_legacy_unknown_actor_type() -> None:
    event = DomainEvent.create(
        aggregate_id="agg-legacy-1",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        payload={"state": LifecycleState.PROPOSED},
        metadata={"actor_type": "unknown", "request_id": "req-3", "subject_id": "legacy"},
    )

    result = reduce_state([event])
    assert result == LifecycleState.PROPOSED


def test_invalid_actor_type_still_rejected() -> None:
    event = DomainEvent.create(
        aggregate_id="agg-invalid-1",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        payload={"state": LifecycleState.PROPOSED},
        metadata={"actor_type": "definitely_invalid", "request_id": "req-4", "subject_id": "x"},
    )

    with pytest.raises(Exception):
        reduce_state([event])
