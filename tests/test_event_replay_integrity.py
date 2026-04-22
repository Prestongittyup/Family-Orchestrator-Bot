from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from household_os.core.lifecycle_state import LifecycleState
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.state_reducer import replay_events


def _valid_event_stream() -> list[DomainEvent]:
    return [
        DomainEvent.create(
            aggregate_id="replay-integrity-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            payload={"state": LifecycleState.PROPOSED},
        ),
        DomainEvent.create(
            aggregate_id="replay-integrity-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            payload={"state": LifecycleState.APPROVED},
        ),
        DomainEvent.create(
            aggregate_id="replay-integrity-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],
            payload={"state": LifecycleState.COMMITTED},
        ),
    ]


def test_replay_returns_enum() -> None:
    state = replay_events(_valid_event_stream())
    assert isinstance(state, LifecycleState)
    assert state == LifecycleState.COMMITTED


def test_replay_rejects_legacy_event() -> None:
    legacy_events = [
        DomainEvent(
            event_id=str(uuid4()),
            aggregate_id="replay-integrity-legacy-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            timestamp=datetime.now(UTC),
            payload={"state": LifecycleState.PROPOSED.value},
            metadata={},
        ),
        DomainEvent(
            event_id=str(uuid4()),
            aggregate_id="replay-integrity-legacy-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],
            timestamp=datetime.now(UTC),
            payload={"state": "executed"},
            metadata={},
        ),
    ]

    with pytest.raises(ValueError):
        replay_events(legacy_events)
