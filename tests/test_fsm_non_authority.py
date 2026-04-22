from __future__ import annotations

from apps.api.core.state_machine import ActionState, validate_transition
from household_os.core.lifecycle_state import LifecycleState
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.state_reducer import reduce_state


def _fsm_validate(events: list[DomainEvent]) -> LifecycleState:
    current = ActionState.PROPOSED
    event_to_state = {
        LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"]: ActionState.PROPOSED,
        LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"]: ActionState.APPROVED,
        LIFECYCLE_EVENT_TYPES["ACTION_REJECTED"]: ActionState.REJECTED,
        LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"]: ActionState.COMMITTED,
        LIFECYCLE_EVENT_TYPES["ACTION_FAILED"]: ActionState.FAILED,
    }

    if events[0].event_type != LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"]:
        raise ValueError("first event must be action_proposed")

    for event in events[1:]:
        target = event_to_state[event.event_type]
        validate_transition(current, target)
        current = target

    return LifecycleState(current.value)


def test_fsm_does_not_override_state() -> None:
    events = [
        DomainEvent.create(
            aggregate_id="fsm-authority-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            payload={"state": LifecycleState.PROPOSED},
        ),
        DomainEvent.create(
            aggregate_id="fsm-authority-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            payload={"state": LifecycleState.APPROVED},
        ),
        DomainEvent.create(
            aggregate_id="fsm-authority-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],
            payload={"state": LifecycleState.COMMITTED},
        ),
    ]

    derived = reduce_state(events)
    fsm_result = _fsm_validate(events)

    assert derived == fsm_result
