"""
State Reducer - Pure Function for Deriving Lifecycle State from Events

Core principle: State is computed by replaying events in order.
This enables:
- Deterministic state reconstruction
- Event sourcing correctness
- Audit trail of all state changes
- Easy temporal queries (state at any point in time)
"""

from __future__ import annotations

from household_os.core.lifecycle_state import (
    LifecycleState,
    assert_lifecycle_state,
    parse_lifecycle_state,
)
from apps.api.observability.execution_trace import trace_function
from household_os.runtime.domain_event import (
    DomainEvent,
    LifecycleEventState,
    LifecycleSnapshot,
    LIFECYCLE_EVENT_TYPES,
)
from household_os.runtime.lifecycle_firewall import enforce_lifecycle_integrity


class StateReductionError(Exception):
    """Raised when state reduction encounters an invalid event sequence."""

    pass


def reduce_state(events: list[DomainEvent]) -> LifecycleState:
    """
    Derive current lifecycle state from a sequence of events.

    This is the SINGLE SOURCE OF TRUTH for lifecycle state derivation.
    This is a PURE FUNCTION:
    - No side effects
    - Deterministic: same input events always produce same state
    - Idempotent: replaying the same events produces the same result

    State transitions are determined solely by event types:
    - ACTION_PROPOSED       → LifecycleState.PROPOSED
    - ACTION_APPROVED       → LifecycleState.APPROVED
    - ACTION_REJECTED       → LifecycleState.REJECTED
    - ACTION_FAILED         → LifecycleState.FAILED
    - ACTION_COMMITTED      → LifecycleState.COMMITTED

    The reducer transitions state based on the current state and the event type:

    From PROPOSED:
    - ACTION_APPROVED    → APPROVED (if approval not required)
    - ACTION_FAILED      → FAILED
    - ACTION_REJECTED    → REJECTED

    From PENDING_APPROVAL:
    - ACTION_APPROVED    → APPROVED
    - ACTION_FAILED      → FAILED
    - ACTION_REJECTED    → REJECTED

    From APPROVED:
    - ACTION_COMMITTED   → COMMITTED
    - ACTION_FAILED      → FAILED

    Terminal states:
    - COMMITTED, REJECTED, FAILED have no valid outgoing transitions

    Args:
        events: List of domain events in order (earliest first)

    Returns:
        Current derived state as LifecycleState enum

    Raises:
        StateReductionError: If event sequence is invalid
    """
    if not events:
        raise StateReductionError("Cannot reduce state from empty event list")

    current_state: LifecycleState | None = None

    for event in events:
        _validate_event_payload_state(event)
        current_state = _apply_event(current_state, event)

    if current_state is None:
        raise StateReductionError("State reduction produced None state")

    # Validate and enforce output is properly typed.
    return enforce_lifecycle_integrity(assert_lifecycle_state(current_state))


@trace_function(entrypoint="state_reducer.replay_events", actor_type="system_worker", source="event_replay")
def replay_events(events: list[DomainEvent]) -> LifecycleState:
    """Replay lifecycle events and return canonical enum state."""
    return reduce_state(events)


def _validate_event_payload_state(event: DomainEvent) -> None:
    """Fail fast on invalid lifecycle state payloads in historical streams."""
    raw_state = event.payload.get("state") if isinstance(event.payload, dict) else None
    if raw_state is None:
        return

    parsed_state = parse_lifecycle_state(raw_state)
    expected_by_type = {
        LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"]: LifecycleState.PROPOSED,
        LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"]: LifecycleState.APPROVED,
        LIFECYCLE_EVENT_TYPES["ACTION_REJECTED"]: LifecycleState.REJECTED,
        LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"]: LifecycleState.COMMITTED,
        LIFECYCLE_EVENT_TYPES["ACTION_FAILED"]: LifecycleState.FAILED,
    }
    expected_state = expected_by_type.get(event.event_type)
    if expected_state is not None and parsed_state != expected_state:
        raise StateReductionError(
            f"Event payload state mismatch for {event.event_type}: {parsed_state} != {expected_state}"
        )


def _apply_event(
    current_state: LifecycleState | None, event: DomainEvent
) -> LifecycleState:
    """
    Apply a single event to the current state.

    Args:
        current_state: State before event (None if this is first event)
        event: Event to apply

    Returns:
        New state after event (as LifecycleState enum)

    Raises:
        StateReductionError: If transition is invalid
    """
    event_type = event.event_type

    # Initial event must be ACTION_PROPOSED
    if current_state is None:
        if event_type == LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"]:
            return LifecycleState.PROPOSED
        else:
            raise StateReductionError(
                f"First event must be ACTION_PROPOSED, got {event_type}"
            )

    # From PROPOSED state
    if current_state == LifecycleState.PROPOSED:
        if event_type == LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"]:
            return LifecycleState.APPROVED
        elif event_type == LIFECYCLE_EVENT_TYPES["ACTION_FAILED"]:
            return LifecycleState.FAILED
        elif event_type == LIFECYCLE_EVENT_TYPES["ACTION_REJECTED"]:
            return LifecycleState.REJECTED
        else:
            raise StateReductionError(
                f"Invalid transition from {LifecycleState.PROPOSED.value} on event {event_type}"
            )

    # From PENDING_APPROVAL state (intermediate if approval required)
    if current_state == LifecycleState.PENDING_APPROVAL:
        if event_type == LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"]:
            return LifecycleState.APPROVED
        elif event_type == LIFECYCLE_EVENT_TYPES["ACTION_FAILED"]:
            return LifecycleState.FAILED
        elif event_type == LIFECYCLE_EVENT_TYPES["ACTION_REJECTED"]:
            return LifecycleState.REJECTED
        else:
            raise StateReductionError(
                f"Invalid transition from {LifecycleState.PENDING_APPROVAL.value} on event {event_type}"
            )

    # From APPROVED state
    if current_state == LifecycleState.APPROVED:
        if event_type == LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"]:
            return LifecycleState.COMMITTED
        elif event_type == LIFECYCLE_EVENT_TYPES["ACTION_FAILED"]:
            return LifecycleState.FAILED
        else:
            raise StateReductionError(
                f"Invalid transition from {LifecycleState.APPROVED.value} on event {event_type}"
            )

    # Terminal states (no transitions allowed)
    if current_state in {LifecycleState.COMMITTED, LifecycleState.FAILED, LifecycleState.REJECTED}:
        raise StateReductionError(f"Cannot transition from terminal state {current_state.value}")

    raise StateReductionError(f"Unknown state {current_state}")


def compute_snapshot(
    aggregate_id: str, events: list[DomainEvent]
) -> LifecycleSnapshot:
    """
    Create a point-in-time snapshot of derived state.

    Useful for:
    - Caching state (avoid replaying all events)
    - Temporal queries
    - Audit reports

    Args:
        aggregate_id: ID of the aggregate
        events: All events for this aggregate

    Returns:
        Immutable snapshot of current state

    Raises:
        StateReductionError: If events are invalid
    """
    if not events:
        raise StateReductionError(f"Cannot snapshot aggregate {aggregate_id} with no events")

    current_state = reduce_state(events)
    last_event = events[-1]

    return LifecycleSnapshot(
        aggregate_id=aggregate_id,
        current_state=current_state,
        last_event_id=last_event.event_id,
        last_event_timestamp=last_event.timestamp,
        event_count=len(events),
    )


def is_terminal_state(state: LifecycleState) -> bool:
    """
    Check if state is terminal (no further transitions possible).

    Args:
        state: Lifecycle state

    Returns:
        True if state is terminal (COMMITTED, FAILED, or REJECTED)
    """
    parsed = assert_lifecycle_state(state)
    return parsed.is_terminal()


def get_valid_next_events(
    state: LifecycleState,
) -> set[str]:
    """
    Get valid event types that can occur from a given state.

    Useful for:
    - Validating commands
    - UI state (what actions are available)
    - Transition validation

    Args:
        state: Current lifecycle state

    Returns:
        Set of valid event type strings
    """
    parsed = assert_lifecycle_state(state)
    transitions = {
        LifecycleState.PROPOSED: {
            LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            LIFECYCLE_EVENT_TYPES["ACTION_FAILED"],
            LIFECYCLE_EVENT_TYPES["ACTION_REJECTED"],
        },
        LifecycleState.PENDING_APPROVAL: {
            LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            LIFECYCLE_EVENT_TYPES["ACTION_FAILED"],
            LIFECYCLE_EVENT_TYPES["ACTION_REJECTED"],
        },
        LifecycleState.APPROVED: {
            LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],
            LIFECYCLE_EVENT_TYPES["ACTION_FAILED"],
        },
        LifecycleState.COMMITTED: set(),
        LifecycleState.FAILED: set(),
        LifecycleState.REJECTED: set(),
    }
    return transitions.get(parsed, set())
