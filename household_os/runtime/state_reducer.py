"""
State Reducer - Pure Function for Deriving Lifecycle State from Events

Core principle: State is computed by replaying events in order.
This enables:
- Deterministic state reconstruction
- Event sourcing correctness
- Audit trail of all state changes
- Easy temporal queries (state at any point in time)

CQRS Constraint:
- This is a READ MODEL ONLY (Query side)
- Reducer validates transitions but does NOT define them
- Transition rules are OWNED by StateMachine in apps/api/core/state_machine.py
- Reducer calls validate_transition() from FSM to audit historical events
- Reducer does NOT decide what transitions are legal (FSM decides)
- Reducer maps event types to states (event stream determines state progression)
- NO state mutation occurs here (this is pure read/derivation only)
"""

from __future__ import annotations

from apps.api.core.state_machine import ActionState, TransitionError, validate_transition
from apps.api.observability.logging import log_error
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
from household_os.security.trust_boundary_enforcer import validate_replay_call


class StateReductionError(Exception):
    """Raised when state reduction encounters an invalid event sequence."""

    pass


def reduce_state(events: list[DomainEvent]) -> LifecycleState:
    """
    Derive current lifecycle state from a sequence of events.

    CQRS Read Model (Query side):
    - This is a PURE FUNCTION for replaying events
    - No state mutation (read-only)
    - Deterministic: same input events always produce same state
    - Idempotent: replaying the same events produces the same result

    Event Sourcing:
    - State progression is determined by event types, not rules
    - Events are immutable historical facts
    - Replaying all events derives current state

    Transition Validation:
    - This function calls validate_transition() from StateMachine to audit historical events
    - Validation is for correctness checking only (catching data corruption)
    - Validation does NOT define what transitions are allowed
    - Transition rules are OWNED by StateMachine (apps/api/core/state_machine.py)
    - Validation uses FSM's ALLOWED_TRANSITIONS to verify replayed events were legal

    Event-to-State Mapping (determined by event stream, not by reducer logic):
    - ACTION_PROPOSED       → PROPOSED
    - ACTION_APPROVED       → APPROVED
    - ACTION_REJECTED       → REJECTED  
    - ACTION_FAILED         → FAILED
    - ACTION_COMMITTED      → COMMITTED

    Args:
        events: List of domain events in order (earliest first)

    Returns:
        Current derived state as LifecycleState enum

    Raises:
        StateReductionError: If event sequence is invalid or violates FSM rules
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
    validate_replay_call(
        skip_modules={
            "household_os.runtime.state_reducer",
            "apps.api.observability.eil.tracer",
        }
    )
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
    event_to_state = {
        LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"]: LifecycleState.PROPOSED,
        LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"]: LifecycleState.APPROVED,
        LIFECYCLE_EVENT_TYPES["ACTION_REJECTED"]: LifecycleState.REJECTED,
        LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"]: LifecycleState.COMMITTED,
        LIFECYCLE_EVENT_TYPES["ACTION_FAILED"]: LifecycleState.FAILED,
    }
    target_state = event_to_state.get(event_type)
    if target_state is None:
        raise StateReductionError(f"Unsupported lifecycle event type: {event_type}")

    raw_actor_type = event.metadata.get("actor_type")
    if raw_actor_type is None:
        actor_ctx = event.metadata.get("actor_context")
        if isinstance(actor_ctx, dict):
            raw_actor_type = actor_ctx.get("actor_type")
    if raw_actor_type is None:
        raise StateReductionError("Missing actor_type in replay event metadata")

    event_actor_type = str(raw_actor_type).strip().lower()
    if event_actor_type in {"", "unknown"}:
        # Legacy streams often omit actor provenance; treat as internal replay actor.
        event_actor_type = "system_worker"
    if event_actor_type == "api_user":
        event_actor_type = "user"
    allowed_actor_types = {"user", "assistant", "system_worker", "scheduler"}
    if event_actor_type not in allowed_actor_types:
        raise StateReductionError(
            f"Unknown actor_type in replay event: {event_actor_type!r}; allowed={sorted(allowed_actor_types)}"
        )
    if not event.signature:
        raise StateReductionError("Unsigned event rejected during replay")
    if not event.verify_signature():
        raise StateReductionError("Event signature mismatch during replay")

    # Replay is authoritative only from event stream; first event must bootstrap lifecycle.
    if current_state is None:
        if target_state != LifecycleState.PROPOSED:
            raise StateReductionError(
                f"First event must be ACTION_PROPOSED, got {event_type}"
            )
        return target_state

    requires_approval = bool(event.payload.get("requires_approval", False)) if isinstance(event.payload, dict) else False

    try:
        validate_transition(
            from_state=ActionState(current_state.value),
            to_state=ActionState(target_state.value),
            context={
                "actor_type": event_actor_type,
                "requires_approval": requires_approval,
            },
        )
    except (TransitionError, ValueError) as exc:
        log_error(
            "event_replay_validation_failed",
            exc,
            event_id=event.event_id,
            reason=str(exc),
            actor_type=event_actor_type,
        )
        raise StateReductionError(
            f"Invalid transition from {current_state.value} on event {event_type}"
        ) from exc

    return target_state


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
