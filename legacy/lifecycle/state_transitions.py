"""
Lifecycle state transition policy.

Defines the single source of truth for allowed workflow lifecycle transitions.
All transitions are explicit and strictly validated.
"""

from __future__ import annotations

from legacy.lifecycle.execution_state_machine import LifecycleState


class TransitionValidationError(ValueError):
    """Raised when a transition is not explicitly allowed."""


ALLOWED_TRANSITIONS: dict[LifecycleState, frozenset[LifecycleState]] = {
    LifecycleState.CREATED: frozenset(
        {
            LifecycleState.AWAITING_APPROVAL,
            LifecycleState.CANCELLED,
        }
    ),
    LifecycleState.AWAITING_APPROVAL: frozenset(
        {
            LifecycleState.APPROVED,
            LifecycleState.CANCELLED,
        }
    ),
    LifecycleState.APPROVED: frozenset(
        {
            LifecycleState.QUEUED,
        }
    ),
    LifecycleState.QUEUED: frozenset(
        {
            LifecycleState.RUNNING,
            LifecycleState.CANCELLED,
            LifecycleState.FAILED,
        }
    ),
    LifecycleState.RUNNING: frozenset(
        {
            LifecycleState.PAUSED,
            LifecycleState.COMPLETED,
            LifecycleState.FAILED,
            LifecycleState.CANCELLED,
        }
    ),
    LifecycleState.PAUSED: frozenset(
        {
            LifecycleState.RUNNING,
            LifecycleState.CANCELLED,
        }
    ),
    LifecycleState.COMPLETED: frozenset(),
    LifecycleState.FAILED: frozenset(),
    LifecycleState.CANCELLED: frozenset(),
}


def allowed_targets(state: LifecycleState) -> frozenset[LifecycleState]:
    """Return explicit allowed targets for the current state."""
    return ALLOWED_TRANSITIONS[state]


def is_allowed_transition(current: LifecycleState, target: LifecycleState) -> bool:
    """True only when target is explicitly listed for current."""
    return target in ALLOWED_TRANSITIONS[current]


def validate_transition_strict(current: LifecycleState, target: LifecycleState) -> None:
    """
    Validate a transition against explicit policy.

    Fails hard when:
      - transition is a no-op
      - transition is not explicitly permitted
    """
    if current == target:
        raise TransitionValidationError(
            f"Invalid transition: {current.value} -> {target.value} (no-op forbidden)"
        )

    if not is_allowed_transition(current, target):
        allowed = sorted(s.value for s in allowed_targets(current))
        raise TransitionValidationError(
            "Invalid transition: "
            f"{current.value} -> {target.value}. Allowed targets: {allowed}"
        )
