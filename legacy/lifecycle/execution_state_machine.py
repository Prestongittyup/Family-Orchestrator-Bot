"""
Workflow Execution Lifecycle State Machine.

Defines deterministic, explicit state transitions for workflow execution.
No implicit transitions are permitted; every transition must be requested
and validated against the transition table.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class LifecycleState(str, Enum):
    CREATED = "created"
    AWAITING_APPROVAL = "awaiting_approval"
    APPROVED = "approved"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class InvalidTransitionError(ValueError):
    """Raised when attempting an invalid lifecycle state transition."""


# Single source of truth for allowed transitions.
# Deterministic rule: a transition is valid iff target is in this mapping.
_ALLOWED_TRANSITIONS: dict[LifecycleState, frozenset[LifecycleState]] = {
    LifecycleState.CREATED: frozenset(
        {
            LifecycleState.AWAITING_APPROVAL,
            LifecycleState.QUEUED,
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
            LifecycleState.CANCELLED,
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
            LifecycleState.FAILED,
        }
    ),
    LifecycleState.COMPLETED: frozenset(),
    LifecycleState.FAILED: frozenset(),
    LifecycleState.CANCELLED: frozenset(),
}


def allowed_transitions(state: LifecycleState) -> frozenset[LifecycleState]:
    """Return all valid target states for the provided current state."""
    return _ALLOWED_TRANSITIONS[state]


def can_transition(current: LifecycleState, target: LifecycleState) -> bool:
    """Return True only if current -> target is explicitly allowed."""
    return target in _ALLOWED_TRANSITIONS[current]


def validate_transition(current: LifecycleState, target: LifecycleState) -> None:
    """
    Validate a transition and raise InvalidTransitionError if not allowed.

    No-op transitions (state -> same state) are treated as invalid to avoid
    implicit behavior and accidental retries masquerading as transitions.
    """
    if current == target:
        raise InvalidTransitionError(
            f"Invalid transition: {current.value} -> {target.value} (no-op not allowed)"
        )

    if not can_transition(current, target):
        allowed = sorted(s.value for s in allowed_transitions(current))
        raise InvalidTransitionError(
            "Invalid transition: "
            f"{current.value} -> {target.value}. Allowed targets: {allowed}"
        )


def execute_transition(current: LifecycleState, target: LifecycleState) -> LifecycleState:
    """
    Validate and execute a state transition.

    Returns the target state on success; raises InvalidTransitionError on failure.
    """
    validate_transition(current, target)
    return target


@dataclass(frozen=True)
class TransitionEvent:
    """Immutable record of a successful state transition."""

    previous_state: LifecycleState
    new_state: LifecycleState
    at: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LifecycleStateMachine:
    """
    Mutable state transition executor for a single workflow lifecycle.

    Deterministic behavior:
      - Each call to transition(target) either succeeds with an explicit state
        change or raises InvalidTransitionError.
      - No automatic jumps or multi-step transitions are performed.
    """

    state: LifecycleState = LifecycleState.CREATED
    history: list[TransitionEvent] = field(default_factory=list)

    def transition(
        self,
        target: LifecycleState,
        metadata: dict[str, Any] | None = None,
    ) -> TransitionEvent:
        """
        Execute exactly one explicit transition from current state to target.

        Raises InvalidTransitionError for any transition not defined in the table.
        """
        previous = self.state
        new_state = execute_transition(previous, target)

        event = TransitionEvent(
            previous_state=previous,
            new_state=new_state,
            metadata=metadata or {},
        )
        self.state = new_state
        self.history.append(event)
        return event

    def reset(self) -> None:
        """Reset machine to CREATED and clear history."""
        self.state = LifecycleState.CREATED
        self.history = []
