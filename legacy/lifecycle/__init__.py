"""Lifecycle state machine package."""

from legacy.lifecycle.execution_state_machine import (
    InvalidTransitionError,
    LifecycleState,
    LifecycleStateMachine,
    TransitionEvent,
    allowed_transitions,
    can_transition,
    execute_transition,
    validate_transition,
)
from legacy.lifecycle.workflow_tracker import (
    ReplayResult,
    TransitionSource,
    WorkflowTracker,
    WorkflowTransitionRecord,
)
from legacy.lifecycle.user_control_interface import ControlResult, UserControlInterface
from legacy.lifecycle.state_transitions import (
    ALLOWED_TRANSITIONS,
    TransitionValidationError,
    allowed_targets,
    is_allowed_transition,
    validate_transition_strict,
)
from legacy.lifecycle.failure_behavior import StateMachine

__all__ = [
    "LifecycleState",
    "InvalidTransitionError",
    "TransitionEvent",
    "LifecycleStateMachine",
    "allowed_transitions",
    "can_transition",
    "validate_transition",
    "execute_transition",
    "TransitionSource",
    "WorkflowTransitionRecord",
    "ReplayResult",
    "WorkflowTracker",
    "ControlResult",
    "UserControlInterface",
    "ALLOWED_TRANSITIONS",
    "TransitionValidationError",
    "allowed_targets",
    "is_allowed_transition",
    "validate_transition_strict",
    "StateMachine",
]
