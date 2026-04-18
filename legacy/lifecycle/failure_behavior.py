"""
Failure behavior compatibility facade.

Provides simple, script-friendly helpers matching the Step 18 verification
shape while delegating to strict lifecycle policy.
"""

from __future__ import annotations

from legacy.lifecycle.execution_state_machine import InvalidTransitionError
from legacy.lifecycle.execution_state_machine import LifecycleState
from legacy.lifecycle.state_transitions import (
    TransitionValidationError,
    validate_transition_strict,
)


class StateMachine:
    """Simple transition validator facade used by verification scripts."""

    @staticmethod
    def transition(current: str, target: str) -> bool:
        current_state = LifecycleState(current.lower())
        target_state = LifecycleState(target.lower())
        try:
            validate_transition_strict(current_state, target_state)
        except TransitionValidationError as exc:
            raise InvalidTransitionError(str(exc)) from exc
        return True
