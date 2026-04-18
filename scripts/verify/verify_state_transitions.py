from __future__ import annotations

import pytest

from legacy.lifecycle.execution_state_machine import LifecycleState
from legacy.lifecycle.state_transitions import (
    allowed_targets,
    is_allowed_transition,
    validate_transition_strict,
    TransitionValidationError,
)


def test_example_rules_allow_expected_paths():
    assert is_allowed_transition(LifecycleState.CREATED, LifecycleState.AWAITING_APPROVAL)
    assert is_allowed_transition(LifecycleState.AWAITING_APPROVAL, LifecycleState.APPROVED)
    assert is_allowed_transition(LifecycleState.AWAITING_APPROVAL, LifecycleState.CANCELLED)
    assert is_allowed_transition(LifecycleState.APPROVED, LifecycleState.QUEUED)
    assert is_allowed_transition(LifecycleState.QUEUED, LifecycleState.RUNNING)
    assert is_allowed_transition(LifecycleState.RUNNING, LifecycleState.PAUSED)
    assert is_allowed_transition(LifecycleState.RUNNING, LifecycleState.COMPLETED)
    assert is_allowed_transition(LifecycleState.RUNNING, LifecycleState.FAILED)
    assert is_allowed_transition(LifecycleState.PAUSED, LifecycleState.RUNNING)
    assert is_allowed_transition(LifecycleState.PAUSED, LifecycleState.CANCELLED)


def test_no_bypass_allowed():
    assert not is_allowed_transition(LifecycleState.CREATED, LifecycleState.RUNNING)
    assert not is_allowed_transition(LifecycleState.APPROVED, LifecycleState.RUNNING)
    assert not is_allowed_transition(LifecycleState.AWAITING_APPROVAL, LifecycleState.QUEUED)


@pytest.mark.parametrize(
    "current,target",
    [
        (LifecycleState.CREATED, LifecycleState.RUNNING),
        (LifecycleState.APPROVED, LifecycleState.COMPLETED),
        (LifecycleState.PAUSED, LifecycleState.COMPLETED),
        (LifecycleState.COMPLETED, LifecycleState.RUNNING),
    ],
)
def test_invalid_transitions_fail_hard(current: LifecycleState, target: LifecycleState):
    with pytest.raises(TransitionValidationError):
        validate_transition_strict(current, target)


def test_noop_transition_fails_hard():
    with pytest.raises(TransitionValidationError):
        validate_transition_strict(LifecycleState.QUEUED, LifecycleState.QUEUED)


def test_terminal_states_have_no_outgoing_transitions():
    assert allowed_targets(LifecycleState.COMPLETED) == frozenset()
    assert allowed_targets(LifecycleState.FAILED) == frozenset()
    assert allowed_targets(LifecycleState.CANCELLED) == frozenset()
