from __future__ import annotations

import pytest

from legacy.lifecycle.execution_state_machine import (
    InvalidTransitionError,
    LifecycleState,
    LifecycleStateMachine,
    can_transition,
    execute_transition,
    validate_transition,
)


def test_valid_explicit_transition():
    validate_transition(LifecycleState.CREATED, LifecycleState.AWAITING_APPROVAL)


def test_invalid_transition_raises():
    with pytest.raises(InvalidTransitionError):
        validate_transition(LifecycleState.CREATED, LifecycleState.RUNNING)


def test_noop_transition_raises():
    with pytest.raises(InvalidTransitionError):
        validate_transition(LifecycleState.QUEUED, LifecycleState.QUEUED)


def test_terminal_states_reject_all_outgoing():
    terminals = [
        LifecycleState.COMPLETED,
        LifecycleState.FAILED,
        LifecycleState.CANCELLED,
    ]
    for state in terminals:
        for target in LifecycleState:
            if target is state:
                continue
            assert can_transition(state, target) is False


def test_execute_transition_returns_target_state():
    new_state = execute_transition(LifecycleState.APPROVED, LifecycleState.QUEUED)
    assert new_state == LifecycleState.QUEUED


def test_executor_tracks_history_and_updates_state():
    sm = LifecycleStateMachine()
    sm.transition(LifecycleState.AWAITING_APPROVAL)
    event = sm.transition(LifecycleState.APPROVED, metadata={"by": "reviewer"})

    assert sm.state == LifecycleState.APPROVED
    assert event.previous_state == LifecycleState.AWAITING_APPROVAL
    assert event.new_state == LifecycleState.APPROVED
    assert event.metadata["by"] == "reviewer"
    assert len(sm.history) == 2


def test_executor_invalid_transition_raises_and_preserves_state():
    sm = LifecycleStateMachine(state=LifecycleState.CREATED)

    with pytest.raises(InvalidTransitionError):
        sm.transition(LifecycleState.RUNNING)

    assert sm.state == LifecycleState.CREATED
    assert sm.history == []


def test_no_implicit_multi_step_transition_allowed():
    sm = LifecycleStateMachine(state=LifecycleState.CREATED)

    with pytest.raises(InvalidTransitionError):
        sm.transition(LifecycleState.RUNNING)


def test_deterministic_transition_path():
    path = [
        LifecycleState.AWAITING_APPROVAL,
        LifecycleState.APPROVED,
        LifecycleState.QUEUED,
        LifecycleState.RUNNING,
        LifecycleState.COMPLETED,
    ]
    sm1 = LifecycleStateMachine()
    sm2 = LifecycleStateMachine()

    for step in path:
        sm1.transition(step)
        sm2.transition(step)

    assert sm1.state == sm2.state == LifecycleState.COMPLETED
    assert [e.new_state for e in sm1.history] == [e.new_state for e in sm2.history]
