from __future__ import annotations

from legacy.lifecycle.execution_state_machine import InvalidTransitionError, LifecycleState
from legacy.lifecycle.workflow_tracker import WorkflowTracker


def test_bind_workflow_sets_initial_state_and_record():
    tracker = WorkflowTracker()

    record = tracker.bind_workflow("wf-1")

    assert record.workflow_id == "wf-1"
    assert record.previous_state is None
    assert record.new_state == LifecycleState.CREATED
    assert tracker.get_state("wf-1") == LifecycleState.CREATED


def test_transition_updates_state_and_appends_log():
    tracker = WorkflowTracker()
    tracker.bind_workflow("wf-1")

    r1 = tracker.track_transition("wf-1", LifecycleState.AWAITING_APPROVAL, "dag")
    r2 = tracker.track_transition("wf-1", LifecycleState.APPROVED, "scheduler")

    assert r1.index == 1
    assert r2.index == 2
    assert tracker.get_state("wf-1") == LifecycleState.APPROVED
    assert len(tracker.audit_log()) == 3


def test_invalid_transition_raises_error():
    tracker = WorkflowTracker()
    tracker.bind_workflow("wf-1")

    try:
        tracker.track_transition("wf-1", LifecycleState.RUNNING, "engine")
        assert False, "Expected InvalidTransitionError"
    except InvalidTransitionError:
        assert True


def test_unknown_workflow_transition_raises_keyerror():
    tracker = WorkflowTracker()

    try:
        tracker.track_transition("missing", LifecycleState.QUEUED, "scheduler")
        assert False, "Expected KeyError"
    except KeyError:
        assert True


def test_append_only_audit_indices_monotonic():
    tracker = WorkflowTracker()
    tracker.bind_workflow("wf-1")
    tracker.track_transition("wf-1", LifecycleState.AWAITING_APPROVAL, "dag")
    tracker.track_transition("wf-1", LifecycleState.APPROVED, "scheduler")

    tracker.bind_workflow("wf-2")
    tracker.track_transition("wf-2", LifecycleState.QUEUED, "scheduler")

    indices = [r.index for r in tracker.audit_log()]
    assert indices == sorted(indices)
    assert indices == list(range(len(indices)))


def test_history_is_replayable_per_workflow():
    tracker = WorkflowTracker()
    tracker.bind_workflow("wf-1")
    tracker.track_transition("wf-1", LifecycleState.AWAITING_APPROVAL, "dag")
    tracker.track_transition("wf-1", LifecycleState.APPROVED, "scheduler")
    tracker.track_transition("wf-1", LifecycleState.QUEUED, "scheduler")

    replay = tracker.replay("wf-1")

    assert replay.workflow_id == "wf-1"
    assert replay.initial_state == LifecycleState.CREATED
    assert replay.final_state == LifecycleState.QUEUED
    assert [r.new_state for r in replay.transitions] == [
        LifecycleState.CREATED,
        LifecycleState.AWAITING_APPROVAL,
        LifecycleState.APPROVED,
        LifecycleState.QUEUED,
    ]


def test_timestamps_present_for_each_transition():
    tracker = WorkflowTracker()
    tracker.bind_workflow("wf-1")
    tracker.track_transition("wf-1", LifecycleState.AWAITING_APPROVAL, "dag")

    history = tracker.get_history("wf-1")
    assert all(r.at is not None for r in history)


def test_deterministic_history_order():
    tracker = WorkflowTracker()

    tracker.bind_workflow("wf-1")
    tracker.track_transition("wf-1", LifecycleState.AWAITING_APPROVAL, "dag")
    tracker.track_transition("wf-1", LifecycleState.APPROVED, "scheduler")

    first = [
        (r.previous_state, r.new_state, r.source)
        for r in tracker.get_history("wf-1")
    ]
    second = [
        (r.previous_state, r.new_state, r.source)
        for r in tracker.get_history("wf-1")
    ]

    assert first == second
