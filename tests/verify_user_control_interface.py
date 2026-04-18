from __future__ import annotations

import pytest

from safety.graph_models import DAG, DAGNode
from legacy.lifecycle.execution_state_machine import LifecycleState
from legacy.lifecycle.user_control_interface import UserControlInterface
from legacy.lifecycle.workflow_tracker import WorkflowTracker
from safety.execution_gate import ExecutionStatus


def _low_risk_dag() -> DAG:
    return DAG(
        dag_id="d-low",
        intent_id="i-low",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="task",
                operation="create_task",
            )
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )


def _high_risk_dag() -> DAG:
    return DAG(
        dag_id="d-high",
        intent_id="i-high",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="task",
                operation="delete_task",
            )
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )


def _mismatched_dag() -> DAG:
    return DAG(
        dag_id="d-bad",
        intent_id="i-bad",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="task",
                operation="create_task",
            )
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "uX", "household_id": "hX"},
    )


def test_approve_goes_through_state_machine_transition():
    tracker = WorkflowTracker()
    tracker.bind_workflow("wf-1", LifecycleState.AWAITING_APPROVAL)
    ui = UserControlInterface(tracker)

    result = ui.approve_workflow("wf-1", _high_risk_dag(), "u1", "h1")

    assert result.gate_decision is not None
    assert result.gate_decision.status == ExecutionStatus.REQUIRE_APPROVAL
    assert result.state == LifecycleState.APPROVED
    assert tracker.get_state("wf-1") == LifecycleState.APPROVED


def test_approve_denied_when_gate_rejects():
    tracker = WorkflowTracker()
    tracker.bind_workflow("wf-2", LifecycleState.AWAITING_APPROVAL)
    ui = UserControlInterface(tracker)

    result = ui.approve_workflow("wf-2", _mismatched_dag(), "u1", "h1")

    assert result.gate_decision is not None
    assert result.gate_decision.status == ExecutionStatus.REJECT
    assert result.transition is None
    assert tracker.get_state("wf-2") == LifecycleState.AWAITING_APPROVAL


def test_pause_and_resume_low_risk():
    tracker = WorkflowTracker()
    tracker.bind_workflow("wf-3")
    tracker.track_transition("wf-3", LifecycleState.QUEUED, "scheduler")
    tracker.track_transition("wf-3", LifecycleState.RUNNING, "engine")
    ui = UserControlInterface(tracker)

    paused = ui.pause_workflow("wf-3")
    resumed = ui.resume_workflow("wf-3", _low_risk_dag(), "u1", "h1")

    assert paused.state == LifecycleState.PAUSED
    assert resumed.state == LifecycleState.RUNNING
    assert resumed.gate_decision is not None
    assert resumed.gate_decision.status == ExecutionStatus.ALLOW


def test_resume_blocked_when_gate_requires_approval():
    tracker = WorkflowTracker()
    tracker.bind_workflow("wf-4")
    tracker.track_transition("wf-4", LifecycleState.QUEUED, "scheduler")
    tracker.track_transition("wf-4", LifecycleState.RUNNING, "engine")
    tracker.track_transition("wf-4", LifecycleState.PAUSED, "manual")
    ui = UserControlInterface(tracker)

    resumed = ui.resume_workflow("wf-4", _high_risk_dag(), "u1", "h1")

    assert resumed.gate_decision is not None
    assert resumed.gate_decision.status == ExecutionStatus.REQUIRE_APPROVAL
    assert resumed.transition is None
    assert tracker.get_state("wf-4") == LifecycleState.PAUSED


def test_reject_workflow_cancels_via_state_machine():
    tracker = WorkflowTracker()
    tracker.bind_workflow("wf-5", LifecycleState.AWAITING_APPROVAL)
    ui = UserControlInterface(tracker)

    result = ui.reject_workflow("wf-5")

    assert result.state == LifecycleState.CANCELLED
    assert tracker.get_state("wf-5") == LifecycleState.CANCELLED


def test_cancel_workflow_from_running():
    tracker = WorkflowTracker()
    tracker.bind_workflow("wf-6")
    tracker.track_transition("wf-6", LifecycleState.QUEUED, "scheduler")
    tracker.track_transition("wf-6", LifecycleState.RUNNING, "engine")
    ui = UserControlInterface(tracker)

    result = ui.cancel_workflow("wf-6")

    assert result.state == LifecycleState.CANCELLED
    assert tracker.get_state("wf-6") == LifecycleState.CANCELLED


def test_interface_never_executes_dag_only_transitions_state():
    tracker = WorkflowTracker()
    tracker.bind_workflow("wf-7", LifecycleState.AWAITING_APPROVAL)
    ui = UserControlInterface(tracker)

    before = len(tracker.audit_log())
    ui.approve_workflow("wf-7", _high_risk_dag(), "u1", "h1")
    after = len(tracker.audit_log())

    # Exactly one new transition record, no execution artifacts.
    assert after == before + 1
    assert tracker.get_state("wf-7") == LifecycleState.APPROVED
