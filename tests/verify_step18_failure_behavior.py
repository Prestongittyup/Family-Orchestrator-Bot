from __future__ import annotations

import pytest

from safety.graph_models import DAG, DAGNode
from legacy.lifecycle.execution_state_machine import InvalidTransitionError, LifecycleState
from legacy.lifecycle.failure_behavior import StateMachine
from legacy.lifecycle.workflow_tracker import WorkflowTracker


def _low_risk_dag() -> DAG:
    return DAG(
        dag_id="dag-low",
        intent_id="intent-low",
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
        dag_id="dag-high",
        intent_id="intent-high",
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


def test_valid_state_transitions():
    sm = StateMachine()

    assert sm.transition("CREATED", "AWAITING_APPROVAL") is True
    assert sm.transition("AWAITING_APPROVAL", "APPROVED") is True


def test_invalid_transition_blocked():
    sm = StateMachine()

    with pytest.raises(InvalidTransitionError):
        sm.transition("CREATED", "RUNNING")


def test_full_execution_flow():
    wf = WorkflowTracker()

    wf.create("wf1")
    wf.approve("wf1")
    wf.queue("wf1")
    wf.run("wf1", dag=_low_risk_dag(), user_id="u1", household_id="h1")
    wf.complete("wf1")

    assert wf.get_state("wf1") == LifecycleState.COMPLETED


def test_pause_resume_cycle():
    wf = WorkflowTracker()

    wf.create("wf1")
    wf.approve("wf1")
    wf.queue("wf1")
    wf.run("wf1", dag=_low_risk_dag(), user_id="u1", household_id="h1")
    wf.pause("wf1", safe_checkpoint=True)
    wf.resume("wf1", dag=_low_risk_dag(), user_id="u1", household_id="h1")

    assert wf.get_state("wf1") == LifecycleState.RUNNING


def test_cancel_blocks_execution():
    wf = WorkflowTracker()

    wf.create("wf1")
    wf.cancel("wf1")

    assert wf.get_state("wf1") == LifecycleState.CANCELLED

    with pytest.raises(InvalidTransitionError):
        wf.queue("wf1")


def test_pause_requires_safe_checkpoint():
    wf = WorkflowTracker()

    wf.create("wf1")
    wf.approve("wf1")
    wf.queue("wf1")
    wf.run("wf1", dag=_low_risk_dag(), user_id="u1", household_id="h1")

    with pytest.raises(RuntimeError):
        wf.pause("wf1", safe_checkpoint=False)


def test_approval_missing_must_not_queue_or_run():
    wf = WorkflowTracker()

    wf.create("wf1", require_approval=True)
    assert wf.get_state("wf1") == LifecycleState.AWAITING_APPROVAL

    with pytest.raises(InvalidTransitionError):
        wf.queue("wf1")

    with pytest.raises(InvalidTransitionError):
        wf.run("wf1", dag=_low_risk_dag(), user_id="u1", household_id="h1")

    assert wf.get_state("wf1") == LifecycleState.AWAITING_APPROVAL


def test_high_risk_cannot_enter_running_without_explicit_approval_flow():
    wf = WorkflowTracker()

    wf.create("wf1")
    wf.approve("wf1")
    wf.queue("wf1")

    with pytest.raises(PermissionError):
        wf.run("wf1", dag=_high_risk_dag(), user_id="u1", household_id="h1")

    assert wf.get_state("wf1") == LifecycleState.QUEUED
