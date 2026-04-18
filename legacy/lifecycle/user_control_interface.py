"""
User Control Interface — Safe, user-facing lifecycle controls.

Exposes explicit user actions:
  - approve workflow
  - reject workflow
  - pause workflow
  - resume workflow
  - cancel workflow

Safety rules:
  - ALL actions flow through the lifecycle state machine via WorkflowTracker
  - Safety gate is enforced before execution-path actions
  - No DAG execution is ever triggered here
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from legacy.lifecycle.execution_state_machine import InvalidTransitionError, LifecycleState
from legacy.lifecycle.workflow_tracker import WorkflowTracker, WorkflowTransitionRecord
from safety.execution_gate import ExecutionDecision, ExecutionGate, ExecutionStatus
from safety.graph_models import DAG

ControlAction = Literal["approve", "reject", "pause", "resume", "cancel"]


@dataclass(frozen=True)
class ControlResult:
    """Deterministic result of a user control action."""

    workflow_id: str
    action: ControlAction
    state: LifecycleState
    gate_decision: ExecutionDecision | None
    transition: WorkflowTransitionRecord | None
    reasons: list[str] = field(default_factory=list)


class UserControlInterface:
    """
    Safe control facade over workflow lifecycle transitions.

    This class never executes a DAG. It only validates permission/safety and
    records explicit transitions through WorkflowTracker.
    """

    def __init__(
        self,
        tracker: WorkflowTracker,
        gate: ExecutionGate | None = None,
    ) -> None:
        self._tracker = tracker
        self._gate = gate or ExecutionGate()

    def approve_workflow(
        self,
        workflow_id: str,
        dag: DAG,
        user_id: str,
        household_id: str,
        context: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ControlResult:
        """
        Approve a workflow currently awaiting approval.

        Gate is checked first:
          - REJECT: action denied
          - REQUIRE_APPROVAL / ALLOW: transition may proceed
        """
        decision = self._gate.evaluate(dag, user_id, household_id, context)
        if decision.status == ExecutionStatus.REJECT:
            return self._no_transition_result(
                workflow_id=workflow_id,
                action="approve",
                gate_decision=decision,
                reasons=list(decision.reasons),
            )

        transition = self._tracker.track_transition(
            workflow_id,
            LifecycleState.APPROVED,
            "manual",
            metadata=self._merge_metadata(metadata, action="approve", user_id=user_id),
        )
        return ControlResult(
            workflow_id=workflow_id,
            action="approve",
            state=transition.new_state,
            gate_decision=decision,
            transition=transition,
            reasons=[],
        )

    def reject_workflow(
        self,
        workflow_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> ControlResult:
        """Reject a workflow by transitioning it to CANCELLED."""
        transition = self._tracker.track_transition(
            workflow_id,
            LifecycleState.CANCELLED,
            "manual",
            metadata=self._merge_metadata(metadata, action="reject"),
        )
        return ControlResult(
            workflow_id=workflow_id,
            action="reject",
            state=transition.new_state,
            gate_decision=None,
            transition=transition,
            reasons=[],
        )

    def pause_workflow(
        self,
        workflow_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> ControlResult:
        """Pause a running workflow (RUNNING -> PAUSED)."""
        transition = self._tracker.track_transition(
            workflow_id,
            LifecycleState.PAUSED,
            "manual",
            metadata=self._merge_metadata(metadata, action="pause"),
        )
        return ControlResult(
            workflow_id=workflow_id,
            action="pause",
            state=transition.new_state,
            gate_decision=None,
            transition=transition,
            reasons=[],
        )

    def resume_workflow(
        self,
        workflow_id: str,
        dag: DAG,
        user_id: str,
        household_id: str,
        context: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ControlResult:
        """
        Resume a paused workflow (PAUSED -> RUNNING).

        Gate is enforced before entering RUNNING (execution path):
          - REJECT: denied
          - REQUIRE_APPROVAL: denied (must run explicit approval flow first)
          - ALLOW: transition proceeds
        """
        decision = self._gate.evaluate(dag, user_id, household_id, context)
        if decision.status == ExecutionStatus.REJECT:
            return self._no_transition_result(
                workflow_id=workflow_id,
                action="resume",
                gate_decision=decision,
                reasons=list(decision.reasons),
            )

        if decision.status == ExecutionStatus.REQUIRE_APPROVAL:
            return self._no_transition_result(
                workflow_id=workflow_id,
                action="resume",
                gate_decision=decision,
                reasons=[
                    "Resume denied: workflow requires explicit approval before execution"
                ],
            )

        transition = self._tracker.track_transition(
            workflow_id,
            LifecycleState.RUNNING,
            "manual",
            metadata=self._merge_metadata(metadata, action="resume", user_id=user_id),
        )
        return ControlResult(
            workflow_id=workflow_id,
            action="resume",
            state=transition.new_state,
            gate_decision=decision,
            transition=transition,
            reasons=[],
        )

    def cancel_workflow(
        self,
        workflow_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> ControlResult:
        """Cancel a workflow using explicit lifecycle transition rules."""
        transition = self._tracker.track_transition(
            workflow_id,
            LifecycleState.CANCELLED,
            "manual",
            metadata=self._merge_metadata(metadata, action="cancel"),
        )
        return ControlResult(
            workflow_id=workflow_id,
            action="cancel",
            state=transition.new_state,
            gate_decision=None,
            transition=transition,
            reasons=[],
        )

    @staticmethod
    def _merge_metadata(
        metadata: dict[str, Any] | None,
        **extra: Any,
    ) -> dict[str, Any]:
        merged = dict(metadata or {})
        merged.update(extra)
        return merged

    def _no_transition_result(
        self,
        workflow_id: str,
        action: ControlAction,
        gate_decision: ExecutionDecision,
        reasons: list[str],
    ) -> ControlResult:
        current_state = self._tracker.get_state(workflow_id)
        if current_state is None:
            raise KeyError(f"Unknown workflow_id '{workflow_id}'. Bind it first.")
        return ControlResult(
            workflow_id=workflow_id,
            action=action,
            state=current_state,
            gate_decision=gate_decision,
            transition=None,
            reasons=reasons,
        )
