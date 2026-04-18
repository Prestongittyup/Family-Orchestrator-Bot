"""
Workflow Tracker — Append-only lifecycle tracking across DAG, scheduler,
and execution engine.

This module tracks workflow state without executing workflows and without
modifying DAGs. It maintains:
  - workflow_id -> latest execution state binding
  - timestamped transition records
  - append-only global audit log
  - replayable per-workflow state history
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from legacy.lifecycle.execution_state_machine import (
    InvalidTransitionError,
    LifecycleState,
    validate_transition,
)
from safety.execution_gate import ExecutionGate, ExecutionStatus

TransitionSource = Literal["dag", "scheduler", "engine", "manual", "system"]


@dataclass(frozen=True)
class WorkflowTransitionRecord:
    """Immutable, append-only audit record for a single workflow transition."""

    index: int
    workflow_id: str
    previous_state: LifecycleState | None
    new_state: LifecycleState
    source: TransitionSource
    at: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReplayResult:
    """Replay output for one workflow reconstructed from the append-only log."""

    workflow_id: str
    initial_state: LifecycleState | None
    final_state: LifecycleState | None
    transitions: list[WorkflowTransitionRecord]


class WorkflowTracker:
    """
    Append-only workflow state tracker.

    Guarantees:
      - Deterministic transition validation via lifecycle state machine rules
      - No implicit transitions (all must be explicit track_transition calls)
      - Audit log entries are append-only and immutable
      - Latest state is derived from recorded transitions
    """

    def __init__(self, gate: ExecutionGate | None = None) -> None:
        self._audit_log: list[WorkflowTransitionRecord] = []
        self._state_by_workflow: dict[str, LifecycleState] = {}
        self._indices_by_workflow: dict[str, list[int]] = {}
        self._gate = gate or ExecutionGate()

    # Convenience lifecycle controls (strict, fail-hard)

    def create(
        self,
        workflow_id: str,
        require_approval: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """
        Create and initialize workflow lifecycle.

        If require_approval=True, immediately transitions CREATED ->
        AWAITING_APPROVAL so execution cannot be queued/run without approval.
        """
        self.bind_workflow(workflow_id, LifecycleState.CREATED, source="dag", metadata=metadata)
        if require_approval:
            self.track_transition(
                workflow_id,
                LifecycleState.AWAITING_APPROVAL,
                "dag",
                metadata={"reason": "approval_required"},
            )
        return True

    def approve(self, workflow_id: str, metadata: dict[str, Any] | None = None) -> bool:
        self.track_transition(workflow_id, LifecycleState.APPROVED, "manual", metadata=metadata)
        return True

    def queue(self, workflow_id: str, metadata: dict[str, Any] | None = None) -> bool:
        self.track_transition(workflow_id, LifecycleState.QUEUED, "scheduler", metadata=metadata)
        return True

    def run(
        self,
        workflow_id: str,
        dag: Any | None = None,
        user_id: str | None = None,
        household_id: str | None = None,
        context: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """
        Transition to RUNNING.

        If DAG + user/household context are provided, Step 17 gate is enforced:
          - REJECT => hard fail
          - REQUIRE_APPROVAL => hard fail
        """
        self._enforce_gate_if_supplied(dag, user_id, household_id, context)
        self.track_transition(workflow_id, LifecycleState.RUNNING, "engine", metadata=metadata)
        return True

    def complete(self, workflow_id: str, metadata: dict[str, Any] | None = None) -> bool:
        self.track_transition(workflow_id, LifecycleState.COMPLETED, "engine", metadata=metadata)
        return True

    def pause(
        self,
        workflow_id: str,
        *,
        safe_checkpoint: bool,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """
        Pause a running workflow.

        Must only happen after a safe checkpoint; otherwise fail hard.
        """
        if not safe_checkpoint:
            raise RuntimeError("Pause denied: safe checkpoint not reached")
        merged = dict(metadata or {})
        merged["safe_checkpoint"] = True
        self.track_transition(workflow_id, LifecycleState.PAUSED, "engine", metadata=merged)
        return True

    def resume(
        self,
        workflow_id: str,
        dag: Any | None = None,
        user_id: str | None = None,
        household_id: str | None = None,
        context: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """
        Resume paused workflow to RUNNING.

        Gate enforcement matches run().
        """
        self._enforce_gate_if_supplied(dag, user_id, household_id, context)
        self.track_transition(workflow_id, LifecycleState.RUNNING, "engine", metadata=metadata)
        return True

    def cancel(self, workflow_id: str, metadata: dict[str, Any] | None = None) -> bool:
        """
        Authoritatively cancel workflow.

        After CANCELLED, further scheduling/execution transitions are invalid
        by lifecycle policy and will fail hard.
        """
        self.track_transition(workflow_id, LifecycleState.CANCELLED, "manual", metadata=metadata)
        return True

    def bind_workflow(
        self,
        workflow_id: str,
        initial_state: LifecycleState = LifecycleState.CREATED,
        source: TransitionSource = "system",
        metadata: dict[str, Any] | None = None,
    ) -> WorkflowTransitionRecord:
        """
        Bind a workflow_id to an initial state.

        This creates the first append-only record for the workflow. If the
        workflow already exists, an error is raised to preserve deterministic
        history and avoid accidental rebinding.
        """
        if workflow_id in self._state_by_workflow:
            raise ValueError(f"Workflow '{workflow_id}' is already bound")

        record = self._append_record(
            workflow_id=workflow_id,
            previous_state=None,
            new_state=initial_state,
            source=source,
            metadata=metadata,
        )
        self._state_by_workflow[workflow_id] = initial_state
        self._indices_by_workflow[workflow_id] = [record.index]
        return record

    def get_state(self, workflow_id: str) -> LifecycleState | None:
        """Return current known state for the workflow_id, or None if unknown."""
        return self._state_by_workflow.get(workflow_id)

    def track_transition(
        self,
        workflow_id: str,
        new_state: LifecycleState,
        source: TransitionSource,
        metadata: dict[str, Any] | None = None,
    ) -> WorkflowTransitionRecord:
        """
        Validate and append a state transition for workflow_id.

        Rules:
          - workflow_id must already be bound
          - transition must be explicitly valid
          - no implicit multi-step transitions
        """
        if workflow_id not in self._state_by_workflow:
            raise KeyError(f"Unknown workflow_id '{workflow_id}'. Bind it first.")

        current = self._state_by_workflow[workflow_id]
        validate_transition(current, new_state)

        record = self._append_record(
            workflow_id=workflow_id,
            previous_state=current,
            new_state=new_state,
            source=source,
            metadata=metadata,
        )
        self._state_by_workflow[workflow_id] = new_state
        self._indices_by_workflow[workflow_id].append(record.index)
        return record

    def get_history(self, workflow_id: str) -> list[WorkflowTransitionRecord]:
        """Return transition history for workflow_id in append order."""
        indices = self._indices_by_workflow.get(workflow_id, [])
        return [self._audit_log[i] for i in indices]

    def audit_log(self) -> list[WorkflowTransitionRecord]:
        """Return full append-only audit log in global append order."""
        return list(self._audit_log)

    def replay(self, workflow_id: str) -> ReplayResult:
        """
        Replay workflow history from append-only records.

        Returns reconstructed initial state, final state, and all transitions.
        """
        transitions = self.get_history(workflow_id)
        if not transitions:
            return ReplayResult(
                workflow_id=workflow_id,
                initial_state=None,
                final_state=None,
                transitions=[],
            )

        initial = transitions[0].new_state if transitions[0].previous_state is None else transitions[0].previous_state
        final = transitions[-1].new_state
        return ReplayResult(
            workflow_id=workflow_id,
            initial_state=initial,
            final_state=final,
            transitions=transitions,
        )

    def _append_record(
        self,
        workflow_id: str,
        previous_state: LifecycleState | None,
        new_state: LifecycleState,
        source: TransitionSource,
        metadata: dict[str, Any] | None,
    ) -> WorkflowTransitionRecord:
        index = len(self._audit_log)
        record = WorkflowTransitionRecord(
            index=index,
            workflow_id=workflow_id,
            previous_state=previous_state,
            new_state=new_state,
            source=source,
            metadata=dict(metadata or {}),
        )
        self._audit_log.append(record)
        return record

    def _enforce_gate_if_supplied(
        self,
        dag: Any | None,
        user_id: str | None,
        household_id: str | None,
        context: dict[str, Any] | None,
    ) -> None:
        # Gate applies only when caller supplies full context.
        if dag is None or user_id is None or household_id is None:
            return

        decision = self._gate.evaluate(dag, user_id, household_id, context)
        if decision.status == ExecutionStatus.REJECT:
            raise PermissionError(f"Execution denied by safety gate: {decision.reasons}")
        if decision.status == ExecutionStatus.REQUIRE_APPROVAL:
            raise PermissionError(
                "Execution denied by safety gate: explicit approval required"
            )
