"""
Execution Gate — Safety evaluation for compiled workflows.

Pure evaluator that determines whether a DAG is permitted to execute.
No state is modified, no side effects occur.

The gate checks:
  1. Intent ownership (user and household context)
  2. DAG structural validity
  3. High-risk operation detection
  4. Resource constraint satisfaction

If any gate fails, the decision is REJECT + reasoning.  If all gates pass
but high-risk operations are present, the decision is REQUIRE_APPROVAL.
Otherwise, ALLOW.

Design
------
- STATELESS — each check is a pure function over DAG + context
- DETERMINISTIC — same DAG + context → identical decision
- NO MUTATION — the DAG is never modified
- NO EXECUTION — no code is executed, no workflows are triggered
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from safety.graph_models import DAG, DAGNode


# ── Enums ──────────────────────────────────────────────────────────────────────

class ExecutionStatus(str, Enum):
    """Permission status for workflow execution."""

    ALLOW = "allow"
    """The DAG is safe and permitted to execute immediately."""

    REJECT = "reject"
    """The DAG is not safe and must not execute."""

    REQUIRE_APPROVAL = "require_approval"
    """The DAG is safe but contains high-risk operations requiring human approval."""


class RiskLevel(str, Enum):
    """Overall risk classification for the workflow."""

    LOW = "low"
    """No high-risk operations detected."""

    MEDIUM = "medium"
    """High-risk operations present but controllable."""

    HIGH = "high"
    """Dangerous operations or failed safety checks."""


# ── Output ─────────────────────────────────────────────────────────────────────

@dataclass
class ExecutionDecision:
    """
    Immutable decision record for workflow execution gating.

    Carries the decision status, risk level, and human-readable reasons.
    """

    status: ExecutionStatus
    """ALLOW | REJECT | REQUIRE_APPROVAL."""

    risk_level: RiskLevel
    """LOW | MEDIUM | HIGH."""

    reasons: list[str] = field(default_factory=list)
    """Plain English explanations of the decision (may be empty for ALLOW)."""

    def summary(self) -> dict[str, Any]:
        """lightweight dict for logging or API response."""
        return {
            "status": self.status.value,
            "risk_level": self.risk_level.value,
            "reasons": self.reasons,
            "approved": self.status == ExecutionStatus.ALLOW,
        }


# ── Safety checks (module-level for reuse and testing) ────────────────────────

def _check_intent_ownership(
    dag: DAG,
    user_id: str,
    household_id: str,
) -> tuple[bool, list[str]]:
    """
    Verify user/household ownership of the intent that generated the DAG.

    Returns:
        (is_valid, reasons) — is_valid=True if no ownership mismatch detected.
    """
    reasons: list[str] = []

    dag_user = dag.metadata.get("user_id")
    dag_household = dag.metadata.get("household_id")

    if dag_user and dag_user != user_id:
        reasons.append(
            f"DAG was created by user_id='{dag_user}' but request is from '{user_id}'"
        )

    if dag_household and dag_household != household_id:
        reasons.append(
            f"DAG was created for household_id='{dag_household}' "
            f"but request is for '{household_id}'"
        )

    return len(reasons) == 0, reasons


def _check_dag_structure(dag: DAG) -> tuple[bool, list[str]]:
    """
    Validate basic DAG structural integrity.

    Returns:
        (is_valid, reasons) — obvious structural errors surface immediately.
    """
    reasons: list[str] = []

    if not dag.nodes:
        reasons.append("DAG contains no nodes")

    if dag.entry_node and dag.entry_node not in dag.nodes:
        reasons.append(f"Entry node '{dag.entry_node}' not in DAG")

    missing_exits = [e for e in dag.exit_nodes if e not in dag.nodes]
    if missing_exits:
        reasons.append(f"Exit nodes {missing_exits} not in DAG")

    # Check for unreachable nodes (simple heuristic: no path from entry)
    # Build reverse dependency map: for each node, track which nodes depend on it
    if dag.entry_node and dag.entry_node in dag.nodes:
        depends_on: dict[str, list[str]] = {nid: [] for nid in dag.nodes}
        for nid, node in dag.nodes.items():
            for dep_id in node.dependencies:
                if dep_id in depends_on:
                    depends_on[dep_id].append(nid)

        reachable: set[str] = {dag.entry_node}
        queue: list[str] = [dag.entry_node]
        while queue:
            node_id = queue.pop(0)
            for downstream_id in depends_on.get(node_id, []):
                if downstream_id not in reachable:
                    reachable.add(downstream_id)
                    queue.append(downstream_id)
        unreachable = set(dag.nodes.keys()) - reachable
        if unreachable:
            reasons.append(f"Unreachable nodes: {unreachable}")

    return len(reasons) == 0, reasons


def _classify_node_risk(node: DAGNode) -> RiskLevel:
    """
    Determine risk level of a single DAG node operation.

    High-risk operations:
      - Delete, archive, remove (destructive)
      - Modify budget or financial settings
      - Send external notifications
      - Bulk operations
    """
    high_risk_operations = {
        # Destructive
        "delete_task",
        "delete_item",
        "archive_items",
        "remove_member",
        # Financial
        "withdraw_budget",
        "transfer_funds",
        "set_budget_limit",
        # External comms
        "send_email",
        "send_sms",
        "post_notification",
        # Bulk
        "bulk_update",
        "bulk_delete",
    }

    if node.operation in high_risk_operations:
        return RiskLevel.HIGH

    medium_risk_operations = {
        "modify_task",
        "reschedule",
        "update_preferences",
        "sync_external_calendar",
    }

    if node.operation in medium_risk_operations:
        return RiskLevel.MEDIUM

    return RiskLevel.LOW


def _check_high_risk_operations(dag: DAG) -> tuple[RiskLevel, list[str]]:
    """
    Scan DAG for high or medium-risk operations.

    Returns:
        (max_risk_level, [descriptions]) — descriptions explain which
        operations triggered the risk classification.
    """
    max_risk = RiskLevel.LOW
    reasons: list[str] = []

    for node_id, node in dag.nodes.items():
        node_risk = _classify_node_risk(node)
        if node_risk == RiskLevel.HIGH:
            max_risk = RiskLevel.HIGH
            reasons.append(
                f"Node '{node_id}' performs high-risk operation: {node.operation}"
            )
        elif node_risk == RiskLevel.MEDIUM and max_risk != RiskLevel.HIGH:
            max_risk = RiskLevel.MEDIUM
            reasons.append(
                f"Node '{node_id}' performs medium-risk operation: {node.operation}"
            )

    return max_risk, reasons


def _check_resource_constraints(
    dag: DAG,
    context: dict[str, Any],
) -> tuple[bool, list[str]]:
    """
    Verify the DAG respects resource constraints from household context.

    Checks:
      - Budget limit (if financial operations are present)
      - Rate limits (if bulk operations are present)

    Args:
        dag: The workflow DAG.
        context: Optional context dict with keys like
          "budget_limit", "remaining_budget", "rate_limit_per_hour", etc.

    Returns:
        (is_valid, reasons) — is_valid=False if a constraint is violated.
    """
    reasons: list[str] = []
    context = context or {}

    # Check budget if financial operations are present
    has_financial_ops = any(
        "budget" in node.operation.lower() or "withdraw" in node.operation.lower()
        for node in dag.nodes.values()
    )

    if has_financial_ops:
        budget_limit = context.get("budget_limit")
        remaining_budget = context.get("remaining_budget")

        if budget_limit is None:
            reasons.append(
                "DAG contains financial operations but no budget_limit is set"
            )

        if remaining_budget is not None and remaining_budget < 0:
            reasons.append(
                f"Remaining budget is negative (${remaining_budget:.2f}); "
                "cannot execute financial operations"
            )

    # Check rate limits if bulk operations are present
    has_bulk_ops = any(
        "bulk" in node.operation.lower()
        for node in dag.nodes.values()
    )

    if has_bulk_ops:
        rate_limit = context.get("rate_limit_per_hour")
        if rate_limit is not None and rate_limit < 1:
            reasons.append(
                f"Bulk operations are rate-limited to {rate_limit} per hour; "
                "cannot bulk execute now"
            )

    return len(reasons) == 0, reasons


# ── Gate ───────────────────────────────────────────────────────────────────────

class ExecutionGate:
    """
    Evaluates whether a compiled DAG is safe to execute.

    Combines multiple safety checks into a single decision. The gate is
    stateless; identical inputs always produce identical decisions.

    Usage::

        gate = ExecutionGate()
        decision = gate.evaluate(
            dag=compiled_dag,
            user_id="u1",
            household_id="h1",
            context={"budget_limit": 1000.0, "remaining_budget": 500.0},
        )
        if decision.status == ExecutionStatus.ALLOW:
            # execute the DAG
        elif decision.status == ExecutionStatus.REQUIRE_APPROVAL:
            # queue for human review
        else:
            # reject and log reasons
    """

    def evaluate(
        self,
        dag: DAG,
        user_id: str,
        household_id: str,
        context: dict[str, Any] | None = None,
    ) -> ExecutionDecision:
        """
        Evaluate execution safety for a DAG.

        Args:
            dag:           The compiled workflow DAG.
            user_id:       ID of the user requesting execution.
            household_id:  ID of the household context.
            context:       Optional context dict (budget, rate limits, etc.).

        Returns:
            ExecutionDecision with status, risk level, and reasons.
        """
        context = context or {}
        all_reasons: list[str] = []

        # Check 1: Intent ownership
        ownership_ok, ownership_reasons = _check_intent_ownership(
            dag, user_id, household_id
        )
        all_reasons.extend(ownership_reasons)

        if not ownership_ok:
            return ExecutionDecision(
                status=ExecutionStatus.REJECT,
                risk_level=RiskLevel.HIGH,
                reasons=all_reasons,
            )

        # Check 2: DAG structural validity
        structure_ok, structure_reasons = _check_dag_structure(dag)
        all_reasons.extend(structure_reasons)

        if not structure_ok:
            return ExecutionDecision(
                status=ExecutionStatus.REJECT,
                risk_level=RiskLevel.HIGH,
                reasons=all_reasons,
            )

        # Check 3: Resource constraints
        constraints_ok, constraint_reasons = _check_resource_constraints(
            dag, context
        )
        all_reasons.extend(constraint_reasons)

        if not constraints_ok:
            return ExecutionDecision(
                status=ExecutionStatus.REJECT,
                risk_level=RiskLevel.HIGH,
                reasons=all_reasons,
            )

        # Check 4: High-risk operations
        op_risk, op_reasons = _check_high_risk_operations(dag)

        if op_risk == RiskLevel.HIGH:
            return ExecutionDecision(
                status=ExecutionStatus.REQUIRE_APPROVAL,
                risk_level=RiskLevel.HIGH,
                reasons=op_reasons,
            )

        if op_risk == RiskLevel.MEDIUM:
            return ExecutionDecision(
                status=ExecutionStatus.REQUIRE_APPROVAL,
                risk_level=RiskLevel.MEDIUM,
                reasons=op_reasons,
            )

        # All checks passed, no high-risk operations
        return ExecutionDecision(
            status=ExecutionStatus.ALLOW,
            risk_level=RiskLevel.LOW,
            reasons=[],
        )
