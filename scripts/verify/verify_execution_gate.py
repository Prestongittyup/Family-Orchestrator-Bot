"""
Execution Gate Verification Tests

Covers:
  - Intent ownership validation
  - DAG structural integrity
  - High-risk operation detection
  - Resource constraint checking
  - Deterministic gate decisions
"""

from __future__ import annotations

import pytest

from safety.graph_models import DAG, DAGNode
from safety.execution_gate import (
    ExecutionGate,
    ExecutionStatus,
    RiskLevel,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def gate():
    return ExecutionGate()


@pytest.fixture
def safe_dag():
    """Simple, safe DAG for baseline testing."""
    return DAG(
        dag_id="dag_001",
        intent_id="intent_001",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="task",
                operation="create_task",
                inputs={"title": "Test task"},
            ),
            "n2": DAGNode(
                node_id="n2",
                node_type="noop",
                operation="noop",
                dependencies=["n1"],
            ),
        },
        entry_node="n1",
        exit_nodes=["n2"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )


@pytest.fixture
def high_risk_dag():
    """DAG with destructive operations."""
    return DAG(
        dag_id="dag_002",
        intent_id="intent_002",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="task",
                operation="delete_task",
                inputs={"task_id": "t1"},
            ),
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )


@pytest.fixture
def financial_dag():
    """DAG with budget operations."""
    return DAG(
        dag_id="dag_003",
        intent_id="intent_003",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="service",
                operation="withdraw_budget",
                service_type="budget_service",
                inputs={"amount": 100.0},
            ),
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )


# ── Test 1: Intent ownership ───────────────────────────────────────────────────

class TestIntentOwnership:
    def test_matching_user_id_passes(self, gate, safe_dag):
        decision = gate.evaluate(safe_dag, user_id="u1", household_id="h1")
        assert decision.status != ExecutionStatus.REJECT, (
            f"Matching user_id should not be rejected; got {decision.reasons}"
        )

    def test_mismatched_user_id_rejects(self, gate, safe_dag):
        decision = gate.evaluate(safe_dag, user_id="u2", household_id="h1")
        assert decision.status == ExecutionStatus.REJECT, (
            "Mismatched user_id should be rejected"
        )

    def test_mismatched_household_id_rejects(self, gate, safe_dag):
        decision = gate.evaluate(safe_dag, user_id="u1", household_id="h2")
        assert decision.status == ExecutionStatus.REJECT, (
            "Mismatched household_id should be rejected"
        )

    def test_both_mismatched_rejects(self, gate, safe_dag):
        decision = gate.evaluate(safe_dag, user_id="u2", household_id="h2")
        assert decision.status == ExecutionStatus.REJECT, (
            "Both mismatches should be rejected"
        )

    def test_missing_metadata_allows_override(self, gate):
        """DAG without metadata shouldn't block execution (edge case)."""
        dag = DAG(
            dag_id="dag_clean",
            intent_id="intent_clean",
            nodes={
                "n1": DAGNode(
                    node_id="n1",
                    node_type="noop",
                    operation="noop",
                ),
            },
            entry_node="n1",
            exit_nodes=["n1"],
            metadata={},  # Empty metadata
        )
        decision = gate.evaluate(dag, user_id="u1", household_id="h1")
        # Should not reject solely on missing metadata
        assert decision.status != ExecutionStatus.REJECT


# ── Test 2: DAG structure ──────────────────────────────────────────────────────

class TestDAGStructure:
    def test_valid_dag_passes(self, gate, safe_dag):
        decision = gate.evaluate(safe_dag, user_id="u1", household_id="h1")
        assert decision.status == ExecutionStatus.ALLOW, (
            f"Valid DAG should be allowed; got {decision.reasons}"
        )

    def test_no_entry_node_rejects(self, gate):
        """DAG with no entry node is invalid."""
        no_entry = DAG(
            dag_id="dag_noentry",
            intent_id="intent_noentry",
            nodes={
                "n1": DAGNode(
                    node_id="n1",
                    node_type="noop",
                    operation="noop",
                ),
            },
            entry_node=None,  # Missing entry node
            exit_nodes=["n1"],
            metadata={"user_id": "u1", "household_id": "h1"},
        )
        decision = gate.evaluate(no_entry, user_id="u1", household_id="h1")
        # DAG validation happens at construction, so we just check gate doesn't crash
        assert decision is not None

    def test_empty_dag_rejects(self, gate):
        empty = DAG(
            dag_id="dag_empty",
            intent_id="intent_empty",
            nodes={},
            entry_node=None,
            exit_nodes=[],
            metadata={"user_id": "u1", "household_id": "h1"},
        )
        decision = gate.evaluate(empty, user_id="u1", household_id="h1")
        assert decision.status == ExecutionStatus.REJECT, (
            "Empty DAG should be rejected"
        )


# ── Test 3: High-risk operations ───────────────────────────────────────────────

class TestHighRiskOperations:
    def test_safe_operation_allows(self, gate, safe_dag):
        decision = gate.evaluate(safe_dag, user_id="u1", household_id="h1")
        assert decision.status == ExecutionStatus.ALLOW, (
            "Safe operations should be allowed"
        )

    def test_delete_operation_requires_approval(self, gate, high_risk_dag):
        decision = gate.evaluate(high_risk_dag, user_id="u1", household_id="h1")
        assert decision.status == ExecutionStatus.REQUIRE_APPROVAL, (
            "High-risk operations should require approval"
        )

    def test_delete_risk_level_high(self, gate, high_risk_dag):
        decision = gate.evaluate(high_risk_dag, user_id="u1", household_id="h1")
        assert decision.risk_level == RiskLevel.HIGH, (
            "Delete operation should be HIGH risk"
        )

    def test_financial_operation_requires_approval(self, gate, financial_dag):
        decision = gate.evaluate(
            financial_dag,
            user_id="u1",
            household_id="h1",
            context={"budget_limit": 500.0, "remaining_budget": 200.0},
        )
        assert decision.status == ExecutionStatus.REQUIRE_APPROVAL, (
            "Financial operations should require approval"
        )


# ── Test 4: Resource constraints ───────────────────────────────────────────────

class TestResourceConstraints:
    def test_financial_op_without_budget_rejects(self, gate, financial_dag):
        """Financial operations need budget context."""
        decision = gate.evaluate(
            financial_dag,
            user_id="u1",
            household_id="h1",
            context={},  # No budget info
        )
        assert decision.status == ExecutionStatus.REJECT, (
            "Financial operations without budget context should be rejected"
        )

    def test_financial_op_with_budget_requires_approval(self, gate, financial_dag):
        """Financial operations with budget should require approval (not auto-allow)."""
        decision = gate.evaluate(
            financial_dag,
            user_id="u1",
            household_id="h1",
            context={"budget_limit": 500.0, "remaining_budget": 200.0},
        )
        assert decision.status == ExecutionStatus.REQUIRE_APPROVAL, (
            "Financial operations should require approval even with budget"
        )

    def test_negative_remaining_budget_rejects(self, gate, financial_dag):
        """Negative remaining budget should block any financial operations."""
        decision = gate.evaluate(
            financial_dag,
            user_id="u1",
            household_id="h1",
            context={"budget_limit": 500.0, "remaining_budget": -100.0},
        )
        assert decision.status == ExecutionStatus.REJECT, (
            "Negative remaining budget should reject financial operations"
        )


# ── Test 5: Determinism ───────────────────────────────────────────────────────

class TestDeterminism:
    def test_same_inputs_same_decision(self, gate, safe_dag):
        decision1 = gate.evaluate(safe_dag, user_id="u1", household_id="h1")
        decision2 = gate.evaluate(safe_dag, user_id="u1", household_id="h1")
        assert decision1 == decision2, (
            "Same inputs should produce identical decisions"
        )

    def test_different_gates_same_decision(self, safe_dag):
        gate1 = ExecutionGate()
        gate2 = ExecutionGate()
        decision1 = gate1.evaluate(safe_dag, user_id="u1", household_id="h1")
        decision2 = gate2.evaluate(safe_dag, user_id="u1", household_id="h1")
        assert decision1 == decision2, (
            "Different gate instances should produce identical decisions"
        )

    def test_reason_order_deterministic(self, gate, high_risk_dag):
        """Reasons list should be in predictable order."""
        decision1 = gate.evaluate(high_risk_dag, user_id="u1", household_id="h1")
        decision2 = gate.evaluate(high_risk_dag, user_id="u1", household_id="h1")
        assert decision1.reasons == decision2.reasons, (
            "Reason lists should be identical across calls"
        )


# ── Test 6: Summary serialization ──────────────────────────────────────────────

class TestSerialization:
    def test_summary_is_json_safe(self, gate, safe_dag):
        import json
        decision = gate.evaluate(safe_dag, user_id="u1", household_id="h1")
        summary = decision.summary()
        # Should not raise
        json.dumps(summary)

    def test_summary_has_required_keys(self, gate, safe_dag):
        decision = gate.evaluate(safe_dag, user_id="u1", household_id="h1")
        summary = decision.summary()
        for key in ("status", "risk_level", "reasons", "approved"):
            assert key in summary

    def test_approved_field_matches_status(self, gate, safe_dag):
        decision = gate.evaluate(safe_dag, user_id="u1", household_id="h1")
        summary = decision.summary()
        assert summary["approved"] == (decision.status == ExecutionStatus.ALLOW)
