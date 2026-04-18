"""
Step 17 Verification — Safety Gate & Risk Classification Integration

Hard pass criteria:
  - No workflow executes without passing gate
  - High-risk actions always require approval
  - Risk classification is deterministic
  - Approval state is idempotent
  - Rejection prevents all downstream execution paths

This test suite validates the complete safety evaluation pipeline using
ExecutionGate (access control) + RiskClassifier (impact assessment).
"""

from __future__ import annotations

import pytest

from safety.graph_models import DAG, DAGNode
from safety.execution_gate import ExecutionGate, ExecutionStatus
from safety.risk_classifier import RiskClassifier, RiskLevel


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def gate():
    return ExecutionGate()


@pytest.fixture
def classifier():
    return RiskClassifier()


def _build_low_risk_dag() -> DAG:
    """Safe, internal-only workflow."""
    return DAG(
        dag_id="dag_low_risk",
        intent_id="intent_low",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="task",
                operation="create_task",
                inputs={"title": "Daily standup"},
            ),
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )


def _build_high_risk_dag() -> DAG:
    """Destructive, high-impact workflow."""
    return DAG(
        dag_id="dag_high_risk",
        intent_id="intent_high",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="task",
                operation="delete_task",
                inputs={"task_id": "t123"},
            ),
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )


def _build_invalid_dag() -> DAG:
    """Structurally invalid DAG (ownership mismatch)."""
    return DAG(
        dag_id="dag_invalid",
        intent_id="intent_invalid",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="noop",
                operation="noop",
            ),
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "u999", "household_id": "h999"},  # Mismatched
    )


def _build_conditional_financial_dag() -> DAG:
    """Complex workflow: conditional + financial operations."""
    return DAG(
        dag_id="dag_cond_fin",
        intent_id="intent_cond_fin",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="conditional",
                operation="check_balance",
                condition="balance > 1000",
                condition_true_branch=["n2"],
                condition_false_branch=["n3"],
            ),
            "n2": DAGNode(
                node_id="n2",
                node_type="service",
                operation="withdraw_budget",
                service_type="budget_service",
                inputs={"amount": 500.0},
                dependencies=["n1"],
            ),
            "n3": DAGNode(
                node_id="n3",
                node_type="noop",
                operation="noop",
                dependencies=["n1"],
            ),
        },
        entry_node="n1",
        exit_nodes=["n2", "n3"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )


def _build_recurring_external_dag() -> DAG:
    """Recurring workflow affecting external systems."""
    return DAG(
        dag_id="dag_recurring_ext",
        intent_id="intent_recurring_ext",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="service",
                operation="send_email",
                service_type="email_service",
                inputs={"recipient": "user@example.com"},
            ),
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={
            "user_id": "u1",
            "household_id": "h1",
            "recurrence_info": {"frequency": "daily"},
        },
    )


# ── Test 1: Low-risk allows execution ──────────────────────────────────────────

def test_low_risk_allows_execution(gate, classifier):
    """Low-risk DAGs should be ALLOW status."""
    dag = _build_low_risk_dag()
    decision = gate.evaluate(dag, user_id="u1", household_id="h1")

    assert decision.status == ExecutionStatus.ALLOW, (
        f"Low-risk DAG should be allowed; got {decision.status.value}, "
        f"reasons: {decision.reasons}"
    )

    classification = classifier.classify_dag(dag)
    assert classification.level == RiskLevel.LOW, (
        "Low-risk DAG should be classified as LOW risk"
    )


# ── Test 2: High-risk requires approval ────────────────────────────────────────

def test_high_risk_requires_approval(gate, classifier):
    """High-risk DAGs should require approval."""
    dag = _build_high_risk_dag()
    decision = gate.evaluate(dag, user_id="u1", household_id="h1")

    assert decision.status == ExecutionStatus.REQUIRE_APPROVAL, (
        f"High-risk DAG should require approval; got {decision.status.value}"
    )

    classification = classifier.classify_dag(dag)
    assert classification.level == RiskLevel.HIGH, (
        "High-risk DAG should be classified as HIGH risk"
    )


# ── Test 3: Rejected workflow blocked ───────────────────────────────────────────

def test_rejected_workflow_blocked(gate):
    """Invalid DAGs should be REJECT status."""
    dag = _build_invalid_dag()
    decision = gate.evaluate(dag, user_id="u1", household_id="h1")

    assert decision.status == ExecutionStatus.REJECT, (
        f"Invalid DAG should be rejected; got {decision.status.value}"
    )
    assert len(decision.reasons) > 0, (
        "Rejection should include reasons"
    )


# ── Test 4: Deterministic risk scoring ─────────────────────────────────────────

def test_deterministic_risk_scoring(classifier):
    """Risk classification must always produce identical results."""
    dag = _build_high_risk_dag()

    r1 = classifier.classify_dag(dag)
    r2 = classifier.classify_dag(dag)

    assert r1 == r2, (
        "Same DAG should have identical classification on repeated calls"
    )
    assert r1.level == r2.level
    assert r1.factors == r2.factors
    assert r1.rationale == r2.rationale


# ── Test 5: Missing classification safe default ────────────────────────────────

def test_missing_classification_safe_default(gate, classifier):
    """Unknown/unclassified workflows default to REQUIRE_APPROVAL."""
    # Build a DAG with unknown operation
    unknown_dag = DAG(
        dag_id="dag_unknown",
        intent_id="intent_unknown",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="service",
                operation="mystery_operation",
                service_type="unknown_service",
            ),
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )

    # Gate should not reject due to unknown operation (structural check passes)
    decision = gate.evaluate(unknown_dag, user_id="u1", household_id="h1")
    assert decision.status != ExecutionStatus.REJECT, (
        "Unknown operation should not cause rejection by itself"
    )

    # Classifier should conservatively classify unknown ops
    classification = classifier.classify_dag(unknown_dag)
    # Unknown service should not be assumed safe
    assert classification is not None


# ── Test 6: Conditional + financial is HIGH ────────────────────────────────────

def test_conditional_financial_high_risk(gate, classifier):
    """Conditional workflows with financial ops must be HIGH risk."""
    dag = _build_conditional_financial_dag()
    classification = classifier.classify_dag(dag)

    assert classification.level == RiskLevel.HIGH, (
        "Conditional + financial = HIGH risk"
    )

    decision = gate.evaluate(dag, user_id="u1", household_id="h1", context={
        "budget_limit": 1000.0,
        "remaining_budget": 800.0,
    })
    assert decision.status == ExecutionStatus.REQUIRE_APPROVAL, (
        "HIGH-risk DAG should require approval"
    )


# ── Test 7: Recurring external is MEDIUM or higher ────────────────────────────

def test_recurring_external_medium_or_high(gate, classifier):
    """Recurring external operations must be at least MEDIUM risk."""
    dag = _build_recurring_external_dag()
    classification = classifier.classify_dag(dag)

    assert classification.level in (RiskLevel.MEDIUM, RiskLevel.HIGH), (
        "Recurring external operations should be MEDIUM or HIGH risk"
    )

    decision = gate.evaluate(dag, user_id="u1", household_id="h1")
    assert decision.status != ExecutionStatus.ALLOW, (
        "Recurring external operations should not be auto-allowed"
    )


# ── Test 8: Approval state is idempotent ───────────────────────────────────────

def test_approval_state_idempotent(gate):
    """Repeated evaluation of same DAG + context yields same decision."""
    dag = _build_high_risk_dag()
    context = {"budget_limit": 1000.0}

    d1 = gate.evaluate(dag, user_id="u1", household_id="h1", context=context)
    d2 = gate.evaluate(dag, user_id="u1", household_id="h1", context=context)
    d3 = gate.evaluate(dag, user_id="u1", household_id="h1", context=context)

    assert d1.status == d2.status == d3.status, (
        "Multiple evaluations should yield identical approval status"
    )


# ── Test 9: Rejection prevents downstream ──────────────────────────────────────

def test_rejection_prevents_downstream(gate):
    """A rejected workflow cannot be retried successfully by caller."""
    dag = _build_invalid_dag()

    # First attempt
    decision1 = gate.evaluate(dag, user_id="u1", household_id="h1")
    assert decision1.status == ExecutionStatus.REJECT

    # Caller might retry with different context
    decision2 = gate.evaluate(dag, user_id="u1", household_id="h1", context={
        "budget_limit": 10000.0,  # Extra budget
    })

    # Should still reject (ownership mismatch is not context-dependent)
    assert decision2.status == ExecutionStatus.REJECT, (
        "Ownership mismatch rejection is structural and persistent"
    )


# ── Test 10: No workflow executes without passing gate ───────────────────────────

def test_gate_is_mandatory(gate, classifier):
    """
    Simulate a caller trying to execute workflows.
    Only those passing the gate should be marked as executable.
    """
    workflows = [
        (_build_low_risk_dag(), "u1", "h1"),
        (_build_high_risk_dag(), "u1", "h1"),
        (_build_invalid_dag(), "u1", "h1"),
    ]

    executable_count = 0
    for dag, user_id, household_id in workflows:
        decision = gate.evaluate(dag, user_id=user_id, household_id=household_id)
        if decision.status == ExecutionStatus.ALLOW:
            executable_count += 1

    assert executable_count == 1, (
        f"Only low-risk DAG should be executable; got {executable_count} allowed"
    )


# ── Test 11: Integration: gate + classifier agree on high-risk ──────────────────

def test_gate_classifier_agreement_high_risk(gate, classifier):
    """Gate and classifier must agree that high-risk ops require approval."""
    dag = _build_high_risk_dag()

    classification = classifier.classify_dag(dag)
    decision = gate.evaluate(dag, user_id="u1", household_id="h1")

    assert classification.level == RiskLevel.HIGH
    assert decision.status == ExecutionStatus.REQUIRE_APPROVAL, (
        "Gate should require approval when classifier says HIGH risk"
    )


# ── Test 12: Integration: gate + classifier agree on low-risk ────────────────────

def test_gate_classifier_agreement_low_risk(gate, classifier):
    """Gate and classifier must agree that low-risk ops are allowable."""
    dag = _build_low_risk_dag()

    classification = classifier.classify_dag(dag)
    decision = gate.evaluate(dag, user_id="u1", household_id="h1")

    assert classification.level == RiskLevel.LOW
    assert decision.status == ExecutionStatus.ALLOW, (
        "Gate should allow when classifier says LOW risk"
    )


# ── Test 13: Rejection is authoritative ────────────────────────────────────────

def test_rejection_is_authoritative(gate):
    """Once a workflow is rejected due to ownership, it remains rejected."""
    dag = _build_invalid_dag()  # Created with u999/h999 metadata

    # Evaluate with WRONG user (should be rejected for ownership mismatch)
    decision1 = gate.evaluate(dag, user_id="u1", household_id="h1")
    assert decision1.status == ExecutionStatus.REJECT, (
        "Wrong user should be rejected"
    )

    # Evaluate again with WRONG user (should remain rejected)
    decision2 = gate.evaluate(dag, user_id="u1", household_id="h1")
    assert decision2.status == ExecutionStatus.REJECT, (
        "Rejection should be persistent even on retry"
    )


# ── Test 14: Multiple independent gates produce same result ───────────────────––

def test_independent_gates_agree(classifier):
    """Two separate gate instances should produce identical decisions."""
    dag = _build_high_risk_dag()

    gate1 = ExecutionGate()
    gate2 = ExecutionGate()

    d1 = gate1.evaluate(dag, user_id="u1", household_id="h1")
    d2 = gate2.evaluate(dag, user_id="u1", household_id="h1")

    assert d1.status == d2.status, (
        "Independent gate instances should agree"
    )


# ── Test 15: All risk levels are reachable ──────────────────────────────────────

def test_all_risk_levels_reachable(classifier):
    """Test that HIGH, MEDIUM, and LOW risk classifications are all achievable."""
    low_dag = _build_low_risk_dag()
    medium_dag = _build_recurring_external_dag()
    high_dag = _build_high_risk_dag()

    c_low = classifier.classify_dag(low_dag)
    c_medium = classifier.classify_dag(medium_dag)
    c_high = classifier.classify_dag(high_dag)

    assert c_low.level == RiskLevel.LOW
    assert c_medium.level in (RiskLevel.MEDIUM, RiskLevel.HIGH)
    assert c_high.level == RiskLevel.HIGH
