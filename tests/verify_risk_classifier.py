"""
Risk Classifier Verification Tests

Covers:
  - DAG-based classification (operations)
  - Intent-based classification (types + recurrence)
  - Hybrid classification (DAG + Intent combined)
  - Risk factor detection and ordering
  - Deterministic classifications
  - Serialization
"""

from __future__ import annotations

import pytest

from legacy.compiler.intent_parser import Intent, IntentParser
from safety.graph_models import DAG, DAGNode
from safety.execution_gate import RiskLevel
from safety.risk_classifier import RiskClassifier


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def classifier():
    return RiskClassifier()


@pytest.fixture
def parser():
    return IntentParser()


@pytest.fixture
def financial_dag():
    """DAG with financial operations."""
    return DAG(
        dag_id="dag_financial",
        intent_id="intent_financial",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="service",
                operation="withdraw_budget",
                service_type="budget_service",
            ),
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )


@pytest.fixture
def external_dag():
    """DAG with external system operations."""
    return DAG(
        dag_id="dag_external",
        intent_id="intent_external",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="service",
                operation="send_email",
                service_type="email_service",
            ),
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )


@pytest.fixture
def irreversible_dag():
    """DAG with irreversible operations."""
    return DAG(
        dag_id="dag_delete",
        intent_id="intent_delete",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="task",
                operation="delete_task",
            ),
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )


@pytest.fixture
def low_risk_dag():
    """DAG with low-risk operations."""
    return DAG(
        dag_id="dag_safe",
        intent_id="intent_safe",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="task",
                operation="create_task",
            ),
        },
        entry_node="n1",
        exit_nodes=["n1"],
        metadata={"user_id": "u1", "household_id": "h1"},
    )


@pytest.fixture
def conditional_external_dag():
    """DAG with conditional logic and external operations."""
    return DAG(
        dag_id="dag_cond_ext",
        intent_id="intent_cond_ext",
        nodes={
            "n1": DAGNode(
                node_id="n1",
                node_type="conditional",
                operation="check_schedule",
                condition="budget > 100",
                condition_true_branch=["n2"],
                condition_false_branch=["n3"],
            ),
            "n2": DAGNode(
                node_id="n2",
                node_type="service",
                operation="send_email",
                service_type="email_service",
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


# ── Test 1: DAG classification ─────────────────────────────────────────────────

class TestDAGClassification:
    def test_financial_ops_high_risk(self, classifier, financial_dag):
        classification = classifier.classify_dag(financial_dag)
        assert classification.level == RiskLevel.HIGH, (
            "Financial operations should be HIGH risk"
        )
        assert "Financial" in " ".join(classification.factors), (
            "Factors should mention financial operation"
        )

    def test_external_ops_medium_risk(self, classifier, external_dag):
        classification = classifier.classify_dag(external_dag)
        assert classification.level == RiskLevel.MEDIUM, (
            "External operations should be MEDIUM risk"
        )

    def test_irreversible_ops_high_risk(self, classifier, irreversible_dag):
        classification = classifier.classify_dag(irreversible_dag)
        assert classification.level == RiskLevel.HIGH, (
            "Irreversible operations should be HIGH risk"
        )

    def test_low_risk_ops_low_risk(self, classifier, low_risk_dag):
        classification = classifier.classify_dag(low_risk_dag)
        assert classification.level == RiskLevel.LOW, (
            "Low-risk operations should be LOW risk"
        )

    def test_conditional_external_high_risk(self, classifier, conditional_external_dag):
        classification = classifier.classify_dag(conditional_external_dag)
        assert classification.level == RiskLevel.HIGH, (
            "Conditional + external should be HIGH risk"
        )

    def test_factors_present_and_sorted(self, classifier, financial_dag):
        classification = classifier.classify_dag(financial_dag)
        assert len(classification.factors) > 0, "Should detect at least one factor"
        # Factors should be sorted
        assert classification.factors == sorted(classification.factors), (
            "Factors should be deterministically sorted"
        )

    def test_rationale_provided(self, classifier, low_risk_dag):
        classification = classifier.classify_dag(low_risk_dag)
        assert len(classification.rationale) > 0, "Rationale should be provided"


# ── Test 2: Intent classification ──────────────────────────────────────────────

class TestIntentClassification:
    def test_task_creation_low_risk(self, classifier, parser):
        intent = parser.parse("Create a task", household_id="h1", user_id="u1")
        classification = classifier.classify_intent(intent)
        assert classification.level == RiskLevel.LOW, (
            "task_creation should be LOW risk"
        )

    def test_schedule_change_medium_risk(self, classifier, parser):
        intent = parser.parse(
            "Reschedule the meeting",
            household_id="h1",
            user_id="u1",
        )
        classification = classifier.classify_intent(intent)
        assert classification.level == RiskLevel.MEDIUM, (
            "schedule_change should be MEDIUM risk"
        )

    def test_recurring_schedule_change_high_risk(self, classifier, parser):
        intent = parser.parse(
            "Reschedule the meeting every Monday",
            household_id="h1",
            user_id="u1",
        )
        classification = classifier.classify_intent(intent)
        # If recurring + schedule_change, should be HIGH
        if intent.recurrence_hints.get("is_recurring"):
            assert classification.level == RiskLevel.HIGH, (
                "Recurring schedule changes should be HIGH risk"
            )

    def test_reminder_set_low_risk(self, classifier, parser):
        intent = parser.parse("Remind me daily", household_id="h1", user_id="u1")
        classification = classifier.classify_intent(intent)
        assert classification.level == RiskLevel.LOW, (
            "reminder_set should be LOW risk"
        )

    def test_budget_query_high_risk(self, classifier, parser):
        intent = parser.parse("What is my budget?", household_id="h1", user_id="u1")
        classification = classifier.classify_intent(intent)
        assert classification.level == RiskLevel.HIGH, (
            "budget_query should be HIGH risk"
        )

    def test_intent_type_factor_present(self, classifier, parser):
        intent = parser.parse("Create a task", household_id="h1", user_id="u1")
        classification = classifier.classify_intent(intent)
        assert any(
            "task_creation" in f for f in classification.factors
        ), "Intent type should be in factors"


# ── Test 3: Hybrid classification ──────────────────────────────────────────────

class TestHybridClassification:
    def test_low_dag_low_intent_is_low(self, classifier, low_risk_dag, parser):
        intent = parser.parse("Create a task", household_id="h1", user_id="u1")
        classification = classifier.classify_hybrid(low_risk_dag, intent)
        assert classification.level == RiskLevel.LOW, (
            "Low DAG + low intent should be LOW"
        )

    def test_high_dag_low_intent_is_high(self, classifier, financial_dag, parser):
        intent = parser.parse("Create a task", household_id="h1", user_id="u1")
        classification = classifier.classify_hybrid(financial_dag, intent)
        assert classification.level == RiskLevel.HIGH, (
            "High DAG overrides low intent"
        )

    def test_low_dag_high_intent_is_high(self, classifier, low_risk_dag, parser):
        intent = parser.parse("What is my budget?", household_id="h1", user_id="u1")
        classification = classifier.classify_hybrid(low_risk_dag, intent)
        assert classification.level == RiskLevel.HIGH, (
            "High intent overrides low DAG"
        )

    def test_without_intent_dag_only(self, classifier, low_risk_dag):
        classification = classifier.classify_hybrid(low_risk_dag, intent=None)
        assert classification.level == RiskLevel.LOW, (
            "DAG-only classification should work"
        )

    def test_hybrid_combines_factors(self, classifier, external_dag, parser):
        intent = parser.parse("Remind me daily", household_id="h1", user_id="u1")
        classification = classifier.classify_hybrid(external_dag, intent)
        # Should have factors from both DAG and Intent
        assert len(classification.factors) >= 2, (
            "Hybrid should combine factors from DAG and Intent"
        )


# ── Test 4: Determinism ───────────────────────────────────────────────────────

class TestDeterminism:
    def test_same_dag_same_classification(self, classifier, low_risk_dag):
        c1 = classifier.classify_dag(low_risk_dag)
        c2 = classifier.classify_dag(low_risk_dag)
        assert c1 == c2, "Same DAG should always classify identically"

    def test_same_intent_same_classification(self, classifier, parser):
        intent = parser.parse("Create a task", household_id="h1", user_id="u1")
        c1 = classifier.classify_intent(intent)
        c2 = classifier.classify_intent(intent)
        assert c1 == c2, "Same intent should always classify identically"

    def test_different_classifiers_same_result(self, low_risk_dag):
        c1 = RiskClassifier().classify_dag(low_risk_dag)
        c2 = RiskClassifier().classify_dag(low_risk_dag)
        assert c1 == c2, (
            "Different classifier instances should produce identical results"
        )

    def test_factor_ordering_deterministic(self, classifier, external_dag):
        c1 = classifier.classify_dag(external_dag)
        c2 = classifier.classify_dag(external_dag)
        assert c1.factors == c2.factors, (
            "Factor list should be in identical order"
        )


# ── Test 5: Serialization ──────────────────────────────────────────────────────

class TestSerialization:
    def test_summary_json_safe(self, classifier, low_risk_dag):
        import json
        classification = classifier.classify_dag(low_risk_dag)
        summary = classification.summary()
        json.dumps(summary)  # Should not raise

    def test_summary_has_required_keys(self, classifier, low_risk_dag):
        classification = classifier.classify_dag(low_risk_dag)
        summary = classification.summary()
        for key in ("risk_level", "factors", "rationale"):
            assert key in summary, f"Missing key: {key}"

    def test_summary_risk_level_matches(self, classifier, financial_dag):
        classification = classifier.classify_dag(financial_dag)
        summary = classification.summary()
        assert summary["risk_level"] == "high"


# ── Test 6: Edge cases ─────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_multiple_risk_factors_consolidated(self, classifier):
        """DAG with both financial and irreversible ops."""
        multi_risk = DAG(
            dag_id="dag_multi",
            intent_id="intent_multi",
            nodes={
                "n1": DAGNode(
                    node_id="n1",
                    node_type="service",
                    operation="withdraw_budget",
                    service_type="budget_service",
                ),
                "n2": DAGNode(
                    node_id="n2",
                    node_type="task",
                    operation="delete_task",
                    dependencies=["n1"],
                ),
            },
            entry_node="n1",
            exit_nodes=["n2"],
            metadata={"user_id": "u1", "household_id": "h1"},
        )
        classification = classifier.classify_dag(multi_risk)
        assert classification.level == RiskLevel.HIGH, (
            "Multiple high-risk factors should result in HIGH"
        )
        assert len(classification.factors) >= 2, (
            "Should detect multiple risk factors"
        )

    def test_no_factors_for_empty_dag(self, classifier):
        """Empty DAG should have minimal factors."""
        empty_dag = DAG(
            dag_id="empty",
            intent_id="empty_intent",
            nodes={
                "noop": DAGNode(
                    node_id="noop",
                    node_type="noop",
                    operation="noop",
                ),
            },
            entry_node="noop",
            exit_nodes=["noop"],
            metadata={},
        )
        classification = classifier.classify_dag(empty_dag)
        assert classification.level == RiskLevel.LOW
