"""
Risk Classifier — Deterministic workflow risk assessment based on structure.

Analyzes a DAG or Intent to assign a risk level reflecting the potential
impact of execution.  No execution occurs; the classifier only evaluates
structure and metadata.

Risk levels
-----------
HIGH:
  - Financial operations (withdraw, transfer, set limits)
  - External system modifications (email send, calendar delete/modify)
  - Irreversible operations (delete, archive, remove)
  - Multi-step conditional workflows affecting external systems

MEDIUM:
  - Scheduled changes to external systems (reschedule, sync)
  - Recurring automation affecting external state (recurrence + external mod)

LOW:
  - Reminders (passive notifications)
  - Internal tracking (create task, update status)
  - Read-only operations (query, check, list)

Design
------
- STATELESS — same input → identical classification
- NO EXECUTION — only analyzes structure
- NO MUTATION — DAG/Intent unchanged
- DETERMINISTIC — factors list and rationale are fully ordered
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from legacy.compiler.intent_parser import Intent
from safety.graph_models import DAG, DAGNode
from safety.execution_gate import RiskLevel


# ── Output ─────────────────────────────────────────────────────────────────────

@dataclass
class RiskClassification:
    """
    Deterministic risk assessment result for a workflow.

    Combines the assigned risk level with the factors and rationale that
    led to that classification.
    """

    level: RiskLevel
    """HIGH | MEDIUM | LOW."""

    factors: list[str] = field(default_factory=list)
    """
    Specific risk factors detected (e.g., "Financial operation: withdraw_budget",
    "External system: email_service", "Irreversible: delete_task").
    Sorted for determinism.
    """

    rationale: str = ""
    """Human-readable summary of the classification."""

    def summary(self) -> dict[str, Any]:
        """lightweight dict for logging or API response."""
        return {
            "risk_level": self.level.value,
            "factors": self.factors,
            "rationale": self.rationale,
        }


# ── Classifier ─────────────────────────────────────────────────────────────────

class RiskClassifier:
    """
    Assigns risk levels to workflows based on structural analysis.

    The classifier evaluates DAGs or Intents independently, applying a set
    of deterministic rules.  Multiple factors may contribute to a single
    risk level.
    """

    # ── High-risk operation patterns ────────────────────────────────

    _FINANCIAL_OPERATIONS = {
        "withdraw_budget",
        "transfer_funds",
        "set_budget_limit",
        "deduct_from_budget",
        "charge_account",
        "process_payment",
    }

    _EXTERNAL_SYSTEM_OPERATIONS = {
        # Email
        "send_email",
        "send_sms",
        # Calendar
        "delete_event",
        "modify_event",
        "sync_external_calendar",
        # External integrations
        "post_notification",
        "send_webhook",
        "call_external_api",
    }

    _IRREVERSIBLE_OPERATIONS = {
        "delete_task",
        "delete_item",
        "remove_member",
        "delete_event",
        "archive_items",
        "purge_data",
    }

    # ── Medium-risk operation patterns ─────────────────────────────

    _SCHEDULED_EXTERNAL_OPERATIONS = {
        "reschedule",
        "reschedule_event",
        "sync_external_calendar",
        "modify_event",
    }

    # ── Low-risk operation patterns ────────────────────────────────

    _READONLY_OPERATIONS = {
        "check_budget",
        "query_schedule",
        "list_tasks",
        "show_inventory",
        "get_status",
        "read_event",
    }

    _REMINDER_OPERATIONS = {
        "create_reminder",
        "set_reminder",
        "send_reminder",
    }

    _INTERNAL_OPERATIONS = {
        "create_task",
        "update_task",
        "update_status",
        "mark_complete",
        "set_priority",
    }

    def classify_dag(self, dag: DAG) -> RiskClassification:
        """
        Classify a compiled DAG by analyzing its operations.

        Args:
            dag: The workflow DAG to evaluate.

        Returns:
            RiskClassification with level, factors, and rationale.
        """
        factors: list[str] = []

        # Scan all nodes for risk patterns
        has_financial = False
        has_external = False
        has_irreversible = False
        has_readonly = False
        has_scheduled_external = False
        has_recurring_external = False

        for node_id, node in dag.nodes.items():
            if node.operation in self._FINANCIAL_OPERATIONS:
                has_financial = True
                factors.append(f"Financial: {node.operation}")

            if node.operation in self._EXTERNAL_SYSTEM_OPERATIONS:
                has_external = True
                factors.append(f"External system: {node.operation}")

            if node.operation in self._IRREVERSIBLE_OPERATIONS:
                has_irreversible = True
                factors.append(f"Irreversible: {node.operation}")

            if node.operation in self._READONLY_OPERATIONS:
                has_readonly = True

            if node.operation in self._SCHEDULED_EXTERNAL_OPERATIONS:
                has_scheduled_external = True
                factors.append(f"Scheduled external: {node.operation}")

        # Check for recurring workflows
        is_recurring = dag.metadata.get("recurrence_info") is not None
        if is_recurring and has_external:
            has_recurring_external = True
            factors.append("Recurring + external state modification")

        # Check for multi-step conditional workflows affecting external systems
        has_conditional = any(
            node.node_type == "conditional" for node in dag.nodes.values()
        )
        if has_conditional and has_external:
            factors.append("Multi-step conditional + external operations")

        # Determine risk level
        if has_financial or has_irreversible or (has_conditional and has_external):
            level = RiskLevel.HIGH
            rationale = (
                "HIGH: Contains irreversible, financial, or complex conditional "
                "external operations"
            )
        elif has_external or has_scheduled_external or has_recurring_external:
            level = RiskLevel.MEDIUM
            rationale = (
                "MEDIUM: Modifies external systems; requires review before execution"
            )
        else:
            level = RiskLevel.LOW
            rationale = "LOW: Internal operations only; minimal execution risk"

        return RiskClassification(
            level=level,
            factors=sorted(set(factors)),  # deduplicate, sort deterministically
            rationale=rationale,
        )

    def classify_intent(self, intent: Intent) -> RiskClassification:
        """
        Classify an Intent based on its type, recurrence, and metadata.

        Args:
            intent: The parsed Intent to evaluate.

        Returns:
            RiskClassification with level, factors, and rationale.
        """
        factors: list[str] = []

        # Intent type risk assessment
        high_risk_intents = {
            "inventory_update",  # External supplier systems
            "budget_query",      # Financial context (may lead to financial ops)
        }

        medium_risk_intents = {
            "schedule_change",   # Affects calendar/external systems
            "meal_planning",     # May integrate with external meal services
            "health_checkin",    # May sync with external health systems
        }

        low_risk_intents = {
            "task_creation",
            "schedule_query",
            "reminder_set",
            "notification_config",
            "unknown",
        }

        is_recurring = intent.recurrence_hints.get("is_recurring", False)

        if intent.intent_type in high_risk_intents:
            level = RiskLevel.HIGH
            factors.append(f"Intent type: {intent.intent_type}")
            rationale = f"HIGH: {intent.intent_type} affects external systems"

        elif intent.intent_type in medium_risk_intents:
            base_level = RiskLevel.MEDIUM
            factors.append(f"Intent type: {intent.intent_type}")

            if is_recurring:
                level = RiskLevel.HIGH  # Recurring + external = higher risk
                factors.append("Recurring schedule change")
                rationale = "HIGH: Recurring external system modifications"
            else:
                level = base_level
                rationale = (
                    f"MEDIUM: {intent.intent_type} may affect external systems"
                )

        else:  # low_risk_intents
            level = RiskLevel.LOW
            if is_recurring:
                factors.append(f"Recurring: {intent.recurrence_hints.get('frequency')}")
            factors.append(f"Intent type: {intent.intent_type}")
            rationale = "LOW: Primarily internal operations"

        return RiskClassification(
            level=level,
            factors=sorted(set(factors)),  # deduplicate, sort
            rationale=rationale,
        )

    def classify_hybrid(
        self,
        dag: DAG,
        intent: Intent | None = None,
    ) -> RiskClassification:
        """
        Classify by analyzing both DAG structure and originating Intent.

        The higher of the two classifications is returned (MEDIUM > LOW, HIGH > MEDIUM).

        Args:
            dag: The compiled workflow.
            intent: Optional originating Intent for context.

        Returns:
            RiskClassification reflecting the combined assessment.
        """
        dag_classification = self.classify_dag(dag)
        all_factors = list(dag_classification.factors)

        if intent is not None:
            intent_classification = self.classify_intent(intent)
            all_factors.extend(intent_classification.factors)

            # Choose the higher risk level
            if (
                dag_classification.level == RiskLevel.HIGH
                or intent_classification.level == RiskLevel.HIGH
            ):
                level = RiskLevel.HIGH
            elif (
                dag_classification.level == RiskLevel.MEDIUM
                or intent_classification.level == RiskLevel.MEDIUM
            ):
                level = RiskLevel.MEDIUM
            else:
                level = RiskLevel.LOW
        else:
            level = dag_classification.level

        rationale = (
            f"Combined assessment: DAG={dag_classification.level.value}, "
            f"factors={len(all_factors)}"
        )

        return RiskClassification(
            level=level,
            factors=sorted(set(all_factors)),  # deduplicate, sort
            rationale=rationale,
        )
