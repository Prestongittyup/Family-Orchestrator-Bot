"""
Policy Engine - Default Rules
==============================

Defines the default security policies for all intent types.

Rule classification:
  - ALLOW: Safe operations with no side effects
  - REQUIRE_CONFIRMATION: Operations that modify state, require user awareness
  - BLOCK: Dangerous operations that shouldn't proceed without special authorization
"""
from __future__ import annotations

from apps.api.intent_contract.schema import IntentType
from apps.api.policy_engine.schema import PolicyDecision, PolicyRule


# ---------------------------------------------------------------------------
# RULE DEFINITIONS
# ---------------------------------------------------------------------------


class PolicyRules:
    """
    Default policy rules for all supported intent types.

    Safe-by-default approach:
      - ALLOW: Simple, non-destructive operations (create, complete single tasks)
      - REQUIRE_CONFIRMATION: State-modifying operations (reschedule, updates, recompute)
      - BLOCK: Dangerous operations (bulk deletes without safeguards)
    """

    # =====================================================
    # TASK OPERATIONS
    # =====================================================

    CREATE_TASK_ALLOWED = PolicyRule(
        rule_name="create_task_allowed",
        intent_types=[IntentType.CREATE_TASK],
        decision=PolicyDecision.ALLOW,
        reason_code="safe_operation",
        message="Creating a new task is a safe operation",
        priority=10,
    )

    COMPLETE_TASK_ALLOWED = PolicyRule(
        rule_name="complete_task_allowed",
        intent_types=[IntentType.COMPLETE_TASK],
        decision=PolicyDecision.ALLOW,
        reason_code="safe_operation",
        message="Marking a single task complete is safe",
        priority=10,
    )

    RESCHEDULE_TASK_CONFIRM = PolicyRule(
        rule_name="reschedule_task_confirm",
        intent_types=[IntentType.RESCHEDULE_TASK],
        decision=PolicyDecision.REQUIRE_CONFIRMATION,
        reason_code="state_modification",
        message="Rescheduling a task changes the calendar. Please confirm.",
        priority=8,
    )

    # =====================================================
    # EVENT OPERATIONS
    # =====================================================

    CREATE_EVENT_ALLOWED = PolicyRule(
        rule_name="create_event_allowed",
        intent_types=[IntentType.CREATE_EVENT],
        decision=PolicyDecision.ALLOW,
        reason_code="safe_operation",
        message="Creating a new event is a safe operation",
        priority=10,
    )

    UPDATE_EVENT_CONFIRM = PolicyRule(
        rule_name="update_event_confirm",
        intent_types=[IntentType.UPDATE_EVENT],
        decision=PolicyDecision.REQUIRE_CONFIRMATION,
        reason_code="state_modification",
        message="Updating an event may affect scheduling. Please confirm.",
        priority=8,
    )

    DELETE_EVENT_CONFIRM = PolicyRule(
        rule_name="delete_event_confirm",
        intent_types=[IntentType.DELETE_EVENT],
        decision=PolicyDecision.REQUIRE_CONFIRMATION,
        reason_code="destructive_operation",
        message="Deleting an event is irreversible. Please confirm.",
        priority=9,
    )

    # =====================================================
    # PLAN OPERATIONS
    # =====================================================

    CREATE_PLAN_ALLOWED = PolicyRule(
        rule_name="create_plan_allowed",
        intent_types=[IntentType.CREATE_PLAN],
        decision=PolicyDecision.ALLOW,
        reason_code="safe_operation",
        message="Creating a new plan is a safe operation",
        priority=10,
    )

    UPDATE_PLAN_CONFIRM = PolicyRule(
        rule_name="update_plan_confirm",
        intent_types=[IntentType.UPDATE_PLAN],
        decision=PolicyDecision.REQUIRE_CONFIRMATION,
        reason_code="state_modification",
        message="Updating a plan may affect scheduling. Please confirm.",
        priority=8,
    )

    RECOMPUTE_PLAN_CONFIRM = PolicyRule(
        rule_name="recompute_plan_confirm",
        intent_types=[IntentType.RECOMPUTE_PLAN],
        decision=PolicyDecision.REQUIRE_CONFIRMATION,
        reason_code="compute_intensive",
        message="Recomputing a plan may take time and reorganize tasks. Please confirm.",
        priority=7,
    )

    # =====================================================
    # CATCH-ALL RULE
    # =====================================================

    UNKNOWN_INTENT_BLOCK = PolicyRule(
        rule_name="unknown_intent_block",
        intent_types=[],  # Will be overridden in evaluator
        decision=PolicyDecision.BLOCK,
        reason_code="unknown_intent",
        message="This action is not recognized. It cannot be executed.",
        priority=0,
    )

    # =====================================================
    # ALL RULES COLLECTION
    # =====================================================

    @staticmethod
    def get_all_rules() -> list[PolicyRule]:
        """Get all default rules in priority order (highest first)."""
        return [
            PolicyRules.CREATE_TASK_ALLOWED,
            PolicyRules.COMPLETE_TASK_ALLOWED,
            PolicyRules.RESCHEDULE_TASK_CONFIRM,
            PolicyRules.CREATE_EVENT_ALLOWED,
            PolicyRules.DELETE_EVENT_CONFIRM,
            PolicyRules.UPDATE_EVENT_CONFIRM,
            PolicyRules.CREATE_PLAN_ALLOWED,
            PolicyRules.UPDATE_PLAN_CONFIRM,
            PolicyRules.RECOMPUTE_PLAN_CONFIRM,
        ]

    @staticmethod
    def get_default_rule() -> PolicyRule:
        """Get the catch-all rule for unknown intents."""
        return PolicyRules.UNKNOWN_INTENT_BLOCK


# ---------------------------------------------------------------------------
# RULE SUMMARIES
# ---------------------------------------------------------------------------


def get_rule_summary() -> dict:
    """Return a summary of all rules by decision type."""
    rules = PolicyRules.get_all_rules()

    return {
        "allow": [r for r in rules if r.decision == PolicyDecision.ALLOW],
        "require_confirmation": [
            r for r in rules if r.decision == PolicyDecision.REQUIRE_CONFIRMATION
        ],
        "block": [r for r in rules if r.decision == PolicyDecision.BLOCK],
        "total": len(rules),
    }
