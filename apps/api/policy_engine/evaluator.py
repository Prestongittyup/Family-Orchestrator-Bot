"""
Policy Engine - Evaluator
==========================

Evaluates action plans against security policies.

Workflow:
  1. Convert ActionPlan to PolicyInput
  2. Evaluate against rules (highest priority first)
  3. Return PolicyResult with decision and explanation
"""
from __future__ import annotations

from apps.api.intent_contract.action_planner import ActionPlan
from apps.api.policy_engine.rules import PolicyRules
from apps.api.policy_engine.schema import (
    PolicyConfig,
    PolicyDecision,
    PolicyInput,
    PolicyResult,
    PolicyRule,
)


# ---------------------------------------------------------------------------
# POLICY EVALUATOR
# ---------------------------------------------------------------------------


class PolicyEvaluator:
    """
    Evaluates action plans against security policies.

    Safe-by-default: Unknown intents or missing rules default to REQUIRE_CONFIRMATION.
    """

    def __init__(self, config: PolicyConfig | None = None):
        """
        Initialize the policy evaluator.

        Args:
            config: optional PolicyConfig for customization (uses defaults if not provided)
        """
        self.config = config or PolicyConfig()
        self.rules = PolicyRules.get_all_rules()
        self.default_rule = PolicyRules.get_default_rule()

    def evaluate(self, action_plan: ActionPlan) -> PolicyResult:
        """
        Evaluate an action plan and return a policy decision.

        Args:
            action_plan: The action plan to evaluate

        Returns:
            PolicyResult with decision (ALLOW, REQUIRE_CONFIRMATION, or BLOCK)
        """
        # Convert ActionPlan to PolicyInput
        policy_input = self._action_plan_to_policy_input(action_plan)

        # Evaluate against rules (priority order)
        return self._evaluate_policy_input(policy_input)

    def evaluate_input(self, policy_input: PolicyInput) -> PolicyResult:
        """
        Evaluate a policy input directly (for testing).

        Args:
            policy_input: The policy input to evaluate

        Returns:
            PolicyResult with decision
        """
        return self._evaluate_policy_input(policy_input)

    # -----------------------------------------------
    # PRIVATE EVALUATION METHODS
    # -----------------------------------------------

    def _evaluate_policy_input(self, policy_input: PolicyInput) -> PolicyResult:
        """
        Evaluate a PolicyInput against all rules.

        Rules are evaluated in priority order (highest first).
        First matching rule's decision is returned.
        """
        # Sort rules by priority (descending)
        sorted_rules = sorted(self.rules, key=lambda r: r.priority, reverse=True)

        # Check each rule in order
        for rule in sorted_rules:
            if rule.matches(policy_input.intent_type):
                return PolicyResult(
                    decision=rule.decision,
                    reason_code=rule.reason_code,
                    message=rule.message,
                    rule_name=rule.rule_name,
                )

        # No rule matched - use default
        return PolicyResult(
            decision=self.config.default_decision,
            reason_code="no_rule_matched",
            message="This action type is not explicitly recognized. Requiring confirmation for safety.",
            rule_name="default",
        )

    def _action_plan_to_policy_input(self, action_plan: ActionPlan) -> PolicyInput:
        """
        Convert an ActionPlan to a PolicyInput for evaluation.

        Args:
            action_plan: The action plan to convert

        Returns:
            PolicyInput with extracted information
        """
        # Extract entity IDs from action parameters
        entity_ids = self._extract_entity_ids(action_plan)

        # Extract plan_id if present
        plan_id = None
        for action in action_plan.actions:
            if "plan_id" in action.parameters:
                plan_id = action.parameters["plan_id"]
                break

        return PolicyInput(
            intent_type=action_plan.intent_type,
            action_count=len(action_plan.actions),
            entity_ids=entity_ids,
            plan_id=plan_id,
            scope_estimate=self._estimate_scope(entity_ids, len(action_plan.actions)),
        )

    def _extract_entity_ids(self, action_plan: ActionPlan) -> list[str]:
        """Extract all entity IDs referenced in the action plan."""
        entity_ids = []

        for action in action_plan.actions:
            # Check common ID fields
            for id_field in ["task_id", "event_id", "plan_id"]:
                if id_field in action.parameters and action.parameters[id_field]:
                    entity_ids.append(action.parameters[id_field])

        return list(set(entity_ids))  # Remove duplicates

    def _estimate_scope(self, entity_ids: list[str], action_count: int) -> str:
        """Estimate the scope of the action plan."""
        if action_count == 0:
            return "empty"
        elif action_count == 1 and len(entity_ids) <= 1:
            return "single"
        elif action_count > 1:
            return "bulk"
        else:
            return "unknown"

    # -----------------------------------------------
    # RULE INSPECTION (for debugging/testing)
    # -----------------------------------------------

    def get_rules_for_intent(self, intent_type) -> list[PolicyRule]:
        """Get all rules that apply to a given intent type."""
        return [rule for rule in self.rules if rule.matches(intent_type)]

    def get_rule_summary(self) -> dict:
        """Get a summary of all rules grouped by decision."""
        return {
            "allow": [r.rule_name for r in self.rules if r.decision == PolicyDecision.ALLOW],
            "require_confirmation": [
                r.rule_name for r in self.rules if r.decision == PolicyDecision.REQUIRE_CONFIRMATION
            ],
            "block": [r.rule_name for r in self.rules if r.decision == PolicyDecision.BLOCK],
            "total_rules": len(self.rules),
        }
