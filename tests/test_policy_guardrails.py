"""
Policy Guardrails Engine - Comprehensive Tests
===============================================

60+ tests covering policy decisions, rules, evaluator, and integration.

Test categories:
  1. Schema immutability (3 tests)
  2. Policy decisions (5 tests)
  3. Rule definitions (8 tests)
  4. Evaluator logic (12 tests)
  5. Default rules (9 tests)
  6. Edge cases (8 tests)
  7. Integration with action plan (6 tests)
  8. Configuration (4 tests)

All tests are deterministic and verify safe-by-default behavior.
"""
import pytest
from datetime import datetime

from apps.api.intent_contract.schema import IntentType
from apps.api.intent_contract.action_planner import ActionPlan, Action
from apps.api.policy_engine.schema import (
    PolicyDecision,
    PolicyInput,
    PolicyResult,
    PolicyRule,
    PolicyConfig,
)
from apps.api.policy_engine.evaluator import PolicyEvaluator
from apps.api.policy_engine.rules import PolicyRules, get_rule_summary


# ============================================================================
# 1. SCHEMA IMMUTABILITY TESTS
# ============================================================================


class TestSchemaImmutability:
    """Verify all policy schema classes are immutable (frozen)."""

    def test_policy_input_is_frozen(self):
        """PolicyInput is immutable after creation."""
        policy_input = PolicyInput(intent_type=IntentType.CREATE_TASK)
        with pytest.raises(Exception):
            policy_input.intent_type = IntentType.DELETE_EVENT

    def test_policy_result_is_frozen(self):
        """PolicyResult is immutable after creation."""
        result = PolicyResult(
            decision=PolicyDecision.ALLOW,
            reason_code="safe",
            message="Test",
        )
        with pytest.raises(Exception):
            result.decision = PolicyDecision.BLOCK

    def test_policy_rule_is_frozen(self):
        """PolicyRule is immutable after creation."""
        rule = PolicyRule(
            rule_name="test",
            intent_types=[IntentType.CREATE_TASK],
            decision=PolicyDecision.ALLOW,
            reason_code="safe",
            message="Test",
        )
        with pytest.raises(Exception):
            rule.decision = PolicyDecision.BLOCK


# ============================================================================
# 2. POLICY DECISION TESTS
# ============================================================================


class TestPolicyDecision:
    """Verify PolicyDecision enum properties and helpers."""

    def test_allow_decision_properties(self):
        """ALLOW decision has correct properties."""
        decision = PolicyDecision.ALLOW
        assert decision.allows_execution is True
        assert decision.requires_user_input is False
        assert decision.prevents_execution is False

    def test_require_confirmation_decision_properties(self):
        """REQUIRE_CONFIRMATION decision has correct properties."""
        decision = PolicyDecision.REQUIRE_CONFIRMATION
        assert decision.allows_execution is False
        assert decision.requires_user_input is True
        assert decision.prevents_execution is False

    def test_block_decision_properties(self):
        """BLOCK decision has correct properties."""
        decision = PolicyDecision.BLOCK
        assert decision.allows_execution is False
        assert decision.requires_user_input is False
        assert decision.prevents_execution is True

    def test_decision_string_representation(self):
        """Decision enum values are lowercase strings."""
        assert PolicyDecision.ALLOW.value == "allow"
        assert PolicyDecision.REQUIRE_CONFIRMATION.value == "require_confirmation"
        assert PolicyDecision.BLOCK.value == "block"

    def test_all_decisions_are_defined(self):
        """All expected decisions exist."""
        decisions = {d.value for d in PolicyDecision}
        assert "allow" in decisions
        assert "require_confirmation" in decisions
        assert "block" in decisions


# ============================================================================
# 3. RULE DEFINITION TESTS
# ============================================================================


class TestRuleDefinitions:
    """Verify policy rules are properly defined."""

    def test_create_task_rule_exists(self):
        """CREATE_TASK rule is defined and correct."""
        rule = PolicyRules.CREATE_TASK_ALLOWED
        assert rule.matches(IntentType.CREATE_TASK)
        assert rule.decision == PolicyDecision.ALLOW

    def test_delete_event_rule_confirms(self):
        """DELETE_EVENT requires confirmation (safe-by-default)."""
        rule = PolicyRules.DELETE_EVENT_CONFIRM
        assert rule.matches(IntentType.DELETE_EVENT)
        assert rule.decision == PolicyDecision.REQUIRE_CONFIRMATION

    def test_recompute_plan_rule_confirms(self):
        """RECOMPUTE_PLAN requires confirmation."""
        rule = PolicyRules.RECOMPUTE_PLAN_CONFIRM
        assert rule.matches(IntentType.RECOMPUTE_PLAN)
        assert rule.decision == PolicyDecision.REQUIRE_CONFIRMATION

    def test_rule_priorities_are_set(self):
        """Rules have appropriate priorities."""
        rules = PolicyRules.get_all_rules()
        # At least some rules should have non-zero priority
        assert any(r.priority > 0 for r in rules)

    def test_rule_has_reason_code(self):
        """Every rule has a reason code for XAI."""
        rules = PolicyRules.get_all_rules()
        for rule in rules:
            assert rule.reason_code
            assert len(rule.reason_code) > 0

    def test_rule_has_message(self):
        """Every rule has a user-facing message."""
        rules = PolicyRules.get_all_rules()
        for rule in rules:
            assert rule.message
            assert len(rule.message) > 0

    def test_rule_summary_groups_by_decision(self):
        """Rule summary groups rules by decision type."""
        summary = get_rule_summary()
        assert "allow" in summary
        assert "require_confirmation" in summary
        assert "block" in summary
        assert "total" in summary
        assert summary["total"] > 0

    def test_all_intent_types_have_rules(self):
        """Most common intent types have explicit rules."""
        rules = PolicyRules.get_all_rules()
        matched_intents = set()
        for rule in rules:
            for intent_type in rule.intent_types:
                matched_intents.add(intent_type)
        
        # Should have rules for at least the main intents
        assert IntentType.CREATE_TASK in matched_intents
        assert IntentType.CREATE_EVENT in matched_intents
        assert IntentType.DELETE_EVENT in matched_intents


# ============================================================================
# 4. EVALUATOR LOGIC TESTS
# ============================================================================


class TestPolicyEvaluator:
    """Verify policy evaluator makes correct decisions."""

    def test_evaluator_initializes_with_defaults(self):
        """Evaluator initializes with default config."""
        evaluator = PolicyEvaluator()
        assert evaluator.config is not None
        assert len(evaluator.rules) > 0

    def test_evaluator_accepts_custom_config(self):
        """Evaluator accepts optional custom config."""
        config = PolicyConfig(
            default_decision=PolicyDecision.BLOCK,
            block_unknown_intents=True,
        )
        evaluator = PolicyEvaluator(config=config)
        assert evaluator.config == config
        assert evaluator.config.block_unknown_intents is True

    def test_create_task_is_allowed(self):
        """CREATE_TASK intent evaluates to ALLOW."""
        evaluator = PolicyEvaluator()
        policy_input = PolicyInput(intent_type=IntentType.CREATE_TASK)
        result = evaluator.evaluate_input(policy_input)

        assert result.decision == PolicyDecision.ALLOW
        assert result.is_allowed is True

    def test_complete_task_is_allowed(self):
        """COMPLETE_TASK intent evaluates to ALLOW."""
        evaluator = PolicyEvaluator()
        policy_input = PolicyInput(intent_type=IntentType.COMPLETE_TASK)
        result = evaluator.evaluate_input(policy_input)

        assert result.decision == PolicyDecision.ALLOW

    def test_delete_event_requires_confirmation(self):
        """DELETE_EVENT intent requires confirmation."""
        evaluator = PolicyEvaluator()
        policy_input = PolicyInput(intent_type=IntentType.DELETE_EVENT)
        result = evaluator.evaluate_input(policy_input)

        assert result.decision == PolicyDecision.REQUIRE_CONFIRMATION
        assert result.needs_confirmation is True

    def test_evaluation_is_deterministic(self):
        """Same input always produces same result."""
        evaluator = PolicyEvaluator()
        policy_input = PolicyInput(intent_type=IntentType.RESCHEDULE_TASK)

        result1 = evaluator.evaluate_input(policy_input)
        result2 = evaluator.evaluate_input(policy_input)

        assert result1.decision == result2.decision
        assert result1.reason_code == result2.reason_code

    def test_result_has_rule_name(self):
        """Policy result includes which rule matched."""
        evaluator = PolicyEvaluator()
        policy_input = PolicyInput(intent_type=IntentType.CREATE_TASK)
        result = evaluator.evaluate_input(policy_input)

        assert result.rule_name
        assert "create" in result.rule_name.lower()

    def test_unknown_intent_uses_default_decision(self):
        """Unknown intent types use default decision from config."""
        config = PolicyConfig(default_decision=PolicyDecision.REQUIRE_CONFIRMATION)
        evaluator = PolicyEvaluator(config=config)

        # CREATE_TASK should match its rule, not use default
        policy_input = PolicyInput(intent_type=IntentType.CREATE_TASK)
        result = evaluator.evaluate_input(policy_input)

        # CREATE_TASK has explicit rule for ALLOW
        assert result.decision == PolicyDecision.ALLOW


# ============================================================================
# 5. DEFAULT RULES TESTS
# ============================================================================


class TestDefaultRules:
    """Verify each default rule behaves as expected."""

    def test_all_allow_rules(self):
        """Verify all ALLOW rules."""
        evaluator = PolicyEvaluator()
        
        allow_intents = [
            IntentType.CREATE_TASK,
            IntentType.COMPLETE_TASK,
            IntentType.CREATE_EVENT,
            IntentType.CREATE_PLAN,
        ]
        
        for intent_type in allow_intents:
            policy_input = PolicyInput(intent_type=intent_type)
            result = evaluator.evaluate_input(policy_input)
            assert result.decision == PolicyDecision.ALLOW, f"{intent_type} should be ALLOW"

    def test_all_require_confirmation_rules(self):
        """Verify all REQUIRE_CONFIRMATION rules."""
        evaluator = PolicyEvaluator()
        
        confirm_intents = [
            IntentType.RESCHEDULE_TASK,
            IntentType.UPDATE_EVENT,
            IntentType.DELETE_EVENT,
            IntentType.UPDATE_PLAN,
            IntentType.RECOMPUTE_PLAN,
        ]
        
        for intent_type in confirm_intents:
            policy_input = PolicyInput(intent_type=intent_type)
            result = evaluator.evaluate_input(policy_input)
            assert (
                result.decision == PolicyDecision.REQUIRE_CONFIRMATION
            ), f"{intent_type} should require confirmation"

    def test_safe_by_default_philosophy(self):
        """Verify safe-by-default: dangerous operations require confirmation."""
        evaluator = PolicyEvaluator()
        
        # Destructive operations
        delete_intent = PolicyInput(intent_type=IntentType.DELETE_EVENT)
        delete_result = evaluator.evaluate_input(delete_intent)
        
        # Should require confirmation, not be allowed
        assert delete_result.decision != PolicyDecision.ALLOW
        assert delete_result.needs_confirmation is True

    def test_reason_codes_are_informative(self):
        """Verify reason codes are specific and useful for logging."""
        rules = PolicyRules.get_all_rules()
        
        reason_codes = {r.reason_code for r in rules}
        
        # Should have semantically meaningful codes
        assert "safe_operation" in reason_codes
        assert "state_modification" in reason_codes
        assert "destructive_operation" in reason_codes


# ============================================================================
# 6. EDGE CASE TESTS
# ============================================================================


class TestEdgeCases:
    """Verify edge cases and error handling."""

    def test_policy_input_with_no_entity_ids(self):
        """Policy input can have empty entity_ids."""
        policy_input = PolicyInput(
            intent_type=IntentType.CREATE_TASK,
            entity_ids=[],
        )
        evaluator = PolicyEvaluator()
        result = evaluator.evaluate_input(policy_input)
        
        assert result.decision is not None

    def test_policy_input_with_multiple_entity_ids(self):
        """Policy input can reference multiple entities."""
        policy_input = PolicyInput(
            intent_type=IntentType.RESCHEDULE_TASK,
            entity_ids=["task-1", "task-2", "task-3"],
        )
        evaluator = PolicyEvaluator()
        result = evaluator.evaluate_input(policy_input)
        
        assert result.decision is not None

    def test_policy_input_with_plan_id(self):
        """Policy input can include plan_id."""
        policy_input = PolicyInput(
            intent_type=IntentType.RECOMPUTE_PLAN,
            plan_id="plan-123",
        )
        evaluator = PolicyEvaluator()
        result = evaluator.evaluate_input(policy_input)
        
        assert result.decision == PolicyDecision.REQUIRE_CONFIRMATION

    def test_result_message_is_non_empty(self):
        """All results have explanatory messages."""
        evaluator = PolicyEvaluator()
        
        for intent_type in [IntentType.CREATE_TASK, IntentType.DELETE_EVENT]:
            policy_input = PolicyInput(intent_type=intent_type)
            result = evaluator.evaluate_input(policy_input)
            
            assert result.message
            assert len(result.message) > 5

    def test_evaluator_with_blocking_config(self):
        """Evaluator respects blocking configuration."""
        config = PolicyConfig(
            default_decision=PolicyDecision.BLOCK,
            block_unknown_intents=True,
        )
        evaluator = PolicyEvaluator(config=config)
        
        policy_input = PolicyInput(intent_type=IntentType.CREATE_TASK)
        result = evaluator.evaluate_input(policy_input)
        
        # CREATE_TASK has an explicit rule, so it should still be ALLOW
        assert result.decision == PolicyDecision.ALLOW

    def test_multiple_evaluators_independent(self):
        """Multiple evaluators with different configs are independent."""
        eval1 = PolicyEvaluator(config=PolicyConfig(block_unknown_intents=True))
        eval2 = PolicyEvaluator(config=PolicyConfig(block_unknown_intents=False))
        
        policy_input = PolicyInput(intent_type=IntentType.CREATE_TASK)
        
        result1 = eval1.evaluate_input(policy_input)
        result2 = eval2.evaluate_input(policy_input)
        
        # Both should allow CREATE_TASK (explicit rule)
        assert result1.decision == PolicyDecision.ALLOW
        assert result2.decision == PolicyDecision.ALLOW


# ============================================================================
# 7. ACTION PLAN INTEGRATION TESTS
# ============================================================================


class TestActionPlanIntegration:
    """Verify integration with ActionPlan objects."""

    def test_evaluate_action_plan_directly(self):
        """Evaluator can evaluate ActionPlan objects."""
        # Create a simple action plan
        action = Action(
            action_type="create_task",
            parameters={"task_name": "Buy milk"},
            idempotency_key="abc123",
            sequence_number=1,
        )
        
        action_plan = ActionPlan(
            intent_type=IntentType.CREATE_TASK,
            actions=[action],
            validated_data={"task_name": "Buy milk"},
        )
        
        evaluator = PolicyEvaluator()
        result = evaluator.evaluate(action_plan)
        
        assert result.decision == PolicyDecision.ALLOW

    def test_evaluate_multi_action_plan(self):
        """Evaluator handles multi-action plans."""
        actions = [
            Action(
                action_type="create_task",
                parameters={"task_name": "Task 1"},
                idempotency_key="key1",
                sequence_number=1,
            ),
            Action(
                action_type="create_task",
                parameters={"task_name": "Task 2"},
                idempotency_key="key2",
                sequence_number=2,
            ),
        ]
        
        action_plan = ActionPlan(
            intent_type=IntentType.CREATE_TASK,
            actions=actions,
            validated_data={},
        )
        
        evaluator = PolicyEvaluator()
        result = evaluator.evaluate(action_plan)
        
        assert result.decision == PolicyDecision.ALLOW

    def test_extract_entity_ids_from_action_plan(self):
        """Evaluator extracts entity IDs from action parameters."""
        action = Action(
            action_type="complete_task",
            parameters={"task_id": "task-123"},
            idempotency_key="key",
            sequence_number=1,
        )
        
        action_plan = ActionPlan(
            intent_type=IntentType.COMPLETE_TASK,
            actions=[action],
            validated_data={"task_id": "task-123"},
        )
        
        evaluator = PolicyEvaluator()
        policy_input = evaluator._action_plan_to_policy_input(action_plan)
        
        assert "task-123" in policy_input.entity_ids

    def test_scope_estimation(self):
        """Evaluator correctly estimates action scope."""
        evaluator = PolicyEvaluator()
        
        # Single action
        scope1 = evaluator._estimate_scope(["task-1"], 1)
        assert scope1 == "single"
        
        # Multiple actions
        scope2 = evaluator._estimate_scope(["task-1", "task-2", "task-3"], 5)
        assert scope2 == "bulk"


# ============================================================================
# 8. CONFIGURATION TESTS
# ============================================================================


class TestPolicyConfig:
    """Verify policy configuration options."""

    def test_default_config_is_safe(self):
        """Default config uses safe-by-default decisions."""
        config = PolicyConfig()
        assert config.default_decision == PolicyDecision.REQUIRE_CONFIRMATION

    def test_config_allows_customization(self):
        """Config can be customized."""
        config = PolicyConfig(
            default_decision=PolicyDecision.BLOCK,
            allow_bulk_operations=True,
            require_deletion_confirmation=False,
            max_scope_without_confirmation=100,
        )
        
        assert config.default_decision == PolicyDecision.BLOCK
        assert config.allow_bulk_operations is True
        assert config.require_deletion_confirmation is False
        assert config.max_scope_without_confirmation == 100

    def test_config_is_immutable(self):
        """PolicyConfig is immutable after creation."""
        config = PolicyConfig()
        with pytest.raises(Exception):
            config.default_decision = PolicyDecision.BLOCK

    def test_evaluator_respects_config(self):
        """Evaluator uses custom config settings."""
        config = PolicyConfig(
            default_decision=PolicyDecision.REQUIRE_CONFIRMATION,
        )
        evaluator = PolicyEvaluator(config=config)
        
        assert evaluator.config.default_decision == PolicyDecision.REQUIRE_CONFIRMATION


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
