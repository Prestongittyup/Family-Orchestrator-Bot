"""
Policy Engine - Integration Example
====================================

Demonstrates the complete pipeline:

    User Input 
        ↓
    Intent Classifier (deterministic classification)
        ↓
    Intent Validator (schema validation + entity references)
        ↓
    Action Planner (deterministic action sequencing + idempotency)
        ↓
    Policy Engine (safety guardrails + approval decisions)
        ↓
    Action Execution or User Confirmation

Example: Complete task with policy check
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from apps.api.intent_contract.classifier import IntentClassifier
from apps.api.intent_contract.validator import IntentValidator, EntityStore
from apps.api.intent_contract.action_planner import ActionPlanner
from apps.api.policy_engine.evaluator import PolicyEvaluator
from apps.api.policy_engine.schema import PolicyDecision


def example_complete_pipeline():
    """
    Complete end-to-end pipeline: from raw input to policy decision.
    """
    print("=" * 70)
    print("HPAL SYSTEM - COMPLETE PIPELINE EXAMPLE")
    print("=" * 70)
    
    # =============================
    # STEP 1: Classify Intent
    # =============================
    print("\n[1] INTENT CLASSIFICATION")
    print("-" * 70)
    
    user_input = "Complete task #task-abc-123"
    classifier = IntentClassifier()
    classification = classifier.classify(user_input)
    
    print(f"Input:           {user_input!r}")
    print(f"Intent:          {classification.intent_type.value}")
    print(f"Confidence:      {classification.confidence_score:.0%}")
    print(f"Extracted:       {classification.extracted_fields.data}")
    
    # =============================
    # STEP 2: Validate Intent
    # =============================
    print("\n[2] INTENT VALIDATION")
    print("-" * 70)
    
    # Setup entity store with known task
    entity_store = EntityStore()
    entity_store.add_task("task-abc-123", {"name": "Buy groceries", "status": "pending"})
    
    validator = IntentValidator(entity_store=entity_store)
    validation_result = validator.validate(classification)
    
    if hasattr(validation_result, 'validated_data'):
        print(f"Status:          VALID")
        print(f"Intent Type:     {validation_result.intent_type.value}")
        print(f"Data:            {validation_result.validated_data}")
    else:
        print(f"Status:          INVALID")
        print(f"Error:           {validation_result.error_message}")
        return
    
    # =============================
    # STEP 3: Generate Action Plan
    # =============================
    print("\n[3] ACTION PLAN GENERATION")
    print("-" * 70)
    
    planner = ActionPlanner()
    action_plan = planner.plan(validation_result)
    
    print(f"Intent:          {action_plan.intent_type.value}")
    print(f"Actions:         {len(action_plan.actions)}")
    for action in action_plan.actions:
        print(f"  - {action.action_type}")
        print(f"    Parameters:  {action.parameters}")
        print(f"    Idempotency: {action.idempotency_key[:16]}...")
    
    # =============================
    # STEP 4: Policy Decision
    # =============================
    print("\n[4] POLICY ENGINE EVALUATION")
    print("-" * 70)
    
    evaluator = PolicyEvaluator()
    policy_result = evaluator.evaluate(action_plan)
    
    print(f"Decision:        {policy_result.decision.value.upper()}")
    print(f"Rule:            {policy_result.rule_name}")
    print(f"Reason Code:     {policy_result.reason_code}")
    print(f"Message:         {policy_result.message}")
    
    # =============================
    # STEP 5: Execution Guidance
    # =============================
    print("\n[5] EXECUTION GUIDANCE")
    print("-" * 70)
    
    if policy_result.is_allowed:
        print("✓ Action APPROVED - Can execute immediately")
        print(f"  Execute {len(action_plan.actions)} action(s)")
    elif policy_result.needs_confirmation:
        print("⚠ Action REQUIRES CONFIRMATION - Ask user before executing")
        print(f"  Reason: {policy_result.message}")
        print(f"  User should confirm before proceeding with {len(action_plan.actions)} action(s)")
    elif policy_result.is_blocked:
        print("✗ Action BLOCKED - Cannot execute")
        print(f"  Reason: {policy_result.message}")
    
    print("\n" + "=" * 70)


def example_delete_event_workflow():
    """
    Example: Deletion workflow (requires confirmation).
    """
    print("\n\n" + "=" * 70)
    print("EXAMPLE 2: DELETE EVENT WORKFLOW (REQUIRES CONFIRMATION)")
    print("=" * 70)
    
    user_input = "Delete event #event-xyz-789"
    
    # 1. Classify
    classifier = IntentClassifier()
    classification = classifier.classify(user_input)
    print(f"\nStep 1 - Classify: {classification.intent_type.value}")
    
    # 2. Validate
    entity_store = EntityStore()
    entity_store.add_event("event-xyz-789", {"name": "Team Meeting", "time": "2026-04-20 14:00"})
    
    validator = IntentValidator(entity_store=entity_store)
    validation_result = validator.validate(classification)
    print(f"Step 2 - Validate: {validation_result.intent_type.value if hasattr(validation_result, 'intent_type') else 'FAILED'}")
    
    # 3. Plan
    if hasattr(validation_result, 'intent_type'):
        planner = ActionPlanner()
        action_plan = planner.plan(validation_result)
        print(f"Step 3 - Plan:    {len(action_plan.actions)} action(s)")
        
        # 4. Policy
        evaluator = PolicyEvaluator()
        policy_result = evaluator.evaluate(action_plan)
        print(f"Step 4 - Policy:  {policy_result.decision.value}")
        print(f"        Message: {policy_result.message}")
        
        if policy_result.needs_confirmation:
            print("\n→ USER FLOW: Show confirmation dialog before deletion")
            print(f"  'Are you sure you want to delete \"{validation_result.validated_data.get('event_id')}\"?'")
            print(f"  [Cancel] [Delete]")


def example_rule_inspection():
    """
    Inspect all policy rules and their decisions.
    """
    print("\n\n" + "=" * 70)
    print("EXAMPLE 3: POLICY RULE INSPECTION")
    print("=" * 70)
    
    evaluator = PolicyEvaluator()
    summary = evaluator.get_rule_summary()
    
    print(f"\nTotal Rules: {summary['total_rules']}")
    
    print(f"\n✓ ALLOW ({len(summary['allow'])} rules):")
    for rule_name in summary['allow']:
        print(f"  - {rule_name}")
    
    print(f"\n⚠ REQUIRE_CONFIRMATION ({len(summary['require_confirmation'])} rules):")
    for rule_name in summary['require_confirmation']:
        print(f"  - {rule_name}")
    
    print(f"\n✗ BLOCK ({len(summary['block'])} rules):")
    for rule_name in summary['block']:
        print(f"  - {rule_name}")


if __name__ == "__main__":
    # Run examples
    example_complete_pipeline()
    example_delete_event_workflow()
    example_rule_inspection()
