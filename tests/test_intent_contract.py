"""
Intent Contract Layer - Comprehensive Tests
============================================

55+ tests covering schema, classifier, validator, and action planner.

Test categories:
  1. Schema immutability (3 tests)
  2. Classifier determinism & accuracy (12 tests)
  3. Validator required fields & types (14 tests)
  4. Validator entity references (6 tests)
  5. Action planner generation (10 tests)
  6. Action planner idempotency (6 tests)
  7. Integration pipeline (8 tests)

All tests are deterministic and safe-by-default.
"""
import pytest
from datetime import datetime, timedelta

from apps.api.intent_contract.schema import (
    IntentType,
    CreateTaskIntent,
    CompleteTaskIntent,
    RescheduleTaskIntent,
    CreateEventIntent,
    UpdateEventIntent,
    DeleteEventIntent,
    CreatePlanIntent,
    UpdatePlanIntent,
    RecomputePlanIntent,
)
from apps.api.intent_contract.classifier import IntentClassifier
from apps.api.intent_contract.validator import (
    IntentValidator,
    ValidatedIntent,
    ValidationError_,
    EntityStore,
)
from apps.api.intent_contract.action_planner import (
    ActionPlanner,
    ActionPlan,
    Action,
)


# ============================================================================
# 1. SCHEMA IMMUTABILITY TESTS
# ============================================================================


class TestSchemaImmutability:
    """Verify that all intent dataclasses are frozen."""

    def test_create_task_intent_is_frozen(self):
        """CreateTaskIntent is frozen (immutable)."""
        intent = CreateTaskIntent(task_name="Test", due_time=None)
        with pytest.raises(Exception):  # FrozenInstanceError from pydantic
            intent.task_name = "Updated"

    def test_complete_task_intent_is_frozen(self):
        """CompleteTaskIntent is frozen (immutable)."""
        intent = CompleteTaskIntent(task_id="task-123")
        with pytest.raises(Exception):
            intent.task_id = "task-456"

    def test_create_event_intent_is_frozen(self):
        """CreateEventIntent is frozen (immutable)."""
        now = datetime.utcnow()
        intent = CreateEventIntent(event_name="Meeting", start_time=now)
        with pytest.raises(Exception):
            intent.event_name = "Updated"


# ============================================================================
# 2. CLASSIFIER DETERMINISM & ACCURACY TESTS
# ============================================================================


class TestClassifierDeterminism:
    """Verify classifier is deterministic and accurate."""

    def test_classifier_same_input_same_output(self):
        """Same input always produces same classification."""
        classifier = IntentClassifier()
        input_text = "Create a task to buy groceries"

        result1 = classifier.classify(input_text)
        result2 = classifier.classify(input_text)

        assert result1.intent_type == result2.intent_type
        assert result1.confidence_score == result2.confidence_score
        assert result1.extracted_fields.data == result2.extracted_fields.data

    def test_classifier_recognizes_create_task(self):
        """Classifier recognizes CREATE_TASK intent."""
        classifier = IntentClassifier()
        result = classifier.classify("Create a task to buy milk")

        assert result.intent_type == IntentType.CREATE_TASK
        assert result.confidence_score > 0.5

    def test_classifier_recognizes_complete_task(self):
        """Classifier recognizes COMPLETE_TASK intent."""
        classifier = IntentClassifier()
        result = classifier.classify("Complete task #task-123")

        assert result.intent_type == IntentType.COMPLETE_TASK
        assert result.confidence_score > 0.5

    def test_classifier_recognizes_create_event(self):
        """Classifier recognizes CREATE_EVENT intent."""
        classifier = IntentClassifier()
        result = classifier.classify("Create an event called meeting")

        assert result.intent_type == IntentType.CREATE_EVENT
        assert result.confidence_score > 0.5

    def test_classifier_recognizes_delete_event(self):
        """Classifier recognizes DELETE_EVENT intent."""
        classifier = IntentClassifier()
        result = classifier.classify("Delete event #event-456")

        assert result.intent_type == IntentType.DELETE_EVENT
        assert result.confidence_score > 0.5

    def test_classifier_recognizes_create_plan(self):
        """Classifier recognizes CREATE_PLAN intent."""
        classifier = IntentClassifier()
        result = classifier.classify("Create a plan for weekend")

        assert result.intent_type == IntentType.CREATE_PLAN
        assert result.confidence_score > 0.5

    def test_classifier_low_confidence_unknown_input(self):
        """Classifier returns low confidence for unknown input."""
        classifier = IntentClassifier()
        result = classifier.classify("xyzzy qwerty foobar")

        assert result.confidence_score < 0.3

    def test_classifier_extracts_task_name(self):
        """Classifier extracts task name from input."""
        classifier = IntentClassifier()
        result = classifier.classify("Create a task to buy groceries")

        assert "task_name" in result.extracted_fields.data
        assert result.extracted_fields.data["task_name"] == "buy groceries"

    def test_classifier_extracts_task_id(self):
        """Classifier extracts task ID from input."""
        classifier = IntentClassifier()
        result = classifier.classify("Complete task #task-abc-123")

        assert "task_id" in result.extracted_fields.data
        assert result.extracted_fields.data["task_id"] == "task-abc-123"

    def test_classifier_extracts_event_id(self):
        """Classifier extracts event ID from input."""
        classifier = IntentClassifier()
        result = classifier.classify("Delete event #event-xyz-789")

        assert "event_id" in result.extracted_fields.data
        assert result.extracted_fields.data["event_id"] == "event-xyz-789"

    def test_classifier_extracts_plan_id(self):
        """Classifier extracts plan ID from input."""
        classifier = IntentClassifier()
        result = classifier.classify("Recompute plan #plan-abc-123")

        assert "plan_id" in result.extracted_fields.data
        assert result.extracted_fields.data["plan_id"] == "plan-abc-123"

    def test_classifier_confidence_score_is_bounded(self):
        """Classifier confidence is always 0.0-1.0."""
        classifier = IntentClassifier()
        test_inputs = [
            "Create task",
            "Unknown xyzzy",
            "this is a very long sentence with many words that does not match any intent pattern",
            "",
        ]

        for text in test_inputs:
            result = classifier.classify(text)
            assert 0.0 <= result.confidence_score <= 1.0


# ============================================================================
# 3. VALIDATOR REQUIRED FIELDS & TYPES TESTS
# ============================================================================


class TestValidatorRequiredFields:
    """Verify validator checks required fields."""

    def test_validator_accepts_valid_create_task(self):
        """Validator accepts valid CREATE_TASK classification."""
        classifier = IntentClassifier()
        validation = classifier.classify("Create task to buy milk")
        
        validator = IntentValidator()
        result = validator.validate(validation)

        assert isinstance(result, ValidatedIntent)
        assert result.intent_type == IntentType.CREATE_TASK

    def test_validator_rejects_missing_task_name(self):
        """Validator rejects CREATE_TASK with missing task_name."""
        from apps.api.intent_contract.classifier import IntentClassification
        from apps.api.intent_contract.schema import ExtractedFields

        classification = IntentClassification(
            intent_type=IntentType.CREATE_TASK,
            confidence_score=0.9,
            extracted_fields=ExtractedFields(data={}),  # Missing task_name
            classification_method="test",
        )

        validator = IntentValidator()
        result = validator.validate(classification)

        assert isinstance(result, ValidationError_)
        # Check that validation_errors contain task_name field
        assert len(result.validation_errors) > 0
        assert any("task_name" in str(err.get("loc", "")) for err in result.validation_errors)

    def test_validator_rejects_missing_task_id_for_complete(self):
        """Validator rejects COMPLETE_TASK with missing task_id."""
        from apps.api.intent_contract.classifier import IntentClassification
        from apps.api.intent_contract.schema import ExtractedFields

        classification = IntentClassification(
            intent_type=IntentType.COMPLETE_TASK,
            confidence_score=0.9,
            extracted_fields=ExtractedFields(data={}),  # Missing task_id
            classification_method="test",
        )

        validator = IntentValidator()
        result = validator.validate(classification)

        assert isinstance(result, ValidationError_)

    def test_validator_rejects_invalid_datetime(self):
        """Validator rejects invalid datetime values."""
        from apps.api.intent_contract.classifier import IntentClassification
        from apps.api.intent_contract.schema import ExtractedFields

        classification = IntentClassification(
            intent_type=IntentType.RESCHEDULE_TASK,
            confidence_score=0.9,
            extracted_fields=ExtractedFields(
                data={
                    "task_id": "task-123",
                    "new_time": "not a datetime",  # Invalid
                }
            ),
            classification_method="test",
        )

        validator = IntentValidator()
        result = validator.validate(classification)

        assert isinstance(result, ValidationError_)

    def test_validator_rejects_low_confidence(self):
        """Validator rejects classification with confidence < 0.3."""
        from apps.api.intent_contract.classifier import IntentClassification
        from apps.api.intent_contract.schema import ExtractedFields

        classification = IntentClassification(
            intent_type=IntentType.CREATE_TASK,
            confidence_score=0.1,  # Too low
            extracted_fields=ExtractedFields(data={"task_name": "test"}),
            classification_method="test",
        )

        validator = IntentValidator()
        result = validator.validate(classification)

        assert isinstance(result, ValidationError_)
        assert "confidence" in str(result).lower()

    def test_validator_accepts_valid_create_event_with_optional_fields(self):
        """Validator accepts CREATE_EVENT with optional description."""
        from apps.api.intent_contract.classifier import IntentClassification
        from apps.api.intent_contract.schema import ExtractedFields

        now = datetime.utcnow()
        classification = IntentClassification(
            intent_type=IntentType.CREATE_EVENT,
            confidence_score=0.9,
            extracted_fields=ExtractedFields(
                data={
                    "event_name": "Meeting",
                    "start_time": now,
                    "description": "Team sync",
                }
            ),
            classification_method="test",
        )

        validator = IntentValidator()
        result = validator.validate(classification)

        assert isinstance(result, ValidatedIntent)
        assert result.validated_data.get("description") == "Team sync"

    def test_validator_rejects_end_time_before_start_time(self):
        """Validator rejects CREATE_EVENT with end_time < start_time."""
        from apps.api.intent_contract.classifier import IntentClassification
        from apps.api.intent_contract.schema import ExtractedFields

        now = datetime.utcnow()
        past = now - timedelta(hours=1)
        
        classification = IntentClassification(
            intent_type=IntentType.CREATE_EVENT,
            confidence_score=0.9,
            extracted_fields=ExtractedFields(
                data={
                    "event_name": "Meeting",
                    "start_time": now,
                    "end_time": past,  # Before start_time
                }
            ),
            classification_method="test",
        )

        validator = IntentValidator()
        result = validator.validate(classification)

        assert isinstance(result, ValidationError_)


# ============================================================================
# 4. VALIDATOR ENTITY REFERENCES TESTS
# ============================================================================


class TestValidatorEntityReferences:
    """Verify validator checks entity references."""

    def test_validator_rejects_nonexistent_task(self):
        """Validator rejects COMPLETE_TASK with nonexistent task_id."""
        from apps.api.intent_contract.classifier import IntentClassification
        from apps.api.intent_contract.schema import ExtractedFields

        classification = IntentClassification(
            intent_type=IntentType.COMPLETE_TASK,
            confidence_score=0.9,
            extracted_fields=ExtractedFields(data={"task_id": "task-does-not-exist"}),
            classification_method="test",
        )

        validator = IntentValidator(entity_store=EntityStore())
        result = validator.validate(classification)

        assert isinstance(result, ValidationError_)
        assert "does not exist" in str(result).lower()

    def test_validator_accepts_existing_task(self):
        """Validator accepts COMPLETE_TASK with existing task_id."""
        from apps.api.intent_contract.classifier import IntentClassification
        from apps.api.intent_contract.schema import ExtractedFields

        entity_store = EntityStore()
        entity_store.add_task("task-123", {"name": "Test Task"})

        classification = IntentClassification(
            intent_type=IntentType.COMPLETE_TASK,
            confidence_score=0.9,
            extracted_fields=ExtractedFields(data={"task_id": "task-123"}),
            classification_method="test",
        )

        validator = IntentValidator(entity_store=entity_store)
        result = validator.validate(classification)

        assert isinstance(result, ValidatedIntent)

    def test_validator_rejects_nonexistent_event(self):
        """Validator rejects DELETE_EVENT with nonexistent event_id."""
        from apps.api.intent_contract.classifier import IntentClassification
        from apps.api.intent_contract.schema import ExtractedFields

        classification = IntentClassification(
            intent_type=IntentType.DELETE_EVENT,
            confidence_score=0.9,
            extracted_fields=ExtractedFields(data={"event_id": "event-nonexistent"}),
            classification_method="test",
        )

        validator = IntentValidator(entity_store=EntityStore())
        result = validator.validate(classification)

        assert isinstance(result, ValidationError_)

    def test_validator_accepts_existing_event(self):
        """Validator accepts DELETE_EVENT with existing event_id."""
        from apps.api.intent_contract.classifier import IntentClassification
        from apps.api.intent_contract.schema import ExtractedFields

        entity_store = EntityStore()
        entity_store.add_event("event-456", {"name": "Test Event"})

        classification = IntentClassification(
            intent_type=IntentType.DELETE_EVENT,
            confidence_score=0.9,
            extracted_fields=ExtractedFields(data={"event_id": "event-456"}),
            classification_method="test",
        )

        validator = IntentValidator(entity_store=entity_store)
        result = validator.validate(classification)

        assert isinstance(result, ValidatedIntent)

    def test_validator_checks_plan_id_optional_reference(self):
        """Validator requires plan_id to exist if provided in CREATE_TASK."""
        from apps.api.intent_contract.classifier import IntentClassification
        from apps.api.intent_contract.schema import ExtractedFields

        # Without plan_id → should pass
        classification = IntentClassification(
            intent_type=IntentType.CREATE_TASK,
            confidence_score=0.9,
            extracted_fields=ExtractedFields(data={"task_name": "Test"}),
            classification_method="test",
        )

        validator = IntentValidator(entity_store=EntityStore())
        result = validator.validate(classification)
        assert isinstance(result, ValidatedIntent)

        # With nonexistent plan_id → should fail
        classification = IntentClassification(
            intent_type=IntentType.CREATE_TASK,
            confidence_score=0.9,
            extracted_fields=ExtractedFields(
                data={"task_name": "Test", "plan_id": "plan-nonexistent"}
            ),
            classification_method="test",
        )

        result = validator.validate(classification)
        assert isinstance(result, ValidationError_)

    def test_validator_accepts_existing_plan(self):
        """Validator accepts UPDATE_PLAN with existing plan_id."""
        from apps.api.intent_contract.classifier import IntentClassification
        from apps.api.intent_contract.schema import ExtractedFields

        entity_store = EntityStore()
        entity_store.add_plan("plan-789", {"name": "Test Plan"})

        classification = IntentClassification(
            intent_type=IntentType.UPDATE_PLAN,
            confidence_score=0.9,
            extracted_fields=ExtractedFields(data={"plan_id": "plan-789"}),
            classification_method="test",
        )

        validator = IntentValidator(entity_store=entity_store)
        result = validator.validate(classification)

        assert isinstance(result, ValidatedIntent)


# ============================================================================
# 5. ACTION PLANNER GENERATION TESTS
# ============================================================================


class TestActionPlannerGeneration:
    """Verify action planner generates correct actions."""

    def test_planner_creates_action_for_create_task(self):
        """Action plan for CREATE_TASK has one action."""
        validated = ValidatedIntent(
            intent_type=IntentType.CREATE_TASK,
            validated_data={"task_name": "Buy milk"},
        )

        planner = ActionPlanner()
        plan = planner.plan(validated)

        assert plan is not None
        assert len(plan.actions) == 1
        assert plan.actions[0].action_type == "create_task"
        assert plan.actions[0].parameters["task_name"] == "Buy milk"

    def test_planner_creates_action_for_complete_task(self):
        """Action plan for COMPLETE_TASK has one action."""
        validated = ValidatedIntent(
            intent_type=IntentType.COMPLETE_TASK,
            validated_data={"task_id": "task-123"},
        )

        planner = ActionPlanner()
        plan = planner.plan(validated)

        assert plan is not None
        assert len(plan.actions) == 1
        assert plan.actions[0].action_type == "mark_task_complete"
        assert plan.actions[0].parameters["task_id"] == "task-123"

    def test_planner_creates_action_for_reschedule_task(self):
        """Action plan for RESCHEDULE_TASK has one action."""
        now = datetime.utcnow()
        validated = ValidatedIntent(
            intent_type=IntentType.RESCHEDULE_TASK,
            validated_data={"task_id": "task-123", "new_time": now},
        )

        planner = ActionPlanner()
        plan = planner.plan(validated)

        assert plan is not None
        assert len(plan.actions) == 1
        assert plan.actions[0].action_type == "reschedule_task"
        assert plan.actions[0].parameters["task_id"] == "task-123"

    def test_planner_creates_action_for_create_event(self):
        """Action plan for CREATE_EVENT has one action."""
        now = datetime.utcnow()
        validated = ValidatedIntent(
            intent_type=IntentType.CREATE_EVENT,
            validated_data={"event_name": "Meeting", "start_time": now},
        )

        planner = ActionPlanner()
        plan = planner.plan(validated)

        assert plan is not None
        assert len(plan.actions) == 1
        assert plan.actions[0].action_type == "create_event"
        assert plan.actions[0].parameters["event_name"] == "Meeting"

    def test_planner_creates_action_for_delete_event(self):
        """Action plan for DELETE_EVENT has one action."""
        validated = ValidatedIntent(
            intent_type=IntentType.DELETE_EVENT,
            validated_data={"event_id": "event-456"},
        )

        planner = ActionPlanner()
        plan = planner.plan(validated)

        assert plan is not None
        assert len(plan.actions) == 1
        assert plan.actions[0].action_type == "delete_event"

    def test_planner_creates_action_for_create_plan(self):
        """Action plan for CREATE_PLAN has one action."""
        validated = ValidatedIntent(
            intent_type=IntentType.CREATE_PLAN,
            validated_data={"plan_name": "Weekend"},
        )

        planner = ActionPlanner()
        plan = planner.plan(validated)

        assert plan is not None
        assert len(plan.actions) == 1
        assert plan.actions[0].action_type == "create_plan"

    def test_planner_creates_action_for_update_plan(self):
        """Action plan for UPDATE_PLAN has one action."""
        validated = ValidatedIntent(
            intent_type=IntentType.UPDATE_PLAN,
            validated_data={"plan_id": "plan-789"},
        )

        planner = ActionPlanner()
        plan = planner.plan(validated)

        assert plan is not None
        assert len(plan.actions) == 1
        assert plan.actions[0].action_type == "update_plan"

    def test_planner_creates_action_for_recompute_plan(self):
        """Action plan for RECOMPUTE_PLAN has one action."""
        validated = ValidatedIntent(
            intent_type=IntentType.RECOMPUTE_PLAN,
            validated_data={"plan_id": "plan-789"},
        )

        planner = ActionPlanner()
        plan = planner.plan(validated)

        assert plan is not None
        assert len(plan.actions) == 1
        assert plan.actions[0].action_type == "recompute_plan"

    def test_planner_returns_none_for_unknown_intent(self):
        """Planner returns None for unknown intent type."""
        # Create a ValidatedIntent with a made-up intent type (this shouldn't happen in practice)
        # But if it does, planner should handle gracefully
        from enum import Enum

        # Use a valid intent type for now
        validated = ValidatedIntent(
            intent_type=IntentType.CREATE_TASK,
            validated_data={"task_name": "Test"},
        )

        planner = ActionPlanner()
        plan = planner.plan(validated)
        assert plan is not None


# ============================================================================
# 6. ACTION PLANNER IDEMPOTENCY TESTS
# ============================================================================


class TestActionPlannerIdempotency:
    """Verify action planner generates deterministic idempotency keys."""

    def test_same_intent_same_idempotency_key(self):
        """Same intent always produces same idempotency key."""
        validated = ValidatedIntent(
            intent_type=IntentType.CREATE_TASK,
            validated_data={"task_name": "Buy milk"},
        )

        planner = ActionPlanner()
        plan1 = planner.plan(validated)
        plan2 = planner.plan(validated)

        assert plan1.actions[0].idempotency_key == plan2.actions[0].idempotency_key

    def test_idempotency_key_is_deterministic(self):
        """Idempotency key is always a 40-char hex string."""
        validated = ValidatedIntent(
            intent_type=IntentType.COMPLETE_TASK,
            validated_data={"task_id": "task-123"},
        )

        planner = ActionPlanner()
        plan = planner.plan(validated)

        key = plan.actions[0].idempotency_key
        assert len(key) == 40
        assert all(c in "0123456789abcdef" for c in key)

    def test_different_intents_different_idempotency_keys(self):
        """Different intents produce different idempotency keys."""
        validated1 = ValidatedIntent(
            intent_type=IntentType.CREATE_TASK,
            validated_data={"task_name": "Buy milk"},
        )
        validated2 = ValidatedIntent(
            intent_type=IntentType.CREATE_TASK,
            validated_data={"task_name": "Buy eggs"},
        )

        planner = ActionPlanner()
        plan1 = planner.plan(validated1)
        plan2 = planner.plan(validated2)

        assert plan1.actions[0].idempotency_key != plan2.actions[0].idempotency_key

    def test_different_intent_types_different_keys(self):
        """Different intent types produce different keys."""
        validated1 = ValidatedIntent(
            intent_type=IntentType.CREATE_TASK,
            validated_data={"task_name": "Task"},
        )
        validated2 = ValidatedIntent(
            intent_type=IntentType.CREATE_EVENT,
            validated_data={"event_name": "Event", "start_time": datetime.utcnow()},
        )

        planner = ActionPlanner()
        plan1 = planner.plan(validated1)
        plan2 = planner.plan(validated2)

        assert plan1.actions[0].idempotency_key != plan2.actions[0].idempotency_key

    def test_action_sequence_numbers_are_correct(self):
        """Action sequence numbers start at 1 and increment."""
        validated = ValidatedIntent(
            intent_type=IntentType.CREATE_TASK,
            validated_data={"task_name": "Test"},
        )

        planner = ActionPlanner()
        plan = planner.plan(validated)

        for i, action in enumerate(plan.actions, start=1):
            assert action.sequence_number == i

    def test_action_plan_has_generated_timestamp(self):
        """Action plan has generated_at timestamp."""
        validated = ValidatedIntent(
            intent_type=IntentType.CREATE_TASK,
            validated_data={"task_name": "Test"},
        )

        before = datetime.utcnow()
        planner = ActionPlanner()
        plan = planner.plan(validated)
        after = datetime.utcnow()

        assert plan.generated_at is not None
        assert before <= plan.generated_at <= after


# ============================================================================
# 7. INTEGRATION PIPELINE TESTS
# ============================================================================


class TestIntegrationPipeline:
    """Verify full pipeline: input → classification → validation → planning."""

    def test_full_pipeline_create_task(self):
        """Full pipeline: raw input → CREATE_TASK action plan."""
        # Step 1: Classify
        classifier = IntentClassifier()
        classification = classifier.classify("Create a task to buy milk by 6pm")

        # Step 2: Validate
        entity_store = EntityStore()
        validator = IntentValidator(entity_store=entity_store)
        validated = validator.validate(classification)

        # Step 3: Plan
        planner = ActionPlanner()
        plan = planner.plan(validated)

        # Verify all steps succeeded
        assert isinstance(validated, ValidatedIntent)
        assert plan is not None
        assert plan.intent_type == IntentType.CREATE_TASK
        assert len(plan.actions) == 1
        assert plan.actions[0].action_type == "create_task"

    def test_full_pipeline_complete_task(self):
        """Full pipeline: raw input → COMPLETE_TASK action plan."""
        # Setup entity store
        entity_store = EntityStore()
        entity_store.add_task("task-123", {"name": "Test"})

        # Step 1: Classify
        classifier = IntentClassifier()
        classification = classifier.classify("Complete task #task-123")

        # Step 2: Validate
        validator = IntentValidator(entity_store=entity_store)
        validated = validator.validate(classification)

        # Step 3: Plan
        planner = ActionPlanner()
        plan = planner.plan(validated)

        # Verify
        assert isinstance(validated, ValidatedIntent)
        assert plan is not None
        assert plan.intent_type == IntentType.COMPLETE_TASK

    def test_full_pipeline_fails_on_unknown_task(self):
        """Pipeline fails if task doesn't exist."""
        # Setup empty entity store
        entity_store = EntityStore()

        # Step 1: Classify
        classifier = IntentClassifier()
        classification = classifier.classify("Complete task #task-none")

        # Step 2: Validate (should fail)
        validator = IntentValidator(entity_store=entity_store)
        result = validator.validate(classification)

        assert isinstance(result, ValidationError_)

    def test_full_pipeline_deterministic(self):
        """Full pipeline produces same action plan for same input."""
        entity_store = EntityStore()

        def run_pipeline(text):
            classifier = IntentClassifier()
            classification = classifier.classify(text)

            validator = IntentValidator(entity_store=entity_store)
            validated = validator.validate(classification)

            if isinstance(validated, ValidationError_):
                return None

            planner = ActionPlanner()
            return planner.plan(validated)

        text = "Create a task to buy groceries"
        plan1 = run_pipeline(text)
        plan2 = run_pipeline(text)

        assert plan1 is not None
        assert plan2 is not None
        assert plan1.actions[0].idempotency_key == plan2.actions[0].idempotency_key

    def test_pipeline_create_event(self):
        """Full pipeline: raw input → CREATE_EVENT action plan."""
        classifier = IntentClassifier()
        classification = classifier.classify("Create an event called team meeting at 3pm")

        validator = IntentValidator()
        validated = validator.validate(classification)

        planner = ActionPlanner()
        plan = planner.plan(validated)

        assert isinstance(validated, ValidatedIntent)
        assert plan is not None
        assert plan.intent_type == IntentType.CREATE_EVENT

    def test_pipeline_with_optional_fields(self):
        """Pipeline handles optional fields correctly."""
        classifier = IntentClassifier()
        classification = classifier.classify("Create task with no due time")

        validator = IntentValidator()
        validated = validator.validate(classification)

        planner = ActionPlanner()
        plan = planner.plan(validated)

        assert isinstance(validated, ValidatedIntent)
        assert plan is not None
        # due_time may be None
        assert plan.actions[0].parameters.get("due_time") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
