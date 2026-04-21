"""
Intent Validator - Structural Validation
=========================================

Validates classified intents against their schemas and entity references.

Design:
  - Validates all required fields are present
  - Type-checks and coerces values
  - Validates entity references exist (if provided)
  - Returns ValidatedIntent or ValidationError
  - Safe-by-default: fails on any validation issue
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import ValidationError as PydanticValidationError

from apps.api.intent_contract.classifier import IntentClassification
from apps.api.intent_contract.schema import (
    ExtractedFields,
    INTENT_SCHEMA_MAP,
    IntentType,
)


# ---------------------------------------------------------------------------
# VALIDATION RESULT TYPES
# ---------------------------------------------------------------------------


@dataclass
class ValidatedIntent:
    """
    Result of successful intent validation.

    Fields:
      intent_type: the validated intent type
      validated_data: cleaned and type-checked data matching schema
    """

    intent_type: IntentType
    validated_data: Dict[str, Any]

    def __str__(self):
        return f"ValidatedIntent({self.intent_type.value}, {self.validated_data})"


@dataclass
class ValidationError_:
    """
    Result of failed validation.

    Fields:
      intent_type: the intent that failed validation (may be None)
      error_message: human-readable error message
      validation_errors: list of pydantic validation errors (if applicable)
    """

    intent_type: Optional[IntentType]
    error_message: str
    validation_errors: list = field(default_factory=list)

    def __str__(self):
        msg = f"ValidationError({self.error_message}"
        if self.validation_errors:
            msg += f", {len(self.validation_errors)} field error(s)"
        msg += ")"
        return msg


# ---------------------------------------------------------------------------
# INTENT VALIDATOR
# ---------------------------------------------------------------------------


class IntentValidator:
    """
    Validates classified intents against their schemas and context.

    Safe-by-default: all validation errors are explicit and fail-fast.
    """

    def __init__(self, entity_store: Optional[EntityStore] = None):
        """
        Initialize validator with optional entity store for reference validation.

        Args:
            entity_store: optional EntityStore for validating entity IDs
        """
        self.entity_store = entity_store or EntityStore()

    def validate(self, classification: IntentClassification) -> ValidatedIntent | ValidationError_:
        """
        Validate a classified intent.

        Args:
            classification: IntentClassification from classifier

        Returns:
            ValidatedIntent or ValidationError_
        """
        # Check intent was recognized
        if classification.intent_type is None:
            return ValidationError_(
                intent_type=None,
                error_message=f"Intent not recognized. Input classification confidence: {classification.confidence_score:.0%}",
            )

        # Check confidence threshold
        if classification.confidence_score < 0.3:
            return ValidationError_(
                intent_type=classification.intent_type,
                error_message=f"Classification confidence too low: {classification.confidence_score:.0%} (minimum: 30%)",
            )

        # Get schema for this intent type
        schema_class = INTENT_SCHEMA_MAP.get(classification.intent_type)
        if not schema_class:
            return ValidationError_(
                intent_type=classification.intent_type,
                error_message=f"No schema defined for intent {classification.intent_type.value}",
            )

        # Convert extracted fields to schema
        try:
            validated = schema_class(**classification.extracted_fields.data)
            validated_dict = validated.dict()
        except PydanticValidationError as e:
            return ValidationError_(
                intent_type=classification.intent_type,
                error_message="Schema validation failed",
                validation_errors=e.errors(),
            )

        # Validate entity references
        entity_errors = self._validate_entity_references(classification.intent_type, validated_dict)
        if entity_errors:
            return entity_errors

        return ValidatedIntent(
            intent_type=classification.intent_type,
            validated_data=validated_dict,
        )

    def _validate_entity_references(
        self,
        intent_type: IntentType,
        validated_data: Dict[str, Any],
    ) -> Optional[ValidationError_]:
        """
        Validate that referenced entities exist.

        Args:
            intent_type: the intent type
            validated_data: the validated data dict

        Returns:
            ValidationError_ if any references are invalid, else None
        """
        # Check task_id references (only if provided)
        if "task_id" in validated_data and validated_data["task_id"] is not None:
            if not self.entity_store.task_exists(validated_data["task_id"]):
                return ValidationError_(
                    intent_type=intent_type,
                    error_message=f"Task {validated_data['task_id']!r} does not exist",
                )

        # Check event_id references (only if provided)
        if "event_id" in validated_data and validated_data["event_id"] is not None:
            if not self.entity_store.event_exists(validated_data["event_id"]):
                return ValidationError_(
                    intent_type=intent_type,
                    error_message=f"Event {validated_data['event_id']!r} does not exist",
                )

        # Check plan_id references (only if provided)
        if "plan_id" in validated_data and validated_data["plan_id"] is not None:
            if not self.entity_store.plan_exists(validated_data["plan_id"]):
                return ValidationError_(
                    intent_type=intent_type,
                    error_message=f"Plan {validated_data['plan_id']!r} does not exist",
                )

        return None


# ---------------------------------------------------------------------------
# ENTITY STORE — stub for testing entity existence
# ---------------------------------------------------------------------------


class EntityStore:
    """
    Simple in-memory entity store for validation.

    In production, this would query the actual database/state.
    For now, it's a stub that can be mocked in tests.
    """

    def __init__(self):
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.events: Dict[str, Dict[str, Any]] = {}
        self.plans: Dict[str, Dict[str, Any]] = {}

    def add_task(self, task_id: str, task_data: Dict[str, Any]):
        """Add a task to the store (for testing)."""
        self.tasks[task_id] = task_data

    def add_event(self, event_id: str, event_data: Dict[str, Any]):
        """Add an event to the store (for testing)."""
        self.events[event_id] = event_data

    def add_plan(self, plan_id: str, plan_data: Dict[str, Any]):
        """Add a plan to the store (for testing)."""
        self.plans[plan_id] = plan_data

    def task_exists(self, task_id: str) -> bool:
        """Check if a task exists."""
        return task_id in self.tasks

    def event_exists(self, event_id: str) -> bool:
        """Check if an event exists."""
        return event_id in self.events

    def plan_exists(self, plan_id: str) -> bool:
        """Check if a plan exists."""
        return plan_id in self.plans
