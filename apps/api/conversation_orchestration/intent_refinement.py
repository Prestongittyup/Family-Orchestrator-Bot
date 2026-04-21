"""
Conversation Orchestration Layer (COL v1) - Intent Refinement Engine
=====================================================================

Handles multi-turn intent extraction and accumulation.

Responsibilities:
  - Merge new user messages into existing PartialIntent
  - Detect missing required fields per intent type
  - Handle contradiction and ambiguity
  - Determine when intent is ready for execution

Design principles:
  - NEVER overwrite extracted fields without contradiction evidence
  - NEVER fabricate missing field values
  - Safe-by-default: low confidence = not ready
  - Deterministic: same inputs always produce same result
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from apps.api.conversation_orchestration.schema import PartialIntent
from apps.api.intent_contract.classifier import IntentClassification, IntentClassifier
from apps.api.intent_contract.schema import IntentType


# ---------------------------------------------------------------------------
# REQUIRED FIELDS PER INTENT TYPE
# Derived from intent schema definitions — used for completion checking
# ---------------------------------------------------------------------------

REQUIRED_FIELDS_BY_INTENT: Dict[str, List[str]] = {
    IntentType.CREATE_TASK.value: ["task_name"],
    IntentType.COMPLETE_TASK.value: ["task_id"],
    IntentType.RESCHEDULE_TASK.value: ["task_id", "new_time"],
    IntentType.CREATE_EVENT.value: ["event_name", "start_time"],
    IntentType.UPDATE_EVENT.value: ["event_id"],
    IntentType.DELETE_EVENT.value: ["event_id"],
    IntentType.CREATE_PLAN.value: ["plan_name"],
    IntentType.UPDATE_PLAN.value: ["plan_id"],
    IntentType.RECOMPUTE_PLAN.value: ["plan_id"],
}

# Fields where user providing a different value is a contradiction (not an update)
ENTITY_ID_FIELDS: Set[str] = {"task_id", "event_id", "plan_id"}


# ---------------------------------------------------------------------------
# FIELD COMPARISON HELPERS
# ---------------------------------------------------------------------------


def _fields_conflict(existing_value: Any, new_value: Any) -> bool:
    """
    Returns True if the new value contradicts the existing value.

    Rules:
      - If either is None, no conflict
      - For entity ID fields: different value = conflict (user may mean different entity)
      - For text/name fields: different non-empty value = conflict
    """
    if existing_value is None or new_value is None:
        return False
    # Normalize both to string for comparison (handles datetime vs str etc)
    return str(existing_value).strip() != str(new_value).strip()


# ---------------------------------------------------------------------------
# INTENT REFINEMENT ENGINE
# ---------------------------------------------------------------------------


class IntentRefinementEngine:
    """
    Merges new user messages into an existing PartialIntent.

    This is the core multi-turn intelligence of COL.

    Rules:
      - First message establishes the base intent
      - Subsequent messages add to or refine the intent
      - Contradictions mark fields as ambiguous
      - Missing required fields block completion
    """

    CONFIDENCE_THRESHOLD: float = 0.85

    def __init__(self, classifier: Optional[IntentClassifier] = None):
        self._classifier = classifier or IntentClassifier()

    def classify_message(self, message_text: str) -> IntentClassification:
        """
        Classify a single user message using the Intent Contract Layer classifier.

        Returns:
          IntentClassification with intent_type, confidence, extracted_fields
        """
        return self._classifier.classify(message_text)

    def initialize_from_message(self, message_text: str) -> PartialIntent:
        """
        Create a new PartialIntent from the first user message in a session.

        Args:
          message_text: raw user input text

        Returns:
          PartialIntent with intent type, initial extracted fields, and missing fields
        """
        classification = self.classify_message(message_text)

        intent_type = classification.intent_type.value if classification.intent_type else None
        extracted = dict(classification.extracted_fields.data) if classification.extracted_fields else {}

        missing = self._compute_missing_fields(intent_type, extracted)

        return PartialIntent(
            intent_type=intent_type,
            extracted_fields=extracted,
            missing_fields=missing,
            confidence=classification.confidence_score,
            ambiguous_fields=[],
            turn_count=1,
        )

    def merge_message(self, existing: PartialIntent, message_text: str) -> PartialIntent:
        """
        Merge a new user message into the existing PartialIntent.

        Multi-turn accumulation rules:
          1. Classify the new message
          2. If intent_type changes and new confidence is higher → override intent_type (reset fields)
          3. If intent_type stays the same → accumulate new fields
          4. For each new field: if it conflicts with existing → mark ambiguous
          5. Recompute missing_fields for the (possibly updated) intent_type
          6. Update confidence to the higher of (existing, new)

        Args:
          existing: the current PartialIntent being refined
          message_text: raw user input text

        Returns:
          A new PartialIntent with merged state
        """
        classification = self.classify_message(message_text)

        new_intent_type = classification.intent_type.value if classification.intent_type else None
        new_fields = dict(classification.extracted_fields.data) if classification.extracted_fields else {}
        new_confidence = classification.confidence_score

        # Determine working intent_type and base fields
        if self._should_override_intent(existing, new_intent_type, new_confidence):
            # Intent type changed with sufficient confidence — start fresh with new type
            final_intent_type = new_intent_type
            merged_fields = dict(new_fields)
            ambiguous_fields: List[str] = []
            final_confidence = new_confidence
        else:
            # Same intent type — accumulate fields
            final_intent_type = existing.intent_type
            merged_fields, ambiguous_fields = self._merge_fields(
                existing.extracted_fields,
                new_fields,
                existing.ambiguous_fields,
            )
            # Promote confidence to highest observed
            final_confidence = max(existing.confidence, new_confidence)

        # Resolve ambiguity only for fields that were ALREADY ambiguous before this turn.
        # Newly detected conflicts must persist for the next turn.
        previously_ambiguous = list(existing.ambiguous_fields)
        new_conflicts = [f for f in ambiguous_fields if f not in previously_ambiguous]
        resolved_prev = self._resolve_ambiguity(previously_ambiguous, new_fields, merged_fields)
        ambiguous_fields = resolved_prev + new_conflicts

        # Recompute missing required fields
        missing = self._compute_missing_fields(final_intent_type, merged_fields)

        return PartialIntent(
            intent_type=final_intent_type,
            extracted_fields=merged_fields,
            missing_fields=missing,
            confidence=final_confidence,
            ambiguous_fields=ambiguous_fields,
            turn_count=existing.turn_count + 1,
        )

    def get_required_fields(self, intent_type: Optional[str]) -> List[str]:
        """Return the list of required fields for the given intent type."""
        if intent_type is None:
            return []
        return list(REQUIRED_FIELDS_BY_INTENT.get(intent_type, []))

    def build_clarification_prompt(self, partial: PartialIntent) -> str:
        """
        Build a structured clarification question for the user.

        Always targets the first missing or ambiguous field.
        Never includes free-form reasoning — only factual questions.

        Args:
          partial: the current PartialIntent

        Returns:
          A prompt string asking for the specific missing or ambiguous field.
        """
        if partial.ambiguous_fields:
            field_name = partial.ambiguous_fields[0]
            human_name = _human_field_name(field_name)
            return f"I noticed conflicting information for '{human_name}'. Could you clarify which value you intended?"

        if partial.missing_fields:
            field_name = partial.missing_fields[0]
            human_name = _human_field_name(field_name)
            intent_label = partial.intent_type or "your request"
            return f"To proceed with '{intent_label}', I need the {human_name}. Could you provide it?"

        return "Could you provide more details to complete your request?"

    # -----------------------------------------------------------------------
    # PRIVATE HELPERS
    # -----------------------------------------------------------------------

    def _should_override_intent(
        self,
        existing: PartialIntent,
        new_intent_type: Optional[str],
        new_confidence: float,
    ) -> bool:
        """
        Returns True if the new intent type should replace the existing one.

        Only overrides when:
          - New intent_type is different and non-None
          - New confidence is higher than existing
          - (Prevents thrashing on low-confidence re-classifications)
        """
        if new_intent_type is None:
            return False
        if existing.intent_type is None:
            return True
        if new_intent_type == existing.intent_type:
            return False
        # Override only if new classification is more confident
        return new_confidence > existing.confidence

    def _merge_fields(
        self,
        existing_fields: Dict[str, Any],
        new_fields: Dict[str, Any],
        current_ambiguous: List[str],
    ) -> tuple[Dict[str, Any], List[str]]:
        """
        Merge new fields into existing fields.

        Returns:
          (merged_fields, ambiguous_fields)

        - New fields that don't conflict → added normally
        - New fields that conflict with existing → marked ambiguous
        - Existing fields not in new fields → preserved unchanged
        """
        merged = dict(existing_fields)
        ambiguous = list(current_ambiguous)

        for field_name, new_value in new_fields.items():
            if new_value is None:
                continue  # ignore None extractions

            existing_value = merged.get(field_name)

            if existing_value is None:
                # New field, no conflict
                merged[field_name] = new_value
            elif _fields_conflict(existing_value, new_value):
                # Conflict — mark as ambiguous, keep existing value
                if field_name not in ambiguous:
                    ambiguous.append(field_name)
            else:
                # Same value — no change needed
                pass

        return merged, ambiguous

    def _resolve_ambiguity(
        self,
        ambiguous_fields: List[str],
        new_fields: Dict[str, Any],
        merged_fields: Dict[str, Any],
    ) -> List[str]:
        """
        Remove fields from ambiguous list if a clarification was provided.

        A clarification is a message that provides a NEW value for an ambiguous field
        (meaning the user is explicitly resolving the conflict).
        In this case, the new value wins and the field is de-ambiguated.
        """
        still_ambiguous = []
        for field_name in ambiguous_fields:
            if field_name in new_fields and new_fields[field_name] is not None:
                # User provided a clarification — update the merged field and remove from ambiguous
                merged_fields[field_name] = new_fields[field_name]
            else:
                still_ambiguous.append(field_name)
        return still_ambiguous

    def _compute_missing_fields(
        self,
        intent_type: Optional[str],
        extracted_fields: Dict[str, Any],
    ) -> List[str]:
        """
        Return list of required fields not yet present in extracted_fields.

        Args:
          intent_type: string value of intent type (or None)
          extracted_fields: fields extracted so far

        Returns:
          List of field names that are required but not yet provided
        """
        if intent_type is None:
            return []

        required = REQUIRED_FIELDS_BY_INTENT.get(intent_type, [])
        return [
            f for f in required
            if not extracted_fields.get(f)  # missing or empty/None
        ]


# ---------------------------------------------------------------------------
# HELPER: HUMAN-READABLE FIELD NAMES
# ---------------------------------------------------------------------------

_FIELD_NAME_MAP: Dict[str, str] = {
    "task_name": "task name",
    "task_id": "task ID",
    "new_time": "new time",
    "event_name": "event name",
    "event_id": "event ID",
    "start_time": "start time",
    "end_time": "end time",
    "plan_name": "plan name",
    "plan_id": "plan ID",
    "description": "description",
    "reason": "reason",
}


def _human_field_name(field: str) -> str:
    """Convert a field name to a human-readable label."""
    return _FIELD_NAME_MAP.get(field, field.replace("_", " "))
