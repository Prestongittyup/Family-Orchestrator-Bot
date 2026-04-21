"""
Intent Classifier - Intent Type Detection
==========================================

Classifies raw user input into a structured IntentClassification.

Design:
  - Rule-based and deterministic (no LLM here)
  - Uses keyword matching and simple heuristics
  - Extracts candidate fields based on intent patterns
  - Returns confidence score and extracted fields
  - Safe-by-default: unknown inputs get low confidence

Example:
  Input: "Create a task to buy groceries by 6pm"
  Output: IntentClassification(
    intent_type=IntentType.CREATE_TASK,
    confidence_score=0.95,
    extracted_fields=ExtractedFields({"task_name": "buy groceries", "due_time": "18:00"})
  )
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Optional

from apps.api.intent_contract.schema import (
    ExtractedFields,
    IntentType,
)


# ---------------------------------------------------------------------------
# INTENT CLASSIFICATION RESULT
# ---------------------------------------------------------------------------


@dataclass
class IntentClassification:
    """
    Result of intent classification from raw user input.

    Fields:
      intent_type: detected intent (or None if unrecognized)
      confidence_score: float 0.0-1.0 indicating classification confidence
      extracted_fields: ExtractedFields with raw extracted data
      classification_method: string describing how the intent was determined
    """

    intent_type: Optional[IntentType]
    confidence_score: float
    extracted_fields: ExtractedFields
    classification_method: str = "unknown"


# ---------------------------------------------------------------------------
# KEYWORD PATTERNS — intent detection rules
# ---------------------------------------------------------------------------


class IntentKeywords:
    """
    Keyword patterns for each intent type.
    Each pattern is a tuple of (primary_keywords, secondary_keywords).
    Primary keywords are sufficient; secondary boost confidence.
    """

    # Task intents
    CREATE_TASK_PRIMARY = {"create task", "add task", "new task", "schedule task"}
    CREATE_TASK_SECONDARY = {"need to", "should", "must", "do", "remember"}

    COMPLETE_TASK_PRIMARY = {"mark complete", "done", "finished", "complete", "check off"}
    COMPLETE_TASK_SECONDARY = {"task", "finish"}

    RESCHEDULE_TASK_PRIMARY = {
        "reschedule",
        "move",
        "change time",
        "shift",
        "postpone",
        "delay",
    }
    RESCHEDULE_TASK_SECONDARY = {"task", "time", "when"}

    # Event intents
    CREATE_EVENT_PRIMARY = {"create event", "add event", "new event", "schedule event"}
    CREATE_EVENT_SECONDARY = {"calendar", "appointment", "meeting"}

    UPDATE_EVENT_PRIMARY = {"update event", "modify event", "change event", "edit event"}
    UPDATE_EVENT_SECONDARY = {"event", "calendar"}

    DELETE_EVENT_PRIMARY = {"delete event", "remove event", "cancel event"}
    DELETE_EVENT_SECONDARY = {"event", "remove"}

    # Plan intents
    CREATE_PLAN_PRIMARY = {"create plan", "add plan", "new plan", "plan for"}
    CREATE_PLAN_SECONDARY = {"plan", "planning", "organize"}

    UPDATE_PLAN_PRIMARY = {"update plan", "modify plan", "change plan", "edit plan"}
    UPDATE_PLAN_SECONDARY = {"plan"}

    RECOMPUTE_PLAN_PRIMARY = {"recompute plan", "recalculate plan", "redo plan"}
    RECOMPUTE_PLAN_SECONDARY = {"plan", "schedule"}


# ---------------------------------------------------------------------------
# FIELD EXTRACTION PATTERNS
# ---------------------------------------------------------------------------


class FieldExtractors:
    """
    Simple regex and pattern-based field extractors.
    Each method returns (field_value, confidence).
    """

    @staticmethod
    def extract_task_name(text: str) -> Optional[str]:
        """Extract task name from patterns like 'task to X' or 'task: X'."""
        # Pattern: "task to <name>"
        match = re.search(r"task to ([^\.;\,]+)", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Pattern: "task: <name>"
        match = re.search(r"task:?\s+(.+?)(?:\s+by\s+|$)", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Pattern: "create task: <name>" or "add task: <name>"
        match = re.search(r"(?:create|add)\s+task:?\s+(.+?)(?:\s+by\s+|$)", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Fallback: extract quoted string
        match = re.search(r'["\']([^"\']+)["\']', text)
        if match:
            return match.group(1).strip()

        return None

    @staticmethod
    def extract_event_name(text: str) -> Optional[str]:
        """Extract event name from patterns like 'event: X' or quoted strings."""
        # Pattern: "event: <name>"
        match = re.search(r"event:?\s+(.+?)(?:\s+at\s+|$)", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Pattern: quoted strings
        match = re.search(r'["\']([^"\']+)["\']', text)
        if match:
            return match.group(1).strip()

        return None

    @staticmethod
    def extract_datetime(text: str) -> Optional[datetime]:
        """
        Extract datetime from patterns like:
        - "by 6pm", "at 18:00", "on April 20"
        - "2026-04-20T18:00:00" (ISO)
        """
        today = datetime.now().date()

        # ISO format: YYYY-MM-DDTHH:MM:SS
        match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", text)
        if match:
            try:
                return datetime.fromisoformat(match.group(1))
            except ValueError:
                pass

        # Time-only patterns: "by 6pm", "at 18:00", "at 6:00pm"
        time_match = re.search(r"(?:by|at)\s+(\d{1,2}):?(\d{2})?\s*(am|pm)?", text, re.IGNORECASE)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2)) if time_match.group(2) else 0
            am_pm = time_match.group(3)

            if am_pm and am_pm.lower() == "pm" and hour != 12:
                hour += 12
            elif am_pm and am_pm.lower() == "am" and hour == 12:
                hour = 0

            try:
                dt = datetime.combine(today, time(hour, minute, 0))
                return dt
            except ValueError:
                pass

        # Date patterns: "on April 20" or "on 2026-04-20"
        date_match = re.search(r"(?:on|for)\s+(\w+\s+\d{1,2})|(?:on\s+(\d{4}-\d{2}-\d{2}))", text, re.IGNORECASE)
        if date_match:
            try:
                if date_match.group(2):  # ISO format date
                    return datetime.fromisoformat(date_match.group(2))
                else:  # Month day format
                    date_str = date_match.group(1)
                    dt = datetime.strptime(date_str, "%B %d")
                    # Assume current year if not specified
                    dt = dt.replace(year=today.year)
                    if dt.date() < today:
                        dt = dt.replace(year=today.year + 1)
                    return dt
            except ValueError:
                pass

        return None

    @staticmethod
    def extract_entity_id(text: str) -> Optional[str]:
        """Extract entity IDs (task-xyz, event-abc, plan-123, task-abc-123)."""
        # Match task/event/plan followed by one or more hyphenated/underscored segments
        match = re.search(r"(?:task|event|plan)(?:[-_][a-zA-Z0-9]+)+", text)
        if match:
            return match.group(0)
        return None


# ---------------------------------------------------------------------------
# INTENT CLASSIFIER
# ---------------------------------------------------------------------------


class IntentClassifier:
    """
    Classifies raw user input into a structured IntentClassification.

    Deterministic, rule-based classifier with keyword matching and
    simple field extraction. No LLM, no randomness.
    """

    @staticmethod
    def classify(user_input: str) -> IntentClassification:
        """
        Classify user input and extract fields.

        Args:
            user_input: raw user input string

        Returns:
            IntentClassification with intent_type, confidence_score, extracted_fields
        """
        text = user_input.lower().strip()

        if not text:
            return IntentClassification(
                intent_type=None,
                confidence_score=0.0,
                extracted_fields=ExtractedFields(),
                classification_method="empty_input",
            )

        # Simple keyword matching for intent detection
        intent_scores = IntentClassifier._score_intents(text)

        # Find highest-confidence intent
        best_intent = None
        best_score = 0.0
        best_method = "no_match"

        for intent, score in intent_scores.items():
            if score > best_score:
                best_intent = intent
                best_score = score
                best_method = f"keyword_match_{intent.value}"

        # Extract fields based on detected intent
        extracted_fields = ExtractedFields()
        if best_intent:
            extracted_fields = IntentClassifier._extract_fields(best_intent, text)

        return IntentClassification(
            intent_type=best_intent,
            confidence_score=best_score,
            extracted_fields=extracted_fields,
            classification_method=best_method,
        )

    @staticmethod
    def _score_intents(text: str) -> dict:
        """Score each intent based on keyword presence."""
        scores = {}

        # Task intents
        scores[IntentType.CREATE_TASK] = IntentClassifier._score_keywords(
            text, IntentKeywords.CREATE_TASK_PRIMARY, IntentKeywords.CREATE_TASK_SECONDARY
        )
        scores[IntentType.COMPLETE_TASK] = IntentClassifier._score_keywords(
            text, IntentKeywords.COMPLETE_TASK_PRIMARY, IntentKeywords.COMPLETE_TASK_SECONDARY
        )
        scores[IntentType.RESCHEDULE_TASK] = IntentClassifier._score_keywords(
            text, IntentKeywords.RESCHEDULE_TASK_PRIMARY, IntentKeywords.RESCHEDULE_TASK_SECONDARY
        )

        # Event intents
        scores[IntentType.CREATE_EVENT] = IntentClassifier._score_keywords(
            text, IntentKeywords.CREATE_EVENT_PRIMARY, IntentKeywords.CREATE_EVENT_SECONDARY
        )
        scores[IntentType.UPDATE_EVENT] = IntentClassifier._score_keywords(
            text, IntentKeywords.UPDATE_EVENT_PRIMARY, IntentKeywords.UPDATE_EVENT_SECONDARY
        )
        scores[IntentType.DELETE_EVENT] = IntentClassifier._score_keywords(
            text, IntentKeywords.DELETE_EVENT_PRIMARY, IntentKeywords.DELETE_EVENT_SECONDARY
        )

        # Plan intents
        scores[IntentType.CREATE_PLAN] = IntentClassifier._score_keywords(
            text, IntentKeywords.CREATE_PLAN_PRIMARY, IntentKeywords.CREATE_PLAN_SECONDARY
        )
        scores[IntentType.UPDATE_PLAN] = IntentClassifier._score_keywords(
            text, IntentKeywords.UPDATE_PLAN_PRIMARY, IntentKeywords.UPDATE_PLAN_SECONDARY
        )
        scores[IntentType.RECOMPUTE_PLAN] = IntentClassifier._score_keywords(
            text, IntentKeywords.RECOMPUTE_PLAN_PRIMARY, IntentKeywords.RECOMPUTE_PLAN_SECONDARY
        )

        return scores

    @staticmethod
    def _score_keywords(text: str, primary: set, secondary: set) -> float:
        """Score based on primary and secondary keyword matches."""
        # Split text into words for flexible matching
        words = set(text.split())
        
        # Check primary keywords (supports both "create task" and individual words)
        primary_hits = 0
        for kw in primary:
            # Try full phrase match first
            if kw in text:
                primary_hits += 1
            # Then try individual word matching
            else:
                kw_words = set(kw.split())
                if kw_words.issubset(words):
                    primary_hits += 1
        
        # Check secondary keywords similarly
        secondary_hits = 0
        for kw in secondary:
            if kw in text:
                secondary_hits += 1
            else:
                kw_words = set(kw.split())
                if kw_words.issubset(words):
                    secondary_hits += 1

        if primary_hits == 0:
            return 0.0

        # Primary hit = 1.0, + 0.1 per secondary hit
        score = min(1.0, 0.5 + (0.25 * primary_hits) + (0.05 * secondary_hits))
        return score

    @staticmethod
    def _extract_fields(intent: IntentType, text: str) -> ExtractedFields:
        """Extract fields based on intent type."""
        fields = ExtractedFields()

        if intent == IntentType.CREATE_TASK:
            if task_name := FieldExtractors.extract_task_name(text):
                fields.set("task_name", task_name)
            if due_time := FieldExtractors.extract_datetime(text):
                fields.set("due_time", due_time)
            if plan_id := FieldExtractors.extract_entity_id(text):
                if "plan" in plan_id:
                    fields.set("plan_id", plan_id)

        elif intent == IntentType.COMPLETE_TASK:
            if task_id := FieldExtractors.extract_entity_id(text):
                if "task" in task_id:
                    fields.set("task_id", task_id)

        elif intent == IntentType.RESCHEDULE_TASK:
            if task_id := FieldExtractors.extract_entity_id(text):
                if "task" in task_id:
                    fields.set("task_id", task_id)
            if new_time := FieldExtractors.extract_datetime(text):
                fields.set("new_time", new_time)

        elif intent == IntentType.CREATE_EVENT:
            if event_name := FieldExtractors.extract_event_name(text):
                fields.set("event_name", event_name)
            if start_time := FieldExtractors.extract_datetime(text):
                fields.set("start_time", start_time)

        elif intent == IntentType.UPDATE_EVENT:
            if event_id := FieldExtractors.extract_entity_id(text):
                if "event" in event_id:
                    fields.set("event_id", event_id)
            if event_name := FieldExtractors.extract_event_name(text):
                fields.set("event_name", event_name)

        elif intent == IntentType.DELETE_EVENT:
            if event_id := FieldExtractors.extract_entity_id(text):
                if "event" in event_id:
                    fields.set("event_id", event_id)

        elif intent == IntentType.CREATE_PLAN:
            if plan_name := FieldExtractors.extract_task_name(text):
                fields.set("plan_name", plan_name)

        elif intent == IntentType.UPDATE_PLAN:
            if plan_id := FieldExtractors.extract_entity_id(text):
                if "plan" in plan_id:
                    fields.set("plan_id", plan_id)

        elif intent == IntentType.RECOMPUTE_PLAN:
            if plan_id := FieldExtractors.extract_entity_id(text):
                if "plan" in plan_id:
                    fields.set("plan_id", plan_id)

        return fields
