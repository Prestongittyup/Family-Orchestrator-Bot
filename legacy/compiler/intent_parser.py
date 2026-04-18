"""
Intent Parser — Natural language → Structured Intent normalization.

Converts raw user input into a deterministic Intent object suitable for
downstream workflow generation. No workflow execution occurs here; intent
parsing is purely a normalization and extraction pass.

Intent components:
  - intent_type: classified action (task_creation, schedule_change, info_query, etc.)
  - entities: extracted nouns, times, people, resources
  - constraints: detected limitations (deadline, budget, availability, etc.)
  - recurrence_hints: detected frequency patterns (daily, weekly, every N hours, etc.)
  - priority_level: inferred urgency (low, medium, high, critical)
  - ambiguity_flags: uncertainty markers for downstream disambiguation
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal

IntentType = Literal[
    "task_creation",
    "schedule_change",
    "schedule_query",
    "reminder_set",
    "notification_config",
    "budget_query",
    "health_checkin",
    "inventory_update",
    "meal_planning",
    "unknown",
]

PriorityLevel = Literal["low", "medium", "high", "critical"]


@dataclass(frozen=True)
class Intent:
    """Normalized representation of user intent."""

    intent_type: IntentType
    raw_input: str
    household_id: str
    user_id: str
    timestamp: datetime

    # Core extraction
    entities: dict[str, Any] = field(default_factory=dict)
    """
    Extracted nouns, proper names, values, etc.
    {
        "action": "create task" | "check budget" | ...,
        "subject": "weekly grocery shopping" | ...,
        "recipients": ["Alice", "Bob", ...],
        "deadline": datetime | None,
        "values": {key: value, ...},  # amounts, durations, etc.
    }
    """

    constraints: dict[str, Any] = field(default_factory=dict)
    """
    Detected limitations on execution.
    {
        "deadline": datetime | None,
        "budget_limit": float | None,
        "time_slot": (start_time, end_time) | None,
        "dependencies": [intent_id, ...],
        "exclusions": ["after 5pm", "weekends", ...],
    }
    """

    recurrence_hints: dict[str, Any] = field(default_factory=dict)
    """
    Patterns indicating repeated execution.
    {
        "is_recurring": bool,
        "frequency": "daily" | "weekly" | "monthly" | "custom" | None,
        "interval": int | None,  # hours, days, weeks (context-dependent)
        "next_occurrence": datetime | None,
    }
    """

    priority_level: PriorityLevel = "medium"
    """Inferred urgency: low (background), medium (normal), high (soon), critical (now)."""

    ambiguity_flags: list[str] = field(default_factory=list)
    """
    Uncertainty markers requiring disambiguation.
    Examples:
      - "multiple_recipients_unclear"
      - "time_ambiguous" (morning vs afternoon)
      - "deadline_relative" (ASAP vs next week)
      - "resource_missing" (needs budget context)
    """

    context_snapshot: dict[str, Any] = field(default_factory=dict)
    """
    Optional external context (household state, calendar, settings, etc.)
    used to inform intent extraction.
    """

    def is_ambiguous(self) -> bool:
        """True if any ambiguity flags are set."""
        return bool(self.ambiguity_flags)


class IntentParser:
    """
    Parses natural language user input into structured Intent objects.

    Does NOT generate workflows, execute code, or interact with external systems.
    Purely a normalization and extraction layer.
    """

    # Keyword patterns for intent classification
    _TASK_KEYWORDS = {"create", "add", "make", "schedule", "plan", "setup", "water", "garden"}
    _SCHEDULE_KEYWORDS = {"reschedule", "move", "change time", "postpone", "defer"}
    _QUERY_KEYWORDS = {"check", "show", "list", "what", "when", "how much"}
    _REMINDER_KEYWORDS = {"remind", "alert", "notify", "remember", "daily", "weekly"}
    _BUDGET_KEYWORDS = {"budget", "expense", "cost", "money", "spend", "afford"}
    _HEALTH_KEYWORDS = {"exercise", "walk", "run", "stretch", "sleep", "mood", "health", "gym"}
    _MEAL_KEYWORDS = {"meal", "food", "cook", "breakfast", "lunch", "dinner", "recipe"}
    _INVENTORY_KEYWORDS = {"stock", "groceries", "supplies", "inventory", "out of"}
    _CONTROL_KEYWORDS = {"turn on", "turn off", "lights", "lights on", "lights off"}

    # Priority boosters
    _URGENT_KEYWORDS = {"urgent", "asap", "immediately", "now", "critical", "emergency"}
    _LOW_KEYWORDS = {"whenever", "eventually", "no hurry", "background"}

    # Recurrence patterns
    _RECURRENCE_PATTERNS = {
        r"every\s+day": ("daily", 1),
        r"dailies?": ("daily", 1),
        r"every\s+week": ("weekly", 7),
        r"weeklies?": ("weekly", 7),
        r"every\s+(\d+)\s+days?": ("daily", None),  # extract number
        r"every\s+(\d+)\s+hours?": ("custom", None),  # extract number
        r"monthly": ("monthly", 30),
        r"bi-weekly": ("weekly", 14),
    }

    def __init__(self) -> None:
        pass

    def parse(
        self,
        raw_input: str,
        household_id: str,
        user_id: str,
        context_snapshot: dict[str, Any] | None = None,
    ) -> Intent:
        """
        Parse raw user input into a structured Intent.

        Args:
            raw_input: Natural language user query or command.
            household_id: Household context for entity resolution.
            user_id: User making the request.
            context_snapshot: Optional household state (calendar, budget, settings, etc.).

        Returns:
            Structured Intent object ready for downstream processing.
        """
        normalized = raw_input.lower().strip()
        context_snapshot = context_snapshot or {}
        timestamp = datetime.now()

        # Step 1: Classify intent type
        intent_type = self._classify_intent(normalized)

        # Step 2: Extract entities
        entities = self._extract_entities(normalized, intent_type, context_snapshot)

        # Step 3: Detect constraints
        constraints = self._extract_constraints(normalized, context_snapshot)

        # Step 4: Identify recurrence hints
        recurrence_hints = self._extract_recurrence(normalized)

        # Step 5: Infer priority
        priority_level = self._infer_priority(normalized)

        # Step 6: Collect ambiguity flags
        ambiguity_flags = self._collect_ambiguities(
            normalized, entities, constraints, recurrence_hints, intent_type
        )

        return Intent(
            intent_type=intent_type,
            raw_input=raw_input,
            household_id=household_id,
            user_id=user_id,
            timestamp=timestamp,
            entities=entities,
            constraints=constraints,
            recurrence_hints=recurrence_hints,
            priority_level=priority_level,
            ambiguity_flags=ambiguity_flags,
            context_snapshot=context_snapshot,
        )

    def _classify_intent(self, normalized: str) -> IntentType:
        """Classify the primary intent type from keywords."""
        if self._contains_keywords(normalized, self._TASK_KEYWORDS):
            return "task_creation"
        if self._contains_keywords(normalized, self._SCHEDULE_KEYWORDS):
            return "schedule_change"
        if self._contains_keywords(normalized, self._REMINDER_KEYWORDS):
            return "reminder_set"
        if self._contains_keywords(normalized, self._BUDGET_KEYWORDS):
            return "budget_query"
        if self._contains_keywords(normalized, self._HEALTH_KEYWORDS):
            return "health_checkin"
        if self._contains_keywords(normalized, self._MEAL_KEYWORDS):
            return "meal_planning"
        if self._contains_keywords(normalized, self._INVENTORY_KEYWORDS):
            return "inventory_update"
        if self._contains_keywords(normalized, self._QUERY_KEYWORDS):
            return "schedule_query"
        return "unknown"

    def _extract_entities(
        self,
        normalized: str,
        intent_type: IntentType,
        context_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        """Extract named entities, amounts, and relevant nouns."""
        entities: dict[str, Any] = {
            "action": self._extract_verb_phrase(normalized),
            "subject": self._extract_subject_phrase(normalized),
            "recipients": self._extract_recipients(normalized, context_snapshot),
            "deadline": self._extract_datetime(normalized),
            "values": self._extract_numeric_values(normalized),
        }
        return entities

    def _extract_constraints(
        self, normalized: str, context_snapshot: dict[str, Any]
    ) -> dict[str, Any]:
        """Extract detected limitations and dependencies."""
        deadline = self._extract_datetime(normalized)
        constraints: dict[str, Any] = {
            "deadline": deadline,
            "budget_limit": self._extract_budget(normalized),
            "time_slot": self._extract_time_slot(normalized),
            "dependencies": [],
            "exclusions": self._extract_exclusions(normalized),
        }
        return constraints

    def _extract_recurrence(self, normalized: str) -> dict[str, Any]:
        """Detect frequency patterns and recurrence hints."""
        is_recurring = False
        frequency = None
        interval = None

        for pattern, (freq, interval_val) in self._RECURRENCE_PATTERNS.items():
            match = re.search(pattern, normalized)
            if match:
                is_recurring = True
                frequency = freq
                # If pattern captures a number, extract it
                if interval_val is None and match.groups():
                    try:
                        interval = int(match.group(1))
                    except (ValueError, IndexError):
                        interval = interval_val
                else:
                    interval = interval_val
                break

        recurrence_hints: dict[str, Any] = {
            "is_recurring": is_recurring,
            "frequency": frequency,
            "interval": interval,
            "next_occurrence": None,
        }
        return recurrence_hints

    def _infer_priority(self, normalized: str) -> PriorityLevel:
        """Infer priority level from language cues."""
        if self._contains_keywords(normalized, self._URGENT_KEYWORDS):
            return "critical"
        if "today" in normalized or "asap" in normalized:
            return "high"
        if self._contains_keywords(normalized, self._LOW_KEYWORDS):
            return "low"
        return "medium"

    def _collect_ambiguities(
        self,
        normalized: str,
        entities: dict[str, Any],
        constraints: dict[str, Any],
        recurrence_hints: dict[str, Any],
        intent_type: IntentType,
    ) -> list[str]:
        """Identify uncertainty markers for disambiguation."""
        flags: list[str] = []

        # Multiple recipients without clear assignment
        recipients = entities.get("recipients", [])
        if isinstance(recipients, list) and len(recipients) > 1:
            flags.append("multiple_recipients_unclear")

        # Relative deadlines (ASAP, soon, etc.)
        if isinstance(constraints.get("deadline"), str):
            flags.append("deadline_relative")

        # Ambiguous time references (morning, afternoon, evening)
        if any(t in normalized for t in ["morning", "afternoon", "evening", "tonight"]):
            flags.append("time_ambiguous")

        # Unclear resource requirements (only for task creation, not for queries)
        if intent_type in {"task_creation", "reminder_set"}:
            if "budget" in normalized and constraints.get("budget_limit") is None:
                flags.append("resource_missing")

        # Vague frequency ("sometime", "occasionally")
        if "sometime" in normalized or "occasionally" in normalized:
            flags.append("frequency_vague")

        return flags

    # ── Helper methods ──────────────────────────────────────────────────────

    @staticmethod
    def _contains_keywords(text: str, keywords: set[str]) -> bool:
        """Check if any keyword appears in text."""
        words = set(re.findall(r"\b\w+\b", text))
        return bool(words & keywords)

    @staticmethod
    def _extract_verb_phrase(normalized: str) -> str:
        """Extract the main action verb phrase."""
        # Simple heuristic: first few words often contain the action
        words = normalized.split()[:5]
        return " ".join(words).rstrip(",.")

    @staticmethod
    def _extract_subject_phrase(normalized: str) -> str:
        """Extract the main subject/object of the action."""
        # Remove common verbs and prepositions; return remaining phrase
        stop_words = {
            "a", "an", "the", "to", "for", "by", "in", "on", "at", "before", "after"
        }
        words = [w for w in normalized.split() if w not in stop_words]
        return " ".join(words[:3]) if words else ""

    @staticmethod
    def _extract_recipients(
        normalized: str, context_snapshot: dict[str, Any]
    ) -> list[str]:
        """Extract household members mentioned."""
        # Simple regex: all-caps words or names after pronouns/prepositions
        family = context_snapshot.get("family_members", [])
        recipients = []
        for member in family:
            if member.lower() in normalized.lower():
                recipients.append(member)
        return recipients

    @staticmethod
    def _extract_datetime(normalized: str) -> datetime | None:
        """Extract absolute datetime references (today, tomorrow, specific dates)."""
        now = datetime.now()
        if "today" in normalized:
            return now.replace(hour=0, minute=0, second=0, microsecond=0)
        if "tomorrow" in normalized:
            return (now + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        if "next week" in normalized:
            return (now + timedelta(weeks=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        # Could add date parsing (e.g., "April 20"), but keep simple for now
        return None

    @staticmethod
    def _extract_budget(normalized: str) -> float | None:
        """Extract budget limits from amount mentions."""
        # Simple pattern: $ or numbers followed by "dollars", "usd", etc.
        matches = re.findall(r"\$\s*(\d+(?:\.\d{2})?)", normalized)
        return float(matches[0]) if matches else None

    @staticmethod
    def _extract_time_slot(normalized: str) -> tuple[str, str] | None:
        """Extract preferred time slots (morning, afternoon, evening)."""
        if "morning" in normalized:
            return ("06:00", "12:00")
        if "afternoon" in normalized:
            return ("12:00", "18:00")
        if "evening" in normalized:
            return ("18:00", "22:00")
        return None

    @staticmethod
    def _extract_exclusions(normalized: str) -> list[str]:
        """Extract schedule exclusions (weekends, after hours, etc.)."""
        exclusions = []
        if "not on weekends" in normalized or "weekdays only" in normalized:
            exclusions.append("weekends")
        if "after 5" in normalized or "after work" in normalized:
            exclusions.append("after_hours")
        if "not on holidays" in normalized:
            exclusions.append("holidays")
        return exclusions

    @staticmethod
    def _extract_numeric_values(normalized: str) -> dict[str, Any]:
        """Extract all numeric values (durations, quantities, etc.)."""
        values = {}
        # Durations: "30 minutes", "2 hours"
        duration_match = re.search(r"(\d+)\s*(?:minutes?|hours?|days?)", normalized)
        if duration_match:
            values["duration_amount"] = int(duration_match.group(1))
        # Quantities: "3 items", "5 servings"
        qty_match = re.search(r"(\d+)\s*(?:items?|servings?|units?)", normalized)
        if qty_match:
            values["quantity"] = int(qty_match.group(1))
        return values
