"""
Clarification Engine — Generate deterministic clarification questions for
incomplete intent.

Given a partial Intent, a list of missing/ambiguous fields, and optional
context, this engine produces the minimal ordered set of questions needed to
make the intent compilable. It never guesses missing values and never
over-asks.

Design principles
-----------------
1. DETERMINISTIC  — Identical (intent, missing_fields, context) → identical
                    ClarificationPlan.  Questions are sorted by a fixed
                    priority ranking before return.

2. MINIMAL        — Only fields that are on the critical path to compilation
                    are included.  Optional or inferable fields are suppressed.

3. NON-GUESSING   — No default values are substituted.  Every question asks
                    the user, never silently fills in an answer.

4. ISOLATED       — No workflow execution, no DAG calls, no scheduler
                    interaction.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

from legacy.compiler.intent_parser import Intent, IntentType


# ── Priority levels ────────────────────────────────────────────────────────────

class ClarificationPriority(IntEnum):
    """Numeric priority (lower = asked first)."""

    CRITICAL = 1  # Blocks any further processing
    HIGH     = 2  # Required for the specific intent type
    MEDIUM   = 3  # Enriches but not absolutely required
    LOW      = 4  # Nice-to-have


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class ClarificationQuestion:
    """
    A single structured clarification question.

    Designed to map directly to a conversation turn and to be
    serialisable without circular imports.
    """

    question_id: str
    """Stable ID derived from the flag name — same input → same ID."""

    field: str
    """The ambiguity flag or missing field this question resolves."""

    question: str
    """Natural language question to present to the user."""

    priority: ClarificationPriority
    """Lower value = asked sooner."""

    options: list[str] = field(default_factory=list)
    """
    Constrained choice set, or empty for free text.
    When non-empty the answer MUST be one of these values.
    """

    hint: str = ""
    """
    Optional one-sentence hint shown alongside the question.
    Explains WHY this information is needed without guessing.
    """

    is_blocking: bool = True
    """
    True  — intent cannot be compiled until this is resolved.
    False — enriches but does not block compilation.
    """


@dataclass
class ClarificationPlan:
    """
    Ordered minimal set of clarification questions for a partial intent.

    Questions are sorted by priority (ascending) then field name
    (alphabetically) for full determinism.
    """

    intent_type: IntentType | None
    questions: list[ClarificationQuestion]
    """Ordered, deduplicated, minimal question list."""

    skipped_fields: list[str] = field(default_factory=list)
    """
    Fields that were considered but suppressed because they are
    non-blocking or resolvable from context.
    """

    @property
    def has_blocking(self) -> bool:
        """True if any question blocks compilation."""
        return any(q.is_blocking for q in self.questions)

    @property
    def blocking_fields(self) -> list[str]:
        return [q.field for q in self.questions if q.is_blocking]

    @property
    def non_blocking_fields(self) -> list[str]:
        return [q.field for q in self.questions if not q.is_blocking]

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent_type": self.intent_type,
            "has_blocking": self.has_blocking,
            "blocking_fields": self.blocking_fields,
            "non_blocking_fields": self.non_blocking_fields,
            "questions": [
                {
                    "question_id": q.question_id,
                    "field": q.field,
                    "question": q.question,
                    "priority": q.priority.name,
                    "options": q.options,
                    "hint": q.hint,
                    "is_blocking": q.is_blocking,
                }
                for q in self.questions
            ],
            "skipped_fields": self.skipped_fields,
        }


# ── Question catalogue ─────────────────────────────────────────────────────────
#
# Keyed by ambiguity flag / missing field name.
# Each entry: (question, priority, options, hint, is_blocking)
#
# This catalogue is the single source of truth.  Adding a new flag here
# is the only change required to support it throughout the system.

_CATALOGUE: dict[str, tuple[str, ClarificationPriority, list[str], str, bool]] = {
    # ── Structural / identity ─────────────────────────────────────────────
    "household_context_missing": (
        "I couldn't find a household profile. What is your household ID?",
        ClarificationPriority.CRITICAL,
        [],
        "Required to look up preferences, members and settings.",
        True,
    ),
    "user_context_missing": (
        "I don't have a profile for you yet. What is your name or user ID?",
        ClarificationPriority.CRITICAL,
        [],
        "Required to personalise scheduling and notifications.",
        True,
    ),
    # ── Recipients / assignment ───────────────────────────────────────────
    "multiple_recipients_unclear": (
        "Who should this task be assigned to?",
        ClarificationPriority.HIGH,
        [],
        "Multiple people were mentioned; please name one.",
        True,
    ),
    "member_not_found": (
        "That name doesn't match any household member. Who did you mean?",
        ClarificationPriority.HIGH,
        [],
        "Provide a name exactly as it appears in your household.",
        True,
    ),
    # ── Timing / scheduling ───────────────────────────────────────────────
    "deadline_relative": (
        "When exactly is this needed?",
        ClarificationPriority.HIGH,
        [],
        "A relative deadline like 'soon' or 'ASAP' can't be scheduled precisely. Try: 'by Friday 5 pm'.",
        True,
    ),
    "time_ambiguous": (
        "Which part of the day did you mean?",
        ClarificationPriority.MEDIUM,
        ["morning (06:00–12:00)", "afternoon (12:00–18:00)", "evening (18:00–22:00)"],
        "Helps schedule the task in your preferred window.",
        False,
    ),
    # ── Budget / resources ────────────────────────────────────────────────
    "budget_limit_unset": (
        "What is the budget limit for this?",
        ClarificationPriority.HIGH,
        [],
        "No household budget is set; an explicit amount is required.",
        True,
    ),
    "resource_missing": (
        "What resources or budget amount are needed for this task?",
        ClarificationPriority.MEDIUM,
        [],
        "Required to validate feasibility before scheduling.",
        False,
    ),
    # ── Recurrence ────────────────────────────────────────────────────────
    "frequency_vague": (
        "How often should this repeat?",
        ClarificationPriority.MEDIUM,
        ["daily", "every 2 days", "weekly", "monthly", "custom"],
        "A vague frequency like 'sometimes' can't produce a schedule.",
        True,
    ),
}

# ── Fields that are blocking for each intent type ─────────────────────────────
#
# A field is blocking if its absence prevents the WorkflowCompiler from
# producing a valid DAG for that intent type.

_BLOCKING_FIELDS_BY_INTENT: dict[str, set[str]] = {
    "task_creation":       {"multiple_recipients_unclear", "household_context_missing",
                            "user_context_missing", "member_not_found"},
    "schedule_change":     {"deadline_relative", "household_context_missing",
                            "user_context_missing"},
    "schedule_query":      {"household_context_missing"},
    "reminder_set":        {"frequency_vague", "household_context_missing",
                            "user_context_missing"},
    "budget_query":        {"household_context_missing"},
    "health_checkin":      {"household_context_missing"},
    "meal_planning":       {"household_context_missing"},
    "inventory_update":    {"household_context_missing"},
    "notification_config": {"household_context_missing", "user_context_missing"},
    "unknown":             {"household_context_missing", "user_context_missing"},
}


# ── Engine ─────────────────────────────────────────────────────────────────────

class ClarificationEngine:
    """
    Generates a deterministic, minimal ClarificationPlan from a partial Intent.

    Usage::

        engine = ClarificationEngine()
        plan = engine.generate(
            intent=my_intent,
            missing_fields=["deadline_relative", "time_ambiguous"],
        )
        for q in plan.questions:
            print(q.question)
    """

    def generate(
        self,
        intent: Intent,
        missing_fields: list[str],
        context: dict[str, Any] | None = None,
    ) -> ClarificationPlan:
        """
        Generate a minimal, ordered ClarificationPlan.

        Args:
            intent:
                The partial Intent obtained from IntentParser.
            missing_fields:
                Explicit list of field names / ambiguity flags that must be
                resolved.  Typically sourced from
                ``intent.ambiguity_flags + enriched.new_ambiguities``.
            context:
                Optional dict with household/user/time context already
                resolved.  Fields whose values are present in context are
                suppressed from the output plan.

        Returns:
            ClarificationPlan with questions sorted by priority then field
            name.  Same arguments always produce the same plan.
        """
        context = context or {}

        # Step 1: Deduplicate the requested fields (preserve first occurrence)
        seen: set[str] = set()
        unique_fields: list[str] = []
        for f in missing_fields:
            if f not in seen:
                seen.add(f)
                unique_fields.append(f)

        # Step 2: Suppress fields already satisfied by context
        unsatisfied, skipped = self._filter_satisfied(
            unique_fields, intent.intent_type, context
        )

        # Step 3: Build a question per unsatisfied field
        questions: list[ClarificationQuestion] = []
        for field_name in unsatisfied:
            q = self._build_question(field_name, intent.intent_type)
            if q is not None:
                questions.append(q)
            # Unknown flags produce no question — they're left for the caller

        # Step 4: Deterministic sort — priority ASC, field name ASC
        questions.sort(key=lambda q: (q.priority, q.field))

        return ClarificationPlan(
            intent_type=intent.intent_type,
            questions=questions,
            skipped_fields=skipped,
        )

    def generate_from_flags(
        self,
        flags: list[str],
        intent_type: IntentType | None = None,
        context: dict[str, Any] | None = None,
    ) -> ClarificationPlan:
        """
        Convenience method: generate a plan from a plain list of flags without
        a full Intent object.  Useful in unit tests and adapter layers.

        Args:
            flags:       Ambiguity flag names.
            intent_type: Optional intent type for blocking determination.
            context:     Optional context for suppression.
        """
        context = context or {}
        seen: set[str] = set()
        unique: list[str] = []
        for f in flags:
            if f not in seen:
                seen.add(f)
                unique.append(f)

        unsatisfied, skipped = self._filter_satisfied(unique, intent_type, context)

        questions: list[ClarificationQuestion] = []
        for field_name in unsatisfied:
            q = self._build_question(field_name, intent_type)
            if q is not None:
                questions.append(q)

        questions.sort(key=lambda q: (q.priority, q.field))

        return ClarificationPlan(
            intent_type=intent_type,
            questions=questions,
            skipped_fields=skipped,
        )

    # ── Private helpers ────────────────────────────────────────────────────

    def _filter_satisfied(
        self,
        fields: list[str],
        intent_type: str | None,
        context: dict[str, Any],
    ) -> tuple[list[str], list[str]]:
        """
        Split fields into (unsatisfied, skipped).

        A field is skipped (suppressed from the plan) when EITHER:
          - The context dict already contains a non-None value for that key.
          - The field is not blocking for this intent type AND context
            provides enough to proceed.

        Non-blocking fields that lack a context value are retained so the
        caller can still present them as optional enrichment.
        """
        unsatisfied: list[str] = []
        skipped: list[str] = []

        blocking_for_intent = _BLOCKING_FIELDS_BY_INTENT.get(
            intent_type or "unknown", set()
        )

        for f in fields:
            # Already present in context → skip
            if context.get(f) is not None:
                skipped.append(f)
                continue

            # Not in the catalogue and not blocking → skip (unknown optional)
            if f not in _CATALOGUE and f not in blocking_for_intent:
                skipped.append(f)
                continue

            unsatisfied.append(f)

        return unsatisfied, skipped

    def _build_question(
        self,
        field_name: str,
        intent_type: str | None,
    ) -> ClarificationQuestion | None:
        """
        Build a ClarificationQuestion from the catalogue.

        Returns None if the field is entirely unknown so the plan stays
        minimal and doesn't surface garbage questions.
        """
        entry = _CATALOGUE.get(field_name)
        if entry is None:
            return None

        question_text, base_priority, options, hint, base_blocking = entry

        # Override blocking status based on this specific intent type
        blocking_for_intent = _BLOCKING_FIELDS_BY_INTENT.get(
            intent_type or "unknown", set()
        )
        is_blocking = (field_name in blocking_for_intent) or base_blocking

        # Priority escalates to CRITICAL if it's intent-specific blocking
        priority = (
            ClarificationPriority.CRITICAL
            if field_name in blocking_for_intent
            and base_priority > ClarificationPriority.CRITICAL
            else base_priority
        )

        # Stable question_id: deterministic from field name (no UUID)
        question_id = f"cq_{field_name}"

        return ClarificationQuestion(
            question_id=question_id,
            field=field_name,
            question=question_text,
            priority=priority,
            options=list(options),  # copy so catalogue entries aren't mutated
            hint=hint,
            is_blocking=is_blocking,
        )
