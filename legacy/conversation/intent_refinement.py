"""
Intent Refinement — Merge user responses into a partial Intent and track
readiness for Step 15 compilation.

This module is the bridge between the conversation layer and the compiler.
After each clarification turn, the converstion engine holds a partial
Intent plus a dict of user-provided overrides.  IntentRefiner:

  1. Applies overrides to the frozen Intent, producing a new richer Intent.
  2. Scores completeness as a fraction of required structural slots filled.
  3. Checks whether the enriched Intent is safe to pass to
     WorkflowCompiler.compile() — without actually calling the compiler.

Design constraints
------------------
- STATELESS   — every method is a pure function over its arguments.
- NO EXECUTION — no workflow generation, no DAG calls, no scheduler.
- NO GUESSING  — overrides are applied verbatim; values are never inferred.
- FROZEN OUTPUT — the refined Intent is a new frozen dataclass instance
                  created via dataclasses.replace().
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any

from legacy.compiler.intent_parser import Intent, IntentType


# ── Compiler-blocking flags ────────────────────────────────────────────────────
#
# These mirror WorkflowCompiler._check_ambiguities() exactly.  If any of
# these remain in intent.ambiguity_flags after overrides are applied, the
# compiler will raise AmbiguousIntentError.
#
# Kept here (not imported from workflow_compiler) to avoid pulling execution
# code into the conversation layer.

_COMPILER_BLOCKING_FLAGS: frozenset[str] = frozenset({
    "multiple_recipients_unclear",
    "resource_missing",
})

# ── Required slot specs ───────────────────────────────────────────────────────
#
# A slot spec is (section, key):
#
#   ("intent_type",  None)          → intent_type != "unknown"
#   ("identity",     "household_id") → intent.household_id is truthy
#   ("identity",     "user_id")      → intent.user_id is truthy
#   ("entities",     "subject")      → intent.entities["subject"] is truthy
#   ("constraints",  "deadline")     → intent.constraints["deadline"] is not None
#   ("recurrence",   "frequency")    → intent.recurrence_hints["frequency"] is truthy
#
# The score is filled_slots / (total_slots + remaining_ambiguity_flags).

_SlotSpec = tuple[str, str | None]

_REQUIRED_SLOTS_BY_INTENT: dict[str, list[_SlotSpec]] = {
    "task_creation": [
        ("intent_type", None),
        ("identity",    "household_id"),
        ("identity",    "user_id"),
        ("entities",    "subject"),
    ],
    "schedule_change": [
        ("intent_type", None),
        ("identity",    "household_id"),
        ("identity",    "user_id"),
        ("constraints", "deadline"),
    ],
    "schedule_query": [
        ("intent_type", None),
        ("identity",    "household_id"),
        ("identity",    "user_id"),
    ],
    "reminder_set": [
        ("intent_type", None),
        ("identity",    "household_id"),
        ("identity",    "user_id"),
        ("entities",    "subject"),
        ("recurrence",  "frequency"),
    ],
    "notification_config": [
        ("intent_type", None),
        ("identity",    "household_id"),
        ("identity",    "user_id"),
    ],
    "budget_query": [
        ("intent_type", None),
        ("identity",    "household_id"),
        ("identity",    "user_id"),
    ],
    "health_checkin": [
        ("intent_type", None),
        ("identity",    "household_id"),
        ("identity",    "user_id"),
    ],
    "inventory_update": [
        ("intent_type", None),
        ("identity",    "household_id"),
        ("identity",    "user_id"),
    ],
    "meal_planning": [
        ("intent_type", None),
        ("identity",    "household_id"),
        ("identity",    "user_id"),
    ],
    # "unknown" can never satisfy the intent_type slot — score caps below 1.0
    "unknown": [
        ("intent_type", None),
        ("identity",    "household_id"),
        ("identity",    "user_id"),
    ],
}


# ── Flag → field mapping ───────────────────────────────────────────────────────

def _apply_flag_resolution(
    flag: str,
    value: Any,
    entities: dict[str, Any],
    constraints: dict[str, Any],
    recurrence: dict[str, Any],
) -> None:
    """
    Mutate the working copies of entity/constraint/recurrence dicts to
    reflect the resolution of a single ambiguity flag.

    Only top-level keys are written; inner objects are never mutated.
    This is the single authoritative mapping from flag name → field path.
    """
    if flag in ("multiple_recipients_unclear", "member_not_found"):
        entities["recipients"] = [value] if not isinstance(value, list) else value

    elif flag == "deadline_relative":
        constraints["deadline"] = value

    elif flag == "time_ambiguous":
        vals = dict(entities.get("values") or {})
        vals["time_of_day"] = value
        entities["values"] = vals

    elif flag in ("resource_missing", "budget_limit_unset"):
        constraints["budget_limit"] = value

    elif flag == "frequency_vague":
        recurrence["frequency"] = value
        recurrence["is_recurring"] = True

    # "household_context_missing" / "user_context_missing":
    # These flags are cleared by their presence in overrides; no field
    # update is required because household_id / user_id are identity
    # fields set at Intent creation time, not overridable here.


# ── Output ─────────────────────────────────────────────────────────────────────

@dataclass
class RefinementResult:
    """
    Immutable result of a single refinement pass.

    Contains a new enriched Intent, a completeness score, and a readiness
    gate for the Step 15 compiler.  Does not contain any execution
    instructions or workflow definitions.
    """

    refined_intent: Intent
    """
    New frozen Intent built from the original with overrides applied.
    The original Intent is never modified.
    """

    completeness: float
    """
    Fraction of required structural slots filled, in [0.0, 1.0].
    Incorporates a penalty for unresolved ambiguity flags:
      score = filled_slots / (total_slots + remaining_flag_count)
    A score of 1.0 requires both all slots filled AND no ambiguity flags.
    """

    remaining_flags: list[str]
    """Ambiguity flags still unresolved after this refinement pass."""

    resolved_fields: list[str]
    """Flag names consumed by this refinement pass (sorted for determinism)."""

    is_compiler_ready: bool
    """
    True if and only if the refined Intent can be passed to
    WorkflowCompiler.compile() without raising.

    Mirrors three compiler gates (no compiler import required):
      1. intent_type != "unknown"
      2. No blocking ambiguity flags in intent.ambiguity_flags
      3. household_id and user_id are non-empty strings
    """

    blocking_flags: list[str]
    """
    Subset of remaining_flags that would raise AmbiguousIntentError.
    An empty list does NOT guarantee compiler readiness on its own —
    check is_compiler_ready for the full gate.
    """

    def summary(self) -> dict[str, Any]:
        """Lightweight dict suitable for logging or session metadata."""
        return {
            "intent_type": self.refined_intent.intent_type,
            "completeness": round(self.completeness, 3),
            "is_compiler_ready": self.is_compiler_ready,
            "remaining_flags": self.remaining_flags,
            "blocking_flags": self.blocking_flags,
            "resolved_fields": self.resolved_fields,
        }


# ── Engine ─────────────────────────────────────────────────────────────────────

class IntentRefiner:
    """
    Merges user responses into a partial Intent and tracks readiness for
    the Step 15 workflow compiler.

    Stateless — no internal state is maintained between calls.  The caller
    (typically ConversationEngine or its test harness) owns the Intent and
    the overrides dict.

    This class:
      - DOES NOT compile intents to DAGs
      - DOES NOT execute workflows
      - DOES NOT call the scheduler
      - DOES NOT make network or database calls
    """

    def apply_patch(
        self,
        intent: Intent,
        overrides: dict[str, Any],
    ) -> RefinementResult:
        """
        Apply user-provided overrides to a partial Intent.

        Creates and returns a new frozen Intent.  The original is unchanged.

        Args:
            intent:
                The current partial Intent (e.g., from
                ConversationSession.current_intent).
            overrides:
                Mapping of {ambiguity_flag: resolved_value} collected from
                the conversation (e.g., ConversationSession.intent_overrides
                or a single new clarification response).

        Returns:
            RefinementResult containing the enriched Intent, completeness
            score, and compiler readiness flag.
        """
        # Shallow copies of the nested dicts — we only write top-level keys.
        entities: dict[str, Any]    = dict(intent.entities)
        constraints: dict[str, Any] = dict(intent.constraints)
        recurrence: dict[str, Any]  = dict(intent.recurrence_hints)

        current_flags: set[str] = set(intent.ambiguity_flags)
        resolved_fields: list[str] = []

        for flag, value in overrides.items():
            if flag in current_flags:
                _apply_flag_resolution(flag, value, entities, constraints, recurrence)
                current_flags.discard(flag)
                resolved_fields.append(flag)

        remaining_flags = [f for f in intent.ambiguity_flags if f in current_flags]

        # dataclasses.replace() works on frozen dataclasses — it constructs a
        # new instance without calling __setattr__ on the frozen original.
        refined: Intent = dataclasses.replace(
            intent,
            entities=entities,
            constraints=constraints,
            recurrence_hints=recurrence,
            ambiguity_flags=remaining_flags,
        )

        completeness   = self._compute_score(refined)
        blocking_flags = [f for f in remaining_flags if f in _COMPILER_BLOCKING_FLAGS]
        ready          = self._check_compiler_ready(refined)

        return RefinementResult(
            refined_intent=refined,
            completeness=completeness,
            remaining_flags=remaining_flags,
            resolved_fields=sorted(resolved_fields),
            is_compiler_ready=ready,
            blocking_flags=blocking_flags,
        )

    def score(
        self,
        intent: Intent,
        overrides: dict[str, Any] | None = None,
    ) -> float:
        """
        Compute a completeness score without producing a refined Intent.

        If ``overrides`` are provided, they are hypothetically applied
        before scoring; the original intent is never modified.

        Returns:
            Float in [0.0, 1.0].
        """
        if overrides:
            return self.apply_patch(intent, overrides).completeness
        return self._compute_score(intent)

    def is_compiler_ready(
        self,
        intent: Intent,
        overrides: dict[str, Any] | None = None,
    ) -> bool:
        """
        Check compiler readiness without producing a refined Intent.

        If ``overrides`` are provided they are hypothetically applied
        before the check; the original intent is never modified.

        Returns:
            True if the (optionally enriched) Intent can be compiled.
        """
        if overrides:
            return self.apply_patch(intent, overrides).is_compiler_ready
        return self._check_compiler_ready(intent)

    # ── Private helpers ────────────────────────────────────────────────────

    def _compute_score(self, intent: Intent) -> float:
        """
        Score = filled_required_slots / (total_required_slots + remaining_flags).

        Unresolved ambiguity flags each add one unit to the denominator so
        an intent with outstanding flags can never reach 1.0.
        """
        slots = _REQUIRED_SLOTS_BY_INTENT.get(
            intent.intent_type,
            _REQUIRED_SLOTS_BY_INTENT["unknown"],
        )
        total = len(slots) + len(intent.ambiguity_flags)
        if total == 0:
            return 1.0
        filled = sum(1 for s in slots if _slot_filled(intent, s))
        return min(1.0, filled / total)

    @staticmethod
    def _check_compiler_ready(intent: Intent) -> bool:
        """
        Mirror of WorkflowCompiler's two-stage validation (no import needed):
          Stage 1 — UnsupportedIntentError: intent_type == "unknown"
          Stage 2 — AmbiguousIntentError:   blocking flags still present
          Stage 3 — Structural:             identity fields must be non-empty
        """
        if intent.intent_type == "unknown":
            return False
        if set(intent.ambiguity_flags) & _COMPILER_BLOCKING_FLAGS:
            return False
        if not intent.household_id or not intent.user_id:
            return False
        return True


# ── Slot-filled predicate (module-level for testability) ──────────────────────

def _slot_filled(intent: Intent, slot: _SlotSpec) -> bool:
    section, key = slot
    if section == "intent_type":
        return intent.intent_type != "unknown"
    if section == "identity":
        return bool(getattr(intent, key, None))
    if section == "entities":
        return bool(intent.entities.get(key))
    if section == "constraints":
        return intent.constraints.get(key) is not None
    if section == "recurrence":
        return bool(intent.recurrence_hints.get(key))
    return False
