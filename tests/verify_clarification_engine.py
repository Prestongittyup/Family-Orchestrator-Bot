"""
Verification tests for ClarificationEngine.

Covers:
  - Determinism (same input → same plan always)
  - Minimal output (no over-questioning)
  - Context suppression (already-known fields are skipped)
  - Priority ordering (blocking/critical questions come first)
  - Non-guessing (engine never fills in an answer)
  - Unknown flag handling (no crash, no garbage question)
  - Blocking vs non-blocking classification
  - Plan serialisation
  - Integration with IntentParser ambiguity flags
"""

from __future__ import annotations

import pytest

from legacy.compiler.intent_parser import IntentParser
from legacy.conversation.clarification_engine import (
    ClarificationEngine,
    ClarificationPriority,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def engine():
    return ClarificationEngine()


@pytest.fixture
def parser():
    return IntentParser()


# ── Determinism ────────────────────────────────────────────────────────────────

class TestDeterminism:
    def test_same_flags_same_plan_structure(self, engine, parser):
        flags = ["multiple_recipients_unclear", "deadline_relative"]
        intent = parser.parse("Create task", household_id="h1", user_id="u1")

        plan1 = engine.generate(intent, flags)
        plan2 = engine.generate(intent, flags)

        assert [q.field for q in plan1.questions] == [q.field for q in plan2.questions]
        assert [q.question for q in plan1.questions] == [q.question for q in plan2.questions]
        assert [q.question_id for q in plan1.questions] == [q.question_id for q in plan2.questions]

    def test_question_ids_are_stable_not_random(self, engine, parser):
        """question_id must be deterministic (derived from field name, not UUID)."""
        flags = ["time_ambiguous", "frequency_vague"]
        intent = parser.parse("Remind me", household_id="h1", user_id="u1")

        plan = engine.generate(intent, flags)
        for q in plan.questions:
            assert q.question_id == f"cq_{q.field}"

    def test_duplicate_flags_deduped(self, engine, parser):
        flags = ["time_ambiguous", "time_ambiguous", "deadline_relative"]
        intent = parser.parse("Create task", household_id="h1", user_id="u1")

        plan = engine.generate(intent, flags)
        fields = [q.field for q in plan.questions]
        assert fields.count("time_ambiguous") == 1

    def test_repeated_generation_idempotent(self, engine):
        flags = ["household_context_missing", "deadline_relative", "frequency_vague"]
        results = [
            engine.generate_from_flags(flags)
            for _ in range(5)
        ]
        ref = [(q.field, q.priority) for q in results[0].questions]
        for plan in results[1:]:
            assert [(q.field, q.priority) for q in plan.questions] == ref


# ── Minimal output ─────────────────────────────────────────────────────────────

class TestMinimalOutput:
    def test_empty_flags_produces_empty_plan(self, engine, parser):
        intent = parser.parse("Create task", household_id="h1", user_id="u1")
        plan = engine.generate(intent, [])
        assert plan.questions == []

    def test_only_requested_fields_appear(self, engine, parser):
        flags = ["deadline_relative"]
        intent = parser.parse("Create task", household_id="h1", user_id="u1")
        plan = engine.generate(intent, flags)
        assert len(plan.questions) == 1
        assert plan.questions[0].field == "deadline_relative"

    def test_unknown_field_produces_no_question(self, engine, parser):
        """Unknown / unrecognised flags must not surface garbage questions."""
        flags = ["totally_made_up_flag"]
        intent = parser.parse("Create task", household_id="h1", user_id="u1")
        plan = engine.generate(intent, flags)
        # Unknown flag skipped (not in catalogue or blocking set)
        assert all(q.field != "totally_made_up_flag" for q in plan.questions)


# ── Context suppression ────────────────────────────────────────────────────────

class TestContextSuppression:
    def test_context_satisfied_field_is_skipped(self, engine, parser):
        flags = ["time_ambiguous", "deadline_relative"]
        intent = parser.parse("Create task", household_id="h1", user_id="u1")

        # Provide time_ambiguous in context → should be skipped
        plan = engine.generate(
            intent, flags,
            context={"time_ambiguous": "morning"}
        )
        fields = [q.field for q in plan.questions]
        assert "time_ambiguous" not in fields
        assert "deadline_relative" in fields

    def test_satisfied_field_lands_in_skipped_fields(self, engine, parser):
        flags = ["time_ambiguous"]
        intent = parser.parse("Create task", household_id="h1", user_id="u1")
        plan = engine.generate(intent, flags, context={"time_ambiguous": "afternoon"})
        assert "time_ambiguous" in plan.skipped_fields

    def test_all_context_satisfied_yields_empty_plan(self, engine, parser):
        flags = ["time_ambiguous", "frequency_vague"]
        intent = parser.parse("Create task", household_id="h1", user_id="u1")
        plan = engine.generate(
            intent, flags,
            context={"time_ambiguous": "morning", "frequency_vague": "daily"}
        )
        assert plan.questions == []
        assert set(plan.skipped_fields) == {"time_ambiguous", "frequency_vague"}


# ── Priority ordering ──────────────────────────────────────────────────────────

class TestPriorityOrdering:
    def test_critical_before_medium(self, engine):
        plan = engine.generate_from_flags(
            ["time_ambiguous", "household_context_missing"]
        )
        fields = [q.field for q in plan.questions]
        assert fields.index("household_context_missing") < fields.index("time_ambiguous")

    def test_high_before_medium(self, engine):
        plan = engine.generate_from_flags(
            ["time_ambiguous", "deadline_relative"]
        )
        fields = [q.field for q in plan.questions]
        assert fields.index("deadline_relative") < fields.index("time_ambiguous")

    def test_same_priority_ordered_alphabetically(self, engine):
        """Tie-break by field name ensures full determinism."""
        plan = engine.generate_from_flags(
            ["user_context_missing", "household_context_missing"]
        )
        fields = [q.field for q in plan.questions]
        assert fields == sorted(fields, key=lambda f: (
            next(q.priority for q in plan.questions if q.field == f), f
        ))


# ── Blocking classification ────────────────────────────────────────────────────

class TestBlockingClassification:
    def test_critical_fields_are_blocking(self, engine):
        plan = engine.generate_from_flags(["household_context_missing"])
        assert plan.has_blocking is True
        assert "household_context_missing" in plan.blocking_fields

    def test_non_blocking_field_not_in_blocking_list(self, engine):
        plan = engine.generate_from_flags(["time_ambiguous"])
        # time_ambiguous is not in any intent's blocking set
        assert "time_ambiguous" not in plan.blocking_fields

    def test_blocking_field_for_specific_intent(self, engine):
        # deadline_relative is blocking for schedule_change
        plan = engine.generate_from_flags(
            ["deadline_relative"],
            intent_type="schedule_change",
        )
        assert "deadline_relative" in plan.blocking_fields

    def test_plan_blocking_false_when_only_non_blocking(self, engine):
        plan = engine.generate_from_flags(["time_ambiguous"])
        # time_ambiguous not in any blocking set and base_blocking=False
        assert plan.has_blocking is False


# ── Non-guessing guarantee ─────────────────────────────────────────────────────

class TestNonGuessing:
    def test_questions_have_no_pre_filled_answer(self, engine):
        """Each question is a question, not an answer."""
        plan = engine.generate_from_flags([
            "household_context_missing",
            "multiple_recipients_unclear",
            "deadline_relative",
            "time_ambiguous",
            "frequency_vague",
        ])
        for q in plan.questions:
            assert q.question.strip().endswith("?"), (
                f"Question '{q.question}' should end with '?' (it's a question, not a statement)"
            )

    def test_no_question_contains_assumed_value(self, engine):
        """No question text second-guesses the answer."""
        plan = engine.generate_from_flags(["time_ambiguous", "deadline_relative"])
        forbidden_phrases = ["I assume", "probably", "likely", "guessing", "default to"]
        for q in plan.questions:
            for phrase in forbidden_phrases:
                assert phrase.lower() not in q.question.lower(), (
                    f"Question '{q.question}' contains guessing phrase '{phrase}'"
                )


# ── Plan serialisation ─────────────────────────────────────────────────────────

class TestSerialization:
    def test_to_dict_is_json_safe(self, engine):
        import json
        plan = engine.generate_from_flags(["household_context_missing", "time_ambiguous"])
        d = plan.to_dict()
        # Should not raise
        json.dumps(d)

    def test_to_dict_has_required_keys(self, engine):
        plan = engine.generate_from_flags(["deadline_relative"])
        d = plan.to_dict()
        for key in ("intent_type", "has_blocking", "blocking_fields",
                    "non_blocking_fields", "questions", "skipped_fields"):
            assert key in d

    def test_question_dict_has_required_keys(self, engine):
        plan = engine.generate_from_flags(["deadline_relative"])
        for q_dict in plan.to_dict()["questions"]:
            for key in ("question_id", "field", "question", "priority",
                        "options", "hint", "is_blocking"):
                assert key in q_dict


# ── Integration with IntentParser ─────────────────────────────────────────────

class TestIntegration:
    def test_flags_from_parser_drive_plan(self, engine, parser):
        """Ambiguity flags produced by IntentParser feed directly into engine."""
        intent = parser.parse(
            "Create a task for Alice, Bob, or Charlie",
            household_id="h001",
            user_id="u1",
            context_snapshot={"family_members": ["Alice", "Bob", "Charlie"]},
        )
        assert "multiple_recipients_unclear" in intent.ambiguity_flags

        plan = engine.generate(intent, intent.ambiguity_flags)
        assert any(q.field == "multiple_recipients_unclear" for q in plan.questions)

    def test_plan_empty_when_no_parser_flags(self, engine, parser):
        intent = parser.parse("Create a task", household_id="h001", user_id="u1")
        # Force-clear any flags for a clean test
        object.__setattr__(intent, "ambiguity_flags", [])
        plan = engine.generate(intent, intent.ambiguity_flags)
        assert plan.questions == []
