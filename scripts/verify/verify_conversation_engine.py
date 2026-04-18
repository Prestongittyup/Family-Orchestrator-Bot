"""
Verification tests for ConversationEngine — multi-turn state management.

Covers:
  - Session creation
  - Message history with bounded window
  - Intent application and state transitions
  - Clarification queue management
  - Output state correctness
  - Isolation: no execution, no DAG, no scheduler
"""

from __future__ import annotations

import pytest

from legacy.compiler.intent_parser import IntentParser
from legacy.conversation import (
    ConversationEngine,
    ConversationSession,
    ConversationState,
    ClarificationRequest,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def engine():
    return ConversationEngine(max_history=10)


@pytest.fixture
def parser():
    return IntentParser()


@pytest.fixture
def session(engine):
    return engine.new_session(user_id="user_alice", household_id="h001")


# ── Session creation ───────────────────────────────────────────────────────────

class TestSessionCreation:
    def test_new_session_has_unique_ids(self, engine):
        s1 = engine.new_session("u1", "h1")
        s2 = engine.new_session("u1", "h1")
        assert s1.session_id != s2.session_id

    def test_new_session_initial_state(self, engine):
        s = engine.new_session("u1", "h1")
        assert s.state == ConversationState.AWAITING_CLARIFICATION
        assert s.current_intent is None
        assert s.history == []
        assert s.clarification_queue == []
        assert s.intent_overrides == {}

    def test_new_session_captures_identifiers(self, engine):
        s = engine.new_session("user_bob", "h999", metadata={"channel": "web"})
        assert s.user_id == "user_bob"
        assert s.household_id == "h999"
        assert s.metadata["channel"] == "web"


# ── Message history ────────────────────────────────────────────────────────────

class TestMessageHistory:
    def test_ingest_adds_to_history(self, engine, session):
        engine.ingest_message(session, "Hello", role="user")
        assert len(session.history) == 1
        assert session.history[0].content == "Hello"
        assert session.history[0].role == "user"

    def test_bounded_window_evicts_oldest(self, engine):
        small_engine = ConversationEngine(max_history=3)
        s = small_engine.new_session("u1", "h1")
        for i in range(5):
            small_engine.ingest_message(s, f"msg {i}")
        assert len(s.history) == 3
        assert s.history[0].content == "msg 2"  # first two evicted

    def test_history_preserves_roles(self, engine, session):
        engine.ingest_message(session, "What can I help with?", role="assistant")
        engine.ingest_message(session, "Create a task", role="user")
        assert session.history[0].role == "assistant"
        assert session.history[1].role == "user"

    def test_message_metadata_stored(self, engine, session):
        engine.ingest_message(session, "Test", metadata={"source": "api"})
        assert session.history[0].metadata["source"] == "api"


# ── Intent application and state transitions ───────────────────────────────────

class TestIntentApplication:
    def test_unambiguous_intent_transitions_to_ready(self, engine, session, parser):
        intent = parser.parse("Create a task", household_id="h001", user_id="user_alice")
        # Clear any flags that might have been set
        object.__setattr__(intent, "ambiguity_flags", [])
        engine.apply_intent(session, intent)
        assert session.current_intent is intent
        assert session.state == ConversationState.READY_FOR_COMPILATION

    def test_ambiguous_intent_transitions_to_awaiting(self, engine, session, parser):
        intent = parser.parse(
            "Create a task for Alice, Bob, or Charlie",
            household_id="h001",
            user_id="user_alice",
            context_snapshot={"family_members": ["Alice", "Bob", "Charlie"]},
        )
        # Should have multiple_recipients_unclear flag
        assert "multiple_recipients_unclear" in intent.ambiguity_flags
        engine.apply_intent(session, intent)
        assert session.state == ConversationState.AWAITING_CLARIFICATION

    def test_apply_intent_enqueues_clarifications(self, engine, session, parser):
        intent = parser.parse(
            "Create a task for Alice, Bob, or Charlie",
            household_id="h001",
            user_id="user_alice",
            context_snapshot={"family_members": ["Alice", "Bob", "Charlie"]},
        )
        engine.apply_intent(session, intent)
        pending = [cq for cq in session.clarification_queue if not cq.resolved]
        assert len(pending) == len(intent.ambiguity_flags)

    def test_apply_intent_does_not_duplicate_flags(self, engine, session, parser):
        intent = parser.parse(
            "Create a task for Alice, Bob, or Charlie",
            household_id="h001",
            user_id="user_alice",
            context_snapshot={"family_members": ["Alice", "Bob", "Charlie"]},
        )
        # Apply same intent twice
        engine.apply_intent(session, intent)
        engine.apply_intent(session, intent)
        flags = [cq.ambiguity_flag for cq in session.clarification_queue]
        assert len(flags) == len(set(flags))  # no duplicates


# ── Clarification queue ────────────────────────────────────────────────────────

class TestClarificationQueue:
    def test_active_clarification_is_first_unresolved(self, engine, session):
        engine.enqueue_clarification(session, "time_ambiguous")
        engine.enqueue_clarification(session, "deadline_relative")
        active = session.active_clarification
        assert active.ambiguity_flag == "time_ambiguous"

    def test_resolving_advances_to_next(self, engine, session):
        engine.enqueue_clarification(session, "time_ambiguous")
        engine.enqueue_clarification(session, "deadline_relative")
        engine.apply_clarification_response(session, "morning")
        active = session.active_clarification
        assert active.ambiguity_flag == "deadline_relative"

    def test_all_resolved_transitions_state(self, engine, session, parser):
        intent = parser.parse(
            "Create a task for Alice, Bob, or Charlie",
            household_id="h001",
            user_id="user_alice",
            context_snapshot={"family_members": ["Alice", "Bob", "Charlie"]},
        )
        engine.apply_intent(session, intent)
        assert session.state == ConversationState.AWAITING_CLARIFICATION

        # Resolve all pending clarifications
        while session.active_clarification is not None:
            engine.apply_clarification_response(session, "Alice")

        assert session.state == ConversationState.READY_FOR_COMPILATION
        assert session.all_clarifications_resolved is True

    def test_resolve_stores_answer_in_overrides(self, engine, session):
        engine.enqueue_clarification(session, "time_ambiguous")
        engine.apply_clarification_response(session, "afternoon")
        assert session.intent_overrides.get("time_ambiguous") == "afternoon"

    def test_no_duplicate_flags_enqueued(self, engine, session):
        engine.enqueue_clarification(session, "time_ambiguous")
        engine.enqueue_clarification(session, "time_ambiguous")  # duplicate
        flags = [cq.ambiguity_flag for cq in session.clarification_queue]
        assert flags.count("time_ambiguous") == 1

    def test_enqueue_clarification_sets_awaiting_state(self, engine, session, parser):
        # Start with a ready session
        intent = parser.parse("Create a task", household_id="h001", user_id="user_alice")
        object.__setattr__(intent, "ambiguity_flags", [])
        engine.apply_intent(session, intent)
        assert session.state == ConversationState.READY_FOR_COMPILATION

        # Adding a clarification should revert to awaiting
        engine.enqueue_clarification(session, "deadline_relative")
        assert session.state == ConversationState.AWAITING_CLARIFICATION

    def test_get_next_question_returns_text(self, engine, session):
        engine.enqueue_clarification(session, "time_ambiguous")
        question = engine.get_next_question(session)
        assert question is not None
        assert len(question) > 0

    def test_get_next_question_none_when_empty(self, engine, session):
        assert engine.get_next_question(session) is None

    def test_question_includes_options_when_present(self, engine, session):
        engine.enqueue_clarification(session, "time_ambiguous")
        question = engine.get_next_question(session)
        # time_ambiguous has predefined options
        assert "morning" in question or "Options" in question


# ── Output state correctness ───────────────────────────────────────────────────

class TestOutputStates:
    def test_no_intent_is_awaiting(self, engine, session):
        assert session.state == ConversationState.AWAITING_CLARIFICATION
        assert session.current_intent is None

    def test_intent_no_flags_is_ready(self, engine, session, parser):
        intent = parser.parse("Check the household budget", household_id="h001", user_id="user_alice")
        object.__setattr__(intent, "ambiguity_flags", [])
        engine.apply_intent(session, intent)
        assert session.state == ConversationState.READY_FOR_COMPILATION

    def test_reset_returns_to_awaiting(self, engine, session, parser):
        intent = parser.parse("Create a task", household_id="h001", user_id="user_alice")
        object.__setattr__(intent, "ambiguity_flags", [])
        engine.apply_intent(session, intent)
        assert session.state == ConversationState.READY_FOR_COMPILATION

        engine.reset_for_new_intent(session)
        assert session.state == ConversationState.AWAITING_CLARIFICATION
        assert session.current_intent is None
        assert session.clarification_queue == []

    def test_reset_preserves_history(self, engine, session, parser):
        engine.ingest_message(session, "First message")
        engine.ingest_message(session, "Second message")
        engine.reset_for_new_intent(session)
        assert len(session.history) == 2  # history preserved


# ── Execution isolation ────────────────────────────────────────────────────────

class TestExecutionIsolation:
    def test_engine_has_no_execute(self, engine):
        assert not hasattr(engine, "execute")
        assert not hasattr(engine, "dispatch")
        assert not hasattr(engine, "run_workflow")

    def test_session_has_no_execute(self, engine, session):
        assert not hasattr(session, "execute")
        assert not hasattr(session, "compile")
        assert not hasattr(session, "dispatch")

    def test_conversation_state_is_strings(self):
        # All states are string values, not callables
        for state in ConversationState:
            assert isinstance(state.value, str)
            assert not callable(state.value)


# ── Summary ────────────────────────────────────────────────────────────────────

class TestSummary:
    def test_summary_returns_required_fields(self, engine, session):
        s = engine.summary(session)
        for key in ("session_id", "user_id", "household_id", "state",
                    "message_count", "intent_type", "pending_clarifications",
                    "intent_overrides_applied", "updated_at"):
            assert key in s

    def test_summary_reflects_correct_state(self, engine, session, parser):
        engine.ingest_message(session, "Hello")
        engine.enqueue_clarification(session, "time_ambiguous")
        s = engine.summary(session)
        assert s["message_count"] == 1
        assert s["pending_clarifications"] == 1
        assert s["state"] == ConversationState.AWAITING_CLARIFICATION.value
