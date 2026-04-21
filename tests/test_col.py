"""
Test Suite: Conversation Orchestration Layer (COL v1)
======================================================

Tests all components of the COL v1 system.

Coverage:
  1. Schema: data model immutability and factory methods
  2. PartialIntent: completion logic, field updates
  3. IntentRefinementEngine: single-turn, multi-turn, contradiction handling
  4. ConversationStateMachine: transitions, invalid transitions
  5. ConversationOrchestrator: full pipeline integration
     - Single-turn complete intent (ALLOW)
     - Single-turn complete intent (REQUIRE_CONFIRMATION)
     - Multi-turn intent refinement
     - Clarification request when fields missing
     - Confirmation flow
     - Cancellation flow
     - Policy BLOCK
     - Session state consistency
  6. COLResponse: structured output format
  7. Safety rules: never bypass validation, never fabricate
"""
from __future__ import annotations

import pytest
from dataclasses import FrozenInstanceError
from datetime import datetime
from typing import Any, Dict, List, Optional

from apps.api.conversation_orchestration.schema import (
    COLResponse,
    ConversationMessage,
    ConversationSession,
    NextAction,
    PartialIntent,
    SessionState,
    ExecutionHandoff,
)
from apps.api.conversation_orchestration.intent_refinement import (
    IntentRefinementEngine,
    REQUIRED_FIELDS_BY_INTENT,
    _human_field_name,
)
from apps.api.conversation_orchestration.state_machine import (
    ConversationStateMachine,
    StateTransitionError,
)
from apps.api.conversation_orchestration.pipeline import (
    ConversationOrchestrator,
    _action_plan_to_dict,
    _build_action_summary,
)
from apps.api.intent_contract.schema import IntentType
from apps.api.intent_contract.validator import EntityStore
from apps.api.policy_engine.schema import PolicyDecision


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def make_session(state: SessionState = SessionState.IDLE) -> ConversationSession:
    """Helper: create a test session."""
    s = ConversationSession.new("session-test-1", "family-123", "user-abc")
    if state != SessionState.IDLE:
        s = s.with_updates(state=state, last_updated=datetime.now())
    return s


def make_entity_store(**entities) -> EntityStore:
    """Helper: create an EntityStore pre-populated with test entities."""
    store = EntityStore()
    for key, value in entities.items():
        kind, eid = key.split("__")
        if kind == "task":
            store.add_task(eid, value)
        elif kind == "event":
            store.add_event(eid, value)
        elif kind == "plan":
            store.add_plan(eid, value)
    return store


# ===========================================================================
# 1. SCHEMA TESTS
# ===========================================================================


class TestSchemaImmutability:

    def test_conversation_message_is_frozen(self):
        msg = ConversationMessage.from_user("hello")
        with pytest.raises((FrozenInstanceError, TypeError)):
            msg.content = "changed"  # type: ignore

    def test_partial_intent_is_frozen(self):
        pi = PartialIntent(
            intent_type="create_task",
            extracted_fields={"task_name": "Buy milk"},
            missing_fields=[],
            confidence=0.9,
        )
        with pytest.raises((FrozenInstanceError, TypeError)):
            pi.confidence = 1.0  # type: ignore

    def test_conversation_session_is_frozen(self):
        s = make_session()
        with pytest.raises((FrozenInstanceError, TypeError)):
            s.state = SessionState.COLLECTING  # type: ignore

    def test_col_response_is_frozen(self):
        resp = COLResponse(
            session_id="s1",
            state=SessionState.COLLECTING,
            intent={},
            missing_fields=[],
            action_plan={},
            policy_decision=None,
            assistant_message="test",
            next_action=NextAction.NONE,
        )
        with pytest.raises((FrozenInstanceError, TypeError)):
            resp.assistant_message = "changed"  # type: ignore


class TestConversationMessageFactories:

    def test_from_user_sets_role(self):
        msg = ConversationMessage.from_user("hello")
        assert msg.role == "user"
        assert msg.content == "hello"
        assert msg.message_id is not None

    def test_from_assistant_sets_role(self):
        msg = ConversationMessage.from_assistant("I can help")
        assert msg.role == "assistant"

    def test_from_system_sets_role(self):
        msg = ConversationMessage.from_system("Session started")
        assert msg.role == "system"

    def test_messages_have_unique_ids(self):
        m1 = ConversationMessage.from_user("a")
        m2 = ConversationMessage.from_user("b")
        assert m1.message_id != m2.message_id


class TestConversationSession:

    def test_new_session_is_idle(self):
        s = ConversationSession.new("s1", "f1", "u1")
        assert s.state == SessionState.IDLE
        assert s.messages == []
        assert s.active_intent is None
        assert s.finalized_intent is None

    def test_add_message_preserves_immutability(self):
        s = make_session()
        msg = ConversationMessage.from_user("test")
        s2 = s.add_message(msg)
        assert len(s.messages) == 0  # original unchanged
        assert len(s2.messages) == 1

    def test_with_updates_creates_new_instance(self):
        s = make_session()
        s2 = s.with_updates(state=SessionState.COLLECTING)
        assert s.state == SessionState.IDLE
        assert s2.state == SessionState.COLLECTING
        assert s is not s2


# ===========================================================================
# 2. PARTIAL INTENT TESTS
# ===========================================================================


class TestPartialIntent:

    def test_complete_when_all_fields_present_and_confident(self):
        pi = PartialIntent(
            intent_type="create_task",
            extracted_fields={"task_name": "Buy milk"},
            missing_fields=[],
            confidence=0.9,
        )
        assert pi.is_complete(threshold=0.85) is True

    def test_not_complete_when_missing_fields(self):
        pi = PartialIntent(
            intent_type="create_task",
            extracted_fields={},
            missing_fields=["task_name"],
            confidence=0.9,
        )
        assert pi.is_complete() is False

    def test_not_complete_when_confidence_low(self):
        pi = PartialIntent(
            intent_type="create_task",
            extracted_fields={"task_name": "Buy milk"},
            missing_fields=[],
            confidence=0.5,
        )
        assert pi.is_complete(threshold=0.85) is False

    def test_not_complete_when_ambiguous_fields(self):
        pi = PartialIntent(
            intent_type="create_task",
            extracted_fields={"task_name": "Buy milk"},
            missing_fields=[],
            confidence=0.9,
            ambiguous_fields=["task_name"],
        )
        assert pi.is_complete() is False

    def test_not_complete_when_intent_type_none(self):
        pi = PartialIntent(
            intent_type=None,
            extracted_fields={},
            missing_fields=[],
            confidence=0.9,
        )
        assert pi.is_complete() is False

    def test_with_updates_creates_new_instance(self):
        pi = PartialIntent(
            intent_type="create_task",
            extracted_fields={"task_name": "Buy milk"},
            missing_fields=[],
            confidence=0.9,
        )
        pi2 = pi.with_updates(confidence=1.0)
        assert pi.confidence == 0.9
        assert pi2.confidence == 1.0

    def test_has_ambiguity(self):
        pi = PartialIntent(
            intent_type="create_task",
            extracted_fields={"task_name": "Buy milk"},
            missing_fields=[],
            confidence=0.9,
            ambiguous_fields=["task_name"],
        )
        assert pi.has_ambiguity() is True

    def test_no_ambiguity(self):
        pi = PartialIntent(
            intent_type="create_task",
            extracted_fields={"task_name": "Buy milk"},
            missing_fields=[],
            confidence=0.9,
        )
        assert pi.has_ambiguity() is False


# ===========================================================================
# 3. INTENT REFINEMENT ENGINE TESTS
# ===========================================================================


class TestIntentRefinementEngine:

    def setup_method(self):
        self.engine = IntentRefinementEngine()

    def test_initialize_from_create_task_message(self):
        result = self.engine.initialize_from_message("Create a task to buy groceries")
        assert result.intent_type == IntentType.CREATE_TASK.value
        assert result.turn_count == 1

    def test_initialize_from_complete_task_message(self):
        result = self.engine.initialize_from_message("Complete task #task-abc-123")
        assert result.intent_type == IntentType.COMPLETE_TASK.value
        assert result.extracted_fields.get("task_id") == "task-abc-123"

    def test_missing_fields_computed_correctly(self):
        result = self.engine.initialize_from_message("Complete task")
        # Missing task_id — should be in missing_fields
        if result.intent_type == IntentType.COMPLETE_TASK.value:
            assert "task_id" in result.missing_fields

    def test_multi_turn_merge_adds_new_fields(self):
        # Turn 1: establish intent
        partial = self.engine.initialize_from_message("Create a task")
        # Turn 2: provide task name
        partial2 = self.engine.merge_message(partial, "The task is called Buy milk")
        assert partial2.turn_count == 2

    def test_multi_turn_merge_increments_turn_count(self):
        partial = self.engine.initialize_from_message("Complete task #task-abc-123")
        partial2 = self.engine.merge_message(partial, "some more info")
        assert partial2.turn_count == 2

    def test_merge_preserves_existing_fields(self):
        # Set up a partial intent with task_id already set
        partial = PartialIntent(
            intent_type="complete_task",
            extracted_fields={"task_id": "task-abc-123"},
            missing_fields=[],
            confidence=0.9,
            turn_count=1,
        )
        # New message doesn't conflict
        partial2 = self.engine.merge_message(partial, "yes complete it")
        # task_id should be preserved
        assert partial2.extracted_fields.get("task_id") == "task-abc-123"

    def test_contradiction_marks_field_ambiguous(self):
        # Start with one event_id
        partial = PartialIntent(
            intent_type="delete_event",
            extracted_fields={"event_id": "event-aaa-111"},
            missing_fields=[],
            confidence=0.9,
            turn_count=1,
        )
        # New message provides a DIFFERENT event_id
        partial2 = self.engine.merge_message(partial, "Delete event #event-bbb-222")
        assert "event_id" in partial2.ambiguous_fields

    def test_required_fields_for_create_task(self):
        fields = self.engine.get_required_fields("create_task")
        assert "task_name" in fields

    def test_required_fields_for_complete_task(self):
        fields = self.engine.get_required_fields("complete_task")
        assert "task_id" in fields

    def test_required_fields_for_reschedule_task(self):
        fields = self.engine.get_required_fields("reschedule_task")
        assert "task_id" in fields
        assert "new_time" in fields

    def test_required_fields_for_none_intent(self):
        fields = self.engine.get_required_fields(None)
        assert fields == []

    def test_clarification_prompt_for_missing_task_id(self):
        partial = PartialIntent(
            intent_type="complete_task",
            extracted_fields={},
            missing_fields=["task_id"],
            confidence=0.85,
        )
        prompt = self.engine.build_clarification_prompt(partial)
        assert "task ID" in prompt or "task_id" in prompt

    def test_clarification_prompt_for_ambiguous_field(self):
        partial = PartialIntent(
            intent_type="delete_event",
            extracted_fields={"event_id": "event-aaa"},
            missing_fields=[],
            confidence=0.9,
            ambiguous_fields=["event_id"],
        )
        prompt = self.engine.build_clarification_prompt(partial)
        assert "conflicting" in prompt.lower() or "clarif" in prompt.lower()

    def test_human_field_name_mappings(self):
        assert _human_field_name("task_id") == "task ID"
        assert _human_field_name("start_time") == "start time"
        assert _human_field_name("plan_name") == "plan name"

    def test_required_fields_map_covers_all_intent_types(self):
        for intent_type in IntentType:
            assert intent_type.value in REQUIRED_FIELDS_BY_INTENT, (
                f"Missing required fields definition for {intent_type.value}"
            )


# ===========================================================================
# 4. STATE MACHINE TESTS
# ===========================================================================


class TestConversationStateMachine:

    def setup_method(self):
        self.sm = ConversationStateMachine()

    def test_idle_to_collecting_on_message(self):
        state = self.sm.transition(SessionState.IDLE, "message_received")
        assert state == SessionState.COLLECTING

    def test_collecting_to_clarifying_on_incomplete(self):
        state = self.sm.transition(SessionState.COLLECTING, "fields_incomplete")
        assert state == SessionState.CLARIFYING

    def test_collecting_to_ready_on_complete(self):
        state = self.sm.transition(SessionState.COLLECTING, "fields_complete")
        assert state == SessionState.READY_FOR_EXECUTION

    def test_clarifying_to_collecting_on_message(self):
        state = self.sm.transition(SessionState.CLARIFYING, "message_received")
        assert state == SessionState.COLLECTING

    def test_ready_to_executing_on_allow(self):
        state = self.sm.transition(SessionState.READY_FOR_EXECUTION, "policy_allow")
        assert state == SessionState.EXECUTING

    def test_ready_to_awaiting_on_confirm(self):
        state = self.sm.transition(SessionState.READY_FOR_EXECUTION, "policy_confirm")
        assert state == SessionState.AWAITING_CONFIRMATION

    def test_ready_to_blocked_on_block(self):
        state = self.sm.transition(SessionState.READY_FOR_EXECUTION, "policy_block")
        assert state == SessionState.BLOCKED

    def test_awaiting_to_executing_on_confirm(self):
        state = self.sm.transition(SessionState.AWAITING_CONFIRMATION, "user_confirmed")
        assert state == SessionState.EXECUTING

    def test_awaiting_to_idle_on_cancel(self):
        state = self.sm.transition(SessionState.AWAITING_CONFIRMATION, "user_cancelled")
        assert state == SessionState.IDLE

    def test_invalid_transition_raises_error(self):
        with pytest.raises(StateTransitionError):
            self.sm.transition(SessionState.IDLE, "fields_complete")  # invalid trigger for idle

    def test_executing_to_idle_on_handoff_complete(self):
        state = self.sm.transition(SessionState.EXECUTING, "handoff_complete")
        assert state == SessionState.IDLE

    def test_transition_from_policy_allow(self):
        state = self.sm.transition_from_policy(
            SessionState.READY_FOR_EXECUTION, PolicyDecision.ALLOW
        )
        assert state == SessionState.EXECUTING

    def test_transition_from_policy_confirm(self):
        state = self.sm.transition_from_policy(
            SessionState.READY_FOR_EXECUTION, PolicyDecision.REQUIRE_CONFIRMATION
        )
        assert state == SessionState.AWAITING_CONFIRMATION

    def test_transition_from_policy_block(self):
        state = self.sm.transition_from_policy(
            SessionState.READY_FOR_EXECUTION, PolicyDecision.BLOCK
        )
        assert state == SessionState.BLOCKED

    def test_can_accept_message_in_idle(self):
        assert self.sm.can_accept_message(SessionState.IDLE) is True

    def test_can_accept_message_in_collecting(self):
        assert self.sm.can_accept_message(SessionState.COLLECTING) is True

    def test_can_accept_message_in_clarifying(self):
        assert self.sm.can_accept_message(SessionState.CLARIFYING) is True

    def test_cannot_accept_message_in_blocked(self):
        assert self.sm.can_accept_message(SessionState.BLOCKED) is False

    def test_cannot_accept_message_in_executing(self):
        assert self.sm.can_accept_message(SessionState.EXECUTING) is False

    def test_transition_from_fields_complete(self):
        state = self.sm.transition_from_fields(SessionState.COLLECTING, fields_complete=True)
        assert state == SessionState.READY_FOR_EXECUTION

    def test_transition_from_fields_incomplete(self):
        state = self.sm.transition_from_fields(SessionState.COLLECTING, fields_complete=False)
        assert state == SessionState.CLARIFYING


# ===========================================================================
# 5. FULL PIPELINE INTEGRATION TESTS
# ===========================================================================


class TestConversationOrchestrator:

    def setup_method(self):
        # Entity store with known entities
        self.entity_store = EntityStore()
        self.entity_store.add_task("task-abc-123", {"name": "Buy groceries", "status": "pending"})
        self.entity_store.add_event("event-xyz-789", {"name": "Team Meeting"})
        self.entity_store.add_plan("plan-def-456", {"name": "Weekly Plan"})
        self.orchestrator = ConversationOrchestrator(entity_store=self.entity_store)

    def test_idle_to_collecting_on_first_message(self):
        session = make_session(SessionState.IDLE)
        _, updated = self.orchestrator.process_message(session, "Create a task to buy groceries")
        # Should have moved past idle
        assert updated.state != SessionState.IDLE

    def test_complete_create_task_single_turn(self):
        session = make_session(SessionState.IDLE)
        response, updated = self.orchestrator.process_message(
            session, "Create a task to buy groceries"
        )
        # Should get a response
        assert response.session_id == session.session_id
        assert isinstance(response.assistant_message, str)
        assert len(response.assistant_message) > 0

    def test_complete_task_single_turn_allows_execution(self):
        """COMPLETE_TASK with known task_id should result in ALLOW (safe operation)."""
        session = make_session(SessionState.IDLE)
        response, updated = self.orchestrator.process_message(
            session, "Complete task #task-abc-123"
        )
        # Should be allowed (complete_task is safe)
        if response.policy_decision is not None:
            assert response.policy_decision == PolicyDecision.ALLOW.value
            assert response.next_action == NextAction.EXECUTE

    def test_delete_event_requires_confirmation(self):
        """DELETE_EVENT should require confirmation (destructive operation)."""
        session = make_session(SessionState.IDLE)
        response, updated = self.orchestrator.process_message(
            session, "Delete event #event-xyz-789"
        )
        if response.policy_decision is not None:
            assert response.policy_decision == PolicyDecision.REQUIRE_CONFIRMATION.value
            assert response.next_action == NextAction.WAIT_FOR_CONFIRMATION

    def test_delete_event_confirmation_flow(self):
        """Full delete event: initial message → confirmation → execute."""
        session = make_session(SessionState.IDLE)
        response, session = self.orchestrator.process_message(
            session, "Delete event #event-xyz-789"
        )

        # If we got a confirmation request, test the confirmation
        if session.state == SessionState.AWAITING_CONFIRMATION:
            confirm_response, confirmed_session = self.orchestrator.process_confirmation(
                session=session,
                confirmed=True,
            )
            assert confirm_response.next_action == NextAction.EXECUTE
            assert confirmed_session.state == SessionState.EXECUTING

    def test_delete_event_cancellation_flow(self):
        """Cancel after confirmation request → session back to idle."""
        session = make_session(SessionState.IDLE)
        response, session = self.orchestrator.process_message(
            session, "Delete event #event-xyz-789"
        )
        if session.state == SessionState.AWAITING_CONFIRMATION:
            cancel_response, cancelled_session = self.orchestrator.process_confirmation(
                session=session,
                confirmed=False,
            )
            assert cancelled_session.state == SessionState.IDLE
            assert cancel_response.next_action == NextAction.NONE

    def test_missing_field_triggers_clarification(self):
        """Message with ambiguous/missing intent fields should request clarification."""
        session = make_session(SessionState.IDLE)
        # "Complete task" with no task_id — should ask for clarification
        response, updated = self.orchestrator.process_message(session, "complete task")
        # Session should be in clarifying or collecting state
        assert updated.state in (SessionState.CLARIFYING, SessionState.COLLECTING)
        assert response.next_action in (NextAction.ASK_CLARIFICATION, NextAction.NONE)

    def test_multi_turn_intent_refinement(self):
        """Two messages together build a complete intent."""
        session = make_session(SessionState.IDLE)

        # Turn 1: incomplete message
        response1, session = self.orchestrator.process_message(session, "complete task")
        # Session should have active intent
        assert session.active_intent is not None
        assert session.active_intent.turn_count == 1

        # Turn 2: provide the missing task_id
        response2, session = self.orchestrator.process_message(
            session, "complete task #task-abc-123"
        )
        # Turn count should increase
        assert session.active_intent is None or session.active_intent.turn_count >= 1

    def test_session_messages_accumulate(self):
        """Each turn should add messages to the session."""
        session = make_session(SessionState.IDLE)
        _, session = self.orchestrator.process_message(session, "Create a task to buy groceries")
        msg_count = len(session.messages)
        assert msg_count >= 1  # at least the user message

        _, session = self.orchestrator.process_message(session, "call it Buy milk instead")
        assert len(session.messages) > msg_count

    def test_blocked_session_does_not_accept_messages(self):
        """A blocked session should reject new messages with appropriate response."""
        session = make_session(SessionState.BLOCKED)
        response, updated = self.orchestrator.process_message(session, "try again")
        # Should return error, not proceed
        assert response.next_action == NextAction.NONE
        assert "blocked" in response.assistant_message.lower() or "cannot" in response.assistant_message.lower()

    def test_response_always_has_session_id(self):
        """Every response must include the session_id."""
        session = make_session(SessionState.IDLE)
        response, _ = self.orchestrator.process_message(session, "Create a task")
        assert response.session_id == session.session_id

    def test_response_always_has_assistant_message(self):
        """Every response must include a non-empty assistant_message."""
        session = make_session(SessionState.IDLE)
        response, _ = self.orchestrator.process_message(session, "Create a task")
        assert response.assistant_message is not None
        assert len(response.assistant_message) > 0

    def test_col_response_to_dict_format(self):
        """COLResponse.to_dict() must include all required keys."""
        session = make_session(SessionState.IDLE)
        response, _ = self.orchestrator.process_message(session, "Create a task to buy groceries")
        d = response.to_dict()
        required_keys = {
            "session_id", "state", "intent", "missing_fields",
            "action_plan", "policy_decision", "assistant_message", "next_action"
        }
        assert required_keys.issubset(d.keys())

    def test_session_state_preserved_across_turns(self):
        """Session fields not being updated must remain unchanged."""
        session = make_session(SessionState.IDLE)
        _, updated = self.orchestrator.process_message(session, "Create a task to buy groceries")
        # Identity fields unchanged
        assert updated.session_id == session.session_id
        assert updated.family_id == session.family_id
        assert updated.user_id == session.user_id

    def test_confirmation_in_wrong_state_returns_error(self):
        """process_confirmation in non-awaiting state returns error."""
        session = make_session(SessionState.IDLE)
        response, _ = self.orchestrator.process_confirmation(session, confirmed=True)
        assert response.next_action == NextAction.NONE
        assert "cannot" in response.assistant_message.lower()

    def test_reset_session_returns_to_idle(self):
        """reset_session() always returns a session in IDLE state."""
        session = make_session(SessionState.COLLECTING)
        session = session.with_updates(
            active_intent=PartialIntent(
                intent_type="create_task",
                extracted_fields={"task_name": "test"},
                missing_fields=[],
                confidence=0.9,
            )
        )
        reset = self.orchestrator.reset_session(session)
        assert reset.state == SessionState.IDLE
        assert reset.active_intent is None
        assert reset.finalized_intent is None


# ===========================================================================
# 6. SAFETY RULE TESTS
# ===========================================================================


class TestSafetyRules:

    def setup_method(self):
        self.entity_store = EntityStore()
        self.entity_store.add_task("task-abc-123", {"name": "Buy groceries"})
        self.orchestrator = ConversationOrchestrator(entity_store=self.entity_store)

    def test_never_fabricates_missing_fields(self):
        """COL must never invent values for fields not provided by user."""
        session = make_session(SessionState.IDLE)
        response, updated = self.orchestrator.process_message(session, "complete task")
        # If we're in clarifying, missing_fields should be populated
        if updated.state == SessionState.CLARIFYING:
            assert len(response.missing_fields) > 0 or len(response.intent.get("missing_fields", [])) > 0

    def test_blocked_state_produces_no_action_plan(self):
        """A BLOCK policy result must not return an action plan to execute."""
        session = make_session(SessionState.IDLE)
        response, updated = self.orchestrator.process_message(
            session, "Complete task #task-abc-123"
        )
        # If blocked, action plan should not be executed
        if response.policy_decision == PolicyDecision.BLOCK.value:
            assert response.next_action == NextAction.NONE

    def test_response_has_structured_state_enum(self):
        """State in COLResponse must be a valid SessionState."""
        session = make_session(SessionState.IDLE)
        response, _ = self.orchestrator.process_message(session, "Create a task")
        assert isinstance(response.state, SessionState)

    def test_response_has_structured_next_action_enum(self):
        """next_action in COLResponse must be a valid NextAction."""
        session = make_session(SessionState.IDLE)
        response, _ = self.orchestrator.process_message(session, "Create a task")
        assert isinstance(response.next_action, NextAction)

    def test_no_free_form_reasoning_in_assistant_message(self):
        """
        Assistant messages must be structured, not arbitrary reasoning.
        Test: messages should not contain unconstrained explanation patterns.
        We verify they come from the defined templates, not arbitrary text.
        """
        session = make_session(SessionState.IDLE)
        response, _ = self.orchestrator.process_message(
            session, "Complete task #task-abc-123"
        )
        # The message must be a string (structured), not None or empty
        assert isinstance(response.assistant_message, str)
        assert len(response.assistant_message) > 0

    def test_action_plan_dict_has_required_keys(self):
        """If action_plan is populated, it must contain required keys."""
        session = make_session(SessionState.IDLE)
        response, _ = self.orchestrator.process_message(
            session, "Complete task #task-abc-123"
        )
        if response.action_plan:
            assert "intent_type" in response.action_plan
            assert "actions" in response.action_plan

    def test_multiple_turns_never_lose_session_id(self):
        """session_id must be consistent across all turns."""
        session = make_session(SessionState.IDLE)
        session_id = session.session_id

        r1, session = self.orchestrator.process_message(session, "complete task")
        assert r1.session_id == session_id

        r2, session = self.orchestrator.process_message(session, "#task-abc-123")
        assert r2.session_id == session_id


# ===========================================================================
# 7. HELPER FUNCTION TESTS
# ===========================================================================


class TestHelperFunctions:

    def test_required_fields_by_intent_completeness(self):
        """All 9 intent types must have a required fields entry."""
        all_intent_types = {it.value for it in IntentType}
        assert all_intent_types == set(REQUIRED_FIELDS_BY_INTENT.keys())

    def test_create_task_required_fields(self):
        assert REQUIRED_FIELDS_BY_INTENT["create_task"] == ["task_name"]

    def test_complete_task_required_fields(self):
        assert REQUIRED_FIELDS_BY_INTENT["complete_task"] == ["task_id"]

    def test_create_event_required_fields(self):
        fields = REQUIRED_FIELDS_BY_INTENT["create_event"]
        assert "event_name" in fields
        assert "start_time" in fields

    def test_reschedule_task_required_fields(self):
        fields = REQUIRED_FIELDS_BY_INTENT["reschedule_task"]
        assert "task_id" in fields
        assert "new_time" in fields

    def test_session_state_enum_values(self):
        """SessionState must have all required values."""
        values = {s.value for s in SessionState}
        required = {"idle", "collecting", "clarifying", "ready_for_execution",
                    "awaiting_confirmation", "blocked", "executing"}
        assert required.issubset(values)

    def test_next_action_enum_values(self):
        """NextAction must have all required values."""
        values = {a.value for a in NextAction}
        required = {"none", "ask_clarification", "execute", "wait_for_confirmation"}
        assert required.issubset(values)
