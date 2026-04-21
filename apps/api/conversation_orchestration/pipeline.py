"""
Conversation Orchestration Layer (COL v1) - Pipeline Orchestrator
=================================================================

The central runtime bridge between the Chat UI and HPAL backend systems.

Every incoming user message goes through this deterministic pipeline:

  User Message
    ↓
  Session Load
    ↓
  Partial Intent Update (Intent Refinement Engine)
    ↓
  Intent Completion Check
    ↓
  IF incomplete → clarification response
  IF complete → Intent Contract Layer validation
    ↓
  ValidatedIntent
    ↓
  ActionPlan (Action Planner)
    ↓
  Policy Engine evaluation
    ↓
  Decision Routing:
    ALLOW → return execution handoff
    REQUIRE_CONFIRMATION → pause + ask user
    BLOCK → reject with reason_code
    ↓
  COLResponse (structured only)

NON-GOALS (enforced):
  - NEVER executes domain actions directly
  - NEVER bypasses Intent Contract Layer or Policy Engine
  - NEVER mutates HPAL state
  - NEVER fabricates missing fields
  - NEVER generates free-form reasoning in output
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from apps.api.conversation_orchestration.intent_refinement import IntentRefinementEngine
from apps.api.conversation_orchestration.schema import (
    COLResponse,
    ConversationMessage,
    ConversationSession,
    ExecutionHandoff,
    NextAction,
    PartialIntent,
    SessionState,
)
from apps.api.conversation_orchestration.store import (
    ConversationSessionStore,
    DEFAULT_SESSION_STORE,
)
from apps.api.conversation_orchestration.state_machine import (
    ConversationStateMachine,
    StateTransitionError,
)
from apps.api.intent_contract.action_planner import ActionPlan, ActionPlanner
from apps.api.intent_contract.classifier import IntentClassification, IntentClassifier
from apps.api.intent_contract.schema import ExtractedFields, IntentType
from apps.api.intent_contract.validator import (
    EntityStore,
    IntentValidator,
    ValidatedIntent,
    ValidationError_,
)
from apps.api.policy_engine.evaluator import PolicyEvaluator
from apps.api.policy_engine.schema import PolicyConfig, PolicyDecision, PolicyResult


# ---------------------------------------------------------------------------
# COL PIPELINE ORCHESTRATOR
# ---------------------------------------------------------------------------


class ConversationOrchestrator:
    """
    Main entry point for the Conversation Orchestration Layer.

    Processes incoming user messages, manages session state, and produces
    fully structured COLResponse outputs.

    Usage:
      orchestrator = ConversationOrchestrator(entity_store=my_store)
      session = ConversationSession.new("session-id", "family-id", "user-id")

      response, updated_session = orchestrator.process_message(
          session=session,
          user_message="Create a task to buy groceries"
      )
    """

    CONFIDENCE_THRESHOLD: float = 0.85

    def __init__(
        self,
        entity_store: Optional[EntityStore] = None,
        policy_config: Optional[PolicyConfig] = None,
        session_store: Optional[ConversationSessionStore] = None,
    ):
        """
        Initialize the orchestrator with all required sub-components.

        Args:
          entity_store: optional EntityStore for entity reference validation
          policy_config: optional PolicyConfig to customize policy rules
        """
        self._entity_store = entity_store or EntityStore()
        self._refinement_engine = IntentRefinementEngine()
        self._validator = IntentValidator(entity_store=self._entity_store)
        self._planner = ActionPlanner()
        self._evaluator = PolicyEvaluator(config=policy_config)
        self._state_machine = ConversationStateMachine()
        self._session_store = session_store or DEFAULT_SESSION_STORE

    # -----------------------------------------------------------------------
    # PRIMARY ENTRY POINT
    # -----------------------------------------------------------------------

    def process_message(
        self,
        session: ConversationSession,
        user_message: str,
    ) -> tuple[COLResponse, ConversationSession]:
        """
        Process a user message through the full HPAL pipeline.

        Args:
          session: the current conversation session (immutable)
          user_message: raw user input text

        Returns:
          (COLResponse, updated_session) — both are immutable value objects
        """
        # Guard: session must be in a state that accepts messages
        if not self._state_machine.can_accept_message(session.state):
            return self._error_response(
                session=session,
                error_message=f"Session is in state '{session.state.value}' and cannot accept new messages.",
            )

        # Record the user message
        msg = ConversationMessage.from_user(user_message)
        session = session.add_message(msg)
        self._persist_session(session)

        # Transition: idle → collecting (first message)
        if session.state == SessionState.IDLE:
            session = session.with_updates(
                state=SessionState.COLLECTING,
                last_updated=datetime.now(),
            )

        # STEP 1: Update partial intent from the new message
        session, partial_intent = self._update_partial_intent(session, user_message)

        # STEP 2: After clarification turn, go back to collecting
        if session.state == SessionState.CLARIFYING:
            session = session.with_updates(
                state=SessionState.COLLECTING,
                last_updated=datetime.now(),
            )

        # STEP 3: Check if intent is complete enough to proceed
        fields_complete = partial_intent.is_complete(threshold=self.CONFIDENCE_THRESHOLD)

        # STEP 4a: Fields incomplete — ask for clarification
        if not fields_complete:
            next_state = self._state_machine.transition_from_fields(
                current_state=session.state,
                fields_complete=False,
            )
            session = session.with_updates(
                state=next_state,
                active_intent=partial_intent,
                last_updated=datetime.now(),
            )
            clarification = self._refinement_engine.build_clarification_prompt(partial_intent)
            assistant_msg = ConversationMessage.from_assistant(clarification)
            session = session.add_message(assistant_msg)
            self._persist_session(session)

            return (
                COLResponse(
                    session_id=session.session_id,
                    state=session.state,
                    intent=self._partial_to_dict(partial_intent),
                    missing_fields=list(partial_intent.missing_fields),
                    action_plan={},
                    policy_decision=None,
                    assistant_message=clarification,
                    next_action=NextAction.ASK_CLARIFICATION,
                ),
                session,
            )

        # STEP 4b: Intent complete — transition to validation pipeline
        next_state = self._state_machine.transition_from_fields(
            current_state=session.state,
            fields_complete=True,
        )
        session = session.with_updates(
            state=next_state,
            active_intent=partial_intent,
            last_updated=datetime.now(),
        )

        # STEP 5: Validate via Intent Contract Layer
        classification = self._partial_to_classification(partial_intent)
        validation_result = self._validator.validate(classification)

        if isinstance(validation_result, ValidationError_):
            return self._validation_error_response(session, validation_result)

        # STEP 6: Generate ActionPlan
        action_plan = self._planner.plan(validation_result)
        if action_plan is None:
            return self._error_response(
                session=session,
                error_message=f"Could not generate action plan for intent '{partial_intent.intent_type}'.",
            )

        # STEP 7: Policy Engine evaluation
        policy_result = self._evaluator.evaluate(action_plan)

        # STEP 8: Route based on policy decision
        return self._route_policy_decision(
            session=session,
            partial_intent=partial_intent,
            validated_intent=validation_result,
            action_plan=action_plan,
            policy_result=policy_result,
        )

    def process_confirmation(
        self,
        session: ConversationSession,
        confirmed: bool,
    ) -> tuple[COLResponse, ConversationSession]:
        """
        Process user's confirmation or cancellation for REQUIRE_CONFIRMATION actions.

        Args:
          session: session in AWAITING_CONFIRMATION state
          confirmed: True if user confirmed, False if cancelled

        Returns:
          (COLResponse, updated_session)
        """
        if session.state != SessionState.AWAITING_CONFIRMATION:
            return self._error_response(
                session=session,
                error_message=f"Cannot process confirmation in state '{session.state.value}'.",
            )

        if not confirmed:
            # User cancelled
            session = session.with_updates(
                state=SessionState.IDLE,
                active_intent=None,
                finalized_intent=None,
                last_updated=datetime.now(),
            )
            msg = ConversationMessage.from_assistant("Action cancelled. How can I help?")
            session = session.add_message(msg)
            self._persist_session(session)
            return (
                COLResponse(
                    session_id=session.session_id,
                    state=SessionState.IDLE,
                    intent={},
                    missing_fields=[],
                    action_plan={},
                    policy_decision=None,
                    assistant_message="Action cancelled. How can I help?",
                    next_action=NextAction.NONE,
                ),
                session,
            )

        # User confirmed — build execution handoff
        if session.finalized_intent is None:
            return self._error_response(
                session=session,
                error_message="Confirmed but no finalized intent found in session.",
            )

        action_plan = self._planner.plan(session.finalized_intent)
        if action_plan is None:
            return self._error_response(
                session=session,
                error_message="Could not regenerate action plan for confirmed intent.",
            )

        session = session.with_updates(
            state=SessionState.EXECUTING,
            last_updated=datetime.now(),
        )

        handoff = self._build_handoff(session, action_plan, PolicyDecision.REQUIRE_CONFIRMATION)
        action_plan_dict = _action_plan_to_dict(action_plan)

        msg = ConversationMessage.from_assistant("Confirmed. Executing your request now.")
        session = session.add_message(msg)
        self._persist_session(session)

        return (
            COLResponse(
                session_id=session.session_id,
                state=session.state,
                intent={"intent_type": session.finalized_intent.intent_type.value},
                missing_fields=[],
                action_plan=action_plan_dict,
                policy_decision=PolicyDecision.REQUIRE_CONFIRMATION.value,
                assistant_message="Confirmed. Executing your request now.",
                next_action=NextAction.EXECUTE,
            ),
            session,
        )

    def reset_session(self, session: ConversationSession) -> ConversationSession:
        """
        Reset a session back to idle state.

        Used after execution completes, on user request, or after BLOCK.
        """
        updated = session.with_updates(
            state=SessionState.IDLE,
            active_intent=None,
            finalized_intent=None,
            last_updated=datetime.now(),
        )
        self._persist_session(updated)
        return updated

    # -----------------------------------------------------------------------
    # PRIVATE: INTENT UPDATE
    # -----------------------------------------------------------------------

    def _update_partial_intent(
        self,
        session: ConversationSession,
        user_message: str,
    ) -> tuple[ConversationSession, PartialIntent]:
        """
        Update the session's partial intent with the new user message.

        Returns (updated_session, new_partial_intent).
        """
        if session.active_intent is None:
            # First message — initialize from scratch
            partial_intent = self._refinement_engine.initialize_from_message(user_message)
        else:
            # Subsequent message — merge into existing
            partial_intent = self._refinement_engine.merge_message(
                existing=session.active_intent,
                message_text=user_message,
            )

        updated_session = session.with_updates(
            active_intent=partial_intent,
            last_updated=datetime.now(),
        )

        return updated_session, partial_intent

    # -----------------------------------------------------------------------
    # PRIVATE: POLICY ROUTING
    # -----------------------------------------------------------------------

    def _route_policy_decision(
        self,
        session: ConversationSession,
        partial_intent: PartialIntent,
        validated_intent: ValidatedIntent,
        action_plan: ActionPlan,
        policy_result: PolicyResult,
    ) -> tuple[COLResponse, ConversationSession]:
        """
        Route based on PolicyEngine decision: ALLOW / REQUIRE_CONFIRMATION / BLOCK.
        """
        action_plan_dict = _action_plan_to_dict(action_plan)

        next_state = self._state_machine.transition_from_policy(
            current_state=session.state,
            policy_decision=policy_result.decision,
        )
        session = session.with_updates(
            state=next_state,
            finalized_intent=validated_intent if policy_result.decision != PolicyDecision.BLOCK else None,
            last_updated=datetime.now(),
        )

        if policy_result.decision == PolicyDecision.ALLOW:
            # Build execution handoff
            handoff = self._build_handoff(session, action_plan, PolicyDecision.ALLOW)
            assistant_message = f"Executing: {validated_intent.intent_type.value.replace('_', ' ')}."
            next_action = NextAction.EXECUTE

        elif policy_result.decision == PolicyDecision.REQUIRE_CONFIRMATION:
            # Build confirmation prompt
            summary = _build_action_summary(action_plan)
            assistant_message = (
                f"I need your confirmation before proceeding.\n\n"
                f"{summary}\n\n"
                f"Reason: {policy_result.message}\n\n"
                f"Please confirm (yes/no)."
            )
            next_action = NextAction.WAIT_FOR_CONFIRMATION

        else:  # BLOCK
            assistant_message = (
                f"This action cannot be executed: {policy_result.message} "
                f"(reason: {policy_result.reason_code})"
            )
            next_action = NextAction.NONE

        msg = ConversationMessage.from_assistant(assistant_message)
        session = session.add_message(msg)
        self._persist_session(session)

        return (
            COLResponse(
                session_id=session.session_id,
                state=session.state,
                intent=self._partial_to_dict(partial_intent),
                missing_fields=[],
                action_plan=action_plan_dict,
                policy_decision=policy_result.decision.value,
                assistant_message=assistant_message,
                next_action=next_action,
            ),
            session,
        )

    def _persist_session(self, session: ConversationSession) -> None:
        """Persist session state for read models and UI bootstrap aggregation."""
        self._session_store.upsert(session)

    # -----------------------------------------------------------------------
    # PRIVATE: CONVERSION HELPERS
    # -----------------------------------------------------------------------

    def _partial_to_classification(self, partial: PartialIntent) -> IntentClassification:
        """
        Convert a completed PartialIntent into an IntentClassification for validation.

        This bridges the COL intent representation to the Intent Contract Layer.
        """
        intent_type_enum = None
        if partial.intent_type is not None:
            try:
                intent_type_enum = IntentType(partial.intent_type)
            except ValueError:
                pass

        return IntentClassification(
            intent_type=intent_type_enum,
            confidence_score=partial.confidence,
            extracted_fields=ExtractedFields(data=dict(partial.extracted_fields)),
            classification_method="col_multi_turn",
        )

    def _partial_to_dict(self, partial: Optional[PartialIntent]) -> Dict[str, Any]:
        """Convert PartialIntent to a plain dict for JSON serialization."""
        if partial is None:
            return {}
        return {
            "intent_type": partial.intent_type,
            "extracted_fields": dict(partial.extracted_fields),
            "missing_fields": list(partial.missing_fields),
            "confidence": partial.confidence,
            "ambiguous_fields": list(partial.ambiguous_fields),
            "turn_count": partial.turn_count,
        }

    def _build_handoff(
        self,
        session: ConversationSession,
        action_plan: ActionPlan,
        policy_decision: PolicyDecision,
    ) -> ExecutionHandoff:
        """Build an ExecutionHandoff for the HPAL Command Gateway."""
        action_plan_dict = _action_plan_to_dict(action_plan)

        # Deterministic composite idempotency key
        key_data = json.dumps(
            {
                "session_id": session.session_id,
                "action_plan": action_plan_dict,
                "policy_decision": policy_decision.value,
            },
            sort_keys=True,
            default=str,
        )
        idempotency_key = hashlib.sha256(key_data.encode()).hexdigest()[:40]

        return ExecutionHandoff(
            session_id=session.session_id,
            action_plan_data=action_plan_dict,
            policy_decision=policy_decision.value,
            family_id=session.family_id,
            user_id=session.user_id,
            idempotency_key=idempotency_key,
        )

    # -----------------------------------------------------------------------
    # PRIVATE: ERROR RESPONSES
    # -----------------------------------------------------------------------

    def _validation_error_response(
        self,
        session: ConversationSession,
        error: ValidationError_,
    ) -> tuple[COLResponse, ConversationSession]:
        """Build a response for a validation failure."""
        message = f"Validation failed: {error.error_message}"
        if error.validation_errors:
            fields = [e.get("loc", ["?"])[0] for e in error.validation_errors[:3]]
            message += f" (fields: {', '.join(str(f) for f in fields)})"

        # Transition back to clarifying if it's a field error
        if error.validation_errors:
            next_state = SessionState.CLARIFYING
        else:
            next_state = SessionState.COLLECTING

        session = session.with_updates(
            state=next_state,
            last_updated=datetime.now(),
        )
        msg = ConversationMessage.from_assistant(message)
        session = session.add_message(msg)
        self._persist_session(session)

        partial = session.active_intent
        return (
            COLResponse(
                session_id=session.session_id,
                state=session.state,
                intent=self._partial_to_dict(partial),
                missing_fields=list(partial.missing_fields) if partial else [],
                action_plan={},
                policy_decision=None,
                assistant_message=message,
                next_action=NextAction.ASK_CLARIFICATION,
            ),
            session,
        )

    def _error_response(
        self,
        session: ConversationSession,
        error_message: str,
    ) -> tuple[COLResponse, ConversationSession]:
        """Build a generic error response that preserves session state."""
        msg = ConversationMessage.from_assistant(error_message)
        session = session.add_message(msg)
        self._persist_session(session)
        partial = session.active_intent
        return (
            COLResponse(
                session_id=session.session_id,
                state=session.state,
                intent=self._partial_to_dict(partial),
                missing_fields=list(partial.missing_fields) if partial else [],
                action_plan={},
                policy_decision=None,
                assistant_message=error_message,
                next_action=NextAction.NONE,
            ),
            session,
        )


# ---------------------------------------------------------------------------
# MODULE-LEVEL HELPERS
# ---------------------------------------------------------------------------


def _action_plan_to_dict(action_plan: ActionPlan) -> Dict[str, Any]:
    """Convert ActionPlan to a serializable dict."""
    return {
        "intent_type": action_plan.intent_type.value,
        "actions": [
            {
                "action_type": a.action_type,
                "parameters": a.parameters,
                "idempotency_key": a.idempotency_key,
                "sequence_number": a.sequence_number,
            }
            for a in action_plan.actions
        ],
        "validated_data": {
            k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
            for k, v in action_plan.validated_data.items()
        },
    }


def _build_action_summary(action_plan: ActionPlan) -> str:
    """Build a human-readable summary of an action plan for confirmation prompts."""
    lines = [f"Intent: {action_plan.intent_type.value.replace('_', ' ').title()}"]
    for action in action_plan.actions:
        param_str = ", ".join(f"{k}={v}" for k, v in action.parameters.items())
        lines.append(f"  [{action.sequence_number}] {action.action_type}({param_str})")
    return "\n".join(lines)
