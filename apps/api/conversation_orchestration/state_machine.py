"""
Conversation Orchestration Layer (COL v1) - State Machine
==========================================================

Implements deterministic state transitions for conversation sessions.

State Transitions:
  idle         → collecting           (first message arrives)
  collecting   → clarifying           (required field missing or ambiguity)
  collecting   → ready_for_execution  (all fields present, confidence met)
  clarifying   → collecting           (user provides clarification)
  clarifying   → clarifying           (still missing fields after clarification)
  ready_for_execution → awaiting_confirmation (policy = REQUIRE_CONFIRMATION)
  ready_for_execution → executing     (policy = ALLOW)
  awaiting_confirmation → executing   (user confirms)
  awaiting_confirmation → idle        (user cancels)
  * → blocked                         (policy = BLOCK)
  executing    → idle                 (handoff complete)

Design principles:
  - All transitions are deterministic and explicit
  - No transition happens without a causal trigger
  - Invalid transitions raise explicit errors
  - Transition rules are declarative (mapping table)
"""
from __future__ import annotations

from typing import Dict, Optional, Set, Tuple

from apps.api.conversation_orchestration.schema import SessionState
from apps.api.policy_engine.schema import PolicyDecision


# ---------------------------------------------------------------------------
# VALID TRANSITIONS TABLE
# Maps (from_state, trigger) → to_state
# ---------------------------------------------------------------------------

Trigger = str  # string trigger label


TRANSITIONS: Dict[Tuple[SessionState, Trigger], SessionState] = {
    # idle: first message starts collection
    (SessionState.IDLE, "message_received"): SessionState.COLLECTING,

    # collecting: partial intent assessed
    (SessionState.COLLECTING, "fields_complete"): SessionState.READY_FOR_EXECUTION,
    (SessionState.COLLECTING, "fields_incomplete"): SessionState.CLARIFYING,

    # clarifying: user provides more info
    (SessionState.CLARIFYING, "message_received"): SessionState.COLLECTING,

    # ready_for_execution: policy evaluated
    (SessionState.READY_FOR_EXECUTION, "policy_allow"): SessionState.EXECUTING,
    (SessionState.READY_FOR_EXECUTION, "policy_confirm"): SessionState.AWAITING_CONFIRMATION,
    (SessionState.READY_FOR_EXECUTION, "policy_block"): SessionState.BLOCKED,

    # awaiting_confirmation: user responds
    (SessionState.AWAITING_CONFIRMATION, "user_confirmed"): SessionState.EXECUTING,
    (SessionState.AWAITING_CONFIRMATION, "user_cancelled"): SessionState.IDLE,

    # blocked: only reset allowed
    (SessionState.BLOCKED, "reset"): SessionState.IDLE,

    # executing: handoff complete
    (SessionState.EXECUTING, "handoff_complete"): SessionState.IDLE,

    # any state: policy block overrides everything
    (SessionState.COLLECTING, "policy_block"): SessionState.BLOCKED,
    (SessionState.CLARIFYING, "policy_block"): SessionState.BLOCKED,
}

# States where a message arriving means "continue" (re-enter collecting)
ACCEPTING_MESSAGE_STATES: Set[SessionState] = {
    SessionState.IDLE,
    SessionState.COLLECTING,
    SessionState.CLARIFYING,
    SessionState.AWAITING_CONFIRMATION,
}


# ---------------------------------------------------------------------------
# STATE MACHINE
# ---------------------------------------------------------------------------


class ConversationStateMachine:
    """
    Deterministic state machine for conversation sessions.

    All state transitions are explicit and driven by triggers.
    Invalid transitions raise StateTransitionError.
    """

    def transition(
        self,
        from_state: SessionState,
        trigger: Trigger,
    ) -> SessionState:
        """
        Execute a state transition.

        Args:
          from_state: current state
          trigger: event that caused the transition

        Returns:
          New state after transition

        Raises:
          StateTransitionError: if transition is invalid
        """
        key = (from_state, trigger)
        to_state = TRANSITIONS.get(key)

        if to_state is None:
            raise StateTransitionError(
                f"Invalid transition: {from_state.value} + '{trigger}' has no defined target state"
            )

        return to_state

    def transition_from_policy(
        self,
        current_state: SessionState,
        policy_decision: PolicyDecision,
    ) -> SessionState:
        """
        Transition based on a policy engine decision.

        Args:
          current_state: state before policy evaluation
          policy_decision: result from PolicyEvaluator

        Returns:
          New state reflecting the policy decision
        """
        if policy_decision == PolicyDecision.ALLOW:
            return self.transition(current_state, "policy_allow")
        elif policy_decision == PolicyDecision.REQUIRE_CONFIRMATION:
            return self.transition(current_state, "policy_confirm")
        elif policy_decision == PolicyDecision.BLOCK:
            return self.transition(current_state, "policy_block")
        else:
            raise StateTransitionError(f"Unknown policy decision: {policy_decision}")

    def transition_from_fields(
        self,
        current_state: SessionState,
        fields_complete: bool,
    ) -> SessionState:
        """
        Transition based on field completion check after intent extraction.

        Args:
          current_state: state before field check
          fields_complete: whether all required fields are present

        Returns:
          New state reflecting whether more information is needed
        """
        trigger = "fields_complete" if fields_complete else "fields_incomplete"
        return self.transition(current_state, trigger)

    def can_accept_message(self, state: SessionState) -> bool:
        """Returns True if a new user message can be processed in this state."""
        return state in ACCEPTING_MESSAGE_STATES

    def get_valid_triggers(self, state: SessionState) -> list[str]:
        """Return list of valid triggers for a given state."""
        return [trigger for (s, trigger) in TRANSITIONS if s == state]


# ---------------------------------------------------------------------------
# STATE TRANSITION ERROR
# ---------------------------------------------------------------------------


class StateTransitionError(Exception):
    """
    Raised when an invalid state transition is attempted.

    This indicates a programming error — the orchestrator should never
    attempt a transition that is not in the transition table.
    """

    pass
