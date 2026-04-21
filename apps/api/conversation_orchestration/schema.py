"""
Conversation Orchestration Layer (COL v1) - Schema Definitions
===============================================================

Defines the data models for conversation session state, messages,
partial intents, and structured output.

Design principles:
  - Frozen dataclasses for immutability (safe by default)
  - Explicit types — no implicit coercion
  - Session state is self-contained (no side effects in schema)
  - PartialIntent is the critical intermediate state for multi-turn refinement
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from apps.api.intent_contract.validator import ValidatedIntent


# ---------------------------------------------------------------------------
# CONVERSATION TURN STATES
# ---------------------------------------------------------------------------


class SessionState(str, Enum):
    """
    State machine values for a conversation session.

    Transitions:
      idle → collecting (first user message arrives)
      collecting → clarifying (required field missing)
      collecting → ready_for_execution (all fields extracted, confidence met)
      clarifying → collecting (user provides missing info)
      ready_for_execution → awaiting_confirmation (policy = REQUIRE_CONFIRMATION)
      ready_for_execution → executing (policy = ALLOW) — handoff
      awaiting_confirmation → executing (user confirms)
      awaiting_confirmation → idle (user cancels)
      * → blocked (policy = BLOCK)
      executing → idle (after handoff completes)
    """

    IDLE = "idle"
    COLLECTING = "collecting"
    CLARIFYING = "clarifying"
    READY_FOR_EXECUTION = "ready_for_execution"
    AWAITING_CONFIRMATION = "awaiting_confirmation"
    BLOCKED = "blocked"
    EXECUTING = "executing"


# ---------------------------------------------------------------------------
# NEXT ACTION ENUM
# ---------------------------------------------------------------------------


class NextAction(str, Enum):
    """Indicates what the caller should do next."""

    NONE = "none"
    ASK_CLARIFICATION = "ask_clarification"
    EXECUTE = "execute"
    WAIT_FOR_CONFIRMATION = "wait_for_confirmation"


# ---------------------------------------------------------------------------
# CONVERSATION MESSAGE
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConversationMessage:
    """
    A single message in a conversation session.

    Fields:
      message_id: unique message identifier
      role: who sent the message ("user" | "assistant" | "system")
      content: raw message text
      timestamp: when the message was sent
    """

    message_id: str
    role: Literal["user", "assistant", "system"]
    content: str
    timestamp: datetime

    @classmethod
    def from_user(cls, content: str, timestamp: Optional[datetime] = None) -> "ConversationMessage":
        """Factory: create a user message."""
        return cls(
            message_id=str(uuid.uuid4()),
            role="user",
            content=content,
            timestamp=timestamp or datetime.now(),
        )

    @classmethod
    def from_assistant(cls, content: str, timestamp: Optional[datetime] = None) -> "ConversationMessage":
        """Factory: create an assistant message."""
        return cls(
            message_id=str(uuid.uuid4()),
            role="assistant",
            content=content,
            timestamp=timestamp or datetime.now(),
        )

    @classmethod
    def from_system(cls, content: str, timestamp: Optional[datetime] = None) -> "ConversationMessage":
        """Factory: create a system message."""
        return cls(
            message_id=str(uuid.uuid4()),
            role="system",
            content=content,
            timestamp=timestamp or datetime.now(),
        )


# ---------------------------------------------------------------------------
# PARTIAL INTENT — CRITICAL INTERMEDIATE STATE
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PartialIntent:
    """
    Intermediate state for intent refinement over multiple turns.

    A PartialIntent accumulates extracted fields across messages
    until all required fields are present and confidence is sufficient.

    Fields:
      intent_type: detected intent type (Optional — may not be known yet)
      extracted_fields: fields gathered so far
      missing_fields: required fields that still need to be provided
      confidence: float 0.0-1.0 representing classification confidence
      ambiguous_fields: fields where contradictory values were provided
      turn_count: number of turns that contributed to this partial intent
    """

    intent_type: Optional[str]
    extracted_fields: Dict[str, Any]
    missing_fields: List[str]
    confidence: float
    ambiguous_fields: List[str] = field(default_factory=list)
    turn_count: int = 1

    def is_complete(self, threshold: float = 0.85) -> bool:
        """
        Returns True if this intent is ready for execution.

        Conditions:
          - intent_type is known
          - no missing required fields
          - confidence >= threshold
          - no ambiguous fields
        """
        return (
            self.intent_type is not None
            and len(self.missing_fields) == 0
            and self.confidence >= threshold
            and len(self.ambiguous_fields) == 0
        )

    def has_ambiguity(self) -> bool:
        """Returns True if any fields are ambiguous."""
        return len(self.ambiguous_fields) > 0

    def with_updates(self, **changes) -> "PartialIntent":
        """
        Return a new PartialIntent with the specified fields changed.

        Since PartialIntent is frozen, this creates a new instance.
        """
        current = {
            "intent_type": self.intent_type,
            "extracted_fields": dict(self.extracted_fields),
            "missing_fields": list(self.missing_fields),
            "confidence": self.confidence,
            "ambiguous_fields": list(self.ambiguous_fields),
            "turn_count": self.turn_count,
        }
        current.update(changes)
        return PartialIntent(**current)


# ---------------------------------------------------------------------------
# CONVERSATION SESSION
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConversationSession:
    """
    Complete conversation session for a single user.

    Fields:
      session_id: unique session identifier
      family_id: family this session belongs to
      user_id: user who owns this session
      messages: ordered list of all messages in this session
      active_intent: current partial intent being refined (None if idle)
      finalized_intent: fully validated intent (None until ready for execution)
      state: current state machine position
      last_updated: timestamp of last state change
    """

    session_id: str
    family_id: str
    user_id: str
    messages: List[ConversationMessage]
    active_intent: Optional[PartialIntent]
    finalized_intent: Optional[ValidatedIntent]
    state: SessionState
    last_updated: datetime

    @classmethod
    def new(cls, session_id: str, family_id: str, user_id: str) -> "ConversationSession":
        """Factory: create a fresh session in idle state."""
        return cls(
            session_id=session_id,
            family_id=family_id,
            user_id=user_id,
            messages=[],
            active_intent=None,
            finalized_intent=None,
            state=SessionState.IDLE,
            last_updated=datetime.now(),
        )

    def with_updates(self, **changes) -> "ConversationSession":
        """
        Return a new ConversationSession with the specified fields changed.

        Since ConversationSession is frozen, this creates a new instance.
        """
        current = {
            "session_id": self.session_id,
            "family_id": self.family_id,
            "user_id": self.user_id,
            "messages": list(self.messages),
            "active_intent": self.active_intent,
            "finalized_intent": self.finalized_intent,
            "state": self.state,
            "last_updated": self.last_updated,
        }
        current.update(changes)
        return ConversationSession(**current)

    def add_message(self, message: ConversationMessage) -> "ConversationSession":
        """Return a new session with the message appended."""
        return self.with_updates(
            messages=list(self.messages) + [message],
            last_updated=datetime.now(),
        )


# ---------------------------------------------------------------------------
# COL RESPONSE — STRUCTURED OUTPUT (STRICT)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class COLResponse:
    """
    Structured output from the Conversation Orchestration Layer.

    Every response MUST be fully structured — no free-form reasoning.

    Fields:
      session_id: the session this response belongs to
      state: current session state
      intent: dict representation of active or finalized intent
      missing_fields: list of required fields not yet provided
      action_plan: dict representation of generated ActionPlan (if ready)
      policy_decision: string policy decision (if evaluated)
      assistant_message: safe, structured response text (no free-form reasoning)
      next_action: what the caller should do next
    """

    session_id: str
    state: SessionState
    intent: Dict[str, Any]
    missing_fields: List[str]
    action_plan: Dict[str, Any]
    policy_decision: Optional[str]
    assistant_message: str
    next_action: NextAction

    def to_dict(self) -> Dict[str, Any]:
        """Convert to plain dictionary for JSON serialization."""
        return {
            "session_id": self.session_id,
            "state": self.state.value,
            "intent": self.intent,
            "missing_fields": self.missing_fields,
            "action_plan": self.action_plan,
            "policy_decision": self.policy_decision,
            "assistant_message": self.assistant_message,
            "next_action": self.next_action.value,
        }


# ---------------------------------------------------------------------------
# CLARIFICATION REQUEST — STRUCTURED CLARIFICATION PROMPT
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ClarificationRequest:
    """
    Structured clarification request when required fields are missing
    or ambiguity is detected.

    Fields:
      missing_fields: fields that must be provided
      ambiguous_fields: fields with conflicting values
      context_summary: brief summary of what has been understood so far
      prompt_text: exact text to show/send to user
    """

    missing_fields: List[str]
    ambiguous_fields: List[str]
    context_summary: str
    prompt_text: str


# ---------------------------------------------------------------------------
# EXECUTION HANDOFF — STRUCTURED HANDOFF TO EXECUTION LAYER
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExecutionHandoff:
    """
    Structured handoff to the execution layer after policy approval.

    Fields:
      session_id: owning session
      action_plan_data: serialized ActionPlan dict
      policy_decision: the approved decision
      family_id: context propagation
      user_id: context propagation
      idempotency_key: composite idempotency key for the full handoff
    """

    session_id: str
    action_plan_data: Dict[str, Any]
    policy_decision: str
    family_id: str
    user_id: str
    idempotency_key: str
