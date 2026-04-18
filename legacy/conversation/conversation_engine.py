"""
Conversation Engine — Multi-turn conversation state management.

Manages the lifecycle of a user conversation across multiple turns, per
session/user/household. Maintains message history, tracks partial intent
state, queues clarification requests, and signals when intent is ready
for compilation.

Output states:
  - "awaiting_clarification"  — one or more ambiguities require user input
  - "intent_complete"         — intent resolved but not yet validated
  - "ready_for_compilation"   — intent is unambiguous and can be compiled

This module:
  - DOES NOT execute workflows
  - DOES NOT call the DAG engine
  - DOES NOT call the scheduler
  - DOES NOT interact with any external service

It is a pure state-management layer.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Literal

from legacy.compiler.intent_parser import Intent


# ── State machine ──────────────────────────────────────────────────────────────

class ConversationState(str, Enum):
    AWAITING_CLARIFICATION = "awaiting_clarification"
    INTENT_COMPLETE        = "intent_complete"
    READY_FOR_COMPILATION  = "ready_for_compilation"


MessageRole = Literal["user", "assistant", "system"]


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class Message:
    """A single message in the conversation history."""

    role: MessageRole
    content: str
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)
    """
    Optional metadata attached to the message.
    Examples:
      - {"extracted_intent": "task_creation"}
      - {"clarification_id": "cq-001"}
    """


@dataclass
class ClarificationRequest:
    """A pending clarification that must be resolved before compilation."""

    clarification_id: str
    ambiguity_flag: str
    """The ambiguity flag this resolves (e.g., 'multiple_recipients_unclear')."""

    question: str
    """Natural language question to ask the user."""

    options: list[str] = field(default_factory=list)
    """Optional list of acceptable answers (empty = free text)."""

    resolved: bool = False
    resolved_value: Any = None
    """The user's response once resolved."""


@dataclass
class ConversationSession:
    """
    Complete state for a single conversation session.

    Encapsulates all mutable state for one multi-turn exchange between a
    user and the orchestration system.
    """

    session_id: str
    user_id: str
    household_id: str
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    state: ConversationState = ConversationState.AWAITING_CLARIFICATION
    """Current output state of the conversation."""

    history: list[Message] = field(default_factory=list)
    """
    Bounded message window. Oldest messages are evicted when the window is full.
    """

    current_intent: Intent | None = None
    """
    The most recently parsed Intent for this conversation.
    May be partial (ambiguities unresolved) or complete.
    """

    intent_overrides: dict[str, Any] = field(default_factory=dict)
    """
    User-provided resolutions to ambiguities, applied before compilation.
    {ambiguity_flag: resolved_value, ...}
    """

    clarification_queue: list[ClarificationRequest] = field(default_factory=list)
    """
    Ordered list of unresolved clarification requests.
    Processed front-to-back; head is the active question.
    """

    metadata: dict[str, Any] = field(default_factory=dict)
    """
    Arbitrary session metadata (channel, agent_id, version, etc.).
    """

    @property
    def active_clarification(self) -> ClarificationRequest | None:
        """The next unresolved clarification request, or None."""
        for cq in self.clarification_queue:
            if not cq.resolved:
                return cq
        return None

    @property
    def all_clarifications_resolved(self) -> bool:
        """True if every queued clarification has been resolved."""
        return all(cq.resolved for cq in self.clarification_queue)

    def pending_ambiguity_flags(self) -> list[str]:
        """Ambiguity flags from current_intent not yet resolved."""
        if self.current_intent is None:
            return []
        unresolved = [
            flag for flag in self.current_intent.ambiguity_flags
            if flag not in self.intent_overrides
        ]
        # Also include any unresolved clarification requests
        unresolved_cq = [
            cq.ambiguity_flag for cq in self.clarification_queue
            if not cq.resolved
        ]
        return list(dict.fromkeys(unresolved + unresolved_cq))  # deduplicated, ordered


# ── Engine ─────────────────────────────────────────────────────────────────────

class ConversationEngine:
    """
    Manages multi-turn conversation state per session.

    Receives messages, updates intent state, enqueues clarifications, and
    transitions to the appropriate output state.

    Does NOT parse or compile intent (those are compiler responsibilities).
    Does NOT execute workflows or interact with the scheduler.
    """

    # Human-readable questions per ambiguity flag
    _CLARIFICATION_QUESTIONS: dict[str, tuple[str, list[str]]] = {
        "multiple_recipients_unclear": (
            "Who should this task be assigned to?",
            [],
        ),
        "deadline_relative": (
            "When exactly is this needed? (e.g., by 5pm today, by Friday)",
            [],
        ),
        "time_ambiguous": (
            "What time of day did you mean?",
            ["morning (6–12)", "afternoon (12–18)", "evening (18–22)"],
        ),
        "resource_missing": (
            "What budget or resources are needed for this task?",
            [],
        ),
        "frequency_vague": (
            "How often should this recur?",
            ["daily", "weekly", "monthly", "custom interval"],
        ),
        "household_context_missing": (
            "I couldn't find your household profile. Can you confirm your household ID?",
            [],
        ),
        "user_context_missing": (
            "I couldn't find your user profile. Can you tell me your name or preferences?",
            [],
        ),
        "budget_limit_unset": (
            "What is your budget limit for this?",
            [],
        ),
    }

    def __init__(self, max_history: int = 20) -> None:
        """
        Args:
            max_history: Maximum messages retained per session (bounded window).
        """
        if max_history <= 0:
            raise ValueError("max_history must be > 0")
        self._max_history = max_history

    def new_session(
        self,
        user_id: str,
        household_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> ConversationSession:
        """
        Create a new conversation session.

        Args:
            user_id: Identifying the user.
            household_id: Identifying the household.
            metadata: Optional session metadata (channel, agent, etc.).

        Returns:
            A fresh ConversationSession in AWAITING_CLARIFICATION state.
        """
        return ConversationSession(
            session_id=str(uuid.uuid4()),
            user_id=user_id,
            household_id=household_id,
            metadata=metadata or {},
        )

    def ingest_message(
        self,
        session: ConversationSession,
        content: str,
        role: MessageRole = "user",
        metadata: dict[str, Any] | None = None,
    ) -> ConversationSession:
        """
        Add a message to the session history.

        Enforces the bounded window by evicting oldest messages when full.

        Args:
            session: The active session to update.
            content: Message text.
            role: Who sent the message (user, assistant, system).
            metadata: Optional metadata for this message.

        Returns:
            Updated session (mutated in-place).
        """
        message = Message(role=role, content=content, metadata=metadata or {})
        session.history.append(message)

        # Evict oldest if window exceeded
        if len(session.history) > self._max_history:
            session.history = session.history[-self._max_history:]

        session.updated_at = datetime.now()
        return session

    def apply_intent(
        self,
        session: ConversationSession,
        intent: Intent,
    ) -> ConversationSession:
        """
        Attach a parsed Intent to the session and transition state.

        Inspects the Intent's ambiguity_flags, enqueues any unresolved
        clarifications, and sets the appropriate output state.

        Args:
            session:  The active session.
            intent:   Parsed Intent from IntentParser.

        Returns:
            Updated session with state transitioned.
        """
        session.current_intent = intent
        session.updated_at = datetime.now()

        # Identify any NEW ambiguities not already in the queue
        existing_flags = {cq.ambiguity_flag for cq in session.clarification_queue}
        new_flags = [
            flag for flag in intent.ambiguity_flags
            if flag not in existing_flags and flag not in session.intent_overrides
        ]

        for flag in new_flags:
            session.clarification_queue.append(
                self._build_clarification(flag)
            )

        session.state = self._compute_state(session)
        return session

    def apply_clarification_response(
        self,
        session: ConversationSession,
        response: str,
    ) -> ConversationSession:
        """
        Record a user's answer to the active clarification request.

        Marks the head clarification as resolved and re-evaluates state.

        Args:
            session:  The active session.
            response: The user's response text.

        Returns:
            Updated session with clarification resolved and state re-evaluated.
        """
        active = session.active_clarification
        if active is None:
            # No pending clarification — safe no-op
            return session

        active.resolved = True
        active.resolved_value = response
        session.intent_overrides[active.ambiguity_flag] = response
        session.updated_at = datetime.now()

        # Add the response as a user message
        self.ingest_message(
            session, response, role="user",
            metadata={"clarification_id": active.clarification_id},
        )

        session.state = self._compute_state(session)
        return session

    def enqueue_clarification(
        self,
        session: ConversationSession,
        ambiguity_flag: str,
        question: str | None = None,
        options: list[str] | None = None,
    ) -> ConversationSession:
        """
        Manually enqueue a clarification request (e.g., from ContextResolver).

        Args:
            session:        The active session.
            ambiguity_flag: The flag this clarification resolves.
            question:       Custom question text (overrides default).
            options:        Optional list of accepted answers.

        Returns:
            Updated session with the new clarification queued.
        """
        # Don't duplicate already-queued flags
        existing = {cq.ambiguity_flag for cq in session.clarification_queue}
        if ambiguity_flag in existing:
            return session

        cq = self._build_clarification(ambiguity_flag, question, options)
        session.clarification_queue.append(cq)
        session.state = self._compute_state(session)
        session.updated_at = datetime.now()
        return session

    def reset_for_new_intent(
        self, session: ConversationSession
    ) -> ConversationSession:
        """
        Clear intent state while preserving history and session identity.

        Use when the user starts a new topic mid-conversation.
        """
        session.current_intent = None
        session.intent_overrides = {}
        session.clarification_queue = []
        session.state = ConversationState.AWAITING_CLARIFICATION
        session.updated_at = datetime.now()
        return session

    def get_next_question(self, session: ConversationSession) -> str | None:
        """
        Return the question text for the next unresolved clarification.

        Returns None if no clarification is pending.
        """
        cq = session.active_clarification
        if cq is None:
            return None
        if cq.options:
            options_str = ", ".join(f"'{o}'" for o in cq.options)
            return f"{cq.question} Options: {options_str}"
        return cq.question

    def summary(self, session: ConversationSession) -> dict[str, Any]:
        """
        Return a concise snapshot of session state for logging/debugging.
        """
        return {
            "session_id": session.session_id,
            "user_id": session.user_id,
            "household_id": session.household_id,
            "state": session.state.value,
            "message_count": len(session.history),
            "intent_type": (
                session.current_intent.intent_type
                if session.current_intent else None
            ),
            "pending_clarifications": len(
                [cq for cq in session.clarification_queue if not cq.resolved]
            ),
            "intent_overrides_applied": len(session.intent_overrides),
            "updated_at": session.updated_at.isoformat(),
        }

    # ── Private helpers ────────────────────────────────────────────────────

    def _compute_state(
        self, session: ConversationSession
    ) -> ConversationState:
        """
        Derive the output state from session state.

        State transition rules:
          - Any unresolved clarification → AWAITING_CLARIFICATION
          - Intent is set, all clarifications resolved, no ambiguity flags
            remain unresolved in overrides → READY_FOR_COMPILATION
          - Intent is set but clarifications are absent → INTENT_COMPLETE
        """
        if not session.all_clarifications_resolved:
            return ConversationState.AWAITING_CLARIFICATION

        if session.current_intent is None:
            return ConversationState.AWAITING_CLARIFICATION

        remaining = session.pending_ambiguity_flags()
        if remaining:
            # Flags remain but no queued CQs — intent is partially clear
            return ConversationState.INTENT_COMPLETE

        return ConversationState.READY_FOR_COMPILATION

    def _build_clarification(
        self,
        flag: str,
        question: str | None = None,
        options: list[str] | None = None,
    ) -> ClarificationRequest:
        """
        Build a ClarificationRequest for a given ambiguity flag.
        Falls back to a generic question if the flag is unknown.
        """
        default_question, default_options = self._CLARIFICATION_QUESTIONS.get(
            flag, (f"Could you clarify: {flag}?", [])
        )
        return ClarificationRequest(
            clarification_id=str(uuid.uuid4()),
            ambiguity_flag=flag,
            question=question or default_question,
            options=options if options is not None else default_options,
        )
