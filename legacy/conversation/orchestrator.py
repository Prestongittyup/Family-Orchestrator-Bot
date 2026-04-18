"""
Conversation Orchestrator — High-level facade over the conversation layer.

Coordinates:
  - IntentParser         (compiler layer)
  - ConversationEngine   (session state machine)
  - ClarificationEngine  (question generation)
  - IntentRefiner        (completeness scoring)

Provides a user_id-keyed API where each user is fully isolated — no session
data is reachable through another user's key.

Rules
-----
- NO workflow generation
- NO DAG compilation
- NO scheduler interaction
- NO cross-user data access
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass

from legacy.compiler.intent_parser import Intent, IntentParser
from legacy.conversation.clarification_engine import ClarificationEngine, ClarificationPlan
from legacy.conversation.conversation_engine import (
    ConversationEngine as _ConversationEngine,
    ConversationSession,
    ConversationState,
)
from legacy.conversation.intent_refinement import IntentRefiner, RefinementResult


# ── Public output types ────────────────────────────────────────────────────────

@dataclass
class IngestResult:
    """Result returned from each call to ``ConversationOrchestrator.ingest()``."""

    requires_clarification: bool
    """True if the system needs more information before acting."""

    next_question: str | None
    """The clarification question to surface to the user, or None."""

    state_status: str
    """Session state string: awaiting_clarification | intent_complete | ready_for_compilation."""

    completeness: float
    """Intent completeness score in [0.0, 1.0]."""

    is_compiler_ready: bool
    """True if the intent can be passed to WorkflowCompiler without raising."""


@dataclass
class IntentView:
    """Read-only snapshot of the current intent's completeness state."""

    completeness: float
    intent_type: str
    remaining_flags: list[str]
    is_compiler_ready: bool


@dataclass
class SessionView:
    """Read-only snapshot of a conversation session."""

    user_id: str
    status: str
    """State string: awaiting_clarification | intent_complete | ready_for_compilation."""

    intent: IntentView | None
    """Intent snapshot, or None if no intent has been parsed yet."""


# ── Orchestrator ───────────────────────────────────────────────────────────────

class ConversationOrchestrator:
    """
    Stateful per-user conversation manager.

    Wraps the conversation layer components behind a simple
    ``ingest / get_state / clarify`` interface keyed by ``user_id``.

    Session isolation
    -----------------
    Each ``user_id`` maps to exactly one ``ConversationSession`` held in a
    private dict.  No cross-user index exists; a caller that does not hold
    the correct ``user_id`` cannot read or modify another user's data.

    Turn routing
    ------------
    Each call to ``ingest(user_id, text)`` checks whether the user has a
    pending clarification question:

    - **Clarification pending** — text is recorded as the answer and the
      ``IntentRefiner`` re-scores completeness.
    - **No pending clarification** — text is parsed as a new intent, enriched
      with orchestrator-level flags, and applied to the session.

    Orchestrator enrichment rule
    ----------------------------
    ``task_creation`` intents that carry no deadline and no preferred time
    slot are flagged as ``time_ambiguous``.  This guarantees the system asks
    the user *when* before attempting to schedule, even when the parser itself
    raises no flags.

    Usage::

        orch = ConversationOrchestrator()

        result = orch.ingest("user1", "water the garden")
        if result.requires_clarification:
            print(result.next_question)

        result2 = orch.ingest("user1", "every 2 days")
        state = orch.get_state("user1")
        print(state.intent.completeness)

        # Stateless question preview — no session is created or modified
        plan = orch.clarify("water the garden")
    """

    def __init__(self) -> None:
        self._conv_engine = _ConversationEngine(max_history=50)
        self._parser = IntentParser()
        self._clarification_engine = ClarificationEngine()
        self._refiner = IntentRefiner()

        # Isolated per-user stores — never exposed as a collection
        self._sessions: dict[str, ConversationSession] = {}
        self._refinements: dict[str, RefinementResult] = {}

    # ── Public API ─────────────────────────────────────────────────────────

    def ingest(self, user_id: str, text: str) -> IngestResult:
        """
        Process one turn of user input.

        If an unresolved clarification is pending for this user the text is
        treated as the answer.  Otherwise it is parsed as a new intent.

        No workflow or DAG is generated regardless of the resulting state.

        Args:
            user_id: Identifies the speaker.  Scopes all session state.
            text:    Raw user message.

        Returns:
            ``IngestResult`` describing the updated session state.
        """
        session = self._get_or_create(user_id)

        if session.active_clarification is not None:
            session = self._resolve_clarification(session, text)
        else:
            session = self._process_new_intent(session, user_id, text)

        self._sessions[user_id] = session

        # Re-score completeness after every turn
        if session.current_intent is not None:
            refinement = self._refiner.apply_patch(
                session.current_intent, session.intent_overrides
            )
            self._refinements[user_id] = refinement

        refinement = self._refinements.get(user_id)
        requires_clarification = (session.state == ConversationState.AWAITING_CLARIFICATION)

        return IngestResult(
            requires_clarification=requires_clarification,
            next_question=self._conv_engine.get_next_question(session),
            state_status=session.state.value,
            completeness=refinement.completeness if refinement else 0.0,
            is_compiler_ready=refinement.is_compiler_ready if refinement else False,
        )

    def get_state(self, user_id: str) -> SessionView:
        """
        Return a read-only snapshot of the session for ``user_id``.

        Returns an empty ``AWAITING_CLARIFICATION`` view for unknown users.

        Args:
            user_id: The user whose session to inspect.
        """
        session = self._sessions.get(user_id)
        if session is None:
            return SessionView(user_id=user_id, status="awaiting_clarification", intent=None)

        refinement = self._refinements.get(user_id)
        intent_view: IntentView | None = None
        if refinement is not None:
            intent_view = IntentView(
                completeness=refinement.completeness,
                intent_type=refinement.refined_intent.intent_type,
                remaining_flags=list(refinement.remaining_flags),
                is_compiler_ready=refinement.is_compiler_ready,
            )

        return SessionView(
            user_id=user_id,
            status=session.state.value,
            intent=intent_view,
        )

    def clarify(self, text: str) -> ClarificationPlan:
        """
        Generate a ``ClarificationPlan`` for the given text without creating
        or modifying any session.

        Deterministic: identical text always produces an identical plan.

        Args:
            text: Raw user input to analyse.

        Returns:
            Ordered, minimal ``ClarificationPlan``.
        """
        intent = self._parser.parse(text, household_id="system", user_id="system")
        all_flags = list(
            dict.fromkeys(
                list(intent.ambiguity_flags) + self._compute_enrichment_flags(intent)
            )
        )
        return self._clarification_engine.generate(intent, all_flags)

    # ── Private helpers ────────────────────────────────────────────────────

    def _get_or_create(self, user_id: str) -> ConversationSession:
        if user_id not in self._sessions:
            self._sessions[user_id] = self._conv_engine.new_session(
                user_id=user_id,
                household_id=user_id,
            )
        return self._sessions[user_id]

    def _process_new_intent(
        self,
        session: ConversationSession,
        user_id: str,
        text: str,
    ) -> ConversationSession:
        """
        Parse text as a new intent, apply orchestrator enrichment flags,
        and attach the enriched intent to the session.

        If the session already holds a prior intent (topic change), intent
        state is reset while history is preserved.
        """
        if session.current_intent is not None:
            session = self._conv_engine.reset_for_new_intent(session)

        intent = self._parser.parse(text, household_id=user_id, user_id=user_id)
        session = self._conv_engine.ingest_message(session, text, role="user")

        extra = self._compute_enrichment_flags(intent)
        if extra:
            combined = list(
                dict.fromkeys(list(intent.ambiguity_flags) + extra)
            )
            intent = dataclasses.replace(intent, ambiguity_flags=combined)

        session = self._conv_engine.apply_intent(session, intent)
        return session

    def _resolve_clarification(
        self,
        session: ConversationSession,
        response: str,
    ) -> ConversationSession:
        """Apply user text as the answer to the active clarification request."""
        return self._conv_engine.apply_clarification_response(session, response)

    def _compute_enrichment_flags(self, intent: Intent) -> list[str]:
        """
        Add orchestrator-level flags beyond what the parser emits.

        Rule: a ``task_creation`` intent with neither a deadline nor a
        preferred time slot is missing essential scheduling context.
        Flag it as ``time_ambiguous`` so the user is asked before the
        intent reaches the compiler.
        """
        if intent.intent_type != "task_creation":
            return []
        if "time_ambiguous" in intent.ambiguity_flags:
            return []
        constraints = intent.constraints
        if (
            constraints.get("deadline") is None
            and constraints.get("time_slot") is None
        ):
            return ["time_ambiguous"]
        return []
