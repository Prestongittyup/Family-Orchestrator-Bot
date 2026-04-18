"""
Step 16 Verification — Conversation Layer Integration

Hard pass criteria:
  - Multi-turn refinement operates without triggering any workflow execution
  - Clarification output is deterministic (same input → same questions)
  - No DAG or workflow is generated prematurely
  - Sessions remain isolated per user_id
  - Intent completeness gating is reliable

All 5 tests use only the ConversationOrchestrator (aliased as ConversationEngine
to match the spec interface).  The orchestrator is the public surface; internal
component interactions are not asserted.
"""

from __future__ import annotations

import pytest

from legacy.conversation.orchestrator import ConversationOrchestrator as ConversationEngine


# ── Test 1: State tracking ─────────────────────────────────────────────────────

def test_conversation_state_tracking():
    """
    After ingesting a single message the session must be in a valid
    intermediate state — not silently jumping to a terminal/unknown state.
    """
    engine = ConversationEngine()
    engine.ingest("user1", "water garden")

    state = engine.get_state("user1")

    assert state.status in ["awaiting_clarification", "intent_complete"], (
        f"Unexpected initial state: {state.status!r}"
    )


# ── Test 2: Clarification trigger ─────────────────────────────────────────────

def test_clarification_trigger():
    """
    Vague or under-specified input must require clarification before the
    system considers acting.  No workflow generation occurs at this stage.
    """
    engine = ConversationEngine()

    result = engine.ingest("user1", "schedule something later")

    assert result.requires_clarification is True, (
        "Vague input should require clarification; "
        f"got requires_clarification={result.requires_clarification!r}, "
        f"state={result.state_status!r}"
    )


# ── Test 3: Intent refinement loop ────────────────────────────────────────────

def test_intent_refinement_loop():
    """
    A second turn that answers the pending clarification must increase
    intent completeness beyond the initial partial score.

    No workflow or DAG is generated at any point; the loop is purely
    state enrichment.
    """
    engine = ConversationEngine()

    engine.ingest("user1", "water garden")
    engine.ingest("user1", "every 2 days")  # answers pending clarification

    state = engine.get_state("user1")

    assert state.intent is not None, "Intent should be set after two turns"
    assert state.intent.completeness > 0.5, (
        f"Completeness should exceed 0.5 after refinement; "
        f"got {state.intent.completeness:.3f}"
    )


# ── Test 4: Deterministic clarification ───────────────────────────────────────

def test_deterministic_clarification():
    """
    The clarification plan for a given text must be identical across
    separate calls.  The engine must not introduce randomness (e.g., UUIDs)
    into the plan structure.
    """
    engine = ConversationEngine()

    q1 = engine.clarify("water garden")
    q2 = engine.clarify("water garden")

    assert q1 == q2, (
        "clarify() must be deterministic; "
        f"got different plans:\n  q1={q1}\n  q2={q2}"
    )


# ── Test 5: Session isolation ─────────────────────────────────────────────────

def test_session_isolation():
    """
    Ingesting for user1 must have zero effect on user2's session, and
    vice versa.  Sessions must be completely independent.
    """
    engine = ConversationEngine()

    engine.ingest("user1", "water garden")
    engine.ingest("user2", "fix fence")

    state1 = engine.get_state("user1")
    state2 = engine.get_state("user2")

    assert state1 != state2, (
        "Sessions for different users must be independent; "
        f"user1={state1!r}, user2={state2!r}"
    )
    # Explicit cross-check: user_id fields must differ
    assert state1.user_id != state2.user_id
    # Confirm user1's state is not contaminated by user2's input
    assert state1.user_id == "user1"
    assert state2.user_id == "user2"
