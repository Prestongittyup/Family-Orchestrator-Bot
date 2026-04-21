"""
Conversation Orchestration Layer (COL) - Session Store
======================================================

Read/write store for conversation sessions used by UI bootstrap read models.

Design constraints:
  - In-memory, deterministic, and family-scoped
  - No orchestration side effects
  - Supports listing active sessions and unresolved partial intents
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from threading import RLock

from apps.api.conversation_orchestration.schema import ConversationSession, SessionState


@dataclass(frozen=True)
class SessionStoreSnapshot:
    """Immutable snapshot of store state for read-model generation."""

    version: int
    sessions: list[ConversationSession]
    generated_at: datetime


class ConversationSessionStore:
    """Family-scoped store for conversation sessions."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._sessions: dict[str, ConversationSession] = {}
        self._version: int = 0

    def upsert(self, session: ConversationSession) -> None:
        with self._lock:
            self._sessions[session.session_id] = session
            self._version += 1

    def get(self, session_id: str) -> ConversationSession | None:
        with self._lock:
            return self._sessions.get(session_id)

    def list_by_family(self, family_id: str) -> list[ConversationSession]:
        with self._lock:
            return sorted(
                [s for s in self._sessions.values() if s.family_id == family_id],
                key=lambda s: (s.last_updated.isoformat(), s.session_id),
            )

    def list_pending_by_family(self, family_id: str) -> list[ConversationSession]:
        """Return sessions with unresolved intents."""
        pending_states = {
            SessionState.COLLECTING,
            SessionState.CLARIFYING,
            SessionState.READY_FOR_EXECUTION,
            SessionState.AWAITING_CONFIRMATION,
        }
        with self._lock:
            return sorted(
                [
                    s for s in self._sessions.values()
                    if s.family_id == family_id and s.state in pending_states and s.active_intent is not None
                ],
                key=lambda s: (s.last_updated.isoformat(), s.session_id),
            )

    def version(self) -> int:
        with self._lock:
            return self._version

    def snapshot(self, family_id: str) -> SessionStoreSnapshot:
        with self._lock:
            sessions = [s for s in self._sessions.values() if s.family_id == family_id]
            sessions.sort(key=lambda s: (s.last_updated.isoformat(), s.session_id))
            return SessionStoreSnapshot(
                version=self._version,
                sessions=sessions,
                generated_at=datetime.utcnow(),
            )


DEFAULT_SESSION_STORE = ConversationSessionStore()
