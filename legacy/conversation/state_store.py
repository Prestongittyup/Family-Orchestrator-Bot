"""
Conversation State Store — In-process, session-scoped conversation storage.

Holds ConversationSession objects in memory for the duration of their active
lifetime.  Sessions are isolated by session_id — cross-user leakage is
structurally impossible because the store never exposes a collection view and
every lookup requires the exact session_id.

TTL
---
Each session carries its own TTL (default 30 minutes, configurable).  The
store enforces TTL lazily (on every access) and eagerly (via an explicit
``expire()`` sweep).  No background thread is started; the caller decides
when to sweep.

No persistence
--------------
All state is process-local.  A restart clears every session.  This is by
design — ConversationState is ephemeral coordination data, not durable
application state.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from legacy.conversation.conversation_engine import ConversationSession, ConversationEngine


# ── TTL envelope ───────────────────────────────────────────────────────────────

@dataclass
class _Entry:
    """Internal wrapper pairing a session with its expiry timestamp."""

    session: ConversationSession
    expires_at: datetime


# ── Store ──────────────────────────────────────────────────────────────────────

class SessionStateStore:
    """
    Thread-safe, in-process store for short-lived ConversationSession objects.

    Isolation guarantee
    -------------------
    The store is keyed exclusively by ``session_id``.  No index on user_id or
    household_id is maintained.  A caller that does not hold the correct
    session_id cannot retrieve or modify another user's session.

    TTL enforcement
    ---------------
    - **Lazy** — checked on every get/update call; expired sessions are
      dropped and treated as non-existent.
    - **Eager** — call ``expire()`` to sweep all expired entries at once
      (e.g., on a periodic maintenance tick).

    Usage::

        store = SessionStateStore(default_ttl_seconds=1800)
        engine = ConversationEngine()

        session = engine.new_session(user_id="u1", household_id="h1")
        store.save(session)

        retrieved = store.get(session.session_id)
        store.update(session.session_id, updated_session)
        store.reset(session.session_id, engine)
        store.delete(session.session_id)
    """

    def __init__(self, default_ttl_seconds: int = 1800) -> None:
        """
        Args:
            default_ttl_seconds:
                How long a session lives without activity before it is
                treated as expired.  Each successful ``get`` or ``update``
                renews the TTL.  Defaults to 30 minutes.
        """
        if default_ttl_seconds <= 0:
            raise ValueError("default_ttl_seconds must be > 0")

        self._ttl = timedelta(seconds=default_ttl_seconds)
        self._store: dict[str, _Entry] = {}
        self._lock = threading.Lock()

    # ── Public API ─────────────────────────────────────────────────────────

    def save(
        self,
        session: ConversationSession,
        ttl_seconds: int | None = None,
    ) -> None:
        """
        Persist a new session (or overwrite an existing one).

        Args:
            session:     The ConversationSession to store.
            ttl_seconds: Per-session TTL override.  Uses store default if None.
        """
        ttl = timedelta(seconds=ttl_seconds) if ttl_seconds is not None else self._ttl
        expires_at = datetime.now() + ttl
        with self._lock:
            self._store[session.session_id] = _Entry(
                session=session,
                expires_at=expires_at,
            )

    def get(self, session_id: str) -> ConversationSession | None:
        """
        Retrieve a session by its ID.

        Returns None if the session does not exist or has expired.
        A successful retrieval renews the TTL.

        Args:
            session_id: The exact session_id issued at creation.
        """
        with self._lock:
            entry = self._store.get(session_id)
            if entry is None:
                return None
            if self._is_expired(entry):
                del self._store[session_id]
                return None
            # Renew TTL on access
            entry.expires_at = datetime.now() + self._ttl
            return entry.session

    def update(
        self,
        session_id: str,
        session: ConversationSession,
        ttl_seconds: int | None = None,
    ) -> bool:
        """
        Replace the stored session object for an existing session_id.

        Renews the TTL on success.  Returns False if the session does not
        exist or has already expired (use ``save`` to create new entries).

        Args:
            session_id:  The session to update.  Must match session.session_id.
            session:     The updated ConversationSession.
            ttl_seconds: Per-call TTL override.  Uses store default if None.

        Returns:
            True if the session was found and updated, False otherwise.

        Raises:
            ValueError: If session_id does not match session.session_id.
        """
        if session.session_id != session_id:
            raise ValueError(
                f"session_id mismatch: key='{session_id}' "
                f"but session.session_id='{session.session_id}'"
            )
        ttl = timedelta(seconds=ttl_seconds) if ttl_seconds is not None else self._ttl
        with self._lock:
            entry = self._store.get(session_id)
            if entry is None or self._is_expired(entry):
                if session_id in self._store:
                    del self._store[session_id]
                return False
            entry.session = session
            entry.expires_at = datetime.now() + ttl
            return True

    def reset(
        self,
        session_id: str,
        engine: ConversationEngine,
    ) -> ConversationSession | None:
        """
        Replace the stored session with a fresh one that preserves identity
        (user_id, household_id, metadata) but clears all conversation state:
        history, intent, overrides, and clarification queue.

        Resets the TTL clock.  Returns None if the session is not found.

        Args:
            session_id: The session to reset.
            engine:     ConversationEngine used to build the blank replacement.
        """
        with self._lock:
            entry = self._store.get(session_id)
            if entry is None or self._is_expired(entry):
                if session_id in self._store:
                    del self._store[session_id]
                return None

            old = entry.session
            fresh = engine.new_session(
                user_id=old.user_id,
                household_id=old.household_id,
                metadata=dict(old.metadata),
            )
            # Preserve the original session_id so the caller's reference
            # stays valid.
            fresh.session_id = session_id  # type: ignore[misc]

            entry.session = fresh
            entry.expires_at = datetime.now() + self._ttl
            return fresh

    def delete(self, session_id: str) -> bool:
        """
        Immediately remove a session from the store.

        Returns True if a session was present and removed, False if it was
        already absent or expired.

        Args:
            session_id: The session to remove.
        """
        with self._lock:
            if session_id not in self._store:
                return False
            del self._store[session_id]
            return True

    def expire(self) -> int:
        """
        Sweep and discard all sessions whose TTL has elapsed.

        Call this periodically (e.g., on a maintenance tick) to reclaim
        memory.  The store is fully functional without calling this — lazy
        eviction on every access handles correctness.

        Returns:
            Number of sessions evicted.
        """
        now = datetime.now()
        with self._lock:
            expired_keys = [
                sid for sid, entry in self._store.items()
                if entry.expires_at <= now
            ]
            for sid in expired_keys:
                del self._store[sid]
            return len(expired_keys)

    @property
    def active_count(self) -> int:
        """Number of sessions currently in the store (may include expired ones not yet swept)."""
        with self._lock:
            return len(self._store)

    def stats(self) -> dict[str, Any]:
        """
        Non-destructive snapshot of store internals for monitoring.

        Returns counts only — no session data is exposed.
        """
        now = datetime.now()
        with self._lock:
            total = len(self._store)
            live = sum(1 for e in self._store.values() if e.expires_at > now)
        return {
            "total_stored": total,
            "live": live,
            "expired_not_yet_swept": total - live,
            "default_ttl_seconds": int(self._ttl.total_seconds()),
        }

    # ── Private helpers ────────────────────────────────────────────────────

    @staticmethod
    def _is_expired(entry: _Entry) -> bool:
        return entry.expires_at <= datetime.now()
