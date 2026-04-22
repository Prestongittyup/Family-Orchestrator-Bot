"""
SSE Pressure Guard — tracks active SSE connections and enforces global and
per-household limits.

Degradation strategy (preferred over hard rejection when possible):
  1. If the global limit is within the soft-ceiling range, immediately reject
     with HTTP 429 and a structured reason.
  2. Per-household limit is always a hard cap (one noisy household must not
     consume all SSE slots).

Integration
-----------
Import the module-level ``sse_guard`` singleton and call
``sse_guard.acquire(household_id)`` before opening the SSE stream.
Always call ``sse_guard.release(household_id)`` in a ``finally`` block.

The guard also exposes ``snapshot()`` for the runtime-metrics endpoint.
"""
from __future__ import annotations

import os
from threading import Lock

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #
_GLOBAL_SSE_LIMIT = max(1, int(os.getenv("GLOBAL_SSE_LIMIT", "40")))
_PER_HOUSEHOLD_SSE_LIMIT = max(1, int(os.getenv("PER_HOUSEHOLD_SSE_LIMIT", "8")))


class _SSEPressureGuard:
    """Thread-safe guard for SSE connection pressure."""

    def __init__(self, global_limit: int, per_household_limit: int) -> None:
        self._global_limit = global_limit
        self._per_household_limit = per_household_limit
        self._lock = Lock()
        self._active_total: int = 0
        self._per_household: dict[str, int] = {}
        self._rejected_total: int = 0
        self._accepted_total: int = 0

    # ------------------------------------------------------------------ #
    # Public API                                                          #
    # ------------------------------------------------------------------ #

    def acquire(self, household_id: str) -> tuple[bool, str | None]:
        """
        Try to open an SSE slot for *household_id*.

        Returns
        -------
        (True, None)                – slot granted
        (False, reason_string)      – rejected; caller must return HTTP 429
        """
        with self._lock:
            # Per-household cap (hard).
            hh_count = self._per_household.get(household_id, 0)
            if hh_count >= self._per_household_limit:
                self._rejected_total += 1
                return False, "sse_per_household_limit_exceeded"

            # Global cap (hard cap at limit; soft ceiling = limit - 5).
            if self._active_total >= self._global_limit:
                self._rejected_total += 1
                return False, "sse_global_limit_exceeded"

            # Accept.
            self._active_total += 1
            self._per_household[household_id] = hh_count + 1
            self._accepted_total += 1
            return True, None

    def release(self, household_id: str) -> None:
        """Release a slot previously acquired for *household_id*."""
        with self._lock:
            self._active_total = max(0, self._active_total - 1)
            current = self._per_household.get(household_id, 0)
            if current <= 1:
                self._per_household.pop(household_id, None)
            else:
                self._per_household[household_id] = current - 1

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "sse_active": self._active_total,
                "sse_rejected": self._rejected_total,
                "sse_accepted": self._accepted_total,
                "sse_global_limit": self._global_limit,
                "sse_per_household_limit": self._per_household_limit,
                "sse_households_connected": len(self._per_household),
            }


# Module-level singleton.
sse_guard = _SSEPressureGuard(
    global_limit=_GLOBAL_SSE_LIMIT,
    per_household_limit=_PER_HOUSEHOLD_SSE_LIMIT,
)
