"""
Lightweight rule-based alert engine.

Runs in the calling thread — no background threads, no I/O.
Each check is invoked explicitly (typically from broadcaster hot paths
or a periodic health-tick).

Alert behaviour:
  - Logs as ERROR via the structured logger
  - Records the alert in an in-memory ring of recent alerts
  - Increments alert counters in the metrics store

Rules:
  - resync_spike:       resync_required_total grows faster than threshold per window
  - error_spike:        errors_total grows faster than threshold per window
  - watermark_collision: duplicate watermark emitted (should never happen)
  - replay_gap:         events missing between consecutive replayed watermarks
  - duplicate_emission: same event emitted twice on the live stream
"""
from __future__ import annotations

import time
from collections import deque
from threading import Lock
from typing import Any

from apps.api.observability.metrics import metrics

# Lazily imported to avoid circular dependency
_log_error_ref = None


def _log_error(event: str, **fields: Any) -> None:
    global _log_error_ref
    if _log_error_ref is None:
        from apps.api.observability.logging import log_error as _le
        _log_error_ref = _le
    _log_error_ref(event, "alert triggered", **fields)


# ---------------------------------------------------------------------------
# Thresholds (adjustable at runtime via safety controls or env)
# ---------------------------------------------------------------------------

RESYNC_SPIKE_THRESHOLD = 5    # tolerated resyncs per 60s window
ERROR_SPIKE_THRESHOLD = 10    # tolerated errors per 60s window
WINDOW_SECONDS = 60


# ---------------------------------------------------------------------------
# Recent alerts ring buffer
# ---------------------------------------------------------------------------

class _AlertStore:
    MAX_RECENT = 200

    def __init__(self) -> None:
        self._lock = Lock()
        self._recent: deque[dict[str, Any]] = deque(maxlen=self.MAX_RECENT)

    def record(self, alert_type: str, **fields: Any) -> None:
        entry = {
            "alert": alert_type,
            "fired_at": time.time(),
            **fields,
        }
        with self._lock:
            self._recent.append(entry)
        metrics.increment("alerts_fired_total")
        _log_error(f"ALERT_{alert_type.upper()}", **fields)

    def recent(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._recent)


alert_store = _AlertStore()


# ---------------------------------------------------------------------------
# Sliding-window rate tracker
# ---------------------------------------------------------------------------

class _RateTracker:
    """Tracks events per minute using a sliding time window."""

    def __init__(self, window_seconds: float = WINDOW_SECONDS) -> None:
        self._lock = Lock()
        self._window = window_seconds
        self._events: deque[float] = deque()

    def record(self) -> None:
        now = time.time()
        with self._lock:
            self._events.append(now)
            self._evict(now)

    def count_in_window(self) -> int:
        now = time.time()
        with self._lock:
            self._evict(now)
            return len(self._events)

    def _evict(self, now: float) -> None:
        """Remove timestamps outside current window (caller must hold lock)."""
        cutoff = now - self._window
        while self._events and self._events[0] < cutoff:
            self._events.popleft()


# Module-level rate trackers
_resync_rate = _RateTracker()
_error_rate = _RateTracker()


# ---------------------------------------------------------------------------
# Public alert checks — call these from hot paths
# ---------------------------------------------------------------------------

def check_resync_spike() -> None:
    """Call every time a RESYNC_REQUIRED is emitted."""
    _resync_rate.record()
    count = _resync_rate.count_in_window()
    if count > RESYNC_SPIKE_THRESHOLD:
        alert_store.record(
            "resync_spike",
            value=count,
            threshold=RESYNC_SPIKE_THRESHOLD,
            window_seconds=WINDOW_SECONDS,
        )


def check_error_spike() -> None:
    """Call every time an error is recorded."""
    _error_rate.record()
    count = _error_rate.count_in_window()
    if count > ERROR_SPIKE_THRESHOLD:
        alert_store.record(
            "error_spike",
            value=count,
            threshold=ERROR_SPIKE_THRESHOLD,
            window_seconds=WINDOW_SECONDS,
        )


def signal_watermark_collision(watermark: str, household_id: str) -> None:
    """
    Signal that two events were emitted with the same watermark.
    This should NEVER happen — indicates AtomicCounter is broken.
    """
    snap = metrics.snapshot()
    alert_store.record(
        "watermark_collision",
        watermark=watermark,
        household_id=household_id,
        metrics_snapshot=snap["counters"],
    )


def signal_replay_gap(
    household_id: str,
    expected_seq: int,
    got_seq: int,
) -> None:
    """
    Signal that a gap was detected in replayed watermark sequence.
    Indicates ring buffer corruption or out-of-order storage.
    """
    alert_store.record(
        "replay_gap",
        household_id=household_id,
        expected_seq=expected_seq,
        got_seq=got_seq,
    )


def signal_duplicate_emission(watermark: str, household_id: str) -> None:
    """
    Signal that the same event watermark was emitted twice on the live stream.
    """
    alert_store.record(
        "duplicate_emission",
        watermark=watermark,
        household_id=household_id,
    )
