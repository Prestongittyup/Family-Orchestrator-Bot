"""
In-memory metrics collector for production observability.

Thread-safe. No external dependencies. Supports:
- Counters (ever-increasing)
- Gauges  (current value, can go up/down)
- Histograms (simple bucket-based latency tracking)
- Per-household tagging + global rollup
"""
from __future__ import annotations

import time
from collections import defaultdict
from threading import Lock
from typing import Any


# ---------------------------------------------------------------------------
# Histogram buckets for latency (milliseconds)
# ---------------------------------------------------------------------------
_LATENCY_BUCKETS_MS = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000]


class _Histogram:
    """Lightweight histogram with fixed ms-bucketed counts."""

    __slots__ = ("_lock", "_sum", "_count", "_buckets")

    def __init__(self) -> None:
        self._lock = Lock()
        self._sum: float = 0.0
        self._count: int = 0
        # bucket_upper_bound → count of observations ≤ bound
        self._buckets: dict[int, int] = {b: 0 for b in _LATENCY_BUCKETS_MS}
        self._buckets["+Inf"] = 0  # type: ignore[assignment]

    def observe(self, value_ms: float) -> None:
        with self._lock:
            self._sum += value_ms
            self._count += 1
            placed = False
            for bound in _LATENCY_BUCKETS_MS:
                if value_ms <= bound:
                    self._buckets[bound] += 1
                    placed = True
                    break
            if not placed:
                self._buckets["+Inf"] += 1  # type: ignore[assignment]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            avg = (self._sum / self._count) if self._count else 0.0
            return {
                "count": self._count,
                "sum_ms": round(self._sum, 3),
                "avg_ms": round(avg, 3),
                "buckets": dict(self._buckets),
            }


class _MetricsStore:
    """Single global in-memory metrics store."""

    def __init__(self) -> None:
        self._lock = Lock()

        # name → int
        self._counters: dict[str, int] = defaultdict(int)
        # name → float
        self._gauges: dict[str, float] = defaultdict(float)
        # name → _Histogram
        self._histograms: dict[str, _Histogram] = {}

        # Per-household sub-counters: (name, household_id) → int
        self._household_counters: dict[tuple[str, str], int] = defaultdict(int)

        # -- Pre-register known metric names so /metrics always has them --
        for name in (
            "events_broadcast_total",
            "events_replayed_total",
            "resync_required_total",
            "idempotency_hits_total",
            "idempotency_misses_total",
            "errors_total",
            "auth_success_total",
            "auth_failure_total",
            "auth_invalid_token_total",
            "auth_system_failure_total",
            "invalid_token_non_401_count",
            "request_rejection_count",
            "limiter_trigger_count",
            "db_pool_rejection_count",
            "sse_connection_rejections",
            "auth_validation_cache_hits_total",
            "auth_validation_cache_misses_total",
        ):
            self._counters[name]  # ensure key exists

        for name in (
            "active_sse_connections",
            "replay_queue_depth",
            "inflight_request_count",
            "db_pool_in_use",
            "auth_success_rate",
            "auth_failure_rate",
            "auth_failure_rate_invalid_token",
            "auth_failure_rate_system_failure",
        ):
            self._gauges[name]  # ensure key exists

        for name in ("broadcast_latency_ms", "replay_latency_ms"):
            self._histograms[name] = _Histogram()

    # ------------------------------------------------------------------
    # Counter Operations
    # ------------------------------------------------------------------

    def increment(self, name: str, amount: int = 1, household_id: str | None = None) -> None:
        with self._lock:
            self._counters[name] += amount
            if household_id:
                self._household_counters[(name, household_id)] += amount

    # ------------------------------------------------------------------
    # Gauge Operations
    # ------------------------------------------------------------------

    def gauge_set(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def gauge_inc(self, name: str, amount: float = 1.0) -> None:
        with self._lock:
            self._gauges[name] += amount

    def gauge_dec(self, name: str, amount: float = 1.0) -> None:
        with self._lock:
            self._gauges[name] = max(0.0, self._gauges[name] - amount)

    # ------------------------------------------------------------------
    # Histogram Operations
    # ------------------------------------------------------------------

    def histogram_observe(self, name: str, value_ms: float) -> None:
        # Histograms don't need the outer lock; _Histogram has its own
        if name not in self._histograms:
            with self._lock:
                if name not in self._histograms:
                    self._histograms[name] = _Histogram()
        self._histograms[name].observe(value_ms)

    # ------------------------------------------------------------------
    # Read / Snapshot
    # ------------------------------------------------------------------

    def get_counter(self, name: str) -> int:
        with self._lock:
            return self._counters.get(name, 0)

    def get_gauge(self, name: str) -> float:
        with self._lock:
            return self._gauges.get(name, 0.0)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            # per-household breakdown
            per_household: dict[str, dict[str, int]] = defaultdict(dict)
            for (metric, hh), val in self._household_counters.items():
                per_household[hh][metric] = val

        histograms = {name: h.snapshot() for name, h in self._histograms.items()}

        return {
            "counters": counters,
            "gauges": gauges,
            "histograms": histograms,
            "per_household": dict(per_household),
            "snapshot_at": time.time(),
        }

    def record_auth_result(self, success: bool, failure_type: str | None = None) -> None:
        with self._lock:
            if success:
                self._counters["auth_success_total"] += 1
            else:
                self._counters["auth_failure_total"] += 1
                if failure_type in {"missing_bearer_token", "invalid_token"}:
                    self._counters["auth_invalid_token_total"] += 1
                elif failure_type == "system_failure":
                    self._counters["auth_system_failure_total"] += 1

            success_total = self._counters["auth_success_total"]
            failure_total = self._counters["auth_failure_total"]
            total = success_total + failure_total
            invalid_total = self._counters["auth_invalid_token_total"]
            system_total = self._counters["auth_system_failure_total"]

            self._gauges["auth_success_rate"] = (success_total / total) if total else 0.0
            self._gauges["auth_failure_rate"] = (failure_total / total) if total else 0.0
            self._gauges["auth_failure_rate_invalid_token"] = (invalid_total / total) if total else 0.0
            self._gauges["auth_failure_rate_system_failure"] = (system_total / total) if total else 0.0

    def note_request_rejection(self, reason: str | None = None) -> None:
        with self._lock:
            self._counters["request_rejection_count"] += 1
            if reason:
                self._counters[f"request_rejection_{reason}_total"] += 1
            if reason == "max_inflight":
                self._counters["limiter_trigger_count"] += 1

    def note_db_pool_rejection(self) -> None:
        with self._lock:
            self._counters["db_pool_rejection_count"] += 1

    def note_invalid_token_non_401(self) -> None:
        with self._lock:
            self._counters["invalid_token_non_401_count"] += 1

    def note_sse_connection_rejection(self) -> None:
        with self._lock:
            self._counters["sse_connection_rejections"] += 1


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
metrics = _MetricsStore()


# ---------------------------------------------------------------------------
# Convenience context manager for timing
# ---------------------------------------------------------------------------
class _Timer:
    """Usage: with timer("broadcast_latency_ms"): ..."""

    __slots__ = ("_name", "_start")

    def __init__(self, histogram_name: str) -> None:
        self._name = histogram_name

    def __enter__(self) -> "_Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_: Any) -> None:
        elapsed_ms = (time.perf_counter() - self._start) * 1000
        metrics.histogram_observe(self._name, elapsed_ms)


def timer(histogram_name: str) -> _Timer:
    """Return a context manager that records elapsed ms into the named histogram."""
    return _Timer(histogram_name)
