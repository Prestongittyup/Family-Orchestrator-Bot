"""
Soft Backpressure Controller — produces a dynamic admission multiplier in
[0.0, 1.0] from real-time system signals.  A multiplier of 1.0 means full
admission; lower values progressively restrict new requests before the system
collapses.

Inputs sampled externally (typically from ASGI admission state + recent
request observations):
    inflight_requests   – current number of accepted-but-not-completed requests
    queue_depth         – upstream queue / replay depth (0 if unavailable)
    p95_latency_ms      – recent p95 latency window
    timeout_rate        – fraction of recent requests that timed out [0, 1]

Thresholds are configurable via environment variables.  Defaults are
conservative enough for a single-instance server (SQLite + 2 Uvicorn workers).

Integration
-----------
The ASGI admission gate should call ``backpressure.multiplier()`` and use it
to probabilistically skip admission when the value is below 1.0:

    if random.random() > backpressure.multiplier():
        # soft-reject even within cap
        ...

Alternatively the gate may lower MAX_INFLIGHT_CAP proportionally.
"""
from __future__ import annotations

import os
import threading
import time
from collections import deque

# --------------------------------------------------------------------------- #
# Thresholds — override via environment variables                             #
# --------------------------------------------------------------------------- #
_INFLIGHT_SOFT_THRESHOLD = int(os.getenv("BP_INFLIGHT_SOFT", "14"))
_INFLIGHT_HARD_THRESHOLD = int(os.getenv("BP_INFLIGHT_HARD", "18"))
_LATENCY_SOFT_MS = float(os.getenv("BP_LATENCY_SOFT_MS", "600"))
_LATENCY_HARD_MS = float(os.getenv("BP_LATENCY_HARD_MS", "2000"))
_TIMEOUT_SOFT_RATE = float(os.getenv("BP_TIMEOUT_SOFT_RATE", "0.05"))
_TIMEOUT_HARD_RATE = float(os.getenv("BP_TIMEOUT_HARD_RATE", "0.15"))
_QUEUE_SOFT_DEPTH = int(os.getenv("BP_QUEUE_SOFT_DEPTH", "100"))
_QUEUE_HARD_DEPTH = int(os.getenv("BP_QUEUE_HARD_DEPTH", "300"))

# Window for latency trend detection.
_LATENCY_WINDOW = 30


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


def _level(value: float, soft: float, hard: float) -> float:
    """Return [0, 1] pressure contribution from a single signal."""
    if value <= soft:
        return 0.0
    if value >= hard:
        return 1.0
    return (value - soft) / max(1e-9, hard - soft)


class _BackpressureController:
    """Thread-safe, lock-minimal soft backpressure controller."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._latency_window: deque[tuple[float, float]] = deque(maxlen=_LATENCY_WINDOW)
        self._current_multiplier: float = 1.0
        self._last_update: float = 0.0
        self._level_inflight: float = 0.0
        self._level_latency: float = 0.0
        self._level_timeout: float = 0.0
        self._level_queue: float = 0.0

    # ------------------------------------------------------------------ #
    # Feed — call periodically (e.g. from sampler thread)                #
    # ------------------------------------------------------------------ #

    def record_latency(self, p95_ms: float) -> None:
        """Record a p95 latency observation for trend analysis."""
        with self._lock:
            self._latency_window.append((time.monotonic(), p95_ms))

    def update(
        self,
        *,
        inflight_requests: int,
        queue_depth: int = 0,
        p95_latency_ms: float = 0.0,
        timeout_rate: float = 0.0,
    ) -> float:
        """
        Recompute and store the admission multiplier.

        Returns the new multiplier value.
        """
        with self._lock:
            self._level_inflight = _level(inflight_requests, _INFLIGHT_SOFT_THRESHOLD, _INFLIGHT_HARD_THRESHOLD)
            self._level_latency = _level(p95_latency_ms, _LATENCY_SOFT_MS, _LATENCY_HARD_MS)
            self._level_timeout = _level(timeout_rate, _TIMEOUT_SOFT_RATE, _TIMEOUT_HARD_RATE)
            self._level_queue = _level(queue_depth, _QUEUE_SOFT_DEPTH, _QUEUE_HARD_DEPTH)

            # Latency trend bonus: if p95 is growing quickly, escalate faster.
            latency_trend_pressure = 0.0
            entries = list(self._latency_window)
            if len(entries) >= 4:
                xs = [e[0] for e in entries]
                ys = [e[1] for e in entries]
                mean_x = sum(xs) / len(xs)
                mean_y = sum(ys) / len(ys)
                num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
                den = sum((x - mean_x) ** 2 for x in xs) or 1e-9
                slope_ms_per_s = num / den
                if slope_ms_per_s > 20:  # >20 ms/sec latency growth → pressure
                    latency_trend_pressure = _clamp01(slope_ms_per_s / 200.0)

            # Aggregate pressure: max of all signals (conservative).
            total_pressure = max(
                self._level_inflight,
                self._level_latency,
                self._level_timeout,
                self._level_queue,
                latency_trend_pressure,
            )

            # Smooth transitions: never drop multiplier by more than 0.15 per update.
            target = _clamp01(1.0 - total_pressure)
            prev = self._current_multiplier
            if target < prev:
                self._current_multiplier = max(target, prev - 0.15)
            else:
                # Allow faster recovery.
                self._current_multiplier = min(target, prev + 0.25)

            self._last_update = time.monotonic()
            return self._current_multiplier

    # ------------------------------------------------------------------ #
    # Read                                                                 #
    # ------------------------------------------------------------------ #

    def multiplier(self) -> float:
        """Return the current admission multiplier [0.0, 1.0]."""
        with self._lock:
            return self._current_multiplier

    def snapshot(self) -> dict[str, float | str]:
        with self._lock:
            return {
                "backpressure_level": round(1.0 - self._current_multiplier, 4),
                "backpressure_multiplier": round(self._current_multiplier, 4),
                "bp_level_inflight": round(self._level_inflight, 4),
                "bp_level_latency": round(self._level_latency, 4),
                "bp_level_timeout": round(self._level_timeout, 4),
                "bp_level_queue": round(self._level_queue, 4),
            }


# Module-level singleton.
backpressure = _BackpressureController()
