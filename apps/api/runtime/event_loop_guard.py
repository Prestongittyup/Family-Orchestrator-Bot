"""
Event Loop Guard — detects and reacts to asyncio loop starvation.

How it works
------------
A background asyncio task reschedules itself every *tick_interval_s* seconds.
If it wakes up later than expected by more than *lag_threshold_ms*, the loop
is considered congested.

Responses (configurable):
  1. Always:  emit a structured log warning.
  2. If load-shedding is enabled: call backpressure.update() with elevated
     timeout pressure to raise the backpressure multiplier immediately.

Usage
-----
Call ``await event_loop_guard.start()`` once at application startup.
The guard shuts itself down automatically if the loop stops.

The ``snapshot()`` method returns the last observed lag for the metrics endpoint.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time

from apps.api.runtime.backpressure_controller import backpressure
from apps.api.runtime.loop_tracing import trace_loop_context, trace_task_binding

_logger = logging.getLogger("uvicorn.error")

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #
_TICK_INTERVAL_S: float = float(os.getenv("ELG_TICK_INTERVAL_S", "0.1"))
_LAG_WARN_MS: float = float(os.getenv("ELG_LAG_WARN_MS", "80"))
_LAG_SHED_MS: float = float(os.getenv("ELG_LAG_SHED_MS", "300"))
_SHED_ENABLED: bool = os.getenv("ELG_SHED_ENABLED", "1") not in ("0", "false", "False")


class _EventLoopGuard:
    def __init__(self) -> None:
        self._last_lag_ms: float = 0.0
        self._max_lag_ms: float = 0.0
        self._warn_count: int = 0
        self._shed_count: int = 0
        self._running: bool = False
        self._task: asyncio.Task | None = None  # type: ignore[type-arg]

    async def start(self) -> None:
        """Start the background tick task.  Safe to call multiple times."""
        trace_loop_context("event_loop_guard.start")
        if self._running:
            return
        self._running = True
        self._task = asyncio.ensure_future(self._tick_loop())
        trace_task_binding(self._task, "CREATE: apps/api/runtime/event_loop_guard.py:start")

    async def _tick_loop(self) -> None:
        trace_loop_context("event_loop_guard._tick_loop")
        while self._running:
            expected_wake = time.perf_counter() + _TICK_INTERVAL_S
            await asyncio.sleep(_TICK_INTERVAL_S)
            actual_wake = time.perf_counter()
            lag_ms = max(0.0, (actual_wake - expected_wake) * 1000.0)
            self._last_lag_ms = lag_ms
            if lag_ms > self._max_lag_ms:
                self._max_lag_ms = lag_ms

            if lag_ms >= _LAG_WARN_MS:
                self._warn_count += 1
                _logger.warning(
                    json.dumps(
                        {
                            "marker": "EVENT_LOOP_LAG",
                            "lag_ms": round(lag_ms, 2),
                            "warn_count": self._warn_count,
                            "shed_enabled": _SHED_ENABLED,
                        }
                    )
                )

            if lag_ms >= _LAG_SHED_MS and _SHED_ENABLED:
                self._shed_count += 1
                # Escalate backpressure as if the timeout rate is very high.
                backpressure.update(
                    inflight_requests=0,
                    timeout_rate=min(1.0, lag_ms / 1000.0),
                )
                _logger.warning(
                    json.dumps(
                        {
                            "marker": "EVENT_LOOP_SHED_TRIGGERED",
                            "lag_ms": round(lag_ms, 2),
                            "shed_count": self._shed_count,
                        }
                    )
                )

    def stop(self) -> None:
        self._running = False
        if self._task is not None and not self._task.done():
            trace_loop_context("event_loop_guard.stop")
            trace_task_binding(self._task, "USE: apps/api/runtime/event_loop_guard.py:stop")
            self._task.cancel()

    def snapshot(self) -> dict[str, float | int]:
        return {
            "event_loop_lag_ms": round(self._last_lag_ms, 2),
            "event_loop_lag_max_ms": round(self._max_lag_ms, 2),
            "event_loop_warn_count": self._warn_count,
            "event_loop_shed_count": self._shed_count,
        }


# Module-level singleton.
event_loop_guard = _EventLoopGuard()
