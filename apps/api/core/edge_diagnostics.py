from __future__ import annotations

import json
import logging
import os
import time
from threading import Lock
from typing import Any


class EdgeDiagnostics:
    """Separate edge telemetry channel for pre-ASGI saturation diagnosis."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._edge_received_count = 0
        self._active_in_worker = 0
        self._max_active_in_worker = 0
        self._last_request_start_perf: float | None = None
        self._logger = logging.getLogger("uvicorn.error")

    def begin_request(self, *, path: str, client_ip: str, pid: int) -> dict[str, Any]:
        now_perf = time.perf_counter()
        now_ts = time.time()
        with self._lock:
            self._edge_received_count += 1
            if self._last_request_start_perf is None:
                event_loop_lag_ms = 0.0
            else:
                # Simple lag proxy requested for diagnostics pass.
                event_loop_lag_ms = max(0.0, (now_perf - self._last_request_start_perf) * 1000.0)
            self._last_request_start_perf = now_perf

            self._active_in_worker += 1
            if self._active_in_worker > self._max_active_in_worker:
                self._max_active_in_worker = self._active_in_worker

            snapshot = {
                "marker": "REQUEST_RECEIVED_AT_APP_LAYER",
                "ts": round(now_ts, 6),
                "path": path,
                "client_ip": client_ip,
                "pid": pid,
                "edge_received_count": self._edge_received_count,
                "edge_dropped_before_asgi_count": self._estimate_dropped_before_asgi(),
                "active_in_worker": self._active_in_worker,
                "max_active_in_worker": self._max_active_in_worker,
                "event_loop_lag_ms": round(event_loop_lag_ms, 3),
            }

        self._logger.info(json.dumps(snapshot, sort_keys=True))
        return snapshot

    def end_request(self, *, path: str, pid: int, status_code: int) -> None:
        with self._lock:
            self._active_in_worker = max(0, self._active_in_worker - 1)
            snapshot = {
                "marker": "REQUEST_COMPLETED_AT_APP_LAYER",
                "path": path,
                "pid": pid,
                "status_code": status_code,
                "active_in_worker": self._active_in_worker,
                "edge_received_count": self._edge_received_count,
                "edge_dropped_before_asgi_count": self._estimate_dropped_before_asgi(),
            }
        self._logger.debug(json.dumps(snapshot, sort_keys=True))

    def _estimate_dropped_before_asgi(self) -> int | None:
        # Optional estimation channel: set EDGE_EXPECTED_REQUESTS to the expected
        # inbound count from the load generator for this run.
        expected = os.getenv("EDGE_EXPECTED_REQUESTS", "").strip()
        if not expected:
            return None
        try:
            expected_total = int(expected)
        except ValueError:
            return None
        return max(0, expected_total - self._edge_received_count)


edge_diagnostics = EdgeDiagnostics()
