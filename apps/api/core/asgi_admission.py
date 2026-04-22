from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any

from apps.api.core.audit_bypass import is_audit_bypass_request, scope_headers


MAX_INFLIGHT_CAP = 20


@dataclass(frozen=True)
class AdmissionDecision:
    accepted: bool
    inflight_after: int


class _AdmissionState:
    def __init__(self, max_inflight: int) -> None:
        self._max_inflight = max(1, max_inflight)
        self._lock = Lock()
        self._inflight = 0
        self._request_seq = 0
        self._inflight_requests: dict[str, float] = {}
        self._asgi_entry_received_count = 0
        self._admission_accepted_count = 0
        self._admission_rejected_count = 0
        self._completed_requests_count = 0
        self._failed_requests_count = 0
        self._client_timeout_count = 0
        self._max_inflight_observed = 0

    def note_asgi_entry(self, now_ts: float) -> tuple[int, str]:
        with self._lock:
            self._request_seq += 1
            request_id = f"r{self._request_seq}"
            self._asgi_entry_received_count += 1
            self._inflight_requests[request_id] = now_ts
            return self._asgi_entry_received_count, request_id

    def try_admit(self) -> AdmissionDecision:
        # Hot path: bounded O(1) critical section, no await, no I/O.
        with self._lock:
            if self._inflight >= self._max_inflight:
                self._admission_rejected_count += 1
                return AdmissionDecision(False, self._inflight)

            # Soft reject: probabilistic shedding when backpressure < 1.0.
            # Import lazily to avoid circular import at module load time.
            try:
                from apps.api.runtime.backpressure_controller import backpressure as _bp
                mult = _bp.multiplier()
                if mult < 1.0 and random.random() > mult:
                    self._admission_rejected_count += 1
                    return AdmissionDecision(False, self._inflight)
            except Exception:
                pass  # Never let backpressure errors block admission

            self._inflight += 1
            self._admission_accepted_count += 1
            if self._inflight > self._max_inflight_observed:
                self._max_inflight_observed = self._inflight
            return AdmissionDecision(True, self._inflight)

    def release(self) -> int:
        with self._lock:
            self._inflight = max(0, self._inflight - 1)
            return self._inflight

    def note_rejected(self, request_id: str) -> None:
        with self._lock:
            self._inflight_requests.pop(request_id, None)

    def note_completed(self, request_id: str) -> None:
        with self._lock:
            self._completed_requests_count += 1
            self._inflight_requests.pop(request_id, None)

    def note_failed(self, request_id: str) -> None:
        with self._lock:
            self._failed_requests_count += 1
            self._inflight_requests.pop(request_id, None)

    def note_client_timeout(self) -> int:
        with self._lock:
            self._client_timeout_count += 1
            return self._client_timeout_count

    def snapshot(self) -> dict[str, int | float]:
        with self._lock:
            completion_ratio = (
                float(self._completed_requests_count) / float(self._admission_accepted_count)
                if self._admission_accepted_count > 0
                else 0.0
            )
            return {
                # Requested runtime-metrics schema.
                "inflight_current": self._inflight,
                "accepted_total": self._admission_accepted_count,
                "rejected_total": self._admission_rejected_count,
                "completed_total": self._completed_requests_count,
                "failed_total": self._failed_requests_count,
                "completion_ratio": round(completion_ratio, 6),
                # Legacy keys retained for compatibility.
                "ASGI_ENTRY_RECEIVED_COUNT": self._asgi_entry_received_count,
                "ADMISSION_ACCEPTED_COUNT": self._admission_accepted_count,
                "ADMISSION_REJECTED_COUNT": self._admission_rejected_count,
                "COMPLETED_REQUESTS_COUNT": self._completed_requests_count,
                "FAILED_REQUESTS_COUNT": self._failed_requests_count,
                "CLIENT_TIMEOUT_COUNT": self._client_timeout_count,
                "INFLIGHT_CURRENT": self._inflight,
                "INFLIGHT_REQUESTS_TRACKED": len(self._inflight_requests),
                "MAX_INFLIGHT_OBSERVED": self._max_inflight_observed,
                "MAX_INFLIGHT_CAP": self._max_inflight,
            }


_state = _AdmissionState(MAX_INFLIGHT_CAP)
_edge_logger = logging.getLogger("uvicorn.error")


def get_runtime_metrics_snapshot() -> dict[str, int | float]:
    return _state.snapshot()


class AdmissionGateASGI:
    def __init__(self, app: Any) -> None:
        self._app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self._app(scope, receive, send)
            return

        now = round(time.time(), 6)
        path = scope.get("path", "")
        headers = scope_headers(scope)
        client = scope.get("client")
        client_ip = "unknown" if not client else str(client[0])
        pid = os.getpid()

        audit_bypass_active = is_audit_bypass_request(path, headers)
        if audit_bypass_active:
            _edge_logger.info(
                json.dumps(
                    {
                        "marker": "AUDIT_BYPASS_ACTIVE",
                        "layer": "asgi_admission",
                        "path": path,
                        "client_ip": client_ip,
                        "pid": pid,
                        "audit_mode": headers.get("x-audit-mode", "unknown"),
                    },
                    sort_keys=True,
                )
            )

        asgi_entry_count, request_id = _state.note_asgi_entry(now)
        decision = AdmissionDecision(True, _state.snapshot()["inflight_current"]) if audit_bypass_active else _state.try_admit()

        if not decision.accepted:
            _state.note_rejected(request_id)
            _edge_logger.info(
                json.dumps(
                    {
                        "marker": "REJECTED_ASGI",
                        "request_id": request_id,
                        "ts": now,
                        "path": path,
                        "client_ip": client_ip,
                        "pid": pid,
                        "asgi_entry_received_count": asgi_entry_count,
                        "admission_rejected_count": _state.snapshot()["ADMISSION_REJECTED_COUNT"],
                        "queue_depth": decision.inflight_after,
                    },
                    sort_keys=True,
                )
            )
            body = b'{"error":"capacity_exceeded"}'
            await send(
                {
                    "type": "http.response.start",
                    "status": 429,
                    "headers": [
                        [b"content-type", b"application/json"],
                        [b"content-length", str(len(body)).encode("ascii")],
                        [b"retry-after", b"1"],
                    ],
                }
            )
            await send({"type": "http.response.body", "body": body, "more_body": False})
            return

        succeeded = False
        t_start = time.perf_counter()
        try:
            await self._app(scope, receive, send)
            succeeded = True
        except asyncio.CancelledError:
            timeout_count = _state.note_client_timeout()
            _state.note_failed(request_id)
            _edge_logger.warning(
                json.dumps(
                    {
                        "marker": "CLIENT_TIMEOUT",
                        "request_id": request_id,
                        "ts": round(time.time(), 6),
                        "path": path,
                        "client_ip": client_ip,
                        "pid": pid,
                        "client_timeout_count": timeout_count,
                    },
                    sort_keys=True,
                )
            )
            raise
        except Exception:
            _state.note_failed(request_id)
            raise
        finally:
            inflight_after = _state.snapshot()["inflight_current"] if audit_bypass_active else _state.release()
            if succeeded:
                _state.note_completed(request_id)
            # Feed backpressure controller with live signals.
            try:
                from apps.api.runtime.backpressure_controller import backpressure as _bp
                elapsed_ms = (time.perf_counter() - t_start) * 1000.0
                snap = _state.snapshot()
                accepted = snap.get("accepted_total", 0) or 1
                timeout_rate = snap.get("CLIENT_TIMEOUT_COUNT", 0) / accepted
                _bp.record_latency(elapsed_ms)
                _bp.update(
                    inflight_requests=inflight_after,
                    timeout_rate=timeout_rate,
                )
            except Exception:
                pass