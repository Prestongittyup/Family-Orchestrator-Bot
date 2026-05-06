from __future__ import annotations

import asyncio
from collections import deque
from contextvars import ContextVar
from datetime import UTC, datetime
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from threading import Lock
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

from archive.apps.api.core.audit_bypass import is_audit_bypass_request, scope_headers


MAX_INFLIGHT_CAP = 20
_TRACE_BUFFER_MAX = max(100, int(os.getenv("ADMISSION_TRACE_BUFFER_MAX", "2000")))
_TRACE_RESPONSE_BODY_MAX_CHARS = 500
_edge_logger = logging.getLogger("uvicorn.error")
_ADMISSION_STATE_ATTR = "_admission_gate_state"
_CURRENT_ADMISSION_STATE: ContextVar["_AdmissionState | None"] = ContextVar("admission_current_state", default=None)


def _utc_iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _query_from_scope(scope: dict[str, Any]) -> str:
    raw_query = scope.get("query_string", b"")
    if isinstance(raw_query, bytes):
        return raw_query.decode("utf-8", errors="replace")
    return str(raw_query or "")


def _body_preview(value: str) -> str:
    if len(value) <= _TRACE_RESPONSE_BODY_MAX_CHARS:
        return value
    return value[:_TRACE_RESPONSE_BODY_MAX_CHARS]


@dataclass(frozen=True)
class AdmissionDecision:
    accepted: bool
    inflight_after: int
    reason: str | None = None


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
        self._request_trace_lock = Lock()
        self._request_traces: deque[dict[str, Any]] = deque(maxlen=_TRACE_BUFFER_MAX)

    def record_request_trace(self, record: dict[str, Any]) -> None:
        with self._request_trace_lock:
            self._request_traces.append(dict(record))
        _edge_logger.info(json.dumps(record, sort_keys=True))

    def recent_request_traces(self, limit: int = 200) -> list[dict[str, Any]]:
        resolved_limit = max(1, int(limit))
        with self._request_trace_lock:
            rows = list(self._request_traces)
        if len(rows) <= resolved_limit:
            return rows
        return rows[-resolved_limit:]

    def clear_request_traces(self) -> None:
        with self._request_trace_lock:
            self._request_traces.clear()

    def _enforce_invariants_unlocked(self, *, context: str) -> None:
        if self._inflight < 0:
            _edge_logger.warning(
                json.dumps(
                    {
                        "marker": "ADMISSION_INVARIANT_UNDERFLOW",
                        "context": context,
                        "inflight": self._inflight,
                        "threshold": self._max_inflight,
                        "pid": os.getpid(),
                    },
                    sort_keys=True,
                )
            )
            self._inflight = 0

        if self._inflight > self._max_inflight:
            _edge_logger.warning(
                json.dumps(
                    {
                        "marker": "ADMISSION_INVARIANT_OVERFLOW",
                        "context": context,
                        "inflight": self._inflight,
                        "threshold": self._max_inflight,
                        "pid": os.getpid(),
                    },
                    sort_keys=True,
                )
            )
            self._inflight = self._max_inflight

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
            self._enforce_invariants_unlocked(context="try_admit_pre")
            if self._inflight >= self._max_inflight:
                self._admission_rejected_count += 1
                return AdmissionDecision(False, self._inflight, reason="hard_capacity_limit")

            # Soft reject: probabilistic shedding when backpressure < 1.0.
            # Import lazily to avoid circular import at module load time.
            try:
                from archive.apps.api.runtime.backpressure_controller import backpressure as _bp
                mult = _bp.multiplier()
                if mult < 1.0 and random.random() > mult:
                    self._admission_rejected_count += 1
                    return AdmissionDecision(False, self._inflight, reason="soft_backpressure_shed")
            except Exception:
                pass  # Never let backpressure errors block admission

            self._inflight += 1
            self._enforce_invariants_unlocked(context="try_admit_post")
            self._admission_accepted_count += 1
            if self._inflight > self._max_inflight_observed:
                self._max_inflight_observed = self._inflight
            return AdmissionDecision(True, self._inflight, reason="admitted")

    def release(self, *, request_id: str | None = None) -> int:
        with self._lock:
            self._enforce_invariants_unlocked(context="release_pre")
            if self._inflight <= 0:
                _edge_logger.warning(
                    json.dumps(
                        {
                            "marker": "ADMISSION_RELEASE_WITHOUT_INFLIGHT",
                            "request_id": request_id,
                            "inflight": self._inflight,
                            "threshold": self._max_inflight,
                            "pid": os.getpid(),
                        },
                        sort_keys=True,
                    )
                )
                self._inflight = 0
            else:
                self._inflight -= 1
            self._enforce_invariants_unlocked(context="release_post")
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
            self._enforce_invariants_unlocked(context="snapshot")
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


def _empty_runtime_metrics_snapshot() -> dict[str, int | float]:
    return {
        "inflight_current": 0,
        "accepted_total": 0,
        "rejected_total": 0,
        "completed_total": 0,
        "failed_total": 0,
        "completion_ratio": 0.0,
        "ASGI_ENTRY_RECEIVED_COUNT": 0,
        "ADMISSION_ACCEPTED_COUNT": 0,
        "ADMISSION_REJECTED_COUNT": 0,
        "COMPLETED_REQUESTS_COUNT": 0,
        "FAILED_REQUESTS_COUNT": 0,
        "CLIENT_TIMEOUT_COUNT": 0,
        "INFLIGHT_CURRENT": 0,
        "INFLIGHT_REQUESTS_TRACKED": 0,
        "MAX_INFLIGHT_OBSERVED": 0,
        "MAX_INFLIGHT_CAP": MAX_INFLIGHT_CAP,
    }


def _ensure_app_state_container(app: Any) -> Any:
    state = getattr(app, "state", None)
    if state is None:
        state = SimpleNamespace()
        setattr(app, "state", state)
    return state


def _ensure_admission_state(app: Any) -> _AdmissionState:
    app_state = _ensure_app_state_container(app)
    state = _AdmissionState(MAX_INFLIGHT_CAP)
    setattr(app_state, _ADMISSION_STATE_ATTR, state)
    return state


def _get_admission_state(app: Any | None) -> _AdmissionState | None:
    if app is None:
        return None
    app_state = getattr(app, "state", None)
    if app_state is None:
        return None
    state = getattr(app_state, _ADMISSION_STATE_ATTR, None)
    if isinstance(state, _AdmissionState):
        return state
    return None


def get_runtime_metrics_snapshot(app: Any | None = None) -> dict[str, int | float]:
    state = _get_admission_state(app) if app is not None else _CURRENT_ADMISSION_STATE.get()
    if state is None:
        return _empty_runtime_metrics_snapshot()
    return state.snapshot()


def get_recent_request_traces(limit: int = 200, app: Any | None = None) -> list[dict[str, Any]]:
    state = _get_admission_state(app) if app is not None else _CURRENT_ADMISSION_STATE.get()
    if state is None:
        return []
    return state.recent_request_traces(limit)


def clear_recent_request_traces(app: Any | None = None) -> None:
    state = _get_admission_state(app) if app is not None else _CURRENT_ADMISSION_STATE.get()
    if state is None:
        return
    state.clear_request_traces()


def _normalize_path(path: str) -> str:
    """Normalize route path for bypass checks across proxy/deployment shapes."""
    if not path:
        return "/"

    normalized = path
    if normalized.startswith("/api/"):
        normalized = normalized[4:]
    elif normalized == "/api":
        normalized = "/"

    if normalized != "/" and normalized.endswith("/"):
        normalized = normalized[:-1]

    return normalized


def _is_oauth_critical_path(path: str) -> bool:
    """Allow critical OAuth handshakes to bypass admission shedding."""
    normalized = _normalize_path(path)
    return (
        normalized.startswith("/integrations/google-calendar/connect/")
        or normalized == "/integrations/google-calendar/callback"
        or normalized.startswith("/integrations/google-calendar/status/")
    )


_IDENTITY_BOOTSTRAP_PATHS = frozenset(
    {
        "/v1/identity/household/create",
        "/v1/identity/user/register",
        "/v1/identity/device/register",
        "/v1/identity/bootstrap",
        "/v1/identity/session/validate",
        "/v1/ui/bootstrap",
    }
)


def _is_identity_bootstrap_critical_path(path: str) -> bool:
    """Allow identity bootstrap writes and UI bootstrap reads to bypass admission shedding."""
    return _normalize_path(path) in _IDENTITY_BOOTSTRAP_PATHS


class AdmissionGateASGI:
    def __init__(self, app: Any) -> None:
        self._app = app
        # Always attach a fresh state to this wrapped app instance.
        self._state = _ensure_admission_state(self._app)
        startup_snapshot = self._state.snapshot()
        initial_inflight = int(startup_snapshot.get("inflight_current", 0))
        threshold = int(startup_snapshot.get("MAX_INFLIGHT_CAP", MAX_INFLIGHT_CAP))
        pid = os.getpid()
        _edge_logger.info(
            json.dumps(
                {
                    "marker": "ADMISSION_STARTUP_STATE",
                    "pid": pid,
                    "initial_inflight": initial_inflight,
                    "threshold": threshold,
                },
                sort_keys=True,
            )
        )
        if initial_inflight >= threshold:
            _edge_logger.warning(
                json.dumps(
                    {
                        "marker": "ADMISSION_STARTUP_SATURATED",
                        "pid": pid,
                        "initial_inflight": initial_inflight,
                        "threshold": threshold,
                    },
                    sort_keys=True,
                )
            )

    def __getattr__(self, name: str) -> Any:
        # Delegate framework attributes (router, state, url_path_for, etc.) to
        # the wrapped FastAPI app so existing tests and tooling continue to work.
        return getattr(self._app, name)

    @property
    def dependency_overrides(self) -> Any:
        return getattr(self._app, "dependency_overrides")

    @dependency_overrides.setter
    def dependency_overrides(self, value: Any) -> None:
        setattr(self._app, "dependency_overrides", value)

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self._app(scope, receive, send)
            return

        state = self._state
        state_ctx_token = _CURRENT_ADMISSION_STATE.set(state)
        trace_request_id = str(uuid4())
        trace_started_at = _utc_iso_now()
        trace_started_perf = time.perf_counter()
        now = round(time.time(), 6)
        path = scope.get("path", "")
        method = str(scope.get("method", ""))
        query = _query_from_scope(scope)
        headers = scope_headers(scope)
        user_agent = str(headers.get("user-agent", ""))
        client = scope.get("client")
        client_ip = "unknown" if not client else str(client[0])
        pid = os.getpid()
        decision_state = "allowed"
        blocking_reason: str | None = None
        response_status = 0
        response_body_text = ""

        audit_bypass_active = is_audit_bypass_request(path, headers)
        oauth_bypass_active = _is_oauth_critical_path(path)
        identity_bootstrap_bypass_active = _is_identity_bootstrap_critical_path(path)
        bypass_active = audit_bypass_active or oauth_bypass_active or identity_bootstrap_bypass_active
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
        if oauth_bypass_active:
            _edge_logger.info(
                json.dumps(
                    {
                        "marker": "OAUTH_BYPASS_ACTIVE",
                        "layer": "asgi_admission",
                        "path": path,
                        "client_ip": client_ip,
                        "pid": pid,
                    },
                    sort_keys=True,
                )
            )
        if identity_bootstrap_bypass_active:
            _edge_logger.info(
                json.dumps(
                    {
                        "marker": "IDENTITY_BOOTSTRAP_BYPASS_ACTIVE",
                        "layer": "asgi_admission",
                        "path": path,
                        "client_ip": client_ip,
                        "pid": pid,
                    },
                    sort_keys=True,
                )
            )

        snapshot_before = state.snapshot()
        asgi_entry_count, request_id = state.note_asgi_entry(now)
        decision = (
            AdmissionDecision(True, state.snapshot()["inflight_current"], reason="critical_path_bypass")
            if bypass_active
            else state.try_admit()
        )
        slot_acquired = decision.accepted and not bypass_active

        _edge_logger.info(
            json.dumps(
                {
                    "marker": "ADMISSION_DECISION",
                    "trace_request_id": trace_request_id,
                    "admission_request_id": request_id,
                    "ts": trace_started_at,
                    "method": method,
                    "path": path,
                    "query": query,
                    "client_ip": client_ip,
                    "user_agent": user_agent,
                    "pid": pid,
                    "classification": {
                        "audit_bypass": audit_bypass_active,
                        "oauth_bypass": oauth_bypass_active,
                        "identity_bootstrap_bypass": identity_bootstrap_bypass_active,
                    },
                    "decision": "allowed" if decision.accepted else "blocked",
                    "decision_reason": decision.reason,
                    "capacity": {
                        "inflight_before": snapshot_before.get("INFLIGHT_CURRENT", 0),
                        "inflight_after_decision": decision.inflight_after,
                        "threshold": snapshot_before.get("MAX_INFLIGHT_CAP", MAX_INFLIGHT_CAP),
                        "admission_rejected_count": state.snapshot().get("ADMISSION_REJECTED_COUNT", 0),
                    },
                },
                sort_keys=True,
            )
        )

        if not decision.accepted:
            decision_state = "blocked"
            blocking_reason = decision.reason or "capacity_exceeded"
            state.note_rejected(request_id)
            _edge_logger.info(
                json.dumps(
                    {
                        "marker": "REJECTED_ASGI",
                        "request_id": request_id,
                        "ts": now,
                        "path": path,
                        "method": method,
                        "query": query,
                        "trace_request_id": trace_request_id,
                        "decision_reason": blocking_reason,
                        "capacity_threshold": snapshot_before.get("MAX_INFLIGHT_CAP", MAX_INFLIGHT_CAP),
                        "client_ip": client_ip,
                        "user_agent": user_agent,
                        "pid": pid,
                        "asgi_entry_received_count": asgi_entry_count,
                        "admission_rejected_count": state.snapshot()["ADMISSION_REJECTED_COUNT"],
                        "queue_depth": decision.inflight_after,
                    },
                    sort_keys=True,
                )
            )
            body = b'{"error":"capacity_exceeded"}'
            response_status = 429
            response_body_text = body.decode("utf-8", errors="replace")
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
            duration_ms = round((time.perf_counter() - trace_started_perf) * 1000.0, 3)
            state.record_request_trace(
                {
                    "marker": "REQUEST_TRACE",
                    "trace_request_id": trace_request_id,
                    "admission_request_id": request_id,
                    "timestamp": trace_started_at,
                    "method": method,
                    "path": path,
                    "query": query,
                    "client_ip": client_ip,
                    "user_agent": user_agent,
                    "status_code": response_status,
                    "response_body": _body_preview(response_body_text),
                    "duration_ms": duration_ms,
                    "middleware_decision": decision_state,
                    "blocking_reason": blocking_reason,
                    "admission_reason": decision.reason,
                    "capacity_usage": {
                        "inflight_before": snapshot_before.get("INFLIGHT_CURRENT", 0),
                        "inflight_after": state.snapshot().get("INFLIGHT_CURRENT", 0),
                        "threshold": snapshot_before.get("MAX_INFLIGHT_CAP", MAX_INFLIGHT_CAP),
                    },
                }
            )
            _CURRENT_ADMISSION_STATE.reset(state_ctx_token)
            return

        succeeded = False
        t_start = time.perf_counter()
        response_body_chunks: list[str] = []

        async def traced_send(message: dict[str, Any]) -> None:
            nonlocal response_status
            if message.get("type") == "http.response.start":
                response_status = int(message.get("status") or 0)
            elif message.get("type") == "http.response.body":
                raw_body = message.get("body", b"")
                if isinstance(raw_body, bytes):
                    decoded = raw_body.decode("utf-8", errors="replace")
                elif isinstance(raw_body, str):
                    decoded = raw_body
                else:
                    decoded = str(raw_body)
                if decoded:
                    current = "".join(response_body_chunks)
                    if len(current) < _TRACE_RESPONSE_BODY_MAX_CHARS:
                        remaining = _TRACE_RESPONSE_BODY_MAX_CHARS - len(current)
                        response_body_chunks.append(decoded[:remaining])
            await send(message)

        try:
            await self._app(scope, receive, traced_send)
            succeeded = True
        except asyncio.CancelledError:
            timeout_count = state.note_client_timeout()
            state.note_failed(request_id)
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
            state.note_failed(request_id)
            raise
        finally:
            inflight_after = state.release(request_id=request_id) if slot_acquired else state.snapshot()["inflight_current"]
            if succeeded:
                state.note_completed(request_id)
            # Feed backpressure controller with live signals.
            try:
                from archive.apps.api.runtime.backpressure_controller import backpressure as _bp
                elapsed_ms = (time.perf_counter() - t_start) * 1000.0
                snap = state.snapshot()
                accepted = snap.get("accepted_total", 0) or 1
                timeout_rate = snap.get("CLIENT_TIMEOUT_COUNT", 0) / accepted
                _bp.record_latency(elapsed_ms)
                _bp.update(
                    inflight_requests=inflight_after,
                    timeout_rate=timeout_rate,
                )
            except Exception:
                pass

            if succeeded and response_status >= 400:
                decision_state = "blocked"
                if not blocking_reason:
                    blocking_reason = f"upstream_status_{response_status}"

            duration_ms = round((time.perf_counter() - trace_started_perf) * 1000.0, 3)
            state.record_request_trace(
                {
                    "marker": "REQUEST_TRACE",
                    "trace_request_id": trace_request_id,
                    "admission_request_id": request_id,
                    "timestamp": trace_started_at,
                    "method": method,
                    "path": path,
                    "query": query,
                    "client_ip": client_ip,
                    "user_agent": user_agent,
                    "status_code": response_status,
                    "response_body": _body_preview("".join(response_body_chunks)),
                    "duration_ms": duration_ms,
                    "middleware_decision": decision_state,
                    "blocking_reason": blocking_reason,
                    "admission_reason": decision.reason,
                    "capacity_usage": {
                        "inflight_before": snapshot_before.get("INFLIGHT_CURRENT", 0),
                        "inflight_after": state.snapshot().get("INFLIGHT_CURRENT", 0),
                        "threshold": snapshot_before.get("MAX_INFLIGHT_CAP", MAX_INFLIGHT_CAP),
                    },
                }
            )
            _CURRENT_ADMISSION_STATE.reset(state_ctx_token)