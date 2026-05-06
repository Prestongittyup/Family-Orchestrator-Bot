from __future__ import annotations

import contextvars
import hashlib
from datetime import UTC, datetime
from typing import Any


_trace_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("trace_id", default=None)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def derive_trace_id(seed: str) -> str:
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    return f"trc-{digest}"


def set_current_trace_id(trace_id: str | None) -> None:
    _trace_id_var.set(trace_id)


def get_current_trace_id() -> str | None:
    return _trace_id_var.get()


def clear_current_trace_id() -> None:
    _trace_id_var.set(None)


def ensure_event_payload_trace(
    payload: dict[str, Any],
    *,
    idempotency_key: str,
    source: str,
    event_type: str,
    stage: str,
) -> str:
    """
    Ensure event payload contains a correlation block under _obs.trace.

    This only enriches payload metadata and does not alter schema tables.
    """
    trace_id = derive_trace_id(idempotency_key)
    obs = payload.setdefault("_obs", {})
    trace = obs.setdefault("trace", {})
    trace.setdefault("trace_id", trace_id)
    trace.setdefault("idempotency_key", idempotency_key)
    trace.setdefault("source", source)
    trace.setdefault("event_type", event_type)
    trace.setdefault("first_seen_at", utc_now_iso())
    trace["stage"] = stage
    return str(trace["trace_id"])
