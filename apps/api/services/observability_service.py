from __future__ import annotations

from typing import Any

from apps.api.services.event_log_service import get_event_logs


_EXTERNAL_INGESTION_SOURCES = {
    "email_ingestion",
    "calendar_api",
    "reminder_service",
    "webhook_generic",
}


def _is_external_source(source: str) -> bool:
    if source in _EXTERNAL_INGESTION_SOURCES:
        return True
    # Keep this permissive for new adapters without explicit updates.
    return "ingestion" in source or "webhook" in source


def build_brief_observability_snapshot(household_id: str, *, limit: int = 80) -> dict[str, Any]:
    """
    Build lightweight correlation metadata for /brief responses.

    Uses existing event logs and payload metadata only; no schema changes.
    """
    logs = get_event_logs(household_id, limit=limit)

    external_inputs: list[dict[str, Any]] = []
    linked_trace_ids: list[str] = []

    for log in logs:
        source = str(log.source or "")
        if not _is_external_source(source):
            continue

        payload = log.payload if isinstance(log.payload, dict) else {}
        trace = payload.get("_obs", {}).get("trace", {}) if isinstance(payload, dict) else {}
        trace_id = trace.get("trace_id")
        if isinstance(trace_id, str) and trace_id:
            linked_trace_ids.append(trace_id)

        external_inputs.append(
            {
                "event_log_id": log.id,
                "event_type": log.type,
                "source": source,
                "idempotency_key": log.idempotency_key,
                "trace_id": trace_id,
                "created_at": log.created_at.isoformat() if log.created_at is not None else None,
            }
        )

    # Keep stable ordering and dedupe trace ids while preserving order.
    deduped_trace_ids: list[str] = list(dict.fromkeys(linked_trace_ids))

    return {
        "household_id": household_id,
        "external_inputs_considered": len(external_inputs),
        "linked_trace_ids": deduped_trace_ids,
        "recent_external_inputs": external_inputs[:20],
    }
