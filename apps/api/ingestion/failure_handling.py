"""
Ingestion validation failure tracing and quarantine.

This module is ingestion-adapter scoped and does not alter OS-1/OS-2 internals.
It captures structured failure metadata and persists quarantined inputs for triage.
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


_quarantine_lock = threading.Lock()
_QUARANTINE_DIR = Path("data") / "ingestion_quarantine"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_failure_trace(
    *,
    adapter: str,
    stage: str,
    error_type: str,
    error_message: str,
    source: str | None = None,
    event_type: str | None = None,
) -> dict[str, Any]:
    """Build a structured failure trace payload for API responses and logs."""
    trace_id = f"ing-trace-{uuid4().hex[:12]}"
    return {
        "trace_id": trace_id,
        "timestamp": _utc_now_iso(),
        "adapter": adapter,
        "stage": stage,
        "error": {
            "type": error_type,
            "message": error_message,
        },
        "source": source,
        "event_type": event_type,
    }


def quarantine_ingestion_payload(
    *,
    adapter: str,
    payload: dict[str, Any],
    trace: dict[str, Any],
) -> dict[str, Any]:
    """
    Persist a quarantined ingestion payload to JSONL for offline triage.

    Returns a lightweight quarantine reference included in error responses.
    """
    _QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    quarantine_id = f"ing-q-{uuid4().hex[:12]}"
    day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    quarantine_file = _QUARANTINE_DIR / f"{day_key}.jsonl"

    record = {
        "quarantine_id": quarantine_id,
        "created_at": _utc_now_iso(),
        "adapter": adapter,
        "trace": trace,
        "payload": payload,
    }

    with _quarantine_lock:
        with quarantine_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True))
            f.write("\n")

    return {
        "quarantine_id": quarantine_id,
        "path": str(quarantine_file).replace("\\", "/"),
    }
