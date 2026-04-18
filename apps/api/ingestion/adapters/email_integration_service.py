from __future__ import annotations

from dataclasses import asdict
from typing import Any

from apps.api.ingestion.adapters.email_provider_adapter import EmailProviderAdapter
from apps.api.ingestion.models import IngestionError
from apps.api.ingestion.service import ingest_email


def _ingest_parsed_message(parsed: dict[str, Any]) -> dict[str, Any]:
    try:
        result = ingest_email(
            email_id=str(parsed["email_id"]),
            sender=str(parsed["sender"]),
            recipient=str(parsed["recipient"]),
            subject=str(parsed["subject"]),
            body=str(parsed["body"]),
            received_at=str(parsed["received_at"]),
            provider=str(parsed["provider"]),
        )
        return {
            "status": "processed",
            "result": result,
        }
    except IngestionError as exc:
        return {
            "status": "failed",
            "error": {
                "message": exc.message,
                "detail": exc.detail,
                "status_code": exc.status_code,
            },
        }


def ingest_polled_email_messages(adapter: EmailProviderAdapter) -> dict[str, Any]:
    raw_messages = adapter.poll_messages()
    rows: list[dict[str, Any]] = []

    for index, raw in enumerate(raw_messages):
        parsed = adapter.parse_message(raw)
        outcome = _ingest_parsed_message(asdict(parsed))
        rows.append(
            {
                "index": index,
                "provider": adapter.provider_name,
                "raw": raw,
                "parsed": asdict(parsed),
                "outcome": outcome,
            }
        )

    return {
        "status": "ok",
        "provider": adapter.provider_name,
        "mode": "poll",
        "count": len(rows),
        "results": rows,
    }


def ingest_push_email_messages(
    adapter: Any,
    *,
    drain: bool = True,
) -> dict[str, Any]:
    if not hasattr(adapter, "drain_push_messages"):
        raise ValueError("Adapter does not support push-drain simulation")

    raw_messages = adapter.drain_push_messages() if drain else []
    rows: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_messages):
        parsed = adapter.parse_message(raw)
        outcome = _ingest_parsed_message(asdict(parsed))
        rows.append(
            {
                "index": index,
                "provider": adapter.provider_name,
                "raw": raw,
                "parsed": asdict(parsed),
                "outcome": outcome,
            }
        )

    return {
        "status": "ok",
        "provider": adapter.provider_name,
        "mode": "push",
        "count": len(rows),
        "results": rows,
    }
