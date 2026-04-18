from __future__ import annotations

import threading
from typing import Any

from apps.api.ingestion import ingest_webhook
from apps.api.integration_core.feature_flags import (
    INTEGRATION_CORE_INGESTION_ENABLED,
    is_enabled,
)
from apps.api.integration_core.normalization import ExternalEvent


class _IdempotencyStore:
    """Thread-safe in-memory set of processed ExternalEvent.event_id values."""

    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._lock = threading.Lock()

    def is_seen(self, event_id: str) -> bool:
        with self._lock:
            return event_id in self._seen

    def mark_seen(self, event_id: str) -> None:
        with self._lock:
            self._seen.add(event_id)

    def clear(self) -> None:
        """Reset all tracked event IDs. Intended for testing only."""
        with self._lock:
            self._seen.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._seen)


# Module-level singleton — shared across all calls unless explicitly replaced in tests.
_idempotency_store = _IdempotencyStore()


def get_idempotency_store() -> _IdempotencyStore:
    """Return the active idempotency store (injectable for testing)."""
    return _idempotency_store


def _external_event_to_os1_webhook_payload(user_id: str, event: ExternalEvent) -> dict[str, Any]:
    return {
        "source": f"integration_core:{event.provider_name}",
        "type": event.event_type,
        "timestamp": event.timestamp,
        "data": {
            "user_id": str(user_id),
            "external_event_id": event.event_id,
            "provider_name": event.provider_name,
            "payload": dict(event.payload),
        },
    }


def ingest_external_events(
    user_id: str,
    events: list[ExternalEvent],
    *,
    idempotency_store: _IdempotencyStore | None = None,
) -> dict[str, Any]:
    """
    Bridge Integration Core ExternalEvent objects into OS-1 ingestion entrypoint.

    - Gated by feature flag INTEGRATION_CORE_INGESTION_ENABLED (default: disabled)
    - Uses only public OS-1 ingestion API (`ingest_webhook`)
    - Performs format conversion without OS-1 schema changes
    - Deduplicates by ExternalEvent.event_id (idempotency layer)
    - Thread-safe via _IdempotencyStore lock

    Parameters
    ----------
    user_id:
        Owner of the events.
    events:
        Batch of ExternalEvent objects to ingest.
    idempotency_store:
        Override the module-level store. Pass a fresh ``_IdempotencyStore()``
        in tests to get a clean, isolated state.
    """
    uid = str(user_id)

    if not is_enabled(INTEGRATION_CORE_INGESTION_ENABLED):
        return {
            "user_id": uid,
            "total_events": len(events),
            "ingested_count": 0,
            "status": "disabled",
            "results": [
                {"external_event_id": e.event_id, "status": "disabled", "result": None}
                for e in events
            ],
        }

    store = idempotency_store if idempotency_store is not None else _idempotency_store
    uid = str(user_id)
    results: list[dict[str, Any]] = []

    for event in events:
        if store.is_seen(event.event_id):
            results.append(
                {
                    "external_event_id": event.event_id,
                    "status": "duplicate_ignored",
                    "result": None,
                }
            )
            continue

        payload = _external_event_to_os1_webhook_payload(uid, event)
        result = ingest_webhook(payload)
        store.mark_seen(event.event_id)
        results.append(
            {
                "external_event_id": event.event_id,
                "status": result.get("status", "unknown") if isinstance(result, dict) else "unknown",
                "result": result,
            }
        )

    ingested_count = sum(1 for row in results if row.get("status") not in {"duplicate_ignored"})
    return {
        "user_id": uid,
        "total_events": len(events),
        "ingested_count": ingested_count,
        "results": results,
    }
