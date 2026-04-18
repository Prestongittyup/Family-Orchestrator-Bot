"""
event_adapter.py
----------------
Pure transformation adapter: Integration Core ExternalEvent → OS-1 ingestion payload dict.

Constraints:
  - No OS-1 imports
  - No OS-2 imports
  - No persistence
  - Deterministic: same input always produces the same output dict
"""
from __future__ import annotations

import json
from typing import Any

from apps.api.integration_core.normalization import ExternalEvent


def _canonical_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a round-trip-stable dict by serialising and deserialising with sorted keys."""
    return json.loads(json.dumps(payload, sort_keys=True, default=str))


def external_event_to_os1_payload(event: ExternalEvent) -> dict[str, Any]:
    """
    Convert a single ExternalEvent into an OS-1 ingestion-compatible dict.

    Preserved fields
    ----------------
    - event_id   → data.external_event_id
    - timestamp  → top-level timestamp (ISO-8601 string)
    - provider_name → source metadata (format: ``integration_core:<provider_name>``)

    The transformation is *deterministic*: identical ExternalEvent inputs always
    produce identical output dicts.
    """
    return {
        "source": f"integration_core:{event.provider_name}",
        "type": event.event_type,
        "timestamp": event.timestamp,
        "data": {
            "user_id": str(event.user_id),
            "external_event_id": event.event_id,
            "provider_name": event.provider_name,
            "payload": _canonical_payload(event.payload),
        },
    }


def adapt_external_events(events: list[ExternalEvent]) -> list[dict[str, Any]]:
    """
    Transform a batch of ExternalEvent objects into OS-1 ingestion payload dicts.

    Order mirrors the input list.  Each element is independently deterministic.
    """
    return [external_event_to_os1_payload(e) for e in events]
