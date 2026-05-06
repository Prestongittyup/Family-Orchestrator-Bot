from __future__ import annotations

from datetime import date as date_value, datetime
from typing import Any


def _require_non_empty(value: str, *, field_name: str) -> str:
    resolved = str(value or "").strip()
    if not resolved:
        raise ValueError(f"{field_name} is required")
    return resolved


def _timestamp_iso(value: datetime) -> str:
    return value.isoformat()


def handle_decision_complete(
    household_id: str,
    decision_id: str,
    actor_id: str,
    timestamp: datetime,
) -> list[dict[str, Any]]:
    return [
        {
            "event_type": "DecisionCompleted",
            "household_id": _require_non_empty(household_id, field_name="household_id"),
            "decision_id": _require_non_empty(decision_id, field_name="decision_id"),
            "actor_id": _require_non_empty(actor_id, field_name="actor_id"),
            "timestamp": _timestamp_iso(timestamp),
        }
    ]


def handle_decision_defer(
    household_id: str,
    decision_id: str,
    actor_id: str,
    timestamp: datetime,
    defer_to_date: date_value,
) -> list[dict[str, Any]]:
    return [
        {
            "event_type": "DecisionDeferred",
            "household_id": _require_non_empty(household_id, field_name="household_id"),
            "decision_id": _require_non_empty(decision_id, field_name="decision_id"),
            "actor_id": _require_non_empty(actor_id, field_name="actor_id"),
            "defer_to_date": defer_to_date.isoformat(),
            "timestamp": _timestamp_iso(timestamp),
        }
    ]


def handle_decision_ignore(
    household_id: str,
    decision_id: str,
    actor_id: str,
    timestamp: datetime,
) -> list[dict[str, Any]]:
    return [
        {
            "event_type": "DecisionIgnored",
            "household_id": _require_non_empty(household_id, field_name="household_id"),
            "decision_id": _require_non_empty(decision_id, field_name="decision_id"),
            "actor_id": _require_non_empty(actor_id, field_name="actor_id"),
            "timestamp": _timestamp_iso(timestamp),
        }
    ]