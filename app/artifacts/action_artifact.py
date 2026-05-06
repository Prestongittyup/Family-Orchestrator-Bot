from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

import hashlib
from datetime import UTC, date as date_value, datetime
from typing import Any, Mapping


ActionArtifact = dict[str, Any]


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_date(value: Any) -> date_value | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date_value.fromisoformat(text)
    except ValueError:
        return None


def _to_utc_iso(timestamp: datetime) -> str:
    return timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _priority_id(item: Mapping[str, Any]) -> str:
    return str(item.get("id") or "").strip()


def _resolve_item_id(row: Mapping[str, Any]) -> str:
    for key in (
        "id",
        "task_id",
        "schedule_id",
        "coordination_event_id",
        "event_id",
        "conflict_id",
        "responsibility_id",
    ):
        resolved = str(row.get(key) or "").strip()
        if resolved:
            return resolved
    return ""


def _index_today_items(today: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    indexed: dict[str, Mapping[str, Any]] = {}
    rows: list[Mapping[str, Any]] = []

    tasks_due_today = today.get("tasks_due_today")
    if isinstance(tasks_due_today, list):
        rows.extend(row for row in tasks_due_today if isinstance(row, Mapping))

    scheduled_items = today.get("scheduled_items")
    if isinstance(scheduled_items, list):
        rows.extend(row for row in scheduled_items if isinstance(row, Mapping))

    for row in rows:
        item_id = _resolve_item_id(row)
        if item_id and item_id not in indexed:
            indexed[item_id] = row
    return indexed


def _anchor_now_iso(summary: Mapping[str, Any], today: Mapping[str, Any]) -> str:
    resolved_date = _parse_date(summary.get("date")) or _parse_date(today.get("date"))
    if resolved_date is None:
        return "1970-01-01T00:00:00Z"
    return f"{resolved_date.isoformat()}T00:00:00Z"


def _action_mapping(reason_code: str) -> tuple[str, str, str]:
    if reason_code == "OVERDUE":
        return ("escalate", "immediate", "OVERDUE_ESCALATION")
    if reason_code == "CONFLICT":
        return ("notify", "immediate", "CONFLICT_NOTIFICATION")
    if reason_code == "DUE_TODAY":
        return ("remind", "scheduled", "TODAY_REMINDER")
    if reason_code == "UPCOMING":
        return ("schedule", "delayed", "UPCOMING_SCHEDULE")
    raise ValueError(f"Unsupported priority reason_code: {reason_code}")


def _trigger_timestamp(
    *,
    priority_reason_code: str,
    priority_id: str,
    today_index: Mapping[str, Mapping[str, Any]],
    now_iso: str,
) -> str | None:
    if priority_reason_code in {"OVERDUE", "CONFLICT"}:
        return None

    row = today_index.get(priority_id)
    if not isinstance(row, Mapping):
        if priority_reason_code == "DUE_TODAY":
            return now_iso
        return None

    if priority_reason_code == "DUE_TODAY":
        due_at = _parse_datetime(row.get("due_at") or row.get("due_date"))
        if due_at is not None:
            return _to_utc_iso(due_at)
        return now_iso

    start_or_due = _parse_datetime(
        row.get("start_at") or row.get("due_at") or row.get("due_date")
    )
    if start_or_due is not None:
        return _to_utc_iso(start_or_due)
    return None


def _idempotency_key(household_id: str, priority_id: str, action_type: str) -> str:
    payload = f"{household_id}{priority_id}{action_type}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_action_artifact(
    priority: dict[str, Any],
    summary: dict[str, Any],
    today: dict[str, Any],
) -> ActionArtifact:
    household_id = str(
        summary.get("household_id")
        or today.get("household_id")
        or priority.get("household_id")
        or ""
    )
    resolved_date = str(
        summary.get("date")
        or today.get("date")
        or priority.get("date")
        or ""
    )
    now_iso = _anchor_now_iso(summary, today)

    today_index = _index_today_items(today)
    priority_items = priority.get("priority_items")

    ranked_rows: list[tuple[int, dict[str, Any]]] = []
    if isinstance(priority_items, list):
        for rank, item in enumerate(priority_items):
            if not isinstance(item, Mapping):
                continue

            priority_id = _priority_id(item)
            action_type, trigger_type, mapped_reason_code = _action_mapping(
                str(item.get("reason_code") or "").strip()
            )
            trigger_timestamp = _trigger_timestamp(
                priority_reason_code=str(item.get("reason_code") or "").strip(),
                priority_id=priority_id,
                today_index=today_index,
                now_iso=now_iso,
            )

            ranked_rows.append(
                (
                    rank,
                    {
                        "priority_id": priority_id,
                        "source": "priority",
                        "action_type": action_type,
                        "trigger": {
                            "type": trigger_type,
                            "timestamp": trigger_timestamp,
                        },
                        "constraints": {
                            "idempotency_key": _idempotency_key(
                                household_id,
                                priority_id,
                                action_type,
                            ),
                            "no_overlap": True,
                        },
                        "reason_code": mapped_reason_code,
                    },
                )
            )

    actions = [
        row
        for _rank, row in sorted(
            ranked_rows,
            key=lambda item: (item[0], str(item[1].get("priority_id") or "")),
        )
    ]

    return {
        "household_id": household_id,
        "date": resolved_date,
        "actions": actions,
    }