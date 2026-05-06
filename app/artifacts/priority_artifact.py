from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from datetime import UTC, date, datetime, timedelta
from typing import Any, Mapping


PriorityArtifact = dict[str, Any]


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    text = str(value).strip()
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


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


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


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _task_due_datetime(row: Mapping[str, Any]) -> datetime | None:
    return _parse_datetime(row.get("due_at") or row.get("due_date"))


def _upcoming_due_datetime(row: Mapping[str, Any]) -> datetime | None:
    return _parse_datetime(row.get("start_at") or row.get("due_at") or row.get("due_date"))


def build_priority_artifact(
    summary: dict[str, Any],
    overdue: dict[str, Any],
    conflicts: dict[str, Any],
    today: dict[str, Any],
    upcoming: dict[str, Any],
) -> PriorityArtifact:
    reference_date = _parse_date(summary.get("date")) or _parse_date(today.get("date"))
    now_dt = _parse_datetime(upcoming.get("window_start"))
    due_soon_cutoff = now_dt + timedelta(days=1) if now_dt is not None else None

    window_end_dt = _parse_datetime(upcoming.get("window_end"))
    window_days = 0
    if now_dt is not None and window_end_dt is not None and window_end_dt >= now_dt:
        window_days = max(1, (window_end_dt.date() - now_dt.date()).days)

    candidates: list[dict[str, Any]] = []

    overdue_items = overdue.get("overdue_items")
    if isinstance(overdue_items, list):
        for item in overdue_items:
            if not isinstance(item, Mapping):
                continue
            item_id = _resolve_item_id(item)
            if not item_id:
                continue

            days_overdue = 0
            due_dt = _task_due_datetime(item)
            if reference_date is not None and due_dt is not None:
                days_overdue = max(0, (reference_date - due_dt.date()).days)

            score = float(100 + (days_overdue * 10))
            candidates.append(
                {
                    "id": item_id,
                    "source": "overdue",
                    "score": score,
                    "reason_code": "OVERDUE",
                }
            )

    conflict_items = conflicts.get("conflicts")
    if isinstance(conflict_items, list):
        summary_signals = summary.get("signals") if isinstance(summary.get("signals"), Mapping) else {}
        summary_conflict_count = _coerce_int(summary_signals.get("conflict_count"))

        for item in conflict_items:
            if not isinstance(item, Mapping):
                continue

            item_id = _resolve_item_id(item)
            if not item_id:
                item_a_id = str(item.get("item_a_id") or "").strip()
                item_b_id = str(item.get("item_b_id") or "").strip()
                if item_a_id or item_b_id:
                    ordered = sorted(part for part in (item_a_id, item_b_id) if part)
                    item_id = "|".join(ordered)
            if not item_id:
                continue

            conflict_severity_count = _coerce_int(item.get("conflict_severity_count"))
            if conflict_severity_count == 0:
                conflict_severity_count = _coerce_int(item.get("severity_count"))
            if conflict_severity_count == 0:
                conflict_severity_count = _coerce_int(item.get("severity"))
            if conflict_severity_count == 0:
                conflict_severity_count = summary_conflict_count

            score = float(80 + conflict_severity_count)
            candidates.append(
                {
                    "id": item_id,
                    "source": "conflict",
                    "score": score,
                    "reason_code": "CONFLICT",
                }
            )

    today_tasks = today.get("tasks_due_today")
    if isinstance(today_tasks, list):
        for item in today_tasks:
            if not isinstance(item, Mapping):
                continue

            item_id = _resolve_item_id(item)
            if not item_id:
                continue

            due_soon = False
            due_dt = _task_due_datetime(item)
            if due_dt is not None and due_soon_cutoff is not None and due_dt <= due_soon_cutoff:
                due_soon = True

            score = float(50 + (10 if due_soon else 0))
            candidates.append(
                {
                    "id": item_id,
                    "source": "today",
                    "score": score,
                    "reason_code": "DUE_TODAY",
                }
            )

    upcoming_items = upcoming.get("upcoming_items")
    if isinstance(upcoming_items, list):
        for item in upcoming_items:
            if not isinstance(item, Mapping):
                continue

            item_id = _resolve_item_id(item)
            if not item_id:
                continue

            inverse_days_until_due = 0
            due_dt = _upcoming_due_datetime(item)
            if due_dt is not None and now_dt is not None:
                days_until_due = max(0, (due_dt.date() - now_dt.date()).days)
                inverse_days_until_due = max(0, window_days - days_until_due)

            score = float(20 + inverse_days_until_due)
            candidates.append(
                {
                    "id": item_id,
                    "source": "upcoming",
                    "score": score,
                    "reason_code": "UPCOMING",
                }
            )

    priority_items = sorted(
        candidates,
        key=lambda item: (-float(item.get("score") or 0.0), str(item.get("id") or "")),
    )

    return {
        "household_id": str(summary.get("household_id") or today.get("household_id") or ""),
        "date": str(summary.get("date") or today.get("date") or ""),
        "priority_items": priority_items,
    }
