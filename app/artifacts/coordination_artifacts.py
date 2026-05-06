from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from datetime import UTC, datetime, timedelta
from typing import Any, Mapping


ConflictArtifact = dict[str, Any]
UpcomingArtifact = dict[str, Any]
OverdueArtifact = dict[str, Any]

DEFAULT_UPCOMING_DAYS = 3


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


def _to_iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _as_mapping_rows(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, Mapping)]
    if isinstance(value, Mapping):
        rows: list[Mapping[str, Any]] = []
        for _key, row in sorted(value.items(), key=lambda item: str(item[0])):
            if isinstance(row, Mapping):
                rows.append(row)
        return rows
    return []


def _task_is_completed(task: Mapping[str, Any]) -> bool:
    status = str(task.get("status") or "").strip().lower()
    if status == "completed":
        return True

    lifecycle_state = str(task.get("lifecycle_state") or "").strip().lower()
    if lifecycle_state == "completed":
        return True

    completed_at = str(task.get("completed_at") or "").strip()
    return bool(completed_at)


def _row_id(row: Mapping[str, Any]) -> str:
    for key in (
        "task_id",
        "schedule_id",
        "coordination_event_id",
        "event_id",
        "responsibility_id",
        "conflict_id",
    ):
        resolved = str(row.get(key) or "").strip()
        if resolved:
            return resolved
    return ""


def _normalize_conflict_row(row: Mapping[str, Any]) -> dict[str, str] | None:
    related_ids = [
        str(item).strip()
        for item in row.get("related_entity_ids") or []
        if str(item).strip()
    ]

    item_a_id = str(row.get("item_a_id") or row.get("first_item_id") or "").strip()
    item_b_id = str(row.get("item_b_id") or row.get("second_item_id") or "").strip()

    if not item_a_id and len(related_ids) >= 1:
        item_a_id = related_ids[0]
    if not item_b_id and len(related_ids) >= 2:
        item_b_id = related_ids[1]
    if not item_a_id or not item_b_id:
        return None

    start_at = str(row.get("start_at") or row.get("window_start") or "").strip()
    end_at = str(row.get("end_at") or row.get("window_end") or "").strip()

    ordered_ids = sorted((item_a_id, item_b_id))
    return {
        "item_a_id": ordered_ids[0],
        "item_b_id": ordered_ids[1],
        "start_at": start_at,
        "end_at": end_at,
        "type": "overlap",
    }


def _schedule_rows_for_overlap(projection: Mapping[str, Any]) -> list[tuple[datetime, datetime, str]]:
    rows = _as_mapping_rows(projection.get("schedule_list"))
    prepared: list[tuple[datetime, datetime, str]] = []

    for row in rows:
        status = str(row.get("status") or "").strip().lower()
        if status == "cancelled":
            continue

        start_at = _parse_datetime(row.get("start_at"))
        end_at = _parse_datetime(row.get("end_at"))
        item_id = _row_id(row)
        if start_at is None or end_at is None or not item_id:
            continue
        if end_at <= start_at:
            continue
        prepared.append((start_at, end_at, item_id))

    prepared.sort(key=lambda item: (_to_iso_utc(item[0]), _to_iso_utc(item[1]), item[2]))
    return prepared


def _detect_simple_overlaps(projection: Mapping[str, Any]) -> list[dict[str, str]]:
    schedule_rows = _schedule_rows_for_overlap(projection)
    overlaps: list[dict[str, str]] = []

    for index, (start_a, end_a, item_a_id) in enumerate(schedule_rows):
        for start_b, end_b, item_b_id in schedule_rows[index + 1 :]:
            if start_b >= end_a:
                break
            if start_a < end_b and start_b < end_a:
                pair = sorted((item_a_id, item_b_id))
                overlap_start = max(start_a, start_b)
                overlap_end = min(end_a, end_b)
                overlaps.append(
                    {
                        "item_a_id": pair[0],
                        "item_b_id": pair[1],
                        "start_at": _to_iso_utc(overlap_start),
                        "end_at": _to_iso_utc(overlap_end),
                        "type": "overlap",
                    }
                )

    return sorted(
        overlaps,
        key=lambda row: (row["start_at"], row["item_a_id"], row["item_b_id"]),
    )


def build_conflicts(projection: dict[str, Any]) -> ConflictArtifact:
    household_id = str(projection.get("household_id") or "")

    rows: list[Mapping[str, Any]] = []
    if projection.get("conflicts") is not None:
        rows = _as_mapping_rows(projection.get("conflicts"))
    if not rows and isinstance(projection.get("ConflictProjection"), Mapping):
        rows = _as_mapping_rows(projection.get("ConflictProjection", {}).get("open_conflicts"))

    normalized_conflicts: list[dict[str, str]] = []
    for row in rows:
        normalized = _normalize_conflict_row(row)
        if normalized is not None:
            normalized_conflicts.append(normalized)

    if normalized_conflicts:
        normalized_conflicts.sort(
            key=lambda row: (row["start_at"], row["item_a_id"], row["item_b_id"])
        )
        return {
            "household_id": household_id,
            "conflicts": normalized_conflicts,
        }

    return {
        "household_id": household_id,
        "conflicts": _detect_simple_overlaps(projection),
    }


def build_upcoming(
    projection: dict[str, Any],
    now: str,
    *,
    days: int = DEFAULT_UPCOMING_DAYS,
) -> UpcomingArtifact:
    now_dt = _parse_datetime(now)
    if now_dt is None:
        raise ValueError("now must be ISO-8601")

    resolved_days = max(1, int(days))
    window_start = now_dt
    window_end = now_dt + timedelta(days=resolved_days)

    schedule_rows = _as_mapping_rows(projection.get("schedule_list"))
    task_rows = _as_mapping_rows(projection.get("tasks_list"))

    upcoming_items: list[dict[str, Any]] = []

    for row in schedule_rows:
        status = str(row.get("status") or "").strip().lower()
        if status == "cancelled":
            continue

        start_at = _parse_datetime(row.get("start_at"))
        if start_at is None:
            continue
        if window_start <= start_at <= window_end:
            upcoming_items.append(dict(row))

    for row in task_rows:
        due_at = _parse_datetime(row.get("due_at") or row.get("due_date"))
        if due_at is None:
            continue
        if window_start <= due_at <= window_end:
            upcoming_items.append(dict(row))

    upcoming_items.sort(
        key=lambda row: (
            str(row.get("start_at") or row.get("due_at") or row.get("due_date") or ""),
            str(_row_id(row)),
        )
    )

    return {
        "household_id": str(projection.get("household_id") or ""),
        "window_start": _to_iso_utc(window_start),
        "window_end": _to_iso_utc(window_end),
        "upcoming_items": upcoming_items,
    }


def build_overdue(projection: dict[str, Any], now: str) -> OverdueArtifact:
    now_dt = _parse_datetime(now)
    if now_dt is None:
        raise ValueError("now must be ISO-8601")

    task_rows = _as_mapping_rows(projection.get("tasks_list"))
    overdue_items = [
        dict(task)
        for task in task_rows
        if (
            (due_at := _parse_datetime(task.get("due_at") or task.get("due_date"))) is not None
            and due_at < now_dt
            and not _task_is_completed(task)
        )
    ]

    overdue_items.sort(
        key=lambda row: (
            str(row.get("due_at") or row.get("due_date") or ""),
            str(_row_id(row)),
        )
    )

    return {
        "household_id": str(projection.get("household_id") or ""),
        "overdue_items": overdue_items,
    }
