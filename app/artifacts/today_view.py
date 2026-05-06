from __future__ import annotations
print("IMPORT TRACE:", __name__, flush=True)

from datetime import date as date_value
from datetime import datetime
from typing import Any, Mapping


TodayViewArtifact = dict[str, Any]

_ASSIGNEE_KEYS = (
    "owner_user_id",
    "assignee_user_id",
    "assignee_id",
    "assignee",
    "assigned_to",
    "assigned_member_id",
    "member_id",
)


def _extract_date(raw_value: Any) -> str | None:
    if raw_value is None:
        return None

    text = str(raw_value).strip()
    if not text:
        return None

    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        candidate = text[:10]
        try:
            return date_value.fromisoformat(candidate).isoformat()
        except ValueError:
            pass

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed.date().isoformat()


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


def _resolve_assignee(row: Mapping[str, Any]) -> tuple[str, bool]:
    for key in _ASSIGNEE_KEYS:
        if key in row:
            return str(row.get(key) or "").strip(), True
    return "", False


def _sorted_task_rows(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (dict(row) for row in rows),
        key=lambda row: (
            str(row.get("due_at") or row.get("due_date") or ""),
            str(row.get("created_at") or ""),
            str(row.get("task_id") or ""),
        ),
    )


def _sorted_schedule_rows(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (dict(row) for row in rows),
        key=lambda row: (
            str(row.get("start_at") or ""),
            str(row.get("created_at") or ""),
            str(row.get("schedule_id") or ""),
        ),
    )


def _sorted_conflicts(rows: list[Any]) -> list[Any]:
    if all(isinstance(row, Mapping) for row in rows):
        return sorted(
            (dict(row) for row in rows if isinstance(row, Mapping)),
            key=lambda row: (
                str(row.get("created_at") or ""),
                str(row.get("updated_at") or ""),
                str(row.get("conflict_id") or ""),
            ),
        )
    return sorted(rows, key=lambda item: str(item))


def _extract_conflicts(projection: Mapping[str, Any]) -> list[Any]:
    conflicts_value = projection.get("conflicts")
    if isinstance(conflicts_value, list):
        return _sorted_conflicts(list(conflicts_value))
    if isinstance(conflicts_value, Mapping):
        rows = [
            value
            for _key, value in sorted(conflicts_value.items(), key=lambda item: str(item[0]))
        ]
        return _sorted_conflicts(rows)

    conflict_projection = projection.get("ConflictProjection")
    if isinstance(conflict_projection, Mapping):
        open_conflicts = conflict_projection.get("open_conflicts")
        if isinstance(open_conflicts, list):
            return _sorted_conflicts(list(open_conflicts))

    today_projection = projection.get("TodayViewProjection")
    if isinstance(today_projection, Mapping):
        open_conflict_ids = today_projection.get("open_conflict_ids")
        if isinstance(open_conflict_ids, list):
            return sorted(
                str(conflict_id).strip()
                for conflict_id in open_conflict_ids
                if str(conflict_id).strip()
            )

    return []


def _sorted_unassigned_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("due_at") or row.get("start_at") or row.get("created_at") or ""),
            str(row.get("task_id") or row.get("schedule_id") or ""),
        ),
    )


def build_today_view(projection: dict[str, Any], date: str) -> TodayViewArtifact:
    target_date = date_value.fromisoformat(str(date).strip()).isoformat()

    task_rows = _as_mapping_rows(projection.get("tasks_list"))
    schedule_rows = _as_mapping_rows(projection.get("schedule_list"))

    tasks_due_today = _sorted_task_rows(
        [
            task
            for task in task_rows
            if _extract_date(task.get("due_at") or task.get("due_date")) == target_date
        ]
    )

    scheduled_items = _sorted_schedule_rows(
        [
            schedule
            for schedule in schedule_rows
            if _extract_date(schedule.get("start_at") or schedule.get("date")) == target_date
        ]
    )

    overdue_items = _sorted_task_rows(
        [
            task
            for task in task_rows
            if (
                (due_date := _extract_date(task.get("due_at") or task.get("due_date"))) is not None
                and due_date < target_date
                and not _task_is_completed(task)
            )
        ]
    )

    conflicts = _extract_conflicts(projection)

    unassigned_items: list[dict[str, Any]] = []
    for row in [*tasks_due_today, *scheduled_items]:
        assignee, has_assignee_field = _resolve_assignee(row)
        if has_assignee_field and not assignee:
            unassigned_items.append(dict(row))
    unassigned_items = _sorted_unassigned_rows(unassigned_items)

    load_counts: dict[str, int] = {}
    for row in [*tasks_due_today, *scheduled_items]:
        assignee, has_assignee_field = _resolve_assignee(row)
        if has_assignee_field and assignee:
            load_counts[assignee] = load_counts.get(assignee, 0) + 1
    load_by_person = {person: load_counts[person] for person in sorted(load_counts)}

    return {
        "household_id": str(projection.get("household_id") or ""),
        "date": target_date,
        "tasks_due_today": tasks_due_today,
        "scheduled_items": scheduled_items,
        "overdue_items": overdue_items,
        "conflicts": conflicts,
        "unassigned_items": unassigned_items,
        "load_by_person": load_by_person,
    }
