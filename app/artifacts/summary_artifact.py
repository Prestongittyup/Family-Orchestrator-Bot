from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from typing import Any, Mapping


SummaryArtifact = dict[str, Any]


def _safe_count(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    return 0


def _normalized_load(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}

    normalized: dict[str, int] = {}
    for key, raw_count in value.items():
        person = str(key)
        try:
            count = int(raw_count)
        except (TypeError, ValueError):
            count = 0
        normalized[person] = count
    return {person: normalized[person] for person in sorted(normalized)}


def _max_load(load_by_person: Mapping[str, int]) -> tuple[str | None, int]:
    if not load_by_person:
        return None, 0

    ordered = sorted(load_by_person.items(), key=lambda item: (-int(item[1]), str(item[0])))
    max_person, max_value = ordered[0]
    return str(max_person), int(max_value)


def build_summary(
    today_view: dict[str, Any],
    conflicts: dict[str, Any],
    upcoming: dict[str, Any],
    overdue: dict[str, Any],
) -> SummaryArtifact:
    _ = upcoming

    tasks_today = _safe_count(today_view.get("tasks_due_today"))
    scheduled_today = _safe_count(today_view.get("scheduled_items"))
    overdue_count = _safe_count(overdue.get("overdue_items"))
    conflict_count = _safe_count(conflicts.get("conflicts"))
    unassigned_count = _safe_count(today_view.get("unassigned_items"))

    load_by_person = _normalized_load(today_view.get("load_by_person"))
    max_load_person, max_load_value = _max_load(load_by_person)

    has_overdue = overdue_count > 0
    has_conflicts = conflict_count > 0
    has_unassigned = unassigned_count > 0

    overloaded_threshold = 5
    if has_overdue:
        status = "at_risk"
    elif has_conflicts:
        status = "at_risk"
    elif max_load_value >= overloaded_threshold:
        status = "overloaded"
    else:
        status = "stable"

    return {
        "household_id": str(today_view.get("household_id") or ""),
        "date": str(today_view.get("date") or ""),
        "status": status,
        "signals": {
            "tasks_today": tasks_today,
            "scheduled_today": scheduled_today,
            "overdue_count": overdue_count,
            "conflict_count": conflict_count,
            "unassigned_count": unassigned_count,
        },
        "load": {
            "by_person": load_by_person,
            "max_load_person": max_load_person,
            "max_load_value": max_load_value,
        },
        "flags": {
            "has_overdue": has_overdue,
            "has_conflicts": has_conflicts,
            "has_unassigned": has_unassigned,
        },
    }
