from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from typing import Any, Mapping


HouseholdDecisionSurface = dict[str, Any]

_SOURCE_PRIORITY_ORDER = {
    "overdue": 1,
    "conflict": 2,
    "priority": 3,
    "action": 4,
    "execution": 5,
}

_DECISION_TYPE_BY_SOURCE = {
    "overdue": "ACTION",
    "conflict": "BLOCKER",
    "priority": "ACTION",
    "action": "ACTION",
    "execution": "LOAD_REDUCTION",
}


def _as_mapping_list(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, Mapping)]
    return []


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _coerce_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _candidate_id(source: str, row: Mapping[str, Any], source_index: int) -> str:
    for key in (
        "id",
        "task_id",
        "conflict_id",
        "priority_id",
        "action_id",
        "job_id",
        "schedule_id",
    ):
        resolved = str(row.get(key) or "").strip()
        if resolved:
            return resolved

    if source == "conflict":
        item_a_id = str(row.get("item_a_id") or "").strip()
        item_b_id = str(row.get("item_b_id") or "").strip()
        if item_a_id or item_b_id:
            ordered = sorted(part for part in (item_a_id, item_b_id) if part)
            if ordered:
                return "|".join(ordered)

    return f"{source}:{source_index:06d}"


def _priority_rank(row: Mapping[str, Any], source_index: int) -> int:
    try:
        resolved = int(row.get("priority_rank"))
    except (TypeError, ValueError):
        resolved = 0

    if resolved > 0:
        return resolved
    return source_index + 1


def _reason_code(source: str, row: Mapping[str, Any]) -> str:
    resolved = str(row.get("reason_code") or "").strip()
    if resolved:
        return resolved

    if source == "overdue":
        return "OVERDUE"
    if source == "conflict":
        return "CONFLICT"
    if source == "priority":
        return "PRIORITY"
    if source == "action":
        return str(row.get("action_type") or "ACTION").strip() or "ACTION"
    if source == "execution":
        return str(row.get("job_type") or "EXECUTION").strip() or "EXECUTION"
    return "UNKNOWN"


def _title(source: str, row: Mapping[str, Any], candidate_id: str) -> str:
    explicit_title = str(row.get("title") or "").strip()
    if explicit_title:
        return explicit_title

    if source == "conflict":
        item_a_id = str(row.get("item_a_id") or "").strip()
        item_b_id = str(row.get("item_b_id") or "").strip()
        if item_a_id or item_b_id:
            ordered = sorted(part for part in (item_a_id, item_b_id) if part)
            return f"conflict:{'|'.join(ordered)}"

    if source == "execution":
        job_type = str(row.get("job_type") or "").strip()
        if job_type:
            return f"execution:{job_type}:{candidate_id}"

    return f"{source}:{candidate_id}"


def _metadata(source: str, row: Mapping[str, Any], candidate_id: str) -> dict[str, str | None]:
    execution_plan_id: str | None = None
    action_id: str | None = None

    if source == "execution":
        execution_plan_id = str(row.get("job_id") or candidate_id).strip() or None
        action_id = str(row.get("action_id") or "").strip() or None
    elif source == "action":
        action_id = str(row.get("action_id") or row.get("priority_id") or candidate_id).strip() or None

    return {
        "execution_plan_id": execution_plan_id,
        "action_id": action_id,
    }


def _candidate_rows(loop_surface: Mapping[str, Any]) -> list[dict[str, Any]]:
    attention = _as_mapping(loop_surface.get("attention"))

    source_rows: list[tuple[str, list[Mapping[str, Any]]]] = [
        ("overdue", _as_mapping_list(attention.get("overdue"))),
        ("conflict", _as_mapping_list(attention.get("conflicts"))),
        ("priority", _as_mapping_list(attention.get("priority"))),
        ("action", _as_mapping_list(loop_surface.get("actions"))),
        ("execution", _as_mapping_list(loop_surface.get("execution_plans"))),
    ]

    candidates: list[dict[str, Any]] = []
    for source, rows in source_rows:
        for source_index, row in enumerate(rows):
            candidate_id = _candidate_id(source, row, source_index)
            candidates.append(
                {
                    "source": source,
                    "source_priority": int(_SOURCE_PRIORITY_ORDER[source]),
                    "source_index": source_index,
                    "candidate_id": candidate_id,
                    "priority_rank": _priority_rank(row, source_index),
                    "reason_code": _reason_code(source, row),
                    "title": _title(source, row, candidate_id),
                    "metadata": _metadata(source, row, candidate_id),
                }
            )
    return candidates


def _selected_candidate(loop_surface: Mapping[str, Any]) -> dict[str, Any]:
    candidates = _candidate_rows(loop_surface)
    if not candidates:
        return {
            "source": "execution",
            "source_priority": int(_SOURCE_PRIORITY_ORDER["execution"]),
            "source_index": 0,
            "candidate_id": "execution:000000",
            "priority_rank": 1,
            "reason_code": "NO_CANDIDATE",
            "title": "execution:execution:000000",
            "metadata": {
                "execution_plan_id": None,
                "action_id": None,
            },
        }

    ordered = sorted(
        candidates,
        key=lambda row: (
            int(row.get("source_priority") or 0),
            int(row.get("priority_rank") or 0),
            int(row.get("source_index") or 0),
            str(row.get("candidate_id") or ""),
        ),
    )
    return ordered[0]


def build_household_decision_surface(loop_surface: Mapping[str, Any]) -> HouseholdDecisionSurface:
    selected = _selected_candidate(loop_surface)

    attention = _as_mapping(loop_surface.get("attention"))
    drift = _as_mapping(loop_surface.get("drift"))

    overdue_rows = _as_mapping_list(attention.get("overdue"))
    conflict_rows = _as_mapping_list(attention.get("conflicts"))

    source = str(selected.get("source") or "execution")

    return {
        "household_id": str(loop_surface.get("household_id") or ""),
        "date": str(loop_surface.get("date") or ""),
        "decision": {
            "type": str(_DECISION_TYPE_BY_SOURCE.get(source, "LOAD_REDUCTION")),
            "id": str(selected.get("candidate_id") or ""),
            "title": str(selected.get("title") or ""),
            "source": source,
            "priority_rank": int(selected.get("priority_rank") or 0),
            "reason_code": str(selected.get("reason_code") or ""),
            "metadata": {
                "execution_plan_id": selected.get("metadata", {}).get("execution_plan_id"),
                "action_id": selected.get("metadata", {}).get("action_id"),
            },
        },
        "context": {
            "top_overdue_count": len(overdue_rows),
            "top_conflict_count": len(conflict_rows),
            "system_load_index": _coerce_float(drift.get("system_load_index")),
        },
    }
