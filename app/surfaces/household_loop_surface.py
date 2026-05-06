from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from typing import Any, Mapping


HouseholdLoopSurface = dict[str, Any]


def _as_mapping_list(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, Mapping)]
    return []


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _is_completed(row: Mapping[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    if status == "completed":
        return True

    lifecycle_state = str(row.get("lifecycle_state") or "").strip().lower()
    if lifecycle_state == "completed":
        return True

    completed_at = str(row.get("completed_at") or "").strip()
    return bool(completed_at)


def _completion_metrics(today_view: Mapping[str, Any]) -> tuple[int, int, float]:
    tasks_due_today = _as_mapping_list(today_view.get("tasks_due_today"))
    scheduled_items = _as_mapping_list(today_view.get("scheduled_items"))
    total_count = len(tasks_due_today) + len(scheduled_items)

    completed_count = 0
    for row in tasks_due_today:
        if _is_completed(row):
            completed_count += 1
    for row in scheduled_items:
        if _is_completed(row):
            completed_count += 1

    if total_count <= 0:
        return completed_count, total_count, 0.0
    return completed_count, total_count, round(completed_count / float(total_count), 4)


def _priority_churn(priority_items: list[Mapping[str, Any]]) -> int:
    reasons = [str(item.get("reason_code") or "").strip() for item in priority_items]
    if len(reasons) <= 1:
        return 0

    churn = 0
    previous = reasons[0]
    for reason in reasons[1:]:
        if reason != previous:
            churn += 1
        previous = reason
    return churn


def _system_load_index(
    *,
    summary: Mapping[str, Any],
    priority_items: list[Mapping[str, Any]],
    actions: list[Mapping[str, Any]],
) -> float:
    signals = summary.get("signals") if isinstance(summary.get("signals"), Mapping) else {}
    tasks_today = _coerce_int(signals.get("tasks_today"))
    scheduled_today = _coerce_int(signals.get("scheduled_today"))
    overdue_count = _coerce_int(signals.get("overdue_count"))
    conflict_count = _coerce_int(signals.get("conflict_count"))

    denominator = max(1, tasks_today + scheduled_today)
    numerator = overdue_count + conflict_count + len(priority_items) + len(actions)
    return round(numerator / float(denominator), 4)


def build_household_loop_surface(
    today_view: Mapping[str, Any],
    conflicts: Mapping[str, Any],
    upcoming: Mapping[str, Any],
    overdue: Mapping[str, Any],
    summary: Mapping[str, Any],
    priority: Mapping[str, Any],
    actions: Mapping[str, Any],
    execution_plans: Mapping[str, Any] | None = None,
) -> HouseholdLoopSurface:
    del upcoming

    household_id = str(
        summary.get("household_id")
        or today_view.get("household_id")
        or actions.get("household_id")
        or ""
    )
    resolved_date = str(
        summary.get("date")
        or today_view.get("date")
        or actions.get("date")
        or ""
    )

    overdue_items = [dict(row) for row in _as_mapping_list(overdue.get("overdue_items"))]
    conflict_items = [dict(row) for row in _as_mapping_list(conflicts.get("conflicts"))]
    priority_items = [dict(row) for row in _as_mapping_list(priority.get("priority_items"))]
    pending_actions = [dict(row) for row in _as_mapping_list(actions.get("actions"))]

    plan_rows: list[dict[str, Any]] = []
    if isinstance(execution_plans, Mapping):
        plan_rows = [dict(row) for row in _as_mapping_list(execution_plans.get("execution_plans"))]

    completed_count, total_count, completion_ratio = _completion_metrics(today_view)

    signals = summary.get("signals") if isinstance(summary.get("signals"), Mapping) else {}
    summary_overdue = _coerce_int(signals.get("overdue_count"))
    summary_conflicts = _coerce_int(signals.get("conflict_count"))
    today_conflicts = len(today_view.get("conflicts")) if isinstance(today_view.get("conflicts"), list) else 0

    return {
        "household_id": household_id,
        "date": resolved_date,
        "attention": {
            "overdue": overdue_items,
            "conflicts": conflict_items,
            "priority": priority_items,
        },
        "actions": pending_actions,
        "execution_plans": plan_rows,
        "completion": {
            "completed_count": completed_count,
            "total_count": total_count,
            "completion_ratio": completion_ratio,
        },
        "drift": {
            "overdue_trend": summary_overdue - completed_count,
            "conflict_trend": summary_conflicts - today_conflicts,
            "priority_churn": _priority_churn(priority_items),
            "system_load_index": _system_load_index(
                summary=summary,
                priority_items=priority_items,
                actions=pending_actions,
            ),
        },
    }
