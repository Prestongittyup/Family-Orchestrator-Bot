from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from datetime import date as date_value
from typing import Any, Mapping, Sequence


HouseholdInsightSurface = dict[str, Any]

DRIFT_SPIKE_HIGH_THRESHOLD = 1.0
DRIFT_SPIKE_MILD_THRESHOLD = 0.25
ATTENTION_PRESSURE_DENOMINATOR = 10.0
MAX_COMPRESSED_FOCUS_ITEMS = 3

_SEVERITY_ORDER = {
    "high": 0,
    "medium": 1,
    "low": 2,
}

_FOCUS_TYPE_ORDER = {
    "attention": 0,
    "execution": 1,
    "drift": 2,
    "completion": 3,
}

_ALLOWED_DIRECTIONS = frozenset({"improving", "stable", "degrading"})


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _as_mapping_list(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, Mapping)]
    return []


def _coerce_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def _normalized_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return date_value.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return ""


def _ordered_feedback_rows(feedback_surfaces: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    rows = [
        row
        for row in feedback_surfaces
        if isinstance(row, Mapping) and _normalized_date(row.get("date"))
    ]
    return sorted(
        rows,
        key=lambda row: (
            _normalized_date(row.get("date")),
            str(row.get("household_id") or ""),
            str(_as_mapping(row.get("decision")).get("id") or ""),
        ),
    )


def _ordered_trajectory_rows(trajectory_surface: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows = _as_mapping_list(trajectory_surface.get("trajectory"))
    return sorted(
        rows,
        key=lambda row: (
            _normalized_date(row.get("date")),
            str(row.get("state_direction") or ""),
            str(row.get("drift_delta") or ""),
        ),
    )


def _feedback_reference(row: Mapping[str, Any] | None) -> str | None:
    if not isinstance(row, Mapping):
        return None
    row_date = _normalized_date(row.get("date"))
    decision_id = str(_as_mapping(row.get("decision")).get("id") or "").strip()
    if row_date and decision_id:
        return f"{row_date}:{decision_id}"
    if row_date:
        return row_date
    if decision_id:
        return decision_id
    return None


def _severity_from_thresholds(*, overdue_count: int, failed_executions: int, drift_delta: float) -> str:
    if overdue_count > 3 or failed_executions > 1 or drift_delta > DRIFT_SPIKE_HIGH_THRESHOLD:
        return "high"
    if (1 <= overdue_count <= 3) or failed_executions == 1 or drift_delta > DRIFT_SPIKE_MILD_THRESHOLD:
        return "medium"
    return "low"


def _resolved_direction(
    trajectory_surface: Mapping[str, Any],
    trajectory_rows: Sequence[Mapping[str, Any]],
) -> str:
    aggregate = _as_mapping(trajectory_surface.get("aggregate"))
    aggregate_direction = str(aggregate.get("overall_direction") or "").strip().lower()
    if aggregate_direction in _ALLOWED_DIRECTIONS:
        return aggregate_direction

    if trajectory_rows:
        latest_direction = str(trajectory_rows[-1].get("state_direction") or "").strip().lower()
        if latest_direction in _ALLOWED_DIRECTIONS:
            return latest_direction

    return "stable"


def _target_trajectory_row(
    trajectory_rows: Sequence[Mapping[str, Any]],
    *,
    target_date: str,
) -> Mapping[str, Any]:
    for row in trajectory_rows:
        if _normalized_date(row.get("date")) == target_date:
            return row
    if trajectory_rows:
        return trajectory_rows[-1]
    return {}


def _trajectory_trend_changed(trajectory_rows: Sequence[Mapping[str, Any]]) -> bool:
    if len(trajectory_rows) < 2:
        return False

    previous_direction = str(trajectory_rows[-2].get("state_direction") or "").strip().lower()
    current_direction = str(trajectory_rows[-1].get("state_direction") or "").strip().lower()

    if previous_direction not in _ALLOWED_DIRECTIONS:
        return False
    if current_direction not in _ALLOWED_DIRECTIONS:
        return False
    return previous_direction != current_direction


def _execution_success(row: Mapping[str, Any]) -> bool:
    outcome = _as_mapping(row.get("outcome"))
    completed = _coerce_bool(outcome.get("completed"))
    failed = _coerce_bool(outcome.get("failed"))
    pending = _coerce_bool(outcome.get("pending"))
    return bool(completed and not failed and not pending)


def _focus_item(
    *,
    title: str,
    item_type: str,
    severity: str,
    trajectory_ref: str | None,
    feedback_ref: str | None,
    decision_ref: str | None,
    summary: str,
) -> dict[str, Any]:
    return {
        "title": title,
        "type": item_type,
        "severity": severity,
        "source": {
            "trajectory_ref": trajectory_ref,
            "feedback_ref": feedback_ref,
            "decision_ref": decision_ref,
        },
        "summary": summary,
    }


def _ordered_focus_items(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in sorted(
            items,
            key=lambda row: (
                _SEVERITY_ORDER.get(str(row.get("severity") or "").lower(), 99),
                _FOCUS_TYPE_ORDER.get(str(row.get("type") or "").lower(), 99),
                str(row.get("title") or ""),
                str(_as_mapping(row.get("source")).get("trajectory_ref") or ""),
                str(_as_mapping(row.get("source")).get("feedback_ref") or ""),
                str(_as_mapping(row.get("source")).get("decision_ref") or ""),
            ),
        )
    ]


def _compressed_summary(focus_items: Sequence[Mapping[str, Any]], *, date: str) -> str:
    if not focus_items:
        return f"{date}: no focus items."

    parts: list[str] = []
    for item in focus_items[:MAX_COMPRESSED_FOCUS_ITEMS]:
        severity = str(item.get("severity") or "low").strip().upper()
        item_type = str(item.get("type") or "attention").strip().lower()
        summary = str(item.get("summary") or "").strip()
        parts.append(f"{severity} {item_type}: {summary}")
    return " | ".join(parts)


def build_household_insight_surface(
    trajectory_surface: Mapping[str, Any],
    feedback_surfaces: Sequence[Mapping[str, Any]],
    decision_surface: Mapping[str, Any],
) -> HouseholdInsightSurface:
    trajectory_rows = _ordered_trajectory_rows(trajectory_surface)
    ordered_feedback = _ordered_feedback_rows(feedback_surfaces)

    decision = _as_mapping(decision_surface.get("decision"))
    decision_context = _as_mapping(decision_surface.get("context"))

    household_id = str(
        decision_surface.get("household_id")
        or trajectory_surface.get("household_id")
        or (ordered_feedback[0].get("household_id") if ordered_feedback else "")
        or ""
    ).strip()

    resolved_date = _normalized_date(
        decision_surface.get("date")
        or _as_mapping(trajectory_surface.get("time_window")).get("end_date")
        or (trajectory_rows[-1].get("date") if trajectory_rows else "")
        or (ordered_feedback[-1].get("date") if ordered_feedback else "")
    )

    target_trajectory_row = _target_trajectory_row(trajectory_rows, target_date=resolved_date)
    target_feedback_rows = [
        row
        for row in ordered_feedback
        if _normalized_date(row.get("date")) == resolved_date
    ]

    decision_id = str(decision.get("id") or "").strip() or None
    decision_source = str(decision.get("source") or "").strip().lower()

    overdue_count = _coerce_int(decision_context.get("top_overdue_count"))
    conflict_count = _coerce_int(decision_context.get("top_conflict_count"))
    priority_count = 1 if decision_source == "priority" else 0

    failed_executions = 0
    pending_executions = 0
    completed_executions = 0
    executed_total = 0
    executed_success = 0

    for row in ordered_feedback:
        execution = _as_mapping(row.get("execution"))
        outcome = _as_mapping(row.get("outcome"))

        is_executed = _coerce_bool(execution.get("executed"))
        if is_executed:
            executed_total += 1

        if _execution_success(row):
            if is_executed:
                executed_success += 1

        if (
            _coerce_bool(outcome.get("completed"))
            and not _coerce_bool(outcome.get("failed"))
            and not _coerce_bool(outcome.get("pending"))
        ):
            completed_executions += 1

        if _coerce_bool(outcome.get("failed")):
            failed_executions += 1

        if _coerce_bool(outcome.get("pending")):
            pending_executions += 1

    drift_signal = round(_coerce_float(target_trajectory_row.get("drift_delta")), 4)
    focus_items: list[dict[str, Any]] = []

    if overdue_count > 0:
        focus_items.append(
            _focus_item(
                title="Overdue attention",
                item_type="attention",
                severity=_severity_from_thresholds(
                    overdue_count=overdue_count,
                    failed_executions=failed_executions,
                    drift_delta=drift_signal,
                ),
                trajectory_ref=None,
                feedback_ref=None,
                decision_ref=decision_id,
                summary=f"{overdue_count} overdue item(s) currently in focus.",
            )
        )

    if conflict_count > 0:
        focus_items.append(
            _focus_item(
                title="Conflict attention",
                item_type="attention",
                severity=_severity_from_thresholds(
                    overdue_count=overdue_count,
                    failed_executions=failed_executions,
                    drift_delta=drift_signal,
                ),
                trajectory_ref=None,
                feedback_ref=None,
                decision_ref=decision_id,
                summary=f"{conflict_count} conflict item(s) currently in focus.",
            )
        )

    if priority_count > 0:
        focus_items.append(
            _focus_item(
                title="Priority attention",
                item_type="attention",
                severity=_severity_from_thresholds(
                    overdue_count=overdue_count,
                    failed_executions=failed_executions,
                    drift_delta=drift_signal,
                ),
                trajectory_ref=None,
                feedback_ref=None,
                decision_ref=decision_id,
                summary=f"{priority_count} priority item(s) currently in focus.",
            )
        )

    feedback_ref = _feedback_reference(
        target_feedback_rows[0] if target_feedback_rows else (ordered_feedback[-1] if ordered_feedback else None)
    )

    if failed_executions > 0:
        focus_items.append(
            _focus_item(
                title="Execution failures",
                item_type="execution",
                severity=_severity_from_thresholds(
                    overdue_count=overdue_count,
                    failed_executions=failed_executions,
                    drift_delta=drift_signal,
                ),
                trajectory_ref=None,
                feedback_ref=feedback_ref,
                decision_ref=decision_id,
                summary=f"{failed_executions} failed execution(s) in feedback window.",
            )
        )

    if pending_executions > 0:
        focus_items.append(
            _focus_item(
                title="Execution pending",
                item_type="execution",
                severity=_severity_from_thresholds(
                    overdue_count=overdue_count,
                    failed_executions=failed_executions,
                    drift_delta=drift_signal,
                ),
                trajectory_ref=None,
                feedback_ref=feedback_ref,
                decision_ref=decision_id,
                summary=f"{pending_executions} execution(s) still pending.",
            )
        )

    if completed_executions > 0:
        focus_items.append(
            _focus_item(
                title="Execution completed",
                item_type="execution",
                severity=_severity_from_thresholds(
                    overdue_count=0,
                    failed_executions=0,
                    drift_delta=0.0,
                ),
                trajectory_ref=None,
                feedback_ref=feedback_ref,
                decision_ref=decision_id,
                summary=f"{completed_executions} completed execution(s) confirmed.",
            )
        )

    if drift_signal > DRIFT_SPIKE_MILD_THRESHOLD:
        focus_items.append(
            _focus_item(
                title="Drift increase",
                item_type="drift",
                severity=_severity_from_thresholds(
                    overdue_count=0,
                    failed_executions=failed_executions,
                    drift_delta=drift_signal,
                ),
                trajectory_ref=resolved_date or None,
                feedback_ref=None,
                decision_ref=None,
                summary=f"Drift increased by {drift_signal:.4f} on {resolved_date}.",
            )
        )

    if _trajectory_trend_changed(trajectory_rows):
        previous_direction = str(trajectory_rows[-2].get("state_direction") or "stable").strip().lower()
        current_direction = str(trajectory_rows[-1].get("state_direction") or "stable").strip().lower()
        focus_items.append(
            _focus_item(
                title="Drift direction change",
                item_type="drift",
                severity=_severity_from_thresholds(
                    overdue_count=0,
                    failed_executions=failed_executions,
                    drift_delta=drift_signal,
                ),
                trajectory_ref=resolved_date or None,
                feedback_ref=None,
                decision_ref=None,
                summary=(
                    "Cumulative drift direction changed "
                    f"from {previous_direction} to {current_direction}."
                ),
            )
        )

    completion_rows = target_feedback_rows or (ordered_feedback[-1:] if ordered_feedback else [])
    unfulfilled_count = 0
    unsuccessful_execution_count = 0
    for row in completion_rows:
        feedback = _as_mapping(row.get("feedback"))
        if not _coerce_bool(feedback.get("decision_fulfilled")):
            unfulfilled_count += 1
        if not _execution_success(row):
            unsuccessful_execution_count += 1

    if unfulfilled_count > 0 or unsuccessful_execution_count > 0:
        focus_items.append(
            _focus_item(
                title="Completion gap",
                item_type="completion",
                severity=_severity_from_thresholds(
                    overdue_count=overdue_count,
                    failed_executions=failed_executions,
                    drift_delta=drift_signal,
                ),
                trajectory_ref=None,
                feedback_ref=feedback_ref,
                decision_ref=decision_id,
                summary=(
                    f"{unfulfilled_count} unfulfilled decision(s) and "
                    f"{unsuccessful_execution_count} unsuccessful execution(s)."
                ),
            )
        )

    ordered_focus_items = _ordered_focus_items(focus_items)

    execution_health = round(
        executed_success / float(max(1, executed_total)),
        4,
    )

    attention_pressure = round(
        (overdue_count + conflict_count + priority_count)
        / ATTENTION_PRESSURE_DENOMINATOR,
        4,
    )

    direction = _resolved_direction(trajectory_surface, trajectory_rows)

    return {
        "household_id": household_id,
        "date": resolved_date,
        "focus_items": ordered_focus_items,
        "daily_state": {
            "direction": direction,
            "drift_signal": drift_signal,
            "execution_health": execution_health,
            "attention_pressure": attention_pressure,
        },
        "compressed_summary": _compressed_summary(
            ordered_focus_items,
            date=resolved_date,
        ),
    }