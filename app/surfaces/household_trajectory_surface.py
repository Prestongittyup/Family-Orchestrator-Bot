from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from datetime import date as date_value
from typing import Any, Mapping, Sequence


HouseholdTrajectorySurface = dict[str, Any]


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _coerce_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


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


def _direction_from_drift(value: float) -> str:
    rounded = round(value, 4)
    if rounded < 0:
        return "improving"
    if rounded > 0:
        return "degrading"
    return "stable"


def _ordered_feedback_rows(feedback_surfaces: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    indexed = list(enumerate(feedback_surfaces))
    rows = [
        row
        for _index, row in indexed
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


def build_household_trajectory_surface(
    feedback_surfaces: Sequence[Mapping[str, Any]],
) -> HouseholdTrajectorySurface:
    ordered_rows = _ordered_feedback_rows(feedback_surfaces)

    household_id = ""
    if ordered_rows:
        household_id = str(ordered_rows[0].get("household_id") or "").strip()

    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in ordered_rows:
        row_date = _normalized_date(row.get("date"))
        if not row_date:
            continue
        grouped.setdefault(row_date, []).append(row)

    ordered_dates = sorted(grouped.keys())

    trajectory: list[dict[str, Any]] = []
    day_drift_values: list[float] = []

    for row_date in ordered_dates:
        day_rows = grouped.get(row_date, [])
        total_decisions = len(day_rows)

        fulfillment_count = 0
        executed_count = 0
        execution_success_count = 0
        day_drift_delta = 0.0

        for row in day_rows:
            feedback = _as_mapping(row.get("feedback"))
            execution = _as_mapping(row.get("execution"))
            outcome = _as_mapping(row.get("outcome"))
            drift_impact = _as_mapping(row.get("drift_impact"))

            if _coerce_bool(feedback.get("decision_fulfilled")):
                fulfillment_count += 1

            if _coerce_bool(execution.get("executed")):
                executed_count += 1

            if _coerce_bool(outcome.get("completed")):
                execution_success_count += 1

            day_drift_delta += _coerce_float(drift_impact.get("drift_delta"))

        decision_fulfillment_rate = 0.0
        if total_decisions > 0:
            decision_fulfillment_rate = round(
                fulfillment_count / float(total_decisions),
                4,
            )

        execution_success_rate = 0.0
        if executed_count > 0:
            execution_success_rate = round(
                execution_success_count / float(executed_count),
                4,
            )

        day_drift_delta = round(day_drift_delta, 4)
        day_drift_values.append(day_drift_delta)

        window_start = max(0, len(day_drift_values) - 3)
        rolling_three_day = round(sum(day_drift_values[window_start:]), 4)

        trajectory.append(
            {
                "date": row_date,
                "decision_fulfillment_rate": decision_fulfillment_rate,
                "execution_success_rate": execution_success_rate,
                "drift_delta": day_drift_delta,
                "state_direction": _direction_from_drift(rolling_three_day),
            }
        )

    if trajectory:
        start_date = str(trajectory[0].get("date") or "")
        end_date = str(trajectory[-1].get("date") or "")
    else:
        start_date = ""
        end_date = ""

    cumulative_drift = round(
        sum(_coerce_float(row.get("drift_delta")) for row in trajectory),
        4,
    )

    avg_fulfillment_rate = 0.0
    avg_execution_success_rate = 0.0
    if trajectory:
        avg_fulfillment_rate = round(
            sum(_coerce_float(row.get("decision_fulfillment_rate")) for row in trajectory)
            / float(len(trajectory)),
            4,
        )
        avg_execution_success_rate = round(
            sum(_coerce_float(row.get("execution_success_rate")) for row in trajectory)
            / float(len(trajectory)),
            4,
        )

    return {
        "household_id": household_id,
        "time_window": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "trajectory": trajectory,
        "aggregate": {
            "avg_fulfillment_rate": avg_fulfillment_rate,
            "avg_execution_success_rate": avg_execution_success_rate,
            "cumulative_drift": cumulative_drift,
            "overall_direction": _direction_from_drift(cumulative_drift),
        },
    }
