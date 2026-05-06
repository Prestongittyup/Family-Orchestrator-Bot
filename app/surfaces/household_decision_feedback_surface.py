from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from datetime import UTC, date as date_value, datetime
from typing import Any, Mapping


HouseholdDecisionFeedbackSurface = dict[str, Any]


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


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed_date = date_value.fromisoformat(text[:10])
        except ValueError:
            return None
        parsed = datetime(
            parsed_date.year,
            parsed_date.month,
            parsed_date.day,
            tzinfo=UTC,
        )

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _scope_match(artifact: Mapping[str, Any], *, household_id: str, date: str) -> bool:
    artifact_household_id = str(artifact.get("household_id") or "").strip()
    artifact_date = str(artifact.get("date") or "").strip()

    if artifact_household_id and artifact_household_id != household_id:
        return False
    if artifact_date and artifact_date != date:
        return False
    return True


def _decision_refs(decision_surface: Mapping[str, Any]) -> tuple[str | None, str | None]:
    decision = _as_mapping(decision_surface.get("decision"))
    metadata = _as_mapping(decision.get("metadata"))

    execution_plan_id = str(metadata.get("execution_plan_id") or "").strip() or None
    action_id = str(metadata.get("action_id") or "").strip() or None
    return execution_plan_id, action_id


def _action_row_id(row: Mapping[str, Any]) -> str:
    return str(
        row.get("action_id")
        or row.get("priority_id")
        or row.get("id")
        or ""
    ).strip()


def _execution_row_plan_id(row: Mapping[str, Any]) -> str:
    return str(row.get("job_id") or row.get("execution_plan_id") or row.get("id") or "").strip()


def _execution_row_action_id(row: Mapping[str, Any]) -> str:
    return str(row.get("action_id") or "").strip()


def _matched_execution_row(
    execution_rows: list[Mapping[str, Any]],
    *,
    decision_execution_plan_id: str | None,
    decision_action_id: str | None,
) -> Mapping[str, Any] | None:
    indexed_rows = list(enumerate(execution_rows))
    matched: list[tuple[int, str, str, Mapping[str, Any]]] = []

    for source_index, row in indexed_rows:
        execution_plan_id = _execution_row_plan_id(row)
        action_id = _execution_row_action_id(row)

        matches_execution_plan_id = bool(
            decision_execution_plan_id and execution_plan_id == decision_execution_plan_id
        )
        matches_action_id = bool(decision_action_id and action_id == decision_action_id)

        if matches_execution_plan_id or matches_action_id:
            matched.append((source_index, execution_plan_id, action_id, row))

    if not matched:
        return None

    matched.sort(key=lambda item: (item[0], item[1], item[2]))
    return matched[0][3]


def _matched_action_row(
    action_rows: list[Mapping[str, Any]],
    *,
    decision_action_id: str | None,
    execution_action_id: str | None,
) -> Mapping[str, Any] | None:
    indexed_rows = list(enumerate(action_rows))
    matched: list[tuple[int, str, Mapping[str, Any]]] = []

    for source_index, row in indexed_rows:
        action_id = _action_row_id(row)
        if not action_id:
            continue

        matches_decision_action = bool(decision_action_id and action_id == decision_action_id)
        matches_execution_action = bool(execution_action_id and action_id == execution_action_id)

        if matches_decision_action or matches_execution_action:
            matched.append((source_index, action_id, row))

    if not matched:
        return None

    matched.sort(key=lambda item: (item[0], item[1]))
    return matched[0][2]


def _status_tokens(*rows: Mapping[str, Any] | None) -> list[str]:
    tokens: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        for key in ("status", "execution_status", "state", "result", "outcome"):
            value = str(row.get(key) or "").strip().upper()
            if value:
                tokens.append(value)
    return tokens


def _has_completed_signal(*rows: Mapping[str, Any] | None) -> bool:
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if row.get("completed") is True:
            return True
        if str(row.get("completed_at") or "").strip():
            return True
    return False


def _has_failed_signal(*rows: Mapping[str, Any] | None) -> bool:
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if row.get("failed") is True:
            return True
        if str(row.get("failed_at") or "").strip():
            return True
    return False


def _resolve_outcome(*, execution_row: Mapping[str, Any] | None, action_row: Mapping[str, Any] | None) -> dict[str, bool]:
    status_tokens = _status_tokens(execution_row, action_row)
    has_failed_signal = _has_failed_signal(execution_row, action_row)
    has_completed_signal = _has_completed_signal(execution_row, action_row)

    if "FAILED" in status_tokens or has_failed_signal:
        return {
            "executed": True,
            "completed": False,
            "failed": True,
            "pending": False,
        }

    if "SUCCESS" in status_tokens or has_completed_signal:
        return {
            "executed": True,
            "completed": True,
            "failed": False,
            "pending": False,
        }

    if "PENDING" in status_tokens:
        return {
            "executed": False,
            "completed": False,
            "failed": False,
            "pending": True,
        }

    return {
        "executed": False,
        "completed": False,
        "failed": False,
        "pending": True,
    }


def _decision_time_anchor(decision_surface: Mapping[str, Any], household_id: str, date: str) -> datetime:
    del household_id
    decision_date = str(decision_surface.get("date") or date).strip() or date
    parsed = _parse_datetime(f"{decision_date}T00:00:00Z")
    if parsed is None:
        return datetime(1970, 1, 1, tzinfo=UTC)
    return parsed


def _first_observed_timestamp(execution_row: Mapping[str, Any] | None, action_row: Mapping[str, Any] | None) -> datetime | None:
    candidates: list[Any] = []

    if isinstance(execution_row, Mapping):
        payload = _as_mapping(execution_row.get("payload"))
        candidates.extend(
            [
                payload.get("timestamp"),
                execution_row.get("executed_at"),
                execution_row.get("completed_at"),
                execution_row.get("updated_at"),
            ]
        )

    if isinstance(action_row, Mapping):
        trigger = _as_mapping(action_row.get("trigger"))
        candidates.extend(
            [
                trigger.get("timestamp"),
                action_row.get("completed_at"),
                action_row.get("updated_at"),
            ]
        )

    for candidate in candidates:
        parsed = _parse_datetime(candidate)
        if parsed is not None:
            return parsed
    return None


def build_household_decision_feedback_surface(
    decision_surface: Mapping[str, Any],
    execution_plans: Mapping[str, Any],
    actions: Mapping[str, Any],
    pre_loop_surface: Mapping[str, Any],
    post_loop_surface: Mapping[str, Any],
) -> HouseholdDecisionFeedbackSurface:
    household_id = str(
        decision_surface.get("household_id")
        or pre_loop_surface.get("household_id")
        or ""
    ).strip()
    date = str(
        decision_surface.get("date")
        or pre_loop_surface.get("date")
        or ""
    ).strip()

    decision = _as_mapping(decision_surface.get("decision"))
    decision_id = str(decision.get("id") or "").strip()
    decision_type = str(decision.get("type") or "").strip()

    decision_execution_plan_id, decision_action_id = _decision_refs(decision_surface)

    execution_rows: list[Mapping[str, Any]] = []
    if _scope_match(execution_plans, household_id=household_id, date=date):
        execution_rows = _as_mapping_list(execution_plans.get("execution_plans"))

    action_rows: list[Mapping[str, Any]] = []
    if _scope_match(actions, household_id=household_id, date=date):
        action_rows = _as_mapping_list(actions.get("actions"))

    matched_execution = _matched_execution_row(
        execution_rows,
        decision_execution_plan_id=decision_execution_plan_id,
        decision_action_id=decision_action_id,
    )
    matched_execution_plan_id = (
        _execution_row_plan_id(matched_execution) if isinstance(matched_execution, Mapping) else ""
    )
    matched_execution_action_id = (
        _execution_row_action_id(matched_execution) if isinstance(matched_execution, Mapping) else ""
    )

    matched_action = _matched_action_row(
        action_rows,
        decision_action_id=decision_action_id,
        execution_action_id=matched_execution_action_id or None,
    )
    matched_action_id = _action_row_id(matched_action) if isinstance(matched_action, Mapping) else ""

    outcome = _resolve_outcome(execution_row=matched_execution, action_row=matched_action)

    decision_anchor = _decision_time_anchor(decision_surface, household_id, date)
    observed_timestamp = _first_observed_timestamp(matched_execution, matched_action)
    if observed_timestamp is None:
        decision_latency = 0
    else:
        decision_latency = max(0, int((observed_timestamp - decision_anchor).total_seconds()))

    pre_drift = _as_mapping(pre_loop_surface.get("drift"))
    post_drift = _as_mapping(post_loop_surface.get("drift"))

    pre_drift_index = _coerce_float(pre_drift.get("system_load_index"))
    post_drift_index = _coerce_float(post_drift.get("system_load_index"))
    drift_delta = round(post_drift_index - pre_drift_index, 4)

    decision_fulfilled = bool(outcome["completed"] and not outcome["failed"])

    return {
        "household_id": household_id,
        "date": date,
        "decision": {
            "id": decision_id,
            "type": decision_type,
        },
        "execution": {
            "executed": bool(outcome["executed"]),
            "execution_plan_id": matched_execution_plan_id or decision_execution_plan_id,
            "action_id": matched_action_id or matched_execution_action_id or decision_action_id,
        },
        "outcome": {
            "completed": bool(outcome["completed"]),
            "failed": bool(outcome["failed"]),
            "pending": bool(outcome["pending"]),
        },
        "feedback": {
            "decision_fulfilled": decision_fulfilled,
            "decision_latency": int(decision_latency),
        },
        "drift_impact": {
            "pre_drift_index": pre_drift_index,
            "post_drift_index": post_drift_index,
            "drift_delta": drift_delta,
        },
    }
