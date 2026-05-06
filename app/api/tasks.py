from __future__ import annotations

from collections import OrderedDict
from datetime import UTC, date, datetime, timedelta
import hashlib
import logging
import re
from threading import Lock
from time import perf_counter
from typing import Any, Mapping, Sequence, cast

from fastapi import APIRouter, Body, HTTPException, Query

from app.artifacts.artifact_cache import (
    get_actions,
    get_conflicts,
    get_household_decision_surface,
    get_execution_plans,
    get_household_loop_surface,
    get_overdue,
    get_priority,
    get_summary,
    get_today_view,
    get_upcoming,
    get_validation_plan,
)
from app.artifacts.coordination_artifacts import DEFAULT_UPCOMING_DAYS
from app.artifacts.validation_plan_artifact import DEFAULT_VALIDATION_PLAN_VERSION
from app.artifacts.validation_executor import run_validation_plan
from app.api.read_model_shared import (
    cache_get as _shared_cache_get,
    cache_set as _shared_cache_set,
    normalized_limit as _shared_normalized_limit,
    normalized_offset as _shared_normalized_offset,
    normalized_search as _shared_normalized_search,
    paginate_records as _shared_paginate_records,
    projection_cache_state as _shared_projection_cache_state,
    query_cache_state as _shared_query_cache_state,
    safe_int as _shared_safe_int,
    sort_records_with_tie_break as _shared_sort_records_with_tie_break,
)
from app.services.agents import calendar_agent, email_agent, orchestrator
from app.services.commands import CommandActor, get_command_runtime_service


router = APIRouter(tags=["tasks"])

_TASK_STATUS_VALUES = frozenset({"pending", "completed"})
_TASK_SORT_FIELDS = frozenset({"created_at", "completed_at"})
_TASK_SORT_ORDERS = frozenset({"asc", "desc"})
_DEFAULT_PAGE_LIMIT = 25
_MAX_PAGE_LIMIT = 200
_DEFAULT_PAGE_OFFSET = 0
_MATERIALIZED_CACHE_MAX_ENTRIES = 128
_VIEW_CACHE_MAX_ENTRIES = 512
_MAX_UPCOMING_DAYS = 30
_INSIGHT_WINDOW_DAYS = 7
_HOME_DECISION_LIMIT = 3
_HOME_ACTION_LIMIT = 5
_HOME_CALENDAR_LIMIT = 5
_HOME_CHANGE_TRACKER_MAX_ENTRIES = 2048

_MATERIALIZED_RECORDS_CACHE: OrderedDict[
    tuple[str, str, int, str],
    tuple[tuple[dict[str, Any], str], ...],
] = OrderedDict()
_VIEW_CACHE: OrderedDict[
    tuple[str, str, int, str, str, str, str, str, int, int],
    tuple[tuple[dict[str, Any], ...], tuple[int, int, int]],
] = OrderedDict()
_LOGGER = logging.getLogger(__name__)
_HOME_SUMMARY_TRACKER_LOCK = Lock()
_HOME_SUMMARY_TRACKER: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()

_ACTION_EXECUTION_HINTS = frozenset(
    {
        "call",
        "confirm",
        "reply",
        "send",
        "review",
        "book",
        "schedule",
        "pay",
        "submit",
        "complete",
        "approve",
        "update",
        "check",
        "pick",
        "prepare",
    }
)
_ACTION_TIME_HINTS = frozenset(
    {
        "today",
        "tonight",
        "tomorrow",
        "asap",
        "urgent",
        "deadline",
        "this week",
        "week",
        "before",
        "by",
        "eod",
        "end of day",
    }
)
_ACTION_AMBIGUOUS_TITLES = frozenset(
    {
        "action item",
        "household action",
        "household task",
        "review action item",
    }
)
_CALENDAR_CRITICAL_HINTS = frozenset(
    {
        "pickup",
        "doctor",
        "clinic",
        "appointment",
        "deadline",
        "exam",
        "meeting",
        "call",
        "interview",
        "court",
        "payment",
        "bill",
    }
)
_DECISION_IMPACT_REASON_HIGH = frozenset(
    {
        "calendar_conflict_detected",
        "calendar_change_conflict_or_uncertainty",
        "task_conflict_or_uncertainty",
        "upm_critical_recall_override",
    }
)


def reset_home_summary_tracker(*, household_id: str | None = None) -> None:
    with _HOME_SUMMARY_TRACKER_LOCK:
        if household_id is None:
            _HOME_SUMMARY_TRACKER.clear()
            return

        normalized_household_id = str(household_id).strip()
        if not normalized_household_id:
            return

        stale_keys = [
            key
            for key in _HOME_SUMMARY_TRACKER.keys()
            if str(key[0]) == normalized_household_id
        ]
        for key in stale_keys:
            _HOME_SUMMARY_TRACKER.pop(key, None)


def _safe_int(value: Any, *, default: int) -> int:
    return _shared_safe_int(value, default=default)


def _normalized_status_param(status: str | None) -> str | None:
    resolved_status = str(status or "").strip().lower()
    if resolved_status in _TASK_STATUS_VALUES:
        return resolved_status
    return None


def _normalized_sort_by_param(sort_by: str | None) -> str:
    resolved_sort_by = str(sort_by or "").strip().lower()
    if resolved_sort_by in _TASK_SORT_FIELDS:
        return resolved_sort_by
    return "created_at"


def _normalized_order_param(order: str | None) -> str:
    resolved_order = str(order or "").strip().lower()
    if resolved_order in _TASK_SORT_ORDERS:
        return resolved_order
    return "desc"


def _normalized_search_param(search: str | None) -> str | None:
    return _shared_normalized_search(search)


def _normalized_today_date(raw_date: str | None) -> str:
    if raw_date is None or not str(raw_date).strip():
        return datetime.now(UTC).date().isoformat()

    text = str(raw_date).strip()
    try:

        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="date must be YYYY-MM-DD") from exc


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _normalized_upcoming_days(raw_days: str | int | None) -> int:
    if raw_days is None:
        return DEFAULT_UPCOMING_DAYS

    resolved_days = _safe_int(raw_days, default=DEFAULT_UPCOMING_DAYS)
    if resolved_days < 1:
        return DEFAULT_UPCOMING_DAYS
    return min(resolved_days, _MAX_UPCOMING_DAYS)


def _normalized_trajectory_window(
    start_date: str | None,
    end_date: str | None,
) -> tuple[str, str]:
    resolved_end_date = _normalized_today_date(end_date)

    if start_date is None or not str(start_date).strip():
        resolved_start_date = (
            date.fromisoformat(resolved_end_date)
            - timedelta(days=6)
        ).isoformat()
    else:
        resolved_start_date = _normalized_today_date(start_date)

    if date.fromisoformat(resolved_start_date) > date.fromisoformat(resolved_end_date):
        raise HTTPException(
            status_code=422,
            detail="start_date must be <= end_date",
        )

    return resolved_start_date, resolved_end_date


def _normalized_insight_window(target_date: str) -> tuple[str, str]:
    end_date = _normalized_today_date(target_date)
    start_date = (
        date.fromisoformat(end_date)
        - timedelta(days=_INSIGHT_WINDOW_DAYS - 1)
    ).isoformat()
    return start_date, end_date


def _require_body_object(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="body must be a JSON object")
    return body


def _require_non_empty(body: Mapping[str, Any], field_name: str) -> str:
    value = str(body.get(field_name) or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    return value


def _build_home_v0(*, household_id: str, target_date: str, projection: Mapping[str, Any]) -> dict[str, Any]:
    start_time = perf_counter()

    raw_emails = _projection_email_input(projection)
    raw_calendar_events = _projection_calendar_input(projection, target_date=target_date)

    email_items = email_agent(raw_emails)
    calendar_items = calendar_agent(raw_calendar_events)
    ordered_payload = orchestrator(email_items, calendar_items)
    ordered_payload = _apply_home_digest_overlay(
        household_id=household_id,
        payload=ordered_payload,
        projection=projection,
        target_date=target_date,
    )

    decisions = ordered_payload.get("needs_decision")
    decision_rows = [row for row in decisions if isinstance(row, Mapping)] if isinstance(decisions, list) else []
    conflict_count = sum(1 for row in decision_rows if str(row.get("type") or "") == "calendar_conflict")
    action_rows = ordered_payload.get("actions")
    action_count = len(action_rows) if isinstance(action_rows, list) else 0
    calendar_rows = ordered_payload.get("calendar")
    calendar_count = len(calendar_rows) if isinstance(calendar_rows, list) else 0
    summary = str(ordered_payload.get("summary") or "")

    projection_version = _shared_projection_cache_state(projection)
    projection_snapshot_hash = str(projection.get("checksum") or "").strip() or hashlib.sha256(
        repr(projection_version).encode("utf-8")
    ).hexdigest()

    _log_home_v0_diagnostics(
        household_id=household_id,
        target_date=target_date,
        email_count=len(raw_emails),
        action_count=action_count,
        calendar_event_count=calendar_count,
        conflict_count=conflict_count,
        summary=summary,
        request_ms=(perf_counter() - start_time) * 1000,
        projection_snapshot_hash=projection_snapshot_hash,
        email_agent_preview=[dict(item) for item in email_items[:3]],
        calendar_agent_preview=[dict(item) for item in calendar_items[:3]],
        scoring_breakdown={
            "needs_decision": [
                {
                    "id": str(item.get("id") or ""),
                    "type": str(item.get("type") or ""),
                    "priority": str(item.get("priority") or ""),
                }
                for item in decision_rows
            ],
            "actions": [
                {
                    "id": str(item.get("id") or ""),
                    "priority": str(item.get("priority") or ""),
                }
                for item in (action_rows if isinstance(action_rows, list) else [])
                if isinstance(item, Mapping)
            ],
        },
    )

    return ordered_payload


def _iso_date_from_datetime_text(raw_value: Any) -> str | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).date().isoformat()
    except ValueError:
        return None


def _projection_email_input(projection: Mapping[str, Any]) -> list[dict[str, Any]]:
    ingested_raw = projection.get("ingested_emails")
    ingested_emails = [row for row in ingested_raw if isinstance(row, Mapping)] if isinstance(ingested_raw, list) else []

    action_raw = projection.get("email_actions")
    action_rows = [row for row in action_raw if isinstance(row, Mapping)] if isinstance(action_raw, list) else []

    actions_by_email: dict[str, list[dict[str, Any]]] = {}
    for row in action_rows:
        email_id = str(row.get("email_id") or "").strip()
        if not email_id:
            continue
        actions_by_email.setdefault(email_id, []).append(
            {
                "action_id": str(row.get("action_id") or "").strip(),
                "title": str(row.get("title") or "").strip(),
            }
        )

    normalized: list[dict[str, Any]] = []
    seen_email_ids: set[str] = set()
    for row in ingested_emails:
        email_id = str(row.get("email_id") or row.get("id") or "").strip()
        if not email_id:
            continue
        seen_email_ids.add(email_id)
        normalized.append(
            {
                "email_id": email_id,
                "subject": str(row.get("subject") or "").strip(),
                "from": str(row.get("from") or "").strip(),
                "received_at": str(row.get("received_at") or "").strip(),
                "action_items": list(actions_by_email.get(email_id) or []),
            }
        )

    for email_id, items in actions_by_email.items():
        if email_id in seen_email_ids:
            continue
        normalized.append(
            {
                "email_id": email_id,
                "subject": "Action item",
                "from": "",
                "received_at": "",
                "action_items": list(items),
            }
        )

    normalized.sort(key=lambda row: str(row.get("email_id") or ""))
    return normalized


def _projection_calendar_input(projection: Mapping[str, Any], *, target_date: str) -> list[dict[str, Any]]:
    raw_events = projection.get("calendar_events")
    rows = [row for row in raw_events if isinstance(row, Mapping)] if isinstance(raw_events, list) else []

    normalized: list[dict[str, Any]] = []
    for row in rows:
        start_at = str(row.get("start_at") or row.get("start") or "").strip()
        end_at = str(row.get("end_at") or row.get("end") or "").strip()
        if not start_at or not end_at:
            continue
        if _iso_date_from_datetime_text(start_at) != target_date:
            continue

        normalized.append(
            {
                "event_id": str(row.get("event_id") or row.get("id") or "").strip(),
                "title": str(row.get("title") or "").strip(),
                "start_at": start_at,
                "end_at": end_at,
            }
        )

    normalized.sort(key=lambda row: (str(row.get("start_at") or ""), str(row.get("end_at") or ""), str(row.get("event_id") or "")))
    return normalized


_HOME_PRIORITY_RANK = {"high": 0, "medium": 1, "low": 2}
_OPEN_DECISION_CARD_STATES = frozenset({"generated", "surfaced", "acknowledged"})
_DECISION_TYPE_URGENCY_RANK = {
    "calendar_conflict": 0,
    "promotion_decision": 1,
    "time_constraint_violation": 2,
}


def _parse_iso_datetime(raw_value: Any) -> datetime | None:
    text = str(raw_value or "").strip()
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


def _iso_sort_value(raw_value: Any) -> str:
    parsed = _parse_iso_datetime(raw_value)
    if parsed is None:
        return ""
    return parsed.isoformat().replace("+00:00", "Z")


def _home_priority_value(raw_priority: Any) -> str:
    normalized = str(raw_priority or "").strip().lower()
    if normalized in _HOME_PRIORITY_RANK:
        return normalized
    return "low"


def _home_priority_rank(raw_priority: Any) -> int:
    return _HOME_PRIORITY_RANK.get(_home_priority_value(raw_priority), 3)


def _projection_task_rows(projection: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks_payload = projection.get("tasks")
    if not isinstance(tasks_payload, Mapping):
        return []

    rows: list[dict[str, Any]] = []
    for task_id, raw_row in tasks_payload.items():
        if not isinstance(raw_row, Mapping):
            continue
        row = dict(raw_row)
        row.setdefault("task_id", str(task_id))
        rows.append(row)

    rows.sort(
        key=lambda row: (
            _iso_sort_value(row.get("created_at")),
            str(row.get("task_id") or ""),
        )
    )
    return rows


def _projection_schedule_rows(projection: Mapping[str, Any]) -> list[dict[str, Any]]:
    schedules_payload = projection.get("schedules")
    if not isinstance(schedules_payload, Mapping):
        return []

    rows: list[dict[str, Any]] = []
    for schedule_id, raw_row in schedules_payload.items():
        if not isinstance(raw_row, Mapping):
            continue
        row = dict(raw_row)
        row.setdefault("schedule_id", str(schedule_id))
        rows.append(row)

    rows.sort(
        key=lambda row: (
            _iso_sort_value(row.get("start_at")),
            str(row.get("schedule_id") or ""),
        )
    )
    return rows


def _projection_decision_card_rows(projection: Mapping[str, Any]) -> list[dict[str, Any]]:
    decision_cards_payload = projection.get("decision_cards")
    if not isinstance(decision_cards_payload, Mapping):
        return []

    rows: list[dict[str, Any]] = []
    for decision_card_id, raw_row in decision_cards_payload.items():
        if not isinstance(raw_row, Mapping):
            continue
        row = dict(raw_row)
        row.setdefault("decision_card_id", str(decision_card_id))
        rows.append(row)

    rows.sort(
        key=lambda row: (
            _iso_sort_value(row.get("updated_at") or row.get("created_at")),
            str(row.get("decision_card_id") or ""),
        )
    )
    return rows


def _action_due_score(*, raw_due_at: Any, target_date: str) -> int:
    due_date = _iso_date_from_datetime_text(raw_due_at)
    if due_date is None:
        return 0
    if due_date <= target_date:
        return 1
    return 0


def _action_text_blob(row: Mapping[str, Any]) -> str:
    title = str(row.get("title") or "").strip().lower()
    context = str(row.get("_context") or "").strip().lower()
    return f"{title} {context}".strip()


def _action_time_hint_score(row: Mapping[str, Any]) -> int:
    text = _action_text_blob(row)
    if not text:
        return 0
    return int(any(token in text for token in _ACTION_TIME_HINTS))


def _action_is_executable(row: Mapping[str, Any]) -> bool:
    source = str(row.get("source") or "").strip().lower()
    if source == "task":
        return True

    title = str(row.get("title") or "").strip().lower()
    if not title:
        return False

    words = re.findall(r"[a-z0-9']+", title)
    if not words:
        return False

    first_word = str(words[0])
    if first_word in _ACTION_EXECUTION_HINTS:
        return True

    return any(word in _ACTION_EXECUTION_HINTS for word in words)


def _action_is_time_relevant(*, row: Mapping[str, Any], target_date: str) -> bool:
    if int(row.get("_due_score") or 0) > 0:
        return True

    if _home_priority_rank(row.get("priority")) == 0:
        return True

    if _action_time_hint_score(row) > 0:
        return True

    due_date = _iso_date_from_datetime_text(row.get("_due_at"))
    if due_date is None:
        return False

    try:
        horizon = date.fromisoformat(target_date) + timedelta(days=2)
    except ValueError:
        return False

    return date.fromisoformat(due_date) <= horizon


def _action_is_ambiguous(row: Mapping[str, Any]) -> bool:
    title = str(row.get("title") or "").strip()
    lowered = title.lower()
    if not title:
        return True
    if lowered in _ACTION_AMBIGUOUS_TITLES:
        return True
    if "?" in title:
        return True

    words = [part for part in re.findall(r"[A-Za-z0-9']+", title) if part]
    if len(words) < 2:
        return True
    if len(title) < 8:
        return True
    return False


def _action_rank_key(row: Mapping[str, Any]) -> tuple[int, int, int, str, str]:
    return (
        _home_priority_rank(row.get("priority")),
        -int(row.get("_due_score") or 0),
        -int(row.get("_time_hint_score") or 0),
        str(row.get("title") or ""),
        str(row.get("id") or ""),
    )


def _merge_home_actions_with_projection_tasks(
    *,
    base_actions: Sequence[Mapping[str, Any]],
    projection: Mapping[str, Any],
    target_date: str,
) -> list[dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}

    for raw_row in base_actions:
        row_id = str(raw_row.get("id") or "").strip()
        if not row_id:
            continue
        title = str(raw_row.get("title") or "").strip() or "Household action"
        priority = _home_priority_value(raw_row.get("priority"))
        due_at = str(raw_row.get("due_at") or "").strip()
        due_score = _action_due_score(raw_due_at=due_at, target_date=target_date)
        created_sort = _iso_sort_value(raw_row.get("created_at"))
        source = str(raw_row.get("source") or "email").strip() or "email"
        candidates[row_id] = {
            "id": row_id,
            "title": title,
            "source": source,
            "priority": priority,
            "_due_at": due_at,
            "_due_score": due_score,
            "_created_sort": created_sort,
            "_context": str(raw_row.get("context") or "").strip(),
            "_time_hint_score": 0,
        }
        candidates[row_id]["_time_hint_score"] = _action_time_hint_score(candidates[row_id])

    for task_row in _projection_task_rows(projection):
        task_id = str(task_row.get("task_id") or "").strip()
        if not task_id:
            continue

        status = str(task_row.get("status") or "pending").strip().lower()
        if status in {"completed", "ignored", "cancelled"}:
            continue

        title = str(task_row.get("title") or "").strip() or "Household task"
        priority = _home_priority_value(task_row.get("priority"))
        due_at = str(task_row.get("due_at") or "").strip()
        due_score = _action_due_score(raw_due_at=due_at, target_date=target_date)
        created_sort = _iso_sort_value(task_row.get("created_at"))

        candidate = {
            "id": task_id,
            "title": title,
            "source": "task",
            "priority": priority,
            "_due_at": due_at,
            "_due_score": due_score,
            "_created_sort": created_sort,
            "_context": str(task_row.get("description") or "").strip(),
            "_time_hint_score": 0,
        }
        candidate["_time_hint_score"] = _action_time_hint_score(candidate)

        existing = candidates.get(task_id)
        if existing is None:
            candidates[task_id] = candidate
            continue

        existing_key = _action_rank_key(existing)
        candidate_key = _action_rank_key(candidate)
        if candidate_key < existing_key:
            candidates[task_id] = candidate

    ordered_rows = sorted(
        candidates.values(),
        key=_action_rank_key,
    )

    filtered_rows = [
        row
        for row in ordered_rows
        if _action_is_executable(row)
        and _action_is_time_relevant(row=row, target_date=target_date)
        and not _action_is_ambiguous(row)
    ]
    if not filtered_rows:
        filtered_rows = [
            row
            for row in ordered_rows
            if _action_is_executable(row)
            and not _action_is_ambiguous(row)
        ]
    if not filtered_rows:
        filtered_rows = ordered_rows

    limited_rows = filtered_rows[:_HOME_ACTION_LIMIT]
    return [
        {
            "id": str(row.get("id") or ""),
            "title": str(row.get("title") or ""),
            "source": str(row.get("source") or "email"),
            "priority": _home_priority_value(row.get("priority")),
        }
        for row in limited_rows
    ]


def _calendar_sort_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("start") or ""),
        str(row.get("end") or ""),
        str(row.get("id") or ""),
    )


def _calendar_conflict_ids(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    normalized: list[tuple[datetime, datetime, str]] = []
    for row in rows:
        start_dt = _parse_iso_datetime(row.get("start"))
        end_dt = _parse_iso_datetime(row.get("end"))
        row_id = str(row.get("id") or "").strip()
        if not row_id or start_dt is None or end_dt is None or end_dt <= start_dt:
            continue
        normalized.append((start_dt, end_dt, row_id))

    normalized.sort(key=lambda item: (item[0], item[1], item[2]))
    conflict_ids: set[str] = set()
    for left_index in range(len(normalized)):
        left_start, left_end, left_id = normalized[left_index]
        for right_index in range(left_index + 1, len(normalized)):
            right_start, right_end, right_id = normalized[right_index]
            if right_start >= left_end:
                break
            if left_start < right_end and right_start < left_end:
                conflict_ids.add(left_id)
                conflict_ids.add(right_id)
    return conflict_ids


def _calendar_is_critical(row: Mapping[str, Any]) -> bool:
    title = str(row.get("title") or "").strip().lower()
    if any(token in title for token in _CALENDAR_CRITICAL_HINTS):
        return True

    start_dt = _parse_iso_datetime(row.get("start"))
    end_dt = _parse_iso_datetime(row.get("end"))
    if start_dt is None or end_dt is None:
        return False
    return (end_dt - start_dt) >= timedelta(hours=2)


def _merge_home_calendar_with_projection_schedule(
    *,
    base_calendar: Sequence[Mapping[str, Any]],
    projection: Mapping[str, Any],
    target_date: str,
) -> list[dict[str, Any]]:
    rows_by_id: dict[str, dict[str, Any]] = {}

    for raw_row in base_calendar:
        row_id = str(raw_row.get("id") or "").strip()
        if not row_id:
            continue
        rows_by_id[row_id] = {
            "id": row_id,
            "title": str(raw_row.get("title") or "").strip(),
            "start": str(raw_row.get("start") or "").strip(),
            "end": str(raw_row.get("end") or "").strip(),
        }

    for schedule_row in _projection_schedule_rows(projection):
        status = str(schedule_row.get("status") or "scheduled").strip().lower()
        if status == "cancelled":
            continue

        start_at = str(schedule_row.get("start_at") or "").strip()
        end_at = str(schedule_row.get("end_at") or "").strip()
        if not start_at or not end_at:
            continue
        if _iso_date_from_datetime_text(start_at) != target_date:
            continue

        schedule_id = str(schedule_row.get("schedule_id") or "").strip()
        if not schedule_id:
            continue

        rows_by_id.setdefault(
            schedule_id,
            {
                "id": schedule_id,
                "title": str(schedule_row.get("title") or "").strip() or "Household schedule",
                "start": start_at,
                "end": end_at,
            },
        )

    ordered_rows = sorted(rows_by_id.values(), key=_calendar_sort_key)
    if len(ordered_rows) <= _HOME_CALENDAR_LIMIT:
        return ordered_rows

    conflict_ids = _calendar_conflict_ids(ordered_rows)
    selected: dict[str, dict[str, Any]] = {}

    for row in ordered_rows:
        row_id = str(row.get("id") or "").strip()
        if row_id and row_id in conflict_ids:
            selected[row_id] = dict(row)

    for row in ordered_rows:
        if len(selected) >= _HOME_CALENDAR_LIMIT:
            break
        row_id = str(row.get("id") or "").strip()
        if not row_id or row_id in selected:
            continue
        if _calendar_is_critical(row):
            selected[row_id] = dict(row)

    for row in ordered_rows:
        if len(selected) >= _HOME_CALENDAR_LIMIT:
            break
        row_id = str(row.get("id") or "").strip()
        if not row_id or row_id in selected:
            continue
        selected[row_id] = dict(row)

    return sorted(selected.values(), key=_calendar_sort_key)[:_HOME_CALENDAR_LIMIT]


def _decision_type_rank(raw_type: Any) -> int:
    return _DECISION_TYPE_URGENCY_RANK.get(str(raw_type or "").strip().lower(), 5)


def _decision_metadata(row: Mapping[str, Any]) -> Mapping[str, Any]:
    raw_metadata = row.get("_metadata")
    if isinstance(raw_metadata, Mapping):
        return raw_metadata
    raw_metadata = row.get("metadata")
    if isinstance(raw_metadata, Mapping):
        return raw_metadata
    return {}


def _decision_impact_rank(row: Mapping[str, Any]) -> int:
    metadata = _decision_metadata(row)
    decision_reason = str(metadata.get("decision_reason") or "").strip().lower()
    if decision_reason in _DECISION_IMPACT_REASON_HIGH:
        return 0

    conflict_type = str(metadata.get("conflict_type") or "").strip().lower()
    if conflict_type in {"direct", "cross_member", "cascade", "derived"}:
        return 0

    upm_priority_class = str(metadata.get("upm_priority_class") or "").strip().lower()
    if upm_priority_class in {"critical", "high"}:
        return 0
    if upm_priority_class == "medium":
        return 1

    if bool(metadata.get("upm_conflict_risk")) or bool(metadata.get("upm_state_dependency")):
        return 1
    return 2


def _decision_time_sort_value(row: Mapping[str, Any]) -> str:
    metadata = _decision_metadata(row)
    candidate_fields = (
        metadata.get("due_at"),
        metadata.get("defer_to_date"),
        metadata.get("promoted_at"),
        row.get("_updated_sort"),
    )
    for candidate in candidate_fields:
        iso_value = _iso_sort_value(candidate)
        if iso_value:
            return iso_value
    return "9999-12-31T23:59:59Z"


def _decision_is_actionable(row: Mapping[str, Any], *, projection: Mapping[str, Any]) -> bool:
    row_type = str(row.get("type") or "").strip().lower()
    if row_type in {"calendar_conflict", "time_constraint_violation"}:
        return True

    decision_id = str(row.get("id") or "").strip()
    if decision_id:
        tasks_payload = projection.get("tasks")
        if isinstance(tasks_payload, Mapping):
            task_row = tasks_payload.get(decision_id)
            if isinstance(task_row, Mapping):
                status = str(task_row.get("status") or "pending").strip().lower()
                if status not in {"completed", "ignored", "cancelled"}:
                    return True

        schedules_payload = projection.get("schedules")
        if isinstance(schedules_payload, Mapping):
            schedule_row = schedules_payload.get(decision_id)
            if isinstance(schedule_row, Mapping):
                status = str(schedule_row.get("status") or "scheduled").strip().lower()
                if status != "cancelled":
                    return True

    metadata = _decision_metadata(row)
    if str(metadata.get("conflict_schedule_id") or "").strip():
        return True
    if str(metadata.get("dependency_schedule_id") or "").strip():
        return True

    decision_reason = str(metadata.get("decision_reason") or "").strip().lower()
    if decision_reason in _DECISION_IMPACT_REASON_HIGH:
        return True

    upm_priority_class = str(metadata.get("upm_priority_class") or "").strip().lower()
    if upm_priority_class in {"critical", "high", "medium"}:
        return True

    return False


def _decision_rank_key(row: Mapping[str, Any]) -> tuple[int, int, str, int, str, str]:
    return (
        _home_priority_rank(row.get("priority")),
        _decision_impact_rank(row),
        _decision_time_sort_value(row),
        _decision_type_rank(row.get("type")),
        str(row.get("question") or ""),
        str(row.get("id") or ""),
    )


def _merge_home_decisions_with_projection_cards(
    *,
    base_decisions: Sequence[Mapping[str, Any]],
    projection: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows_by_id: dict[str, dict[str, Any]] = {}

    for raw_row in base_decisions:
        row_id = str(raw_row.get("id") or "").strip()
        if not row_id:
            continue
        options_raw = raw_row.get("options")
        options = [
            str(option).strip()
            for option in options_raw
            if str(option).strip()
        ] if isinstance(options_raw, list) else ["Complete", "Defer", "Ignore"]
        rows_by_id[row_id] = {
            "id": row_id,
            "type": str(raw_row.get("type") or "promotion_decision").strip() or "promotion_decision",
            "priority": _home_priority_value(raw_row.get("priority")),
            "question": str(raw_row.get("question") or "Review household decision").strip(),
            "options": options or ["Complete", "Defer", "Ignore"],
            "_metadata": dict(raw_row.get("metadata") or {}) if isinstance(raw_row.get("metadata"), Mapping) else {},
            "_updated_sort": _iso_sort_value(raw_row.get("updated_at") or raw_row.get("created_at")),
        }

    for decision_card in _projection_decision_card_rows(projection):
        decision_card_id = str(decision_card.get("decision_card_id") or "").strip()
        if not decision_card_id or decision_card_id in rows_by_id:
            continue

        state = str(decision_card.get("state") or "").strip().lower()
        if state not in _OPEN_DECISION_CARD_STATES:
            continue

        metadata = dict(decision_card.get("metadata") or {}) if isinstance(decision_card.get("metadata"), Mapping) else {}
        default_priority = "high" if state == "acknowledged" else "medium"
        priority = _home_priority_value(metadata.get("decision_priority") or default_priority)

        rows_by_id[decision_card_id] = {
            "id": decision_card_id,
            "type": "promotion_decision",
            "priority": priority,
            "question": str(decision_card.get("title") or "Review promoted household item").strip(),
            "options": ["Complete", "Defer", "Ignore"],
            "_metadata": metadata,
            "_updated_sort": _iso_sort_value(decision_card.get("updated_at") or decision_card.get("created_at")),
        }

    candidate_rows = [
        row
        for row in rows_by_id.values()
        if _decision_is_actionable(row, projection=projection)
    ]
    if not candidate_rows:
        candidate_rows = list(rows_by_id.values())

    ordered_rows = sorted(candidate_rows, key=_decision_rank_key)
    return [
        {
            "id": str(row.get("id") or ""),
            "type": str(row.get("type") or "promotion_decision"),
            "priority": _home_priority_value(row.get("priority")),
            "question": str(row.get("question") or "Review household decision"),
            "options": [
                str(option).strip()
                for option in list(row.get("options") or [])
                if str(option).strip()
            ] or ["Complete", "Defer", "Ignore"],
        }
        for row in ordered_rows[:_HOME_DECISION_LIMIT]
    ]


def _latest_projection_change_timestamp(projection: Mapping[str, Any]) -> str:
    candidates: list[str] = []

    for row in _projection_task_rows(projection):
        for field_name in ("created_at", "completed_at"):
            value = _iso_sort_value(row.get(field_name))
            if value:
                candidates.append(value)

    for row in _projection_schedule_rows(projection):
        for field_name in ("created_at", "cancelled_at", "start_at"):
            value = _iso_sort_value(row.get(field_name))
            if value:
                candidates.append(value)

    for row in _projection_decision_card_rows(projection):
        for field_name in ("updated_at", "created_at"):
            value = _iso_sort_value(row.get(field_name))
            if value:
                candidates.append(value)

    for field_name in ("ingested_emails", "household_messages", "household_promotions"):
        payload = projection.get(field_name)
        if not isinstance(payload, list):
            continue
        for row in payload:
            if not isinstance(row, Mapping):
                continue
            for candidate_field in ("received_at", "created_at", "promoted_at"):
                value = _iso_sort_value(row.get(candidate_field))
                if value:
                    candidates.append(value)

    if not candidates:
        return ""
    return sorted(candidates)[-1]


def _digest_time_frame_label(target_date: str) -> str:
    today = datetime.now(UTC).date()
    target = date.fromisoformat(target_date)
    if target == today:
        return "today"
    if target == (today + timedelta(days=1)):
        return "tomorrow"
    return target.isoformat()


def _trim_inline(text: str, *, max_chars: int) -> str:
    collapsed = " ".join(str(text or "").split())
    if len(collapsed) <= max_chars:
        return collapsed
    return f"{collapsed[: max_chars - 3].rstrip()}..."


def _format_delta(value: int) -> str:
    if value > 0:
        return f"+{value}"
    return str(value)


def _home_projection_fingerprint(projection: Mapping[str, Any]) -> str:
    last_event_id, state_version, checksum = _shared_projection_cache_state(projection)
    return f"{last_event_id}:{state_version}:{checksum}"


def _summary_change_segment(
    *,
    household_id: str,
    target_date: str,
    projection: Mapping[str, Any],
    decision_count: int,
    action_count: int,
    calendar_count: int,
) -> str:
    key = (str(household_id).strip(), str(target_date).strip())
    fingerprint = _home_projection_fingerprint(projection)
    current_snapshot = {
        "fingerprint": fingerprint,
        "decision_count": int(decision_count),
        "action_count": int(action_count),
        "calendar_count": int(calendar_count),
    }

    with _HOME_SUMMARY_TRACKER_LOCK:
        previous_snapshot = _HOME_SUMMARY_TRACKER.get(key)
        if previous_snapshot is None:
            _HOME_SUMMARY_TRACKER[key] = current_snapshot
            _HOME_SUMMARY_TRACKER.move_to_end(key)
            while len(_HOME_SUMMARY_TRACKER) > _HOME_CHANGE_TRACKER_MAX_ENTRIES:
                _HOME_SUMMARY_TRACKER.popitem(last=False)
            return "No new changes since last check."

        previous_fingerprint = str(previous_snapshot.get("fingerprint") or "")
        if previous_fingerprint == fingerprint:
            return "No new changes since last check."

        decision_delta = int(decision_count) - int(previous_snapshot.get("decision_count") or 0)
        action_delta = int(action_count) - int(previous_snapshot.get("action_count") or 0)
        calendar_delta = int(calendar_count) - int(previous_snapshot.get("calendar_count") or 0)

    if decision_delta == 0 and action_delta == 0 and calendar_delta == 0:
        return "No new changes since last check."

    return (
        "Updated since last check: "
        f"decisions {_format_delta(decision_delta)}, "
        f"actions {_format_delta(action_delta)}, "
        f"calendar {_format_delta(calendar_delta)}."
    )


def _digest_summary(
    *,
    household_id: str,
    target_date: str,
    decisions: Sequence[Mapping[str, Any]],
    actions: Sequence[Mapping[str, Any]],
    calendar: Sequence[Mapping[str, Any]],
    projection: Mapping[str, Any],
) -> str:
    timeframe = _digest_time_frame_label(target_date)
    latest_change_at = _latest_projection_change_timestamp(projection)
    decision_count = len(decisions)
    action_count = len(actions)
    calendar_count = len(calendar)

    if decisions:
        top_decision_question = _trim_inline(str(decisions[0].get("question") or "Review pending decision"), max_chars=84)
        decision_segment = f"Decide now: {top_decision_question}"
    else:
        decision_segment = "No blocking decisions"

    if actions:
        top_action_title = _trim_inline(str(actions[0].get("title") or "Run top action"), max_chars=72)
        action_segment = f"Execute next: {top_action_title}"
    else:
        action_segment = "No executable actions queued"

    if calendar:
        next_event = calendar[0]
        next_title = _trim_inline(str(next_event.get("title") or "Upcoming event"), max_chars=48)
        next_start = str(next_event.get("start") or "").strip() or "unspecified time"
        calendar_segment = f"Calendar focus: {next_title} at {next_start}"
    else:
        calendar_segment = "Calendar focus: clear"

    change_segment = _summary_change_segment(
        household_id=household_id,
        target_date=target_date,
        projection=projection,
        decision_count=decision_count,
        action_count=action_count,
        calendar_count=calendar_count,
    )
    freshness_segment = f"Latest change {latest_change_at}" if latest_change_at else "Latest change n/a"
    return (
        f"Command center {timeframe}: "
        f"{decision_segment}. "
        f"{action_segment}. "
        f"{calendar_segment}. "
        f"{change_segment} "
        f"{freshness_segment}."
    )


def _apply_home_digest_overlay(
    *,
    household_id: str,
    payload: Mapping[str, Any],
    projection: Mapping[str, Any],
    target_date: str,
) -> dict[str, Any]:
    base_decisions = [row for row in payload.get("needs_decision") if isinstance(row, Mapping)] if isinstance(payload.get("needs_decision"), list) else []
    base_actions = [row for row in payload.get("actions") if isinstance(row, Mapping)] if isinstance(payload.get("actions"), list) else []
    base_calendar = [row for row in payload.get("calendar") if isinstance(row, Mapping)] if isinstance(payload.get("calendar"), list) else []

    merged_decisions = _merge_home_decisions_with_projection_cards(
        base_decisions=base_decisions,
        projection=projection,
    )
    merged_actions = _merge_home_actions_with_projection_tasks(
        base_actions=base_actions,
        projection=projection,
        target_date=target_date,
    )
    merged_calendar = _merge_home_calendar_with_projection_schedule(
        base_calendar=base_calendar,
        projection=projection,
        target_date=target_date,
    )
    summary = _digest_summary(
        household_id=household_id,
        target_date=target_date,
        decisions=merged_decisions,
        actions=merged_actions,
        calendar=merged_calendar,
        projection=projection,
    )

    return {
        "needs_decision": merged_decisions,
        "actions": merged_actions,
        "calendar": merged_calendar,
        "summary": summary,
    }


def _log_home_v0_diagnostics(
    *,
    household_id: str,
    target_date: str,
    email_count: int,
    action_count: int,
    calendar_event_count: int,
    conflict_count: int,
    summary: str,
    request_ms: float,
    projection_snapshot_hash: str,
    email_agent_preview: list[dict[str, Any]],
    calendar_agent_preview: list[dict[str, Any]],
    scoring_breakdown: Mapping[str, Any],
) -> None:
    _LOGGER.info(
        (
            "home_v0 household=%s date=%s request_ms=%.2f projection_hash=%s "
            "email_count=%d action_count=%d calendar_event_count=%d conflict_count=%d summary=%s "
            "email_agent_preview=%s calendar_agent_preview=%s scoring_breakdown=%s"
        ),
        household_id,
        target_date,
        request_ms,
        projection_snapshot_hash,
        email_count,
        action_count,
        calendar_event_count,
        conflict_count,
        summary,
        email_agent_preview,
        calendar_agent_preview,
        dict(scoring_breakdown),
    )
    print(
        (
            "[HOME_V0] "
            f"household={household_id} "
            f"date={target_date} "
            f"request_ms={request_ms:.2f} "
            f"projection_hash={projection_snapshot_hash} "
            f"email_count={email_count} "
            f"action_count={action_count} "
            f"calendar_event_count={calendar_event_count} "
            f"conflict_count={conflict_count} "
            f"summary={summary} "
            f"email_agent_preview={email_agent_preview} "
            f"calendar_agent_preview={calendar_agent_preview} "
            f"scoring_breakdown={dict(scoring_breakdown)}"
        ),
        flush=True,
    )


def _dispatch_decision_command(*, command_type: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    runtime = get_command_runtime_service()
    actor = CommandActor(actor_type="api_user", user_id="api")
    result = runtime.handle_command(
        command_type=command_type,
        household_id=str(payload.get("household_id") or ""),
        actor=actor,
        payload={k: v for k, v in payload.items() if k != "household_id"},
        source="api.tasks",
    )
    status = str(result.get("status") or "").strip().lower()
    if status not in {"accepted", "duplicate"}:
        raise HTTPException(status_code=400, detail="command rejected")
    return {
        "status": "accepted",
        "request_id": str(result.get("request_id") or ""),
        "event_id": str(result.get("event_id") or ""),
    }


def _normalized_limit_param(limit: Any) -> int:
    return _shared_normalized_limit(
        limit,
        default_limit=_DEFAULT_PAGE_LIMIT,
        max_limit=_MAX_PAGE_LIMIT,
    )


def _normalized_offset_param(offset: Any) -> int:
    return _shared_normalized_offset(offset, default_offset=_DEFAULT_PAGE_OFFSET)


def _normalized_task_status(task: Mapping[str, Any]) -> str:
    status = str(task.get("status") or "").strip().lower()
    if status in _TASK_STATUS_VALUES:
        return status

    lifecycle_state = str(task.get("lifecycle_state") or "").strip().lower()
    if lifecycle_state == "completed":
        return "completed"
    return "pending"


def _normalize_task_row(task: Mapping[str, Any]) -> dict[str, Any]:
    status = _normalized_task_status(task)
    created_at = str(task.get("created_at") or "").strip()
    completed_at_raw = task.get("completed_at")
    completed_at = str(completed_at_raw).strip() if completed_at_raw is not None else None

    if status != "completed":
        completed_at = None

    return {
        "task_id": str(task.get("task_id") or "").strip(),
        "title": str(task.get("title") or "").strip(),
        "description": str(task.get("description") or "").strip(),
        "priority": str(task.get("priority") or "").strip().lower() or "medium",
        "due_at": str(task.get("due_at") or "").strip() or None,
        "status": status,
        "created_at": created_at,
        "completed_at": completed_at,
    }


def _sorted_tasks(tasks: Sequence[Mapping[str, Any]], *, sort_by: str, order: str) -> list[Mapping[str, Any]]:
    resolved_sort_by = _normalized_sort_by_param(sort_by)
    resolved_order = _normalized_order_param(order)
    return _shared_sort_records_with_tie_break(
        tasks,
        sort_field=resolved_sort_by,
        order=resolved_order,
        created_field="created_at",
        id_field="task_id",
    )


def _status_filtered_tasks(tasks: list[Mapping[str, Any]], *, status: str | None) -> list[Mapping[str, Any]]:
    resolved_status = _normalized_status_param(status)
    if resolved_status is None:
        return list(tasks)
    return [task for task in tasks if _normalized_task_status(task) == resolved_status]


def _searched_tasks(tasks: list[Mapping[str, Any]], *, search: str | None) -> list[Mapping[str, Any]]:
    term = _normalized_search_param(search)
    if term is None:
        return list(tasks)

    matched: list[Mapping[str, Any]] = []
    for task in tasks:
        title = str(task.get("title") or "").lower()
        description = str(task.get("description") or "").lower()
        if term in title or term in description:
            matched.append(task)
    return matched


def _paginated_tasks(tasks: Sequence[Mapping[str, Any]], *, limit: int, offset: int) -> list[Mapping[str, Any]]:
    return _shared_paginate_records(tasks, limit=limit, offset=offset)


def _projection_fingerprint(
    *,
    household_id: str,
    projection: Mapping[str, Any],
 ) -> tuple[str, str, int, str]:
    last_event_id, state_version, checksum = _shared_projection_cache_state(projection)
    return (
        household_id,
        last_event_id,
        state_version,
        checksum,
    )


def _cache_get[K, V](cache: OrderedDict[K, V], key: K) -> V | None:
    return _shared_cache_get(cache, key)


def _cache_set[K, V](cache: OrderedDict[K, V], key: K, value: V, *, max_entries: int) -> None:
    _shared_cache_set(
        cache,
        key,
        value,
        max_entries=max_entries,
    )


def _get_or_build_materialized_records(
    *,
    projection_fingerprint: tuple[str, str, int, str],
    projection_tasks: list[Mapping[str, Any]],
) -> tuple[tuple[dict[str, Any], str], ...]:
    cached = _cache_get(_MATERIALIZED_RECORDS_CACHE, projection_fingerprint)
    if cached is not None:
        return cached

    built = tuple(_materialized_task_records(projection_tasks))
    _cache_set(
        _MATERIALIZED_RECORDS_CACHE,
        projection_fingerprint,
        built,
        max_entries=_MATERIALIZED_CACHE_MAX_ENTRIES,
    )
    return built


def _summary_to_counts(summary: Mapping[str, int]) -> tuple[int, int, int]:
    return (
        int(summary.get("total") or 0),
        int(summary.get("pending") or 0),
        int(summary.get("completed") or 0),
    )


def _summary_from_counts(counts: tuple[int, int, int]) -> dict[str, int]:
    return {
        "total": int(counts[0]),
        "pending": int(counts[1]),
        "completed": int(counts[2]),
    }


def _get_or_build_sorted_view(
    *,
    projection_fingerprint: tuple[str, str, int, str],
    records: tuple[tuple[dict[str, Any], str], ...],
    status: str | None,
    search: str | None,
    sort_by: str,
    order: str,
    limit: int,
    offset: int,
) -> tuple[tuple[dict[str, Any], ...], tuple[int, int, int]]:
    query_state = _shared_query_cache_state(
        filter_state={"status": str(status or "")},
        search=search,
        sort_by=sort_by,
        order=order,
        limit=limit,
        offset=offset,
    )
    view_key = (*projection_fingerprint, *query_state)
    cached_view = _cache_get(_VIEW_CACHE, view_key)
    if cached_view is not None:
        return cached_view

    filtered, summary = _filtered_records_with_summary(
        list(records),
        status=status,
        search=search,
    )
    sorted_tasks = tuple(
        cast(dict[str, Any], task)
        for task in _sorted_tasks(
            filtered,
            sort_by=sort_by,
            order=order,
        )
    )
    summary_counts = _summary_to_counts(summary)
    built_view = (sorted_tasks, summary_counts)
    _cache_set(
        _VIEW_CACHE,
        view_key,
        built_view,
        max_entries=_VIEW_CACHE_MAX_ENTRIES,
    )
    return built_view


def _materialized_task_records(tasks: list[Mapping[str, Any]]) -> list[tuple[dict[str, Any], str]]:
    records: list[tuple[dict[str, Any], str]] = []
    for task in tasks:
        normalized = _normalize_task_row(task)
        search_blob = "\n".join(
            [
                str(normalized.get("title") or "").lower(),
                str(normalized.get("description") or "").lower(),
            ]
        )
        records.append((normalized, search_blob))
    return records


def _filtered_records_with_summary(
    records: Sequence[tuple[dict[str, Any], str]],
    *,
    status: str | None,
    search: str | None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    matched: list[dict[str, Any]] = []
    summary = {
        "total": 0,
        "pending": 0,
        "completed": 0,
    }

    for task, search_blob in records:
        task_status = str(task.get("status") or "")
        if status is not None and task_status != status:
            continue
        if search is not None and search not in search_blob:
            continue

        matched.append(task)
        summary["total"] += 1
        if task_status == "completed":
            summary["completed"] += 1
        else:
            summary["pending"] += 1

    return matched, summary


@router.get("/tasks")
async def get_tasks(
    household_id: str = Query(..., min_length=1),
    status: str | None = Query(default=None),
    sort_by: str = Query(default="created_at"),
    order: str = Query(default="desc"),
    limit: str | int | None = Query(default=_DEFAULT_PAGE_LIMIT),
    offset: str | int | None = Query(default=_DEFAULT_PAGE_OFFSET),
    search: str | None = Query(default=None),
) -> dict[str, Any]:
    normalized_status = _normalized_status_param(status)
    normalized_sort_by = _normalized_sort_by_param(sort_by)
    normalized_order = _normalized_order_param(order)
    normalized_search = _normalized_search_param(search)
    normalized_limit = _normalized_limit_param(limit)
    normalized_offset = _normalized_offset_param(offset)

    projection = get_command_runtime_service().get_projection(household_id)
    projection_tasks_raw = projection.get("tasks_list")
    if not isinstance(projection_tasks_raw, list):
        projection_tasks_raw = []

    projection_tasks = [task for task in projection_tasks_raw if isinstance(task, Mapping)]
    projection_fingerprint = _projection_fingerprint(
        household_id=household_id,
        projection=projection,
    )
    materialized_records = _get_or_build_materialized_records(
        projection_fingerprint=projection_fingerprint,
        projection_tasks=projection_tasks,
    )

    # CONTRACT: This endpoint is considered stable.
    # Behavior changes must be validated against regression tests.
    # Do not modify without explicit contract revision.
    # Strict read-model pipeline: projection -> normalize -> filter -> search -> sort -> tie-break -> paginate -> response.
    sorted_tasks, summary_counts = _get_or_build_sorted_view(
        projection_fingerprint=projection_fingerprint,
        records=materialized_records,
        status=normalized_status,
        search=normalized_search,
        sort_by=normalized_sort_by,
        order=normalized_order,
        limit=normalized_limit,
        offset=normalized_offset,
    )
    summary = _summary_from_counts(summary_counts)

    paginated_tasks = _paginated_tasks(
        sorted_tasks,
        limit=normalized_limit,
        offset=normalized_offset,
    )
    paginated_payload = [dict(task) for task in paginated_tasks]

    pagination = {
        "limit": normalized_limit,
        "offset": normalized_offset,
        "returned": len(paginated_payload),
    }

    return {
        "tasks": paginated_payload,
        "summary": summary,
        "pagination": pagination,
    }


@router.get("/household/{household_id}/today")
async def get_household_today_view(
    household_id: str,
    requested_date: str | None = Query(default=None, alias="date"),
) -> dict[str, Any]:
    normalized_date = _normalized_today_date(requested_date)
    projection = get_command_runtime_service().get_projection(household_id)
    last_event_id, state_version, checksum = _shared_projection_cache_state(projection)
    projection_version = f"{last_event_id}:{state_version}:{checksum}"

    return get_today_view(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        projection=projection,
    )


@router.get("/household/{household_id}/conflicts")
async def get_household_conflicts_artifact(household_id: str) -> dict[str, Any]:
    projection = get_command_runtime_service().get_projection(household_id)
    last_event_id, state_version, checksum = _shared_projection_cache_state(projection)
    projection_version = f"{last_event_id}:{state_version}:{checksum}"

    return get_conflicts(
        household_id=household_id,
        projection_version=projection_version,
        projection=projection,
    )


@router.get("/household/{household_id}/upcoming")
async def get_household_upcoming_artifact(
    household_id: str,
    days: str | int | None = Query(default=DEFAULT_UPCOMING_DAYS),
) -> dict[str, Any]:
    normalized_days = _normalized_upcoming_days(days)
    window_start = _utc_now_iso()
    window_end = (
        datetime.fromisoformat(window_start.replace("Z", "+00:00"))
        + timedelta(days=normalized_days)
    ).astimezone(UTC).isoformat().replace("+00:00", "Z")

    projection = get_command_runtime_service().get_projection(household_id)
    last_event_id, state_version, checksum = _shared_projection_cache_state(projection)
    projection_version = f"{last_event_id}:{state_version}:{checksum}"

    return get_upcoming(
        household_id=household_id,
        projection_version=projection_version,
        window_start=window_start,
        window_end=window_end,
        projection=projection,
        now=window_start,
        days=normalized_days,
    )


@router.get("/household/{household_id}/overdue")
async def get_household_overdue_artifact(household_id: str) -> dict[str, Any]:
    now = _utc_now_iso()
    projection = get_command_runtime_service().get_projection(household_id)
    last_event_id, state_version, checksum = _shared_projection_cache_state(projection)
    projection_version = f"{last_event_id}:{state_version}:{checksum}"

    return get_overdue(
        household_id=household_id,
        projection_version=projection_version,
        projection=projection,
        now=now,
    )


@router.get("/household/{household_id}/summary")
async def get_household_summary_artifact(
    household_id: str,
    requested_date: str | None = Query(default=None, alias="date"),
    upcoming_days: str | int | None = Query(default=DEFAULT_UPCOMING_DAYS, alias="days"),
) -> dict[str, Any]:
    normalized_date = _normalized_today_date(requested_date)
    normalized_days = _normalized_upcoming_days(upcoming_days)
    now = _utc_now_iso()
    window_end = (
        datetime.fromisoformat(now.replace("Z", "+00:00"))
        + timedelta(days=normalized_days)
    ).astimezone(UTC).isoformat().replace("+00:00", "Z")

    projection = get_command_runtime_service().get_projection(household_id)
    last_event_id, state_version, checksum = _shared_projection_cache_state(projection)
    projection_version = f"{last_event_id}:{state_version}:{checksum}"

    today_view = get_today_view(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        projection=projection,
    )
    conflicts = get_conflicts(
        household_id=household_id,
        projection_version=projection_version,
        projection=projection,
    )
    upcoming = get_upcoming(
        household_id=household_id,
        projection_version=projection_version,
        window_start=now,
        window_end=window_end,
        projection=projection,
        now=now,
        days=normalized_days,
    )
    overdue = get_overdue(
        household_id=household_id,
        projection_version=projection_version,
        projection=projection,
        now=now,
    )

    return get_summary(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        today_view=today_view,
        conflicts=conflicts,
        upcoming=upcoming,
        overdue=overdue,
    )


@router.get("/household/{household_id}/priority")
async def get_household_priority_artifact(
    household_id: str,
    requested_date: str | None = Query(default=None, alias="date"),
    upcoming_days: str | int | None = Query(default=DEFAULT_UPCOMING_DAYS, alias="days"),
) -> dict[str, Any]:
    normalized_date = _normalized_today_date(requested_date)
    normalized_days = _normalized_upcoming_days(upcoming_days)
    now = _utc_now_iso()
    window_end = (
        datetime.fromisoformat(now.replace("Z", "+00:00"))
        + timedelta(days=normalized_days)
    ).astimezone(UTC).isoformat().replace("+00:00", "Z")

    projection = get_command_runtime_service().get_projection(household_id)
    last_event_id, state_version, checksum = _shared_projection_cache_state(projection)
    projection_version = f"{last_event_id}:{state_version}:{checksum}"

    today_view = get_today_view(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        projection=projection,
    )
    conflicts = get_conflicts(
        household_id=household_id,
        projection_version=projection_version,
        projection=projection,
    )
    upcoming = get_upcoming(
        household_id=household_id,
        projection_version=projection_version,
        window_start=now,
        window_end=window_end,
        projection=projection,
        now=now,
        days=normalized_days,
    )
    overdue = get_overdue(
        household_id=household_id,
        projection_version=projection_version,
        projection=projection,
        now=now,
    )
    summary = get_summary(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        today_view=today_view,
        conflicts=conflicts,
        upcoming=upcoming,
        overdue=overdue,
    )

    return get_priority(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        summary=summary,
        overdue=overdue,
        conflicts=conflicts,
        today=today_view,
        upcoming=upcoming,
    )


@router.get("/household/{household_id}/actions")
async def get_household_actions_artifact(
    household_id: str,
    requested_date: str | None = Query(default=None, alias="date"),
    upcoming_days: str | int | None = Query(default=DEFAULT_UPCOMING_DAYS, alias="days"),
) -> dict[str, Any]:
    normalized_date = _normalized_today_date(requested_date)
    normalized_days = _normalized_upcoming_days(upcoming_days)
    now = _utc_now_iso()
    window_end = (
        datetime.fromisoformat(now.replace("Z", "+00:00"))
        + timedelta(days=normalized_days)
    ).astimezone(UTC).isoformat().replace("+00:00", "Z")

    projection = get_command_runtime_service().get_projection(household_id)
    last_event_id, state_version, checksum = _shared_projection_cache_state(projection)
    projection_version = f"{last_event_id}:{state_version}:{checksum}"

    today_view = get_today_view(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        projection=projection,
    )
    conflicts = get_conflicts(
        household_id=household_id,
        projection_version=projection_version,
        projection=projection,
    )
    upcoming = get_upcoming(
        household_id=household_id,
        projection_version=projection_version,
        window_start=now,
        window_end=window_end,
        projection=projection,
        now=now,
        days=normalized_days,
    )
    overdue = get_overdue(
        household_id=household_id,
        projection_version=projection_version,
        projection=projection,
        now=now,
    )
    summary = get_summary(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        today_view=today_view,
        conflicts=conflicts,
        upcoming=upcoming,
        overdue=overdue,
    )
    priority = get_priority(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        summary=summary,
        overdue=overdue,
        conflicts=conflicts,
        today=today_view,
        upcoming=upcoming,
    )

    return get_actions(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        priority=priority,
        summary=summary,
        today=today_view,
    )


@router.get("/household/{household_id}/execution-plans")
async def get_household_execution_plan_artifact(
    household_id: str,
    requested_date: str | None = Query(default=None, alias="date"),
    upcoming_days: str | int | None = Query(default=DEFAULT_UPCOMING_DAYS, alias="days"),
) -> dict[str, Any]:
    normalized_date = _normalized_today_date(requested_date)
    normalized_days = _normalized_upcoming_days(upcoming_days)
    now = _utc_now_iso()
    window_end = (
        datetime.fromisoformat(now.replace("Z", "+00:00"))
        + timedelta(days=normalized_days)
    ).astimezone(UTC).isoformat().replace("+00:00", "Z")

    projection = get_command_runtime_service().get_projection(household_id)
    last_event_id, state_version, checksum = _shared_projection_cache_state(projection)
    projection_version = f"{last_event_id}:{state_version}:{checksum}"

    today_view = get_today_view(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        projection=projection,
    )
    conflicts = get_conflicts(
        household_id=household_id,
        projection_version=projection_version,
        projection=projection,
    )
    upcoming = get_upcoming(
        household_id=household_id,
        projection_version=projection_version,
        window_start=now,
        window_end=window_end,
        projection=projection,
        now=now,
        days=normalized_days,
    )
    overdue = get_overdue(
        household_id=household_id,
        projection_version=projection_version,
        projection=projection,
        now=now,
    )
    summary = get_summary(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        today_view=today_view,
        conflicts=conflicts,
        upcoming=upcoming,
        overdue=overdue,
    )
    priority = get_priority(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        summary=summary,
        overdue=overdue,
        conflicts=conflicts,
        today=today_view,
        upcoming=upcoming,
    )

    actions = get_actions(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        priority=priority,
        summary=summary,
        today=today_view,
    )

    return get_execution_plans(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        actions=actions,
    )


@router.get("/household/{household_id}/loop")
async def get_household_loop_surface_view(
    household_id: str,
    requested_date: str | None = Query(default=None, alias="date"),
    upcoming_days: str | int | None = Query(default=DEFAULT_UPCOMING_DAYS, alias="days"),
) -> dict[str, Any]:
    normalized_date = _normalized_today_date(requested_date)
    normalized_days = _normalized_upcoming_days(upcoming_days)
    now = _utc_now_iso()
    window_end = (
        datetime.fromisoformat(now.replace("Z", "+00:00"))
        + timedelta(days=normalized_days)
    ).astimezone(UTC).isoformat().replace("+00:00", "Z")

    projection = get_command_runtime_service().get_projection(household_id)
    last_event_id, state_version, checksum = _shared_projection_cache_state(projection)
    projection_version = f"{last_event_id}:{state_version}:{checksum}"

    today_view = get_today_view(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        projection=projection,
    )
    conflicts = get_conflicts(
        household_id=household_id,
        projection_version=projection_version,
        projection=projection,
    )
    upcoming = get_upcoming(
        household_id=household_id,
        projection_version=projection_version,
        window_start=now,
        window_end=window_end,
        projection=projection,
        now=now,
        days=normalized_days,
    )
    overdue = get_overdue(
        household_id=household_id,
        projection_version=projection_version,
        projection=projection,
        now=now,
    )
    summary = get_summary(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        today_view=today_view,
        conflicts=conflicts,
        upcoming=upcoming,
        overdue=overdue,
    )
    priority = get_priority(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        summary=summary,
        overdue=overdue,
        conflicts=conflicts,
        today=today_view,
        upcoming=upcoming,
    )
    actions = get_actions(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        priority=priority,
        summary=summary,
        today=today_view,
    )
    execution_plans = get_execution_plans(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        actions=actions,
    )

    return get_household_loop_surface(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        today_view=today_view,
        conflicts=conflicts,
        upcoming=upcoming,
        overdue=overdue,
        summary=summary,
        priority=priority,
        actions=actions,
        execution_plans=execution_plans,
    )


@router.get("/household/{household_id}/home")
async def get_household_home_v0(
    household_id: str,
    requested_date: str | None = Query(default=None, alias="date"),
) -> dict[str, Any]:
    normalized_date = _normalized_today_date(requested_date)
    projection = get_command_runtime_service().get_projection(household_id)
    return _build_home_v0(
        household_id=household_id,
        target_date=normalized_date,
        projection=projection,
    )


@router.get("/home")
async def get_home_v0(
    household_id: str = Query(min_length=1),
    requested_date: str | None = Query(default=None, alias="date"),
) -> dict[str, Any]:
    normalized_date = _normalized_today_date(requested_date)
    projection = get_command_runtime_service().get_projection(household_id)
    return _build_home_v0(
        household_id=household_id,
        target_date=normalized_date,
        projection=projection,
    )


@router.post("/decision/complete")
async def decision_complete(body: Any = Body(default=None)) -> dict[str, Any]:
    payload = _require_body_object(body)
    household_id = _require_non_empty(payload, "household_id")
    decision_id = _require_non_empty(payload, "decision_id")
    return _dispatch_decision_command(
        command_type="decision.complete",
        payload={
            "household_id": household_id,
            "decision_id": decision_id,
        },
    )


@router.post("/decision/defer")
async def decision_defer(body: Any = Body(default=None)) -> dict[str, Any]:
    payload = _require_body_object(body)
    household_id = _require_non_empty(payload, "household_id")
    decision_id = _require_non_empty(payload, "decision_id")
    defer_to_date = _require_non_empty(payload, "defer_to_date")

    try:
        date.fromisoformat(defer_to_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="defer_to_date must be YYYY-MM-DD") from exc

    return _dispatch_decision_command(
        command_type="decision.defer",
        payload={
            "household_id": household_id,
            "decision_id": decision_id,
            "defer_to_date": defer_to_date,
        },
    )


@router.post("/decision/ignore")
async def decision_ignore(body: Any = Body(default=None)) -> dict[str, Any]:
    payload = _require_body_object(body)
    household_id = _require_non_empty(payload, "household_id")
    decision_id = _require_non_empty(payload, "decision_id")
    return _dispatch_decision_command(
        command_type="decision.ignore",
        payload={
            "household_id": household_id,
            "decision_id": decision_id,
        },
    )

@router.get("/household/{household_id}/decision")
async def get_household_decision_surface_view(
    household_id: str,
    requested_date: str | None = Query(default=None, alias="date"),
    upcoming_days: str | int | None = Query(default=DEFAULT_UPCOMING_DAYS, alias="days"),
) -> dict[str, Any]:
    normalized_date = _normalized_today_date(requested_date)
    projection = get_command_runtime_service().get_projection(household_id)
    last_event_id, state_version, checksum = _shared_projection_cache_state(projection)
    projection_version = f"{last_event_id}:{state_version}:{checksum}"

    loop_surface = await get_household_loop_surface_view(
        household_id=household_id,
        requested_date=normalized_date,
        upcoming_days=upcoming_days,
    )

    return get_household_decision_surface(
        household_id=household_id,
        projection_version=projection_version,
        date=normalized_date,
        loop_surface=loop_surface,
    )


@router.get("/household/{household_id}/validation/run")
async def run_household_validation_plan(
    household_id: str,
) -> dict[str, Any]:
    validation_plan = get_validation_plan(
        household_id=household_id,
        validation_plan_version=DEFAULT_VALIDATION_PLAN_VERSION,
    )
    return run_validation_plan(validation_plan)
