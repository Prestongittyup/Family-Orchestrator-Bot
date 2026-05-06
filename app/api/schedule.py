from __future__ import annotations

from collections import OrderedDict
from datetime import UTC, datetime
from typing import Any, Mapping, Sequence

from fastapi import APIRouter, Query

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
from app.services.commands import get_command_runtime_service


router = APIRouter(tags=["schedule"])

_SCHEDULE_STATUS_VALUES = frozenset({"scheduled", "cancelled"})
_SCHEDULE_SORT_FIELDS = frozenset({"start_at", "end_at", "created_at", "cancelled_at"})
_SCHEDULE_SORT_ORDERS = frozenset({"asc", "desc"})
_DEFAULT_PAGE_LIMIT = 25
_MAX_PAGE_LIMIT = 200
_DEFAULT_PAGE_OFFSET = 0
_MATERIALIZED_CACHE_MAX_ENTRIES = 128
_VIEW_CACHE_MAX_ENTRIES = 512

_MATERIALIZED_RECORDS_CACHE: OrderedDict[
    tuple[str, str, int, str],
    tuple[tuple[dict[str, Any], str], ...],
] = OrderedDict()
_VIEW_CACHE: OrderedDict[
    tuple[str, str, int, str, str, str, str, str, int, int],
    tuple[tuple[dict[str, Any], ...], tuple[int, int, int]],
] = OrderedDict()


def _safe_int(value: Any, *, default: int) -> int:
    return _shared_safe_int(value, default=default)


def _normalized_status_param(status: str | None) -> str | None:
    resolved_status = str(status or "").strip().lower()
    if resolved_status in _SCHEDULE_STATUS_VALUES:
        return resolved_status
    return None


def _normalized_sort_by_param(sort_by: str | None) -> str:
    resolved_sort_by = str(sort_by or "").strip().lower()
    if resolved_sort_by in _SCHEDULE_SORT_FIELDS:
        return resolved_sort_by
    return "start_at"


def _normalized_order_param(order: str | None) -> str:
    resolved_order = str(order or "").strip().lower()
    if resolved_order in _SCHEDULE_SORT_ORDERS:
        return resolved_order
    return "asc"


def _normalized_search_param(search: str | None) -> str | None:
    return _shared_normalized_search(search)


def _normalized_limit_param(limit: Any) -> int:
    return _shared_normalized_limit(
        limit,
        default_limit=_DEFAULT_PAGE_LIMIT,
        max_limit=_MAX_PAGE_LIMIT,
    )


def _normalized_offset_param(offset: Any) -> int:
    return _shared_normalized_offset(offset, default_offset=_DEFAULT_PAGE_OFFSET)


def _normalized_range_param(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None

    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _normalized_schedule_status(schedule: Mapping[str, Any]) -> str:
    status = str(schedule.get("status") or "").strip().lower()
    if status in _SCHEDULE_STATUS_VALUES:
        return status

    if schedule.get("cancelled_at"):
        return "cancelled"
    return "scheduled"


def _normalize_schedule_row(schedule: Mapping[str, Any]) -> dict[str, Any]:
    status = _normalized_schedule_status(schedule)
    created_at = str(schedule.get("created_at") or "").strip()
    cancelled_at_raw = schedule.get("cancelled_at")
    cancelled_at = str(cancelled_at_raw).strip() if cancelled_at_raw is not None else None

    if status != "cancelled":
        cancelled_at = None

    return {
        "schedule_id": str(schedule.get("schedule_id") or "").strip(),
        "title": str(schedule.get("title") or "").strip(),
        "start_at": str(schedule.get("start_at") or "").strip() or None,
        "end_at": str(schedule.get("end_at") or "").strip() or None,
        "status": status,
        "created_at": created_at,
        "cancelled_at": cancelled_at,
    }


def _sorted_schedule_entries(
    schedules: Sequence[Mapping[str, Any]],
    *,
    sort_by: str,
    order: str,
) -> list[Mapping[str, Any]]:
    resolved_sort_by = _normalized_sort_by_param(sort_by)
    resolved_order = _normalized_order_param(order)
    return _shared_sort_records_with_tie_break(
        schedules,
        sort_field=resolved_sort_by,
        order=resolved_order,
        created_field="created_at",
        id_field="schedule_id",
    )


def _paginated_schedule_entries(
    schedules: Sequence[Mapping[str, Any]],
    *,
    limit: int,
    offset: int,
) -> list[Mapping[str, Any]]:
    return _shared_paginate_records(schedules, limit=limit, offset=offset)


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
    projection_schedule: list[Mapping[str, Any]],
) -> tuple[tuple[dict[str, Any], str], ...]:
    cached = _cache_get(_MATERIALIZED_RECORDS_CACHE, projection_fingerprint)
    if cached is not None:
        return cached

    built = tuple(_materialized_schedule_records(projection_schedule))
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
        int(summary.get("scheduled") or 0),
        int(summary.get("cancelled") or 0),
    )


def _summary_from_counts(counts: tuple[int, int, int]) -> dict[str, int]:
    return {
        "total": int(counts[0]),
        "scheduled": int(counts[1]),
        "cancelled": int(counts[2]),
    }


def _get_or_build_sorted_view(
    *,
    projection_fingerprint: tuple[str, str, int, str],
    records: tuple[tuple[dict[str, Any], str], ...],
    status: str | None,
    search: str | None,
    start_from: str | None,
    end_to: str | None,
    sort_by: str,
    order: str,
    limit: int,
    offset: int,
) -> tuple[tuple[dict[str, Any], ...], tuple[int, int, int]]:
    query_state = _shared_query_cache_state(
        filter_state={
            "status": str(status or ""),
            "start_from": str(start_from or ""),
            "end_to": str(end_to or ""),
        },
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
        start_from=start_from,
        end_to=end_to,
    )
    sorted_schedule = tuple(
        _sorted_schedule_entries(
            filtered,
            sort_by=sort_by,
            order=order,
        )
    )
    summary_counts = _summary_to_counts(summary)
    built_view = (sorted_schedule, summary_counts)
    _cache_set(
        _VIEW_CACHE,
        view_key,
        built_view,
        max_entries=_VIEW_CACHE_MAX_ENTRIES,
    )
    return built_view


def _materialized_schedule_records(schedules: list[Mapping[str, Any]]) -> list[tuple[dict[str, Any], str]]:
    records: list[tuple[dict[str, Any], str]] = []
    for schedule in schedules:
        normalized = _normalize_schedule_row(schedule)
        search_blob = str(normalized.get("title") or "").lower()
        records.append((normalized, search_blob))
    return records


def _filtered_records_with_summary(
    records: Sequence[tuple[dict[str, Any], str]],
    *,
    status: str | None,
    search: str | None,
    start_from: str | None,
    end_to: str | None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    matched: list[dict[str, Any]] = []
    summary = {
        "total": 0,
        "scheduled": 0,
        "cancelled": 0,
    }

    for schedule, search_blob in records:
        schedule_status = str(schedule.get("status") or "")
        if status is not None and schedule_status != status:
            continue
        if search is not None and search not in search_blob:
            continue

        schedule_start_at = str(schedule.get("start_at") or "")
        if start_from is not None:
            if not schedule_start_at or schedule_start_at < start_from:
                continue
        if end_to is not None:
            if not schedule_start_at or schedule_start_at > end_to:
                continue

        matched.append(schedule)
        summary["total"] += 1
        if schedule_status == "cancelled":
            summary["cancelled"] += 1
        else:
            summary["scheduled"] += 1

    return matched, summary


@router.get("/schedule")
async def get_schedule(
    household_id: str = Query(..., min_length=1),
    status: str | None = Query(default=None),
    start_from: str | None = Query(default=None),
    end_to: str | None = Query(default=None),
    sort_by: str = Query(default="start_at"),
    order: str = Query(default="asc"),
    limit: str | int | None = Query(default=_DEFAULT_PAGE_LIMIT),
    offset: str | int | None = Query(default=_DEFAULT_PAGE_OFFSET),
    search: str | None = Query(default=None),
) -> dict[str, Any]:
    normalized_status = _normalized_status_param(status)
    normalized_start_from = _normalized_range_param(start_from)
    normalized_end_to = _normalized_range_param(end_to)
    normalized_sort_by = _normalized_sort_by_param(sort_by)
    normalized_order = _normalized_order_param(order)
    normalized_search = _normalized_search_param(search)
    normalized_limit = _normalized_limit_param(limit)
    normalized_offset = _normalized_offset_param(offset)

    projection = get_command_runtime_service().get_projection(household_id)
    projection_schedule_raw = projection.get("schedule_list")
    if not isinstance(projection_schedule_raw, list):
        projection_schedule_raw = []

    projection_schedule = [row for row in projection_schedule_raw if isinstance(row, Mapping)]
    projection_fingerprint = _projection_fingerprint(
        household_id=household_id,
        projection=projection,
    )
    materialized_records = _get_or_build_materialized_records(
        projection_fingerprint=projection_fingerprint,
        projection_schedule=projection_schedule,
    )

    # CONTRACT: This endpoint is considered stable.
    # Behavior changes must be validated against regression tests.
    # Do not modify without explicit contract revision.
    # Strict read-model pipeline: projection -> normalize -> filter -> search -> sort -> tie-break -> paginate -> response.
    sorted_schedule, summary_counts = _get_or_build_sorted_view(
        projection_fingerprint=projection_fingerprint,
        records=materialized_records,
        status=normalized_status,
        search=normalized_search,
        start_from=normalized_start_from,
        end_to=normalized_end_to,
        sort_by=normalized_sort_by,
        order=normalized_order,
        limit=normalized_limit,
        offset=normalized_offset,
    )
    summary = _summary_from_counts(summary_counts)

    paginated_schedule = _paginated_schedule_entries(
        sorted_schedule,
        limit=normalized_limit,
        offset=normalized_offset,
    )
    paginated_payload = [dict(schedule) for schedule in paginated_schedule]

    pagination = {
        "limit": normalized_limit,
        "offset": normalized_offset,
        "returned": len(paginated_payload),
    }

    return {
        "schedules": paginated_payload,
        "summary": summary,
        "pagination": pagination,
    }
