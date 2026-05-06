from __future__ import annotations

from collections import OrderedDict
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


router = APIRouter(tags=["notifications"])

_DELIVERY_STATUS_VALUES = frozenset({"pending", "delivered"})
_SORT_FIELDS = frozenset({"created_at"})
_SORT_ORDERS = frozenset({"asc", "desc"})
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
    tuple[tuple[dict[str, Any], ...], tuple[int, int]],
] = OrderedDict()


def _safe_int(value: Any, *, default: int) -> int:
    return _shared_safe_int(value, default=default)


def _normalized_delivery_status_param(delivery_status: str | None) -> str | None:
    resolved_status = str(delivery_status or "").strip().lower()
    if resolved_status in _DELIVERY_STATUS_VALUES:
        return resolved_status
    return None


def _normalized_source_type_param(source_type: str | None) -> str | None:
    resolved_source = str(source_type or "").strip().lower()
    if not resolved_source:
        return None
    return resolved_source


def _normalized_sort_by_param(sort_by: str | None) -> str:
    resolved_sort_by = str(sort_by or "").strip().lower()
    if resolved_sort_by in _SORT_FIELDS:
        return resolved_sort_by
    return "created_at"


def _normalized_order_param(order: str | None) -> str:
    resolved_order = str(order or "").strip().lower()
    if resolved_order in _SORT_ORDERS:
        return resolved_order
    return "desc"


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


def _normalize_notification_row(notification: Mapping[str, Any]) -> dict[str, Any]:
    delivery_status = str(notification.get("delivery_status") or "").strip().lower()
    if delivery_status not in _DELIVERY_STATUS_VALUES:
        delivery_status = "pending"

    return {
        "notification_id": str(notification.get("notification_id") or "").strip(),
        "source_event_id": str(notification.get("source_event_id") or "").strip(),
        "source_type": str(notification.get("source_type") or "").strip().lower() or None,
        "source_id": str(notification.get("source_id") or "").strip() or None,
        "message": str(notification.get("message") or "").strip(),
        "created_at": str(notification.get("created_at") or "").strip(),
        "delivery_status": delivery_status,
    }


def _sorted_notifications(
    notifications: Sequence[Mapping[str, Any]],
    *,
    sort_by: str,
    order: str,
) -> list[Mapping[str, Any]]:
    resolved_sort_by = _normalized_sort_by_param(sort_by)
    resolved_order = _normalized_order_param(order)
    return _shared_sort_records_with_tie_break(
        notifications,
        sort_field=resolved_sort_by,
        order=resolved_order,
        created_field="created_at",
        id_field="notification_id",
    )


def _paginated_notifications(
    notifications: Sequence[Mapping[str, Any]],
    *,
    limit: int,
    offset: int,
) -> list[Mapping[str, Any]]:
    return _shared_paginate_records(notifications, limit=limit, offset=offset)


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
    projection_notifications: list[Mapping[str, Any]],
) -> tuple[tuple[dict[str, Any], str], ...]:
    cached = _cache_get(_MATERIALIZED_RECORDS_CACHE, projection_fingerprint)
    if cached is not None:
        return cached

    built = tuple(_materialized_notification_records(projection_notifications))
    _cache_set(
        _MATERIALIZED_RECORDS_CACHE,
        projection_fingerprint,
        built,
        max_entries=_MATERIALIZED_CACHE_MAX_ENTRIES,
    )
    return built


def _summary_to_counts(summary: Mapping[str, int]) -> tuple[int, int]:
    return (
        int(summary.get("total") or 0),
        int(summary.get("pending") or 0),
    )


def _summary_from_counts(counts: tuple[int, int]) -> dict[str, int]:
    return {
        "total": int(counts[0]),
        "pending": int(counts[1]),
    }


def _get_or_build_sorted_view(
    *,
    projection_fingerprint: tuple[str, str, int, str],
    records: tuple[tuple[dict[str, Any], str], ...],
    delivery_status: str | None,
    source_type: str | None,
    search: str | None,
    sort_by: str,
    order: str,
    limit: int,
    offset: int,
) -> tuple[tuple[dict[str, Any], ...], tuple[int, int]]:
    query_state = _shared_query_cache_state(
        filter_state={
            "delivery_status": str(delivery_status or ""),
            "source_type": str(source_type or ""),
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
        delivery_status=delivery_status,
        source_type=source_type,
        search=search,
    )
    sorted_notifications = tuple(
        _sorted_notifications(
            filtered,
            sort_by=sort_by,
            order=order,
        )
    )
    summary_counts = _summary_to_counts(summary)
    built_view = (sorted_notifications, summary_counts)
    _cache_set(
        _VIEW_CACHE,
        view_key,
        built_view,
        max_entries=_VIEW_CACHE_MAX_ENTRIES,
    )
    return built_view


def _materialized_notification_records(
    notifications: list[Mapping[str, Any]],
) -> list[tuple[dict[str, Any], str]]:
    records: list[tuple[dict[str, Any], str]] = []
    for notification in notifications:
        normalized = _normalize_notification_row(notification)
        search_blob = "\n".join(
            [
                str(normalized.get("message") or "").lower(),
                str(normalized.get("source_type") or "").lower(),
            ]
        )
        records.append((normalized, search_blob))
    return records


def _filtered_records_with_summary(
    records: Sequence[tuple[dict[str, Any], str]],
    *,
    delivery_status: str | None,
    source_type: str | None,
    search: str | None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    matched: list[dict[str, Any]] = []
    summary = {
        "total": 0,
        "pending": 0,
    }

    for notification, search_blob in records:
        item_status = str(notification.get("delivery_status") or "")
        item_source = str(notification.get("source_type") or "")
        if delivery_status is not None and item_status != delivery_status:
            continue
        if source_type is not None and item_source != source_type:
            continue
        if search is not None and search not in search_blob:
            continue

        matched.append(notification)
        summary["total"] += 1
        if item_status == "pending":
            summary["pending"] += 1

    return matched, summary


@router.get("/notifications")
async def get_notifications(
    household_id: str = Query(..., min_length=1),
    delivery_status: str | None = Query(default=None),
    source_type: str | None = Query(default=None),
    sort_by: str = Query(default="created_at"),
    order: str = Query(default="desc"),
    limit: str | int | None = Query(default=_DEFAULT_PAGE_LIMIT),
    offset: str | int | None = Query(default=_DEFAULT_PAGE_OFFSET),
    search: str | None = Query(default=None),
) -> dict[str, Any]:
    normalized_delivery_status = _normalized_delivery_status_param(delivery_status)
    normalized_source_type = _normalized_source_type_param(source_type)
    normalized_sort_by = _normalized_sort_by_param(sort_by)
    normalized_order = _normalized_order_param(order)
    normalized_search = _normalized_search_param(search)
    normalized_limit = _normalized_limit_param(limit)
    normalized_offset = _normalized_offset_param(offset)

    projection = get_command_runtime_service().get_projection(household_id)
    projection_notifications_raw = projection.get("notification_list")
    if not isinstance(projection_notifications_raw, list):
        projection_notifications_raw = []

    projection_notifications = [
        notification
        for notification in projection_notifications_raw
        if isinstance(notification, Mapping)
    ]
    projection_fingerprint = _projection_fingerprint(
        household_id=household_id,
        projection=projection,
    )
    materialized_records = _get_or_build_materialized_records(
        projection_fingerprint=projection_fingerprint,
        projection_notifications=projection_notifications,
    )

    # CONTRACT: This endpoint is considered stable.
    # Behavior changes must be validated against regression tests.
    # Do not modify without explicit contract revision.
    # Strict read-model pipeline: projection -> normalize -> filter -> search -> sort -> tie-break -> paginate -> response.
    sorted_notifications, summary_counts = _get_or_build_sorted_view(
        projection_fingerprint=projection_fingerprint,
        records=materialized_records,
        delivery_status=normalized_delivery_status,
        source_type=normalized_source_type,
        search=normalized_search,
        sort_by=normalized_sort_by,
        order=normalized_order,
        limit=normalized_limit,
        offset=normalized_offset,
    )
    summary = _summary_from_counts(summary_counts)

    paginated_notifications = _paginated_notifications(
        sorted_notifications,
        limit=normalized_limit,
        offset=normalized_offset,
    )
    paginated_payload = [dict(notification) for notification in paginated_notifications]

    pagination = {
        "limit": normalized_limit,
        "offset": normalized_offset,
        "returned": len(paginated_payload),
    }

    return {
        "notifications": paginated_payload,
        "summary": summary,
        "pagination": pagination,
    }
