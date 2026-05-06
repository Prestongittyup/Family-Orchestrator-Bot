from __future__ import annotations

import hashlib
from types import MappingProxyType
from collections import OrderedDict
from typing import Any, Mapping, Sequence, cast


def safe_int(value: Any, *, default: int) -> int:
    if value is None:
        return default
    try:
        text = str(value).strip()
        if not text:
            return default
        return int(text)
    except (TypeError, ValueError):
        return default


def normalized_search(search: str | None) -> str | None:
    resolved_search = str(search or "").strip().lower()
    if not resolved_search:
        return None
    return resolved_search


def normalized_limit(limit: Any, *, default_limit: int, max_limit: int) -> int:
    resolved_limit = safe_int(limit, default=default_limit)
    if resolved_limit < 1:
        return default_limit
    return min(resolved_limit, max_limit)


def normalized_offset(offset: Any, *, default_offset: int = 0) -> int:
    resolved_offset = safe_int(offset, default=default_offset)
    return max(default_offset, resolved_offset)


def _normalized_text(value: Any) -> str:
    return str(value or "").strip()


def normalized_filter_state(filter_state: Mapping[str, Any] | None) -> str:
    if not filter_state:
        return ""

    normalized_items = []
    for key, value in sorted(filter_state.items(), key=lambda item: str(item[0])):
        normalized_items.append(f"{_normalized_text(key)}={_normalized_text(value)}")
    return "|".join(normalized_items)


def projection_cache_state(projection: Mapping[str, Any]) -> tuple[str, int, str]:
    return (
        _normalized_text(projection.get("last_event_id")),
        safe_int(projection.get("state_version"), default=0),
        _normalized_text(projection.get("checksum")),
    )


def query_cache_state(
    *,
    filter_state: Mapping[str, Any] | None,
    search: str | None,
    sort_by: str,
    order: str,
    limit: int,
    offset: int,
) -> tuple[str, str, str, str, int, int]:
    return (
        normalized_filter_state(filter_state),
        _normalized_text(search),
        _normalized_text(sort_by),
        _normalized_text(order),
        int(limit),
        int(offset),
    )


def sort_records_with_tie_break(
    records: Sequence[Mapping[str, Any]],
    *,
    sort_field: str,
    order: str,
    created_field: str,
    id_field: str,
    source_event_field: str = "source_event_id",
) -> list[Mapping[str, Any]]:
    descending = order == "desc"
    decorated: list[tuple[tuple[str, str, str, str], Mapping[str, Any]]] = []
    for record in records:
        decorated.append(
            (
                (
                    str(record.get(sort_field) or ""),
                    str(record.get(created_field) or ""),
                    str(record.get(source_event_field) or ""),
                    str(record.get(id_field) or ""),
                ),
                record,
            )
        )

    decorated.sort(key=lambda row: row[0], reverse=descending)
    return [record for _, record in decorated]


def paginate_records(
    records: Sequence[Mapping[str, Any]],
    *,
    limit: int,
    offset: int,
) -> list[Mapping[str, Any]]:
    return list(records[offset : offset + limit])


def cache_get[K, V](cache: OrderedDict[K, V], key: K) -> V | None:
    value = cache.get(key)
    if value is None:
        return None
    return value


def cache_set[K, V](
    cache: OrderedDict[K, V],
    key: K,
    value: V,
    *,
    max_entries: int,
) -> None:
    def _freeze(entry: Any) -> Any:
        if isinstance(entry, Mapping):
            return MappingProxyType({item_key: _freeze(item_value) for item_key, item_value in entry.items()})
        if isinstance(entry, tuple):
            return tuple(_freeze(item) for item in entry)
        if isinstance(entry, list):
            return tuple(_freeze(item) for item in entry)
        return entry

    frozen_value = cast(V, _freeze(value))
    cache[key] = frozen_value
    resolved_max_entries = max(1, int(max_entries))

    def _partition_token(cache_key: K) -> str:
        if isinstance(cache_key, tuple) and cache_key:
            return str(cache_key[0])
        return ""

    def _cache_key_rank(cache_key: K) -> tuple[str, str]:
        key_repr = repr(cache_key)
        digest = hashlib.sha256(f"{_partition_token(cache_key)}|{key_repr}".encode("utf-8")).hexdigest()
        return digest, key_repr

    partition_token = _partition_token(key)
    partition_keys = [
        cache_key
        for cache_key in cache.keys()
        if _partition_token(cache_key) == partition_token
    ]

    while len(partition_keys) > resolved_max_entries:
        eviction_key = max(partition_keys, key=_cache_key_rank)
        partition_keys = [cache_key for cache_key in partition_keys if cache_key != eviction_key]
        cache.pop(eviction_key, None)
