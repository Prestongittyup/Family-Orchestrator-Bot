from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any


RecurrenceType = str


@dataclass(frozen=True)
class ExternalEvent:
    event_id: str
    user_id: str
    provider_name: str
    event_type: str
    timestamp: str
    payload: dict[str, Any]


_SERIES_CANDIDATE_KEYS: tuple[str, ...] = (
    "recurringEventId",
    "recurring_event_id",
    "iCalUID",
    "iCalUid",
    "ical_uid",
    "series_id",
    "seriesId",
)


def _coerce_timestamp(row: dict[str, Any]) -> str:
    for key in ("timestamp", "start", "time", "date"):
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return ""


def _canonical_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _raw_google_event(payload: dict[str, Any]) -> dict[str, Any]:
    raw = payload.get("_raw_google_event")
    if isinstance(raw, dict):
        return raw
    return {}


def _payload_value(payload: dict[str, Any], key: str) -> str:
    direct = _coerce_str(payload.get(key))
    if direct:
        return direct
    raw = _raw_google_event(payload)
    return _coerce_str(raw.get(key))


def _extract_explicit_recurrence_source_id(payload: dict[str, Any]) -> str:
    return _payload_value(payload, "recurringEventId") or _payload_value(payload, "recurring_event_id")


def _extract_series_candidates(payload: dict[str, Any]) -> list[str]:
    candidates: list[str] = []
    for key in _SERIES_CANDIDATE_KEYS:
        value = _payload_value(payload, key)
        if value:
            candidates.append(value)

    # Fallback to provider-native IDs that can still reveal repeated series
    # identifiers in raw payloads.
    provider_event_id = _payload_value(payload, "event_id") or _payload_value(payload, "id")
    if provider_event_id:
        candidates.append(provider_event_id)

    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        ordered.append(candidate)
    return ordered


def _collect_repeated_series_candidates(raw_events: list[dict[str, Any]]) -> set[str]:
    counts: dict[str, int] = {}
    for row in raw_events:
        for candidate in _extract_series_candidates(row):
            counts[candidate] = counts.get(candidate, 0) + 1
    return {candidate for candidate, count in counts.items() if count > 1}


def _first_repeated_series_key(payload: dict[str, Any], repeated_candidates: set[str]) -> str:
    for candidate in _extract_series_candidates(payload):
        if candidate in repeated_candidates:
            return candidate
    return ""


def _extract_recurrence_rule_strings(payload: dict[str, Any]) -> list[str]:
    rules: list[str] = []
    recurrence = payload.get("recurrence")
    raw_recurrence = _raw_google_event(payload).get("recurrence")

    for source in (recurrence, raw_recurrence):
        if isinstance(source, str) and source.strip():
            rules.append(source)
        elif isinstance(source, list):
            for value in source:
                text = _coerce_str(value)
                if text:
                    rules.append(text)
    return rules


def _infer_recurrence_type(payload: dict[str, Any], *, is_recurring: bool) -> RecurrenceType:
    if not is_recurring:
        return "none"

    for rule in _extract_recurrence_rule_strings(payload):
        upper = rule.upper()
        if "FREQ=YEARLY" in upper:
            return "yearly"
        if "FREQ=MONTHLY" in upper:
            return "monthly"
        if "FREQ=WEEKLY" in upper:
            return "weekly"
    return "custom"


def _build_payload_with_recurrence_metadata(
    payload: dict[str, Any],
    *,
    is_recurring: bool,
    recurrence_source_id: str,
) -> dict[str, Any]:
    enriched = dict(payload)
    enriched["is_recurring"] = bool(is_recurring)
    enriched["recurrence_source_id"] = recurrence_source_id
    enriched["recurrence_type"] = _infer_recurrence_type(payload, is_recurring=is_recurring)
    return enriched


def _derive_event_id(
    *,
    user_id: str,
    provider_name: str,
    event_type: str,
    timestamp: str,
    payload: dict[str, Any],
) -> str:
    # Deterministic identifier derived from normalized fields and canonical payload.
    raw = "|".join(
        [
            str(user_id),
            str(provider_name),
            str(event_type),
            str(timestamp),
            _canonical_payload(payload),
        ]
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"ext-{digest[:24]}"


def _derive_series_event_id(
    *,
    user_id: str,
    provider_name: str,
    event_type: str,
    series_id: str,
) -> str:
    raw = "|".join(
        [
            str(user_id),
            str(provider_name),
            str(event_type),
            "series",
            str(series_id),
        ]
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"ext-{digest[:24]}"


def normalize_provider_event(
    *,
    user_id: str,
    provider_name: str,
    raw_event: dict[str, Any],
    event_type: str = "external_event",
) -> ExternalEvent:
    payload = dict(raw_event)
    recurrence_source_id = _extract_explicit_recurrence_source_id(payload)
    is_recurring = bool(recurrence_source_id)
    payload_with_meta = _build_payload_with_recurrence_metadata(
        payload,
        is_recurring=is_recurring,
        recurrence_source_id=recurrence_source_id,
    )
    timestamp = _coerce_timestamp(payload_with_meta)
    if is_recurring:
        event_id = _derive_series_event_id(
            user_id=user_id,
            provider_name=provider_name,
            event_type=event_type,
            series_id=recurrence_source_id,
        )
    else:
        event_id = _derive_event_id(
            user_id=user_id,
            provider_name=provider_name,
            event_type=event_type,
            timestamp=timestamp,
            payload=payload,
        )

    return ExternalEvent(
        event_id=event_id,
        user_id=str(user_id),
        provider_name=str(provider_name),
        event_type=str(event_type),
        timestamp=timestamp,
        payload=payload_with_meta,
    )


def normalize_provider_events(
    *,
    user_id: str,
    provider_name: str,
    raw_events: list[dict[str, Any]],
    event_type: str = "external_event",
) -> list[ExternalEvent]:
    rows = [row for row in raw_events if isinstance(row, dict)]
    repeated_series_candidates = _collect_repeated_series_candidates(rows)

    recurring_series_events: dict[str, ExternalEvent] = {}
    non_recurring_events: list[ExternalEvent] = []

    for row in rows:
        payload = dict(row)
        explicit_source_id = _extract_explicit_recurrence_source_id(payload)
        repeated_series_key = _first_repeated_series_key(payload, repeated_series_candidates)

        is_recurring = bool(explicit_source_id or repeated_series_key)
        recurrence_source_id = explicit_source_id
        series_key = explicit_source_id or repeated_series_key

        payload_with_meta = _build_payload_with_recurrence_metadata(
            payload,
            is_recurring=is_recurring,
            recurrence_source_id=recurrence_source_id,
        )
        timestamp = _coerce_timestamp(payload_with_meta)

        if is_recurring:
            event_id = _derive_series_event_id(
                user_id=user_id,
                provider_name=provider_name,
                event_type=event_type,
                series_id=series_key,
            )
        else:
            event_id = _derive_event_id(
                user_id=user_id,
                provider_name=provider_name,
                event_type=event_type,
                timestamp=timestamp,
                payload=payload,
            )

        event = ExternalEvent(
            event_id=event_id,
            user_id=str(user_id),
            provider_name=str(provider_name),
            event_type=str(event_type),
            timestamp=timestamp,
            payload=payload_with_meta,
        )

        if is_recurring:
            existing = recurring_series_events.get(series_key)
            if existing is None:
                recurring_series_events[series_key] = event
            else:
                current_rank = (
                    event.timestamp,
                    event.provider_name,
                    _canonical_payload(event.payload),
                )
                existing_rank = (
                    existing.timestamp,
                    existing.provider_name,
                    _canonical_payload(existing.payload),
                )
                if current_rank < existing_rank:
                    recurring_series_events[series_key] = event
        else:
            non_recurring_events.append(event)

    normalized = [*non_recurring_events, *recurring_series_events.values()]

    normalized.sort(
        key=lambda event: (
            event.timestamp,
            event.provider_name,
            event.event_id,
        )
    )
    return normalized
