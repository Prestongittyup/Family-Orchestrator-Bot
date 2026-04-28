from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any


ACTIVE_PAST_HOURS = 24
ACTIVE_FUTURE_DAYS = 90
ARCHIVE_OLD_EVENTS = False


class OrchestrationView(Enum):
    SHORT = 7
    MEDIUM = 30
    LONG = 90
    SHORT_TERM = 7
    MID_TERM = 30
    LONG_TERM = 90


def utc_now() -> datetime:
    return datetime.now(UTC)


def to_rfc3339(value: datetime) -> str:
    normalized = value.astimezone(UTC).replace(microsecond=0)
    return normalized.isoformat().replace("+00:00", "Z")


def parse_event_datetime(raw_value: str | None) -> datetime | None:
    if raw_value is None:
        return None

    candidate = str(raw_value).strip()
    if not candidate:
        return None

    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def get_time_window(view: OrchestrationView) -> tuple[datetime, datetime]:
    now = utc_now()
    time_min = now - timedelta(hours=ACTIVE_PAST_HOURS)

    if view in (OrchestrationView.SHORT, OrchestrationView.SHORT_TERM):
        time_max = now + timedelta(days=7)
    elif view in (OrchestrationView.MEDIUM, OrchestrationView.MID_TERM):
        time_max = now + timedelta(days=30)
    elif view in (OrchestrationView.LONG, OrchestrationView.LONG_TERM):
        time_max = now + timedelta(days=90)
    else:
        raise ValueError(f"Unsupported orchestration view: {view}")

    return time_min, time_max


def _archive_dropped_events(events: list[dict[str, Any]]) -> None:
    # Hook point for future cold-storage integration.
    _ = events


def _event_start(row: dict[str, Any]) -> datetime | None:
    return parse_event_datetime(row.get("timestamp") or row.get("start"))


def _event_end(row: dict[str, Any]) -> datetime | None:
    return parse_event_datetime(row.get("end_timestamp") or row.get("end") or row.get("timestamp") or row.get("start"))


def filter_events_to_window(
    events: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    time_min: datetime,
    time_max: datetime,
) -> list[dict[str, Any]]:
    reference_now = now or utc_now()
    hard_cutoff = reference_now - timedelta(hours=ACTIVE_PAST_HOURS)

    kept: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []

    for row in events:
        start_dt = _event_start(row)
        end_dt = _event_end(row)

        if start_dt is None or end_dt is None:
            dropped.append(row)
            continue

        if end_dt < hard_cutoff:
            dropped.append(row)
            continue

        if start_dt < time_min or start_dt > time_max:
            dropped.append(row)
            continue

        kept.append(row)

    if ARCHIVE_OLD_EVENTS and dropped:
        _archive_dropped_events(dropped)

    return kept


def prune_stale_events(
    events: list[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    reference_now = now or utc_now()
    earliest_end = reference_now - timedelta(hours=ACTIVE_PAST_HOURS)
    latest_start = reference_now + timedelta(days=ACTIVE_FUTURE_DAYS)

    kept: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []

    for row in events:
        start_dt = _event_start(row)
        end_dt = _event_end(row)

        if start_dt is None or end_dt is None:
            dropped.append(row)
            continue

        if end_dt >= earliest_end and start_dt <= latest_start:
            kept.append(row)
        else:
            dropped.append(row)

    if ARCHIVE_OLD_EVENTS and dropped:
        _archive_dropped_events(dropped)

    return kept