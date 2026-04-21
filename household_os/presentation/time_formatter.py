from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta


_DATE_TIME_RANGE_PATTERN = re.compile(
    r"(?P<date>\d{4}-\d{2}-\d{2})\s+from\s+(?P<start>\d{2}:\d{2})\s+to\s+(?P<end>\d{2}:\d{2})",
    re.IGNORECASE,
)


def _coerce_datetime(value: str | datetime | None) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str) and value:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    return datetime.now(UTC)


def _time_of_day_label(hour: int) -> str:
    if 5 <= hour < 9:
        return "morning"
    if 9 <= hour < 12:
        return "late morning"
    if 12 <= hour < 17:
        return "afternoon"
    if 17 <= hour < 21:
        return "evening"
    return "early morning"


def _display_time(value: datetime) -> str:
    return value.strftime("%I:%M %p").lstrip("0")


def format_relative_datetime(*, target: str | datetime, reference: str | datetime) -> str:
    target_dt = _coerce_datetime(target)
    reference_dt = _coerce_datetime(reference)

    # If a candidate is already in the past for the same day, roll it to tomorrow for actionable phrasing.
    effective = target_dt
    if target_dt.date() == reference_dt.date() and target_dt <= reference_dt:
        effective = target_dt + timedelta(days=1)

    day_delta = (effective.date() - reference_dt.date()).days
    if day_delta == 0:
        day_phrase = "today"
    elif day_delta == 1:
        day_phrase = "tomorrow"
    elif 1 < day_delta <= 7:
        day_phrase = effective.strftime("%A")
    else:
        day_phrase = effective.strftime("%A")

    return f"{day_phrase} {_time_of_day_label(effective.hour)} at {_display_time(effective)}"


def extract_and_format_relative_time(text: str, *, reference_time: str | datetime | None = None) -> tuple[str, str | None]:
    match = _DATE_TIME_RANGE_PATTERN.search(text)
    if not match:
        return text, None

    reference = _coerce_datetime(reference_time)
    target_dt = datetime.fromisoformat(f"{match.group('date')}T{match.group('start')}:00+00:00").astimezone(UTC)
    relative = format_relative_datetime(target=target_dt, reference=reference)
    rewritten = _DATE_TIME_RANGE_PATTERN.sub(relative, text, count=1)
    return rewritten, relative
