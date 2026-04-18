"""
Lightweight time normalization utility for flexible user input formats.

Supports:
  - HH:MM format (e.g., "11:30", "14:45")
  - Named time blocks ("morning", "afternoon", "evening")
  - Natural aliases ("after lunch", "tonight")

All outputs normalized to ISO 8601 datetime strings using system date.
Raw input preserved for traceability.

Deterministic: same input + same reference_date → same normalized output.
"""

from __future__ import annotations

from datetime import datetime, time
from typing import Optional


# Deterministic default times for named blocks
_TIME_BLOCK_DEFAULTS = {
    "morning": time(9, 0),        # 09:00
    "afternoon": time(14, 0),     # 14:00
    "evening": time(18, 30),      # 18:30
}

# Deterministic aliases
_TIME_ALIASES = {
    "after lunch": time(13, 0),   # 13:00
    "tonight": time(19, 0),       # 19:00
}


def normalize_time_input(
    user_input: str | None,
    reference_date: datetime | None = None,
) -> str | None:
    """
    Normalize flexible time input into ISO 8601 datetime string.

    Args:
        user_input: Raw user input (HH:MM, named block, or alias).
                   If None or empty, returns None.
        reference_date: Date to use for combining with normalized time.
                       Defaults to today (system date).

    Returns:
        ISO 8601 datetime string if successful, None if input invalid/empty.

    Examples:
        >>> normalize_time_input("11:30")
        "2026-04-16T11:30:00"  # (assuming reference_date is 2026-04-16)

        >>> normalize_time_input("morning")
        "2026-04-16T09:00:00"

        >>> normalize_time_input("after lunch")
        "2026-04-16T13:00:00"

        >>> normalize_time_input(None)
        None

        >>> normalize_time_input("")
        None
    """
    if not user_input:
        return None

    user_input_clean = str(user_input).strip().lower()
    if not user_input_clean:
        return None

    if reference_date is None:
        reference_date = datetime.now()

    # Ensure reference_date is a datetime (not just a date)
    if isinstance(reference_date, datetime):
        ref_dt = reference_date
    else:
        ref_dt = datetime.combine(reference_date, time(0, 0))

    # 1. Try HH:MM format
    if ":" in user_input_clean:
        try:
            parts = user_input_clean.split(":")
            if len(parts) == 2:
                hour = int(parts[0].strip())
                minute = int(parts[1].strip())
                if 0 <= hour < 24 and 0 <= minute < 60:
                    normalized_time = time(hour, minute)
                    result_dt = datetime.combine(ref_dt.date(), normalized_time)
                    return result_dt.isoformat()
        except (ValueError, IndexError):
            pass

    # 2. Try exact aliases
    if user_input_clean in _TIME_ALIASES:
        normalized_time = _TIME_ALIASES[user_input_clean]
        result_dt = datetime.combine(ref_dt.date(), normalized_time)
        return result_dt.isoformat()

    # 3. Try named time blocks
    if user_input_clean in _TIME_BLOCK_DEFAULTS:
        normalized_time = _TIME_BLOCK_DEFAULTS[user_input_clean]
        result_dt = datetime.combine(ref_dt.date(), normalized_time)
        return result_dt.isoformat()

    # 4. Try exact datetime parsing (ISO format or common formats)
    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%H:%M"]:
        try:
            parsed = datetime.strptime(user_input_clean, fmt)
            if fmt == "%H:%M":
                result_dt = datetime.combine(ref_dt.date(), parsed.time())
            else:
                result_dt = parsed
            return result_dt.isoformat()
        except ValueError:
            pass

    # 5. If all parsing fails, return None
    return None


def get_time_block_from_iso(iso_datetime_str: str | None) -> str | None:
    """
    Reverse-lookup: given an ISO datetime string, return the time block it belongs to
    ("morning", "afternoon", "evening") or the specific alias.

    Useful for rendering and diagnostics.

    Args:
        iso_datetime_str: ISO 8601 datetime string.

    Returns:
        Time block name ("morning", "afternoon", "evening") or None if not a standard block.
    """
    if not iso_datetime_str:
        return None

    try:
        dt = datetime.fromisoformat(iso_datetime_str)
        hour = dt.hour
        minute = dt.minute

        # Check exact aliases first
        for alias, alias_time in _TIME_ALIASES.items():
            if hour == alias_time.hour and minute == alias_time.minute:
                return alias

        # Check time blocks
        for block_name, block_time in _TIME_BLOCK_DEFAULTS.items():
            if hour == block_time.hour and minute == block_time.minute:
                return block_name

        # Out of range for standard blocks
        return None
    except (ValueError, AttributeError):
        return None


def list_time_aliases() -> dict[str, str]:
    """Return dict of all supported time aliases and their normalized times."""
    result = {}
    for alias, t in _TIME_ALIASES.items():
        result[alias] = f"{t.hour:02d}:{t.minute:02d}"
    for block, t in _TIME_BLOCK_DEFAULTS.items():
        result[block] = f"{t.hour:02d}:{t.minute:02d}"
    return result
