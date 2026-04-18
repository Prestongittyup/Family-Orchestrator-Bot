from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta
from typing import Any

from modules.core.services.contract_registry import (
    validate_decision_input_contract,
    validate_decision_output_contract,
)


TIME_BUCKETS = {
    "morning": 4,
    "afternoon": 4,
    "evening": 3,
}

DAY_START = 8
DAY_END = 21
PLANNING_HORIZON_DAYS = 3

EFFORT_WEIGHT = {
    "low": 1.0,
    "medium": 0.9,
    "high": 0.75,
}


def _parse_numeric_signal_value(message: Any) -> int:
    if not isinstance(message, str):
        return 0
    match = re.search(r"=(\d+)", message)
    if not match:
        return 0
    return int(match.group(1))


def _parse_time_window(description: Any) -> tuple[datetime | None, datetime | None]:
    if not isinstance(description, str):
        return None, None

    match = re.search(r"time_window=([^;]+)", description)
    if not match:
        return None, None

    value = match.group(1).strip()
    if value == "none" or "->" not in value:
        return None, None

    start_raw, end_raw = value.split("->", 1)

    try:
        start_dt = datetime.fromisoformat(start_raw.strip().replace("Z", "+00:00"))
        if start_dt.tzinfo is not None:
            start_dt = start_dt.astimezone(UTC).replace(tzinfo=None)
    except Exception:
        start_dt = None

    try:
        end_dt = datetime.fromisoformat(end_raw.strip().replace("Z", "+00:00"))
        if end_dt.tzinfo is not None:
            end_dt = end_dt.astimezone(UTC).replace(tzinfo=None)
    except Exception:
        end_dt = None

    return start_dt, end_dt


def _parse_priority_hint(proposal: dict[str, Any]) -> float:
    normalized_priority = float(proposal.get("normalized_priority", proposal.get("priority", 3.0)))
    if normalized_priority <= 0:
        return 0.0

    # Clamp to a 0..1 range while preserving relative ordering.
    if normalized_priority >= 10:
        return 1.0
    return normalized_priority / 10.0


def _urgency_score(start_dt: datetime | None, now: datetime) -> float:
    if start_dt is None:
        return 0.30

    hours = (start_dt - now).total_seconds() / 3600.0
    if hours <= 0:
        return 1.00
    if hours <= 2:
        return 0.95
    if hours <= 6:
        return 0.85
    if hours <= 12:
        return 0.75
    if hours <= 24:
        return 0.65
    if hours <= 48:
        return 0.50
    if hours <= 168:
        return 0.35
    return 0.20


def _context_score(
    proposal: dict[str, Any],
    events_today_count: int,
    high_priority_events_count: int,
) -> float:
    source_module = str(proposal.get("source_module", ""))
    proposal_type = str(proposal.get("type", ""))

    score = 0.20

    if events_today_count > 0:
        if source_module == "calendar_module" or "event" in proposal_type:
            score += 0.45
        else:
            score += 0.20

    if high_priority_events_count > 0:
        if proposal_type in {"prioritize_event", "leave_buffer_time", "prepare_for_event"}:
            score += 0.35
        elif source_module == "task_module":
            score += 0.15
        else:
            score += 0.10

    return min(1.0, score)


def _default_duration_units(proposal: dict[str, Any]) -> int:
    if proposal.get("duration") is not None:
        try:
            return max(1, int(proposal.get("duration", 1)))
        except Exception:
            return 1

    description = str(proposal.get("description", ""))
    match = re.search(r"duration_units=(\d+)", description)
    if not match:
        return 1

    value = int(match.group(1))
    return max(1, value)


def _effort_weight(proposal: dict[str, Any]) -> float:
    effort = str(proposal.get("effort", "medium")).strip().lower()
    return float(EFFORT_WEIGHT.get(effort, EFFORT_WEIGHT["medium"]))


def _bucket_from_start(start_dt: datetime | None) -> str:
    if start_dt is None:
        return "afternoon"
    hour = start_dt.hour
    if hour < 12:
        return "morning"
    if hour < 18:
        return "afternoon"
    return "evening"


def _ranges_overlap(
    start_a: datetime,
    end_a: datetime,
    start_b: datetime,
    end_b: datetime,
) -> bool:
    return start_a < end_b and start_b < end_a


def _normalize_calendar_events(
    payload_events: list[dict[str, Any]],
    proposals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []

    for row in payload_events:
        start = row.get("start_time") or row.get("start")
        end = row.get("end_time") or row.get("end")
        if start is None or end is None:
            continue
        start_dt, _ = _parse_time_window(f"time_window={start}->{end}")
        _, end_dt = _parse_time_window(f"time_window={start}->{end}")
        if start_dt is None or end_dt is None or not (start_dt < end_dt):
            continue
        events.append(
            {
                "start_time": start_dt.isoformat(),
                "end_time": end_dt.isoformat(),
                "source": str(row.get("source", "calendar_events")),
            }
        )

    # Fallback derivation from calendar-module proposal time windows when explicit events are absent.
    if not events:
        seen: set[tuple[str, str]] = set()
        for proposal in proposals:
            if str(proposal.get("source_module", "")) != "calendar_module":
                continue
            start_dt, end_dt = _parse_time_window(proposal.get("description"))
            if start_dt is None or end_dt is None or not (start_dt < end_dt):
                continue
            key = (start_dt.isoformat(), end_dt.isoformat())
            if key in seen:
                continue
            seen.add(key)
            events.append(
                {
                    "start_time": key[0],
                    "end_time": key[1],
                    "source": "calendar_module",
                }
            )

    events.sort(key=lambda row: (str(row.get("start_time", "")), str(row.get("end_time", ""))))
    return events


def _select_schedule_date(
    scored: list[dict[str, Any]],
    calendar_events: list[dict[str, Any]],
) -> date:
    dates: list[date] = []
    for row in calendar_events:
        start_dt, _ = _parse_time_window(f"time_window={row['start_time']}->{row['end_time']}")
        if start_dt is not None:
            dates.append(start_dt.date())

    for row in scored:
        if row.get("start_dt") is not None:
            dates.append(row["start_dt"].date())

    if dates:
        return min(dates)
    return datetime.utcnow().date()


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, DAY_START, 0, 0)
    end = datetime(day.year, day.month, day.day, DAY_END, 0, 0)
    return start, end


def _find_first_available_slot(
    duration_units: int,
    day_start: datetime,
    day_end: datetime,
    blocked_ranges: list[tuple[datetime, datetime]],
    taken_ranges: list[tuple[datetime, datetime]],
) -> tuple[datetime, datetime] | None:
    if duration_units <= 0:
        return None

    duration = timedelta(hours=duration_units)
    candidate = day_start
    latest_start = day_end - duration
    while candidate <= latest_start:
        end = candidate + duration
        blocked_conflict = any(_ranges_overlap(candidate, end, b_start, b_end) for b_start, b_end in blocked_ranges)
        taken_conflict = any(_ranges_overlap(candidate, end, t_start, t_end) for t_start, t_end in taken_ranges)
        if not blocked_conflict and not taken_conflict:
            return candidate, end
        candidate += timedelta(hours=1)

    return None


def _bucket_bounds(day_start: datetime, bucket: str) -> tuple[datetime, datetime]:
    if bucket == "morning":
        return day_start, day_start.replace(hour=12)
    if bucket == "afternoon":
        return day_start.replace(hour=12), day_start.replace(hour=18)
    return day_start.replace(hour=18), day_start.replace(hour=DAY_END)


def _filter_ranges_for_interval(
    ranges: list[tuple[datetime, datetime]],
    interval_start: datetime,
    interval_end: datetime,
) -> list[tuple[datetime, datetime]]:
    clipped: list[tuple[datetime, datetime]] = []
    for start_dt, end_dt in ranges:
        if end_dt <= interval_start or start_dt >= interval_end:
            continue
        start_clip = max(start_dt, interval_start)
        end_clip = min(end_dt, interval_end)
        if start_clip < end_clip:
            clipped.append((start_clip, end_clip))
    clipped.sort(key=lambda row: (row[0], row[1]))
    return clipped


def _merge_ranges(ranges: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    if not ranges:
        return []
    ordered = sorted(ranges, key=lambda row: (row[0], row[1]))
    merged: list[tuple[datetime, datetime]] = [ordered[0]]
    for cur_start, cur_end in ordered[1:]:
        prev_start, prev_end = merged[-1]
        if cur_start <= prev_end:
            merged[-1] = (prev_start, max(prev_end, cur_end))
        else:
            merged.append((cur_start, cur_end))
    return merged


def _blocked_hours(day_start: datetime, day_end: datetime, blocked_ranges: list[tuple[datetime, datetime]]) -> float:
    total = 0.0
    for start_dt, end_dt in _merge_ranges(blocked_ranges):
        if end_dt <= day_start or start_dt >= day_end:
            continue
        start_clip = max(start_dt, day_start)
        end_clip = min(end_dt, day_end)
        if start_clip < end_clip:
            total += (end_clip - start_clip).total_seconds() / 3600.0
    return total


def _schedule_load_hours(rows: list[dict[str, Any]]) -> float:
    return float(sum(int(row.get("duration_units", 1)) for row in rows))


def _bucket_capacity_hours(
    day_start: datetime,
    blocked_ranges: list[tuple[datetime, datetime]],
    bucket: str,
) -> float:
    bucket_start, bucket_end = _bucket_bounds(day_start, bucket)
    blocked = _blocked_hours(
        bucket_start,
        bucket_end,
        _filter_ranges_for_interval(blocked_ranges, bucket_start, bucket_end),
    )
    return max(0.0, (bucket_end - bucket_start).total_seconds() / 3600.0 - blocked)


def _bucket_used_hours(rows: list[dict[str, Any]], bucket: str) -> float:
    return float(sum(int(row.get("duration_units", 1)) for row in rows if str(row.get("bucket", "")) == bucket))


def _rejection_diagnostics(
    candidate: dict[str, Any],
    day_start: datetime,
    day_end: datetime,
    blocked_ranges: list[tuple[datetime, datetime]],
    taken_ranges: list[tuple[datetime, datetime]],
) -> tuple[str, list[str]]:
    duration_units = int(candidate.get("duration_units", 1))
    available_with_calendar_only = _find_first_available_slot(
        duration_units=duration_units,
        day_start=day_start,
        day_end=day_end,
        blocked_ranges=blocked_ranges,
        taken_ranges=[],
    )

    if available_with_calendar_only is not None:
        return "lower_priority_than_filled_slots", ["lower_priority_than_filled_slots"]

    day_capacity = max(0.0, float(DAY_END - DAY_START) - _blocked_hours(day_start, day_end, blocked_ranges))
    if day_capacity < float(duration_units):
        return "capacity_exceeded", ["capacity_exceeded"]

    has_blocked_windows = len(blocked_ranges) > 0
    if has_blocked_windows:
        return "calendar_conflict", ["calendar_conflict"]

    return "no_available_slot", ["no_available_slot"]


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            key: value
            for key, value in row.items()
            if key not in {"start_dt", "end_dt", "reference"}
        }
        for row in rows
    ]


def _compute_schedule_score(scheduled_actions: list[dict[str, Any]], unscheduled_actions: list[dict[str, Any]]) -> float:
    # Simple deterministic health score used for diagnostics only.
    return float(len(unscheduled_actions) * 100 + len(scheduled_actions))


def _validate_schedule_constraints(
    scheduled_actions: list[dict[str, Any]],
    blocked_ranges: list[tuple[datetime, datetime]],
    day_start: datetime,
    day_end: datetime,
) -> None:
    # Ensure all slots are valid and non-overlapping.
    ordered = sorted(
        scheduled_actions,
        key=lambda item: (
            item.get("start_dt") or datetime.max,
            str(item.get("proposal_id", "")),
        ),
    )

    total_units = 0
    taken_ranges: list[tuple[datetime, datetime]] = []
    for row in ordered:
        start_dt = row.get("start_dt")
        end_dt = row.get("end_dt")
        duration_units = int(row.get("duration_units", 1))
        total_units += duration_units

        if not isinstance(start_dt, datetime) or not isinstance(end_dt, datetime):
            raise ValueError("scheduled action missing concrete slot bounds")
        if not (day_start <= start_dt < end_dt <= day_end):
            raise ValueError("scheduled action falls outside day bounds")
        if (end_dt - start_dt) != timedelta(hours=duration_units):
            raise ValueError("scheduled action duration mismatch")

        for blocked_start, blocked_end in blocked_ranges:
            if _ranges_overlap(start_dt, end_dt, blocked_start, blocked_end):
                raise ValueError("scheduled action overlaps a calendar block")

        for taken_start, taken_end in taken_ranges:
            if _ranges_overlap(start_dt, end_dt, taken_start, taken_end):
                raise ValueError("scheduled actions overlap")
        taken_ranges.append((start_dt, end_dt))

    blocked = _blocked_hours(day_start, day_end, blocked_ranges)
    available = max(0.0, float(DAY_END - DAY_START) - blocked)
    if float(total_units) > available:
        raise ValueError("scheduled capacity exceeds available day capacity")


def _time_windows_overlap(
    start_a: datetime | None,
    end_a: datetime | None,
    start_b: datetime | None,
    end_b: datetime | None,
) -> bool:
    if start_a is None or start_b is None:
        return False

    # If no explicit end is provided, treat duration as one scheduling unit.
    effective_end_a = end_a or start_a
    effective_end_b = end_b or start_b
    return start_a < effective_end_b and start_b < effective_end_a


def run_decision_engine_v2(
    payload: dict[str, Any],
    *,
    enable_optimization: bool = True,
    include_trace: bool = False,
) -> dict[str, Any]:
    validate_decision_input_contract(payload)

    proposals = [dict(item) for item in payload.get("proposals", [])]
    signals = [dict(item) for item in payload.get("signals", [])]
    payload_calendar_events = [dict(item) for item in payload.get("calendar_events", [])]
    calendar_events = _normalize_calendar_events(payload_calendar_events, proposals)

    signal_counts = {
        "events_today": 0,
        "high_priority_events": 0,
    }
    for signal in signals:
        signal_type = str(signal.get("type", ""))
        if signal_type in signal_counts:
            signal_counts[signal_type] = _parse_numeric_signal_value(signal.get("message"))

    # Quantize to the hour to keep repeated evaluations deterministic.
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    scored: list[dict[str, Any]] = []
    scored_trace: dict[str, dict[str, Any]] = {}
    for proposal in proposals:
        start_dt, end_dt = _parse_time_window(proposal.get("description"))
        priority_hint = _parse_priority_hint(proposal)
        urgency = _urgency_score(start_dt, now)
        context = _context_score(
            proposal,
            signal_counts["events_today"],
            signal_counts["high_priority_events"],
        )
        duration_units = _default_duration_units(proposal)
        effort_weight = _effort_weight(proposal)

        score = (
            (priority_hint * 0.5)
            + (urgency * 0.25)
            + (context * 0.1)
            + ((1 / float(duration_units)) * 0.1)
            + (effort_weight * 0.05)
        )
        duration_cost = (1 / float(duration_units)) * 0.1
        preferred_bucket = _bucket_from_start(start_dt)

        proposal_id = str(proposal.get("id", ""))
        scored_trace[proposal_id] = {
            "proposal_id": proposal_id,
            "computed_score": {
                "priority_component": priority_hint * 0.5,
                "urgency_component": urgency * 0.25,
                "context_component": context * 0.1,
                "duration_component": duration_cost,
                "effort_component": effort_weight * 0.05,
            },
            "priority_hint": priority_hint,
            "urgency_score": urgency,
            "duration_cost": duration_cost,
            "effort_weight": effort_weight,
            "final_score": float(score),
        }

        scored.append(
            {
                "proposal_id": proposal_id,
                "source_module": proposal.get("source_module"),
                "type": proposal.get("type"),
                "priority_hint": priority_hint,
                "urgency_score": urgency,
                "context_score": context,
                "effort": str(proposal.get("effort", "medium")).strip().lower() or "medium",
                "effort_weight": effort_weight,
                "category": str(proposal.get("category", "other")).strip().lower() or "other",
                "score": float(score),
                "duration_units": duration_units,
                "preferred_bucket": preferred_bucket,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "reference": str(proposal.get("description", "")),
            }
        )

    scored.sort(
        key=lambda item: (
            -item["score"],
            -item["priority_hint"],
            str(item.get("proposal_id", "")),
        )
    )

    schedule_day = _select_schedule_date(scored, calendar_events)
    day_start, day_end = _day_bounds(schedule_day)

    blocked_ranges: list[tuple[datetime, datetime]] = []
    for event in calendar_events:
        start_dt, end_dt = _parse_time_window(f"time_window={event['start_time']}->{event['end_time']}")
        if start_dt is None or end_dt is None:
            continue
        if end_dt <= day_start or start_dt >= day_end:
            continue
        clipped_start = max(start_dt, day_start)
        clipped_end = min(end_dt, day_end)
        if clipped_start < clipped_end:
            blocked_ranges.append((clipped_start, clipped_end))

    blocked_ranges.sort(key=lambda row: (row[0], row[1]))

    taken_ranges: list[tuple[datetime, datetime]] = []
    scheduled_actions: list[dict[str, Any]] = []
    unscheduled_actions: list[dict[str, Any]] = []
    scheduled_trace: list[dict[str, Any]] = []
    unscheduled_trace: list[dict[str, Any]] = []

    for candidate in scored:
        slot = _find_first_available_slot(
            duration_units=int(candidate["duration_units"]),
            day_start=day_start,
            day_end=day_end,
            blocked_ranges=blocked_ranges,
            taken_ranges=taken_ranges,
        )
        if slot is None:
            rejection_reason, failed_constraints = _rejection_diagnostics(
                candidate,
                day_start,
                day_end,
                blocked_ranges,
                taken_ranges,
            )
            unscheduled_actions.append(
                {
                    **candidate,
                    "unscheduled_reason": "no_available_time_slot",
                }
            )
            unscheduled_trace.append(
                {
                    "proposal_id": candidate.get("proposal_id"),
                    "rejection_reason": rejection_reason,
                    "failed_constraints": failed_constraints,
                    "computed_score": scored_trace.get(str(candidate.get("proposal_id", "")), {}).get("computed_score", {}),
                    "priority_hint": candidate.get("priority_hint"),
                    "urgency_score": candidate.get("urgency_score"),
                    "duration_cost": scored_trace.get(str(candidate.get("proposal_id", "")), {}).get("duration_cost"),
                    "effort_weight": candidate.get("effort_weight"),
                    "final_score": candidate.get("score"),
                }
            )
            continue

        start_time, end_time = slot
        bucket = _bucket_from_start(start_time)

        scheduled_actions.append(
            {
                **candidate,
                "bucket": bucket,
                "start_dt": start_time,
                "end_dt": end_time,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            }
        )
        scheduled_trace.append(
            {
                "proposal_id": candidate.get("proposal_id"),
                "computed_score": scored_trace.get(str(candidate.get("proposal_id", "")), {}).get("computed_score", {}),
                "priority_hint": candidate.get("priority_hint"),
                "urgency_score": candidate.get("urgency_score"),
                "duration_cost": scored_trace.get(str(candidate.get("proposal_id", "")), {}).get("duration_cost"),
                "effort_weight": candidate.get("effort_weight"),
                "final_score": candidate.get("score"),
                "assigned_bucket": bucket,
                "assigned_time_window": {
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                },
                "reason_assigned": "Highest ranked remaining proposal that fit first available non-conflicting slot.",
            }
        )
        taken_ranges.append((start_time, end_time))
        taken_ranges.sort(key=lambda row: (row[0], row[1]))

    _validate_schedule_constraints(scheduled_actions, blocked_ranges, day_start, day_end)
    schedule_score = _compute_schedule_score(scheduled_actions, unscheduled_actions)

    # Deterministic final ordering in each collection.
    scheduled_actions.sort(
        key=lambda item: (
            item.get("start_dt") or datetime.max,
            -item["score"],
            str(item.get("proposal_id", "")),
        )
    )
    unscheduled_actions.sort(
        key=lambda item: (
            item.get("unscheduled_reason", ""),
            -item["score"],
            str(item.get("proposal_id", "")),
        )
    )

    priorities = [
        {
            "rank": index + 1,
            "proposal_id": row.get("proposal_id"),
            "source_module": row.get("source_module"),
            "score": row.get("score"),
            "priority_hint": row.get("priority_hint"),
            "urgency_score": row.get("urgency_score"),
            "context_score": row.get("context_score"),
            "duration": row.get("duration_units"),
            "effort": row.get("effort"),
            "category": row.get("category"),
        }
        for index, row in enumerate(scored)
    ]

    warnings: list[dict[str, Any]] = []
    risks: list[dict[str, Any]] = []

    high_priority_count = sum(1 for row in scored if row["priority_hint"] >= 0.7)
    if high_priority_count > 5:
        warnings.append(
            {
                "type": "too_many_high_priority_items",
                "count": high_priority_count,
                "message": "High-priority items exceed manageable daily volume.",
            }
        )

    for bucket, capacity in TIME_BUCKETS.items():
        requested = sum(
            row["duration_units"] for row in scored if _bucket_from_start(row.get("start_dt")) == bucket
        )
        if requested > capacity:
            warnings.append(
                {
                    "type": "overloaded_time_bucket",
                    "bucket": bucket,
                    "requested": requested,
                    "capacity": capacity,
                    "message": f"{bucket} has more work than capacity allows.",
                }
            )

    for row in unscheduled_actions:
        if row["priority_hint"] >= 0.7:
            risks.append(
                {
                    "type": "missed_high_priority_item",
                    "proposal_id": row.get("proposal_id"),
                    "source_module": row.get("source_module"),
                    "reason": row.get("unscheduled_reason"),
                }
            )

    scheduled_ids = {str(row.get("proposal_id", "")) for row in scheduled_actions}
    prepare_by_reference: set[str] = set()
    buffer_by_reference: set[str] = set()
    for proposal in proposals:
        description = str(proposal.get("description", ""))
        ref_match = re.search(r"reference=([^;]+)", description)
        if not ref_match:
            continue
        reference = ref_match.group(1).strip()
        proposal_id = str(proposal.get("id", ""))
        if proposal_id not in scheduled_ids:
            continue

        proposal_type = str(proposal.get("type", ""))
        if proposal_type == "prepare_for_event":
            prepare_by_reference.add(reference)
        if proposal_type == "leave_buffer_time":
            buffer_by_reference.add(reference)

    for reference in sorted(prepare_by_reference):
        if reference not in buffer_by_reference:
            risks.append(
                {
                    "type": "event_with_no_prep_buffer",
                    "reference": reference,
                    "reason": "Preparation exists but buffer allocation was not scheduled.",
                }
            )

    blocked_day1 = _blocked_hours(day_start, day_end, blocked_ranges)
    day1_capacity = max(0.0, float(DAY_END - DAY_START) - blocked_day1)
    day1_used = _schedule_load_hours(scheduled_actions)
    day1_slack = max(0.0, day1_capacity - day1_used)
    day1_overload = max(0.0, day1_used - day1_capacity)

    day2_schedule: list[dict[str, Any]] = []
    day3_schedule: list[dict[str, Any]] = []
    backlog = [dict(row) for row in unscheduled_actions]

    result = {
        "priorities": priorities,
        "calendar_events": calendar_events,
        "scheduled_actions": _serialize_rows(scheduled_actions),
        "unscheduled_actions": _serialize_rows(unscheduled_actions),
        "warnings": warnings,
        "risks": risks,
        # Multi-day horizon extension (does not alter existing /brief contract use of scheduled_actions).
        "day_1_schedule": _serialize_rows(scheduled_actions),
        "day_2_schedule": _serialize_rows(day2_schedule),
        "day_3_schedule": _serialize_rows(day3_schedule),
        "backlog": _serialize_rows(backlog),
        # Internal-only diagnostic metadata for deterministic optimization tracking.
        "_internal": {
            "schedule_score": schedule_score,
            "baseline_schedule_score": schedule_score,
            "optimization_applied": False,
            "daily_load_balancer": {
                "day_1": {
                    "total_capacity_used": day1_used,
                    "remaining_slack": day1_slack,
                    "overload_penalty": day1_overload,
                },
                "day_2": {
                    "total_capacity_used": 0.0,
                    "remaining_slack": float(DAY_END - DAY_START),
                    "overload_penalty": 0.0,
                },
                "day_3": {
                    "total_capacity_used": 0.0,
                    "remaining_slack": float(DAY_END - DAY_START),
                    "overload_penalty": 0.0,
                },
            },
        },
    }

    if include_trace:
        capacity_utilization_per_bucket: dict[str, dict[str, float]] = {}
        for bucket in ("morning", "afternoon", "evening"):
            capacity_hours = _bucket_capacity_hours(day_start, blocked_ranges, bucket)
            used_hours = _bucket_used_hours(scheduled_actions, bucket)
            utilization = 0.0 if capacity_hours <= 0 else round(used_hours / capacity_hours, 6)
            capacity_utilization_per_bucket[bucket] = {
                "used_hours": round(used_hours, 6),
                "capacity_hours": round(capacity_hours, 6),
                "utilization": utilization,
            }

        conflict_count = sum(
            1
            for row in unscheduled_trace
            if "calendar_conflict" in list(row.get("failed_constraints", []))
        )

        result["_internal"]["decision_trace"] = {
            "scheduled": sorted(
                scheduled_trace,
                key=lambda row: (
                    str(row.get("proposal_id", "")),
                    str(row.get("assigned_time_window", {}).get("start_time", "")),
                ),
            ),
            "unscheduled": sorted(
                unscheduled_trace,
                key=lambda row: str(row.get("proposal_id", "")),
            ),
            "summary": {
                "total_proposals_evaluated": len(scored),
                "total_scheduled": len(scheduled_actions),
                "total_unscheduled": len(unscheduled_actions),
                "capacity_utilization_per_bucket": capacity_utilization_per_bucket,
                "conflict_count": conflict_count,
                "scheduling_pass_count": 1,
            },
        }

    validate_decision_output_contract(result)
    return result