from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta

from assistant.daily_loop.contracts import DailyScheduleItem, DaySegment, SchedulingGap


DEFAULT_BUFFER_MINUTES = 15
MIN_GAP_MINUTES = 15
SEGMENT_ORDER: dict[DaySegment, int] = {"morning": 0, "midday": 1, "evening": 2}
SEGMENT_SPECS: tuple[tuple[DaySegment, time, time], ...] = (
    ("morning", time(6, 0), time(12, 0)),
    ("midday", time(12, 0), time(17, 0)),
    ("evening", time(17, 0), time(22, 0)),
)


def parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).replace(tzinfo=None)


def parse_runtime_time_block(value: str) -> tuple[datetime, datetime] | None:
    if not value or len(value) < 22:
        return None
    start_raw = value[:16].strip()
    end_raw = value[-5:].strip()
    if value[16] != "-" or len(end_raw) != 5:
        return None
    date_part = start_raw[:10]
    try:
        start_dt = datetime.fromisoformat(start_raw)
        end_dt = datetime.fromisoformat(f"{date_part} {end_raw}")
    except ValueError:
        return None
    return start_dt, end_dt


def to_iso_z(value: datetime) -> str:
    return value.replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")


def to_time_block(start_dt: datetime, end_dt: datetime) -> str:
    return f"{start_dt.strftime('%Y-%m-%d %H:%M')}-{end_dt.strftime('%H:%M')}"


def get_segment_bounds(target_date: date) -> dict[DaySegment, tuple[datetime, datetime]]:
    return {
        segment: (
            datetime.combine(target_date, start_time),
            datetime.combine(target_date, end_time),
        )
        for segment, start_time, end_time in SEGMENT_SPECS
    }


def resolve_segment(start_dt: datetime, end_dt: datetime) -> DaySegment:
    bounds = get_segment_bounds(start_dt.date())
    for segment, (segment_start, segment_end) in bounds.items():
        if start_dt >= segment_start and end_dt <= segment_end:
            return segment
    if end_dt <= bounds["midday"][0]:
        return "morning"
    if end_dt <= bounds["evening"][0]:
        return "midday"
    return "evening"


def _buffered_interval(item: DailyScheduleItem, target_date: date) -> tuple[datetime, datetime] | None:
    start_dt = parse_iso_datetime(item.start) - timedelta(minutes=item.buffer_before_minutes)
    end_dt = parse_iso_datetime(item.end) + timedelta(minutes=item.buffer_after_minutes)
    if start_dt.date() != target_date and end_dt.date() != target_date:
        return None
    return start_dt, end_dt


def _slot_is_available(
    existing_items: list[DailyScheduleItem],
    candidate_start: datetime,
    candidate_end: datetime,
    *,
    target_date: date,
    buffer_before_minutes: int,
    buffer_after_minutes: int,
) -> bool:
    requested_start = candidate_start - timedelta(minutes=buffer_before_minutes)
    requested_end = candidate_end + timedelta(minutes=buffer_after_minutes)

    for item in existing_items:
        interval = _buffered_interval(item, target_date)
        if interval is None:
            continue
        if requested_start < interval[1] and interval[0] < requested_end:
            return False
    return True


def allocate_time_block(
    existing_items: list[DailyScheduleItem],
    desired_start: datetime,
    desired_end: datetime,
    *,
    segment: DaySegment,
    buffer_before_minutes: int = DEFAULT_BUFFER_MINUTES,
    buffer_after_minutes: int = DEFAULT_BUFFER_MINUTES,
    step_minutes: int = 15,
) -> tuple[datetime, datetime] | None:
    target_date = desired_start.date()
    segment_start, segment_end = get_segment_bounds(target_date)[segment]
    duration = desired_end - desired_start
    latest_start = segment_end - duration
    if latest_start < segment_start:
        return None

    baseline = desired_start
    if baseline < segment_start:
        baseline = segment_start
    if baseline > latest_start:
        baseline = latest_start

    candidates: list[datetime] = []
    cursor = segment_start
    while cursor <= latest_start:
        candidates.append(cursor)
        cursor += timedelta(minutes=step_minutes)

    ordered = sorted(
        candidates,
        key=lambda item: (abs((item - baseline).total_seconds()), item),
    )
    for candidate_start in ordered:
        candidate_end = candidate_start + duration
        if _slot_is_available(
            existing_items,
            candidate_start,
            candidate_end,
            target_date=target_date,
            buffer_before_minutes=buffer_before_minutes,
            buffer_after_minutes=buffer_after_minutes,
        ):
            return candidate_start, candidate_end
    return None


def detect_scheduling_gaps(
    schedule: list[DailyScheduleItem],
    *,
    target_date: date,
    min_gap_minutes: int = MIN_GAP_MINUTES,
) -> list[SchedulingGap]:
    gaps: list[SchedulingGap] = []
    bounds = get_segment_bounds(target_date)

    for segment, (segment_start, segment_end) in bounds.items():
        intervals: list[tuple[datetime, datetime]] = []
        for item in schedule:
            interval = _buffered_interval(item, target_date)
            if interval is None or item.segment != segment:
                continue
            clipped_start = max(segment_start, interval[0])
            clipped_end = min(segment_end, interval[1])
            if clipped_start < clipped_end:
                intervals.append((clipped_start, clipped_end))

        intervals.sort(key=lambda value: (value[0], value[1]))
        merged: list[tuple[datetime, datetime]] = []
        for current_start, current_end in intervals:
            if not merged or current_start > merged[-1][1]:
                merged.append((current_start, current_end))
            else:
                merged[-1] = (merged[-1][0], max(merged[-1][1], current_end))

        cursor = segment_start
        for current_start, current_end in merged:
            gap_minutes = int((current_start - cursor).total_seconds() // 60)
            if gap_minutes >= min_gap_minutes:
                gaps.append(
                    SchedulingGap(
                        segment=segment,
                        start=to_iso_z(cursor),
                        end=to_iso_z(current_start),
                        time_block=to_time_block(cursor, current_start),
                        duration_minutes=gap_minutes,
                    )
                )
            cursor = max(cursor, current_end)

        tail_gap_minutes = int((segment_end - cursor).total_seconds() // 60)
        if tail_gap_minutes >= min_gap_minutes:
            gaps.append(
                SchedulingGap(
                    segment=segment,
                    start=to_iso_z(cursor),
                    end=to_iso_z(segment_end),
                    time_block=to_time_block(cursor, segment_end),
                    duration_minutes=tail_gap_minutes,
                )
            )

    return gaps