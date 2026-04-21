from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class CategoryBehaviorProfile:
    category: str
    preferred_start_time: str | None
    commonly_rejected_windows: dict[str, int]
    consistency_rate: float
    time_of_day_success_rates: dict[str, float]
    approved_count: int
    rejected_count: int
    ignored_count: int
    approved_by_segment: dict[str, int] = field(default_factory=dict)
    rejected_by_segment: dict[str, int] = field(default_factory=dict)
    ignored_by_segment: dict[str, int] = field(default_factory=dict)
    preferred_duration_minutes: int | None = None


@dataclass(frozen=True)
class BehaviorProfile:
    categories: dict[str, CategoryBehaviorProfile]

    def for_category(self, category: str) -> CategoryBehaviorProfile:
        return self.categories.get(
            category,
            CategoryBehaviorProfile(
                category=category,
                preferred_start_time=None,
                commonly_rejected_windows={},
                consistency_rate=1.0,
                time_of_day_success_rates={},
                approved_count=0,
                rejected_count=0,
                ignored_count=0,
            ),
        )


class BehaviorProfileBuilder:
    def build(self, graph: dict[str, Any]) -> BehaviorProfile:
        raw_records = list(graph.get("behavior_feedback", {}).get("records", []))
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for record in raw_records:
            grouped[str(record.get("category", "calendar"))].append(record)

        categories = {
            category: self._build_category_profile(category=category, records=records)
            for category, records in grouped.items()
        }
        for category in ("fitness", "meal", "calendar"):
            categories.setdefault(
                category,
                CategoryBehaviorProfile(
                    category=category,
                    preferred_start_time=None,
                    commonly_rejected_windows={},
                    consistency_rate=1.0,
                    time_of_day_success_rates={},
                    approved_count=0,
                    rejected_count=0,
                    ignored_count=0,
                ),
            )
        return BehaviorProfile(categories=categories)

    def next_slot_for_profile(
        self,
        *,
        profile: CategoryBehaviorProfile,
        reference_time: str,
        fallback_slot: str | None,
        default_duration_minutes: int,
    ) -> str | None:
        reference = self._coerce_datetime(reference_time)
        duration = profile.preferred_duration_minutes or default_duration_minutes

        if profile.preferred_start_time:
            return self._slot_for_start_time(
                reference=reference,
                start_time=profile.preferred_start_time,
                duration_minutes=duration,
            )

        if profile.rejected_by_segment.get("morning", 0) > 3:
            return self._slot_for_start_time(
                reference=reference,
                start_time="18:30",
                duration_minutes=duration,
                minimum_day_offset=1,
            )

        return fallback_slot

    def reduced_duration_minutes(self, profile: CategoryBehaviorProfile, *, default_minutes: int) -> int:
        if profile.ignored_count >= 2 and profile.consistency_rate < 0.5:
            return 30
        return profile.preferred_duration_minutes or default_minutes

    def _build_category_profile(self, *, category: str, records: list[dict[str, Any]]) -> CategoryBehaviorProfile:
        approved_count = 0
        rejected_count = 0
        ignored_count = 0
        approved_by_segment: dict[str, int] = defaultdict(int)
        rejected_by_segment: dict[str, int] = defaultdict(int)
        ignored_by_segment: dict[str, int] = defaultdict(int)
        approved_start_times: dict[str, int] = defaultdict(int)
        approved_duration_counts: dict[int, int] = defaultdict(int)
        rejected_windows: dict[str, int] = defaultdict(int)
        total_by_segment: dict[str, int] = defaultdict(int)

        for record in records:
            scheduled = str(record.get("scheduled_time") or "")
            segment = self._segment_for_scheduled_time(scheduled)
            status = str(record.get("status", ""))
            if segment:
                total_by_segment[segment] += 1

            if status == "approved":
                approved_count += 1
                if segment:
                    approved_by_segment[segment] += 1
                start_time = self._extract_start_time(scheduled)
                duration = self._extract_duration_minutes(scheduled)
                if start_time:
                    approved_start_times[start_time] += 1
                if duration is not None:
                    approved_duration_counts[duration] += 1
            elif status == "rejected":
                rejected_count += 1
                if segment:
                    rejected_by_segment[segment] += 1
                    rejected_windows[segment] += 1
            elif status == "ignored":
                ignored_count += 1
                if segment:
                    ignored_by_segment[segment] += 1
                    rejected_windows[segment] += 1

        denominator = approved_count + ignored_count
        consistency_rate = approved_count / denominator if denominator else 1.0

        success_rates = {
            segment: approved_by_segment.get(segment, 0) / count
            for segment, count in total_by_segment.items()
            if count > 0
        }

        preferred_start_time = None
        if approved_start_times:
            preferred_start_time = sorted(
                approved_start_times.items(),
                key=lambda item: (-item[1], item[0]),
            )[0][0]

        preferred_duration = None
        if approved_duration_counts:
            preferred_duration = sorted(
                approved_duration_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )[0][0]

        return CategoryBehaviorProfile(
            category=category,
            preferred_start_time=preferred_start_time,
            commonly_rejected_windows=dict(sorted(rejected_windows.items())),
            consistency_rate=round(consistency_rate, 4),
            time_of_day_success_rates=dict(sorted(success_rates.items())),
            approved_count=approved_count,
            rejected_count=rejected_count,
            ignored_count=ignored_count,
            approved_by_segment=dict(sorted(approved_by_segment.items())),
            rejected_by_segment=dict(sorted(rejected_by_segment.items())),
            ignored_by_segment=dict(sorted(ignored_by_segment.items())),
            preferred_duration_minutes=preferred_duration,
        )

    def _slot_for_start_time(
        self,
        *,
        reference: datetime,
        start_time: str,
        duration_minutes: int,
        minimum_day_offset: int = 0,
    ) -> str:
        start_hour, start_minute = [int(part) for part in start_time.split(":", 1)]
        start_dt = (reference + timedelta(days=minimum_day_offset)).replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
        if start_dt <= reference:
            start_dt = start_dt + timedelta(days=1)
        end_dt = start_dt + timedelta(minutes=duration_minutes)
        return f"{start_dt.strftime('%Y-%m-%d %H:%M')}-{end_dt.strftime('%H:%M')}"

    def _segment_for_scheduled_time(self, scheduled_time: str) -> str | None:
        start_time = self._extract_start_time(scheduled_time)
        if start_time is None:
            return None
        hour = int(start_time.split(":", 1)[0])
        if 5 <= hour < 12:
            return "morning"
        if 12 <= hour < 17:
            return "afternoon"
        if 17 <= hour < 21:
            return "evening"
        return "late"

    def _extract_start_time(self, scheduled_time: str) -> str | None:
        if not scheduled_time or " " not in scheduled_time or "-" not in scheduled_time:
            return None
        _, time_block = scheduled_time.split(" ", 1)
        start, _sep, _end = time_block.partition("-")
        return start if ":" in start else None

    def _extract_duration_minutes(self, scheduled_time: str) -> int | None:
        if not scheduled_time or " " not in scheduled_time or "-" not in scheduled_time:
            return None
        date_part, time_block = scheduled_time.split(" ", 1)
        start_raw, _sep, end_raw = time_block.partition("-")
        if not start_raw or not end_raw:
            return None
        start_dt = datetime.strptime(f"{date_part} {start_raw}", "%Y-%m-%d %H:%M").replace(tzinfo=UTC)
        end_dt = datetime.strptime(f"{date_part} {end_raw}", "%Y-%m-%d %H:%M").replace(tzinfo=UTC)
        return int((end_dt - start_dt).total_seconds() // 60)

    def _coerce_datetime(self, value: str | datetime) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)