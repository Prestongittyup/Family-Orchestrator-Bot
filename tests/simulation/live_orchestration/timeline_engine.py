from __future__ import annotations

import random
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class TimelineEvent:
    event_id: str
    timestamp: str
    type: str
    title: str
    start_time: str
    end_time: str
    participants: list[str]
    payload: dict[str, Any]


_BASE_TYPES = [
    "school_event",
    "work_event",
    "health_event",
]

_MUTATION_TYPES = [
    "interruption",
    "reschedule",
    "cancellation",
]


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _pick_type(rng: random.Random, chaos_level: str) -> str:
    if chaos_level == "low":
        return rng.choice(_BASE_TYPES)
    if chaos_level == "medium":
        return rng.choice(_BASE_TYPES + ["interruption"])
    return rng.choice(_BASE_TYPES + _MUTATION_TYPES)


def build_timeline(
    *,
    seed: int,
    household_size: int,
    event_density: int,
    chaos_level: str,
    scenario_preset: str,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    base_time = datetime(2026, 4, 18, 6, 0, tzinfo=UTC)

    members = [f"Member {idx + 1}" for idx in range(max(1, household_size))]
    density = max(3, min(60, int(event_density)))

    events: list[TimelineEvent] = []

    for idx in range(density):
        event_type = _pick_type(rng, chaos_level)
        start_offset_hours = rng.randint(0, 14)
        start_offset_minutes = rng.choice([0, 15, 30, 45])
        duration_minutes = rng.choice([30, 45, 60, 90])

        start_dt = base_time + timedelta(hours=start_offset_hours, minutes=start_offset_minutes)
        end_dt = start_dt + timedelta(minutes=duration_minutes)
        ts_dt = base_time + timedelta(minutes=idx * 10)

        participants = rng.sample(members, k=min(len(members), rng.randint(1, min(3, len(members)))))

        title_prefix = {
            "school_event": "School",
            "work_event": "Work",
            "health_event": "Health",
            "interruption": "Interruption",
            "reschedule": "Reschedule",
            "cancellation": "Cancellation",
        }[event_type]
        title = f"{title_prefix} {scenario_preset} #{idx + 1}"

        payload = {
            "title": title,
            "category": event_type,
            "participants": participants,
            "priority_hint": "urgent" if event_type in {"health_event", "interruption"} else "normal",
            "start_time": _iso(start_dt),
            "end_time": _iso(end_dt),
            "source": "simulation",
        }

        if event_type == "reschedule":
            payload["shift_minutes"] = rng.choice([15, 30, 60])
        if event_type == "cancellation":
            payload["cancel_target"] = f"evt-{max(1, idx)}"

        events.append(
            TimelineEvent(
                event_id=f"evt-{idx + 1}",
                timestamp=_iso(ts_dt),
                type=event_type,
                title=title,
                start_time=_iso(start_dt),
                end_time=_iso(end_dt),
                participants=participants,
                payload=payload,
            )
        )

    events.sort(key=lambda e: (e.timestamp, e.event_id))
    return [asdict(event) for event in events]
