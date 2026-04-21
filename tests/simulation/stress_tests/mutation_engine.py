from __future__ import annotations

import random
from copy import deepcopy
from typing import Any


def mutate_timeline(
    *,
    timeline_events: list[dict[str, Any]],
    seed: int,
    chaos_level: str,
) -> list[dict[str, Any]]:
    rng = random.Random(seed + 101)
    events = deepcopy(timeline_events)

    mutation_budget = {
        "low": max(1, len(events) // 12),
        "medium": max(2, len(events) // 7),
        "high": max(3, len(events) // 4),
    }.get(chaos_level, 2)

    for _ in range(mutation_budget):
        if not events:
            break
        idx = rng.randrange(len(events))
        event = events[idx]
        mutation = rng.choice([
            "shift_time",
            "cancel",
            "insert_urgent",
            "overlap_conflict",
        ])

        if mutation == "shift_time":
            event.setdefault("payload", {})["shift_minutes"] = rng.choice([15, 30, 45, 60])
            event["type"] = "reschedule"
        elif mutation == "cancel":
            event["type"] = "cancellation"
            event.setdefault("payload", {})["cancel_target"] = event.get("event_id")
        elif mutation == "insert_urgent":
            urgent = deepcopy(event)
            urgent["event_id"] = f"urgent-{event.get('event_id', idx)}"
            urgent["type"] = "health_event"
            urgent.setdefault("payload", {})["priority_hint"] = "urgent"
            urgent["title"] = "Urgent Medical Check"
            events.insert(min(idx + 1, len(events)), urgent)
        elif mutation == "overlap_conflict":
            event["type"] = "interruption"
            event.setdefault("payload", {})["priority_hint"] = "urgent"

    return events
