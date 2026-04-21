from __future__ import annotations

from datetime import date

from policy_engine.contracts import HouseholdMemorySnapshot, ItineraryBlock, ItineraryResponse


def _priority_weight(priority: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}[priority]


def generate_daily_itinerary(
    memory_snapshot: HouseholdMemorySnapshot,
    *,
    target_date: str | None = None,
) -> ItineraryResponse:
    itinerary: list[ItineraryBlock] = []
    preferences = set(memory_snapshot.memory.preferences)
    constraints = list(memory_snapshot.memory.constraints)

    if "medical_events_first" in preferences:
        itinerary.append(
            ItineraryBlock(
                time_block="08:00-08:30",
                event="Medical readiness check",
                reason="Memory indicates medical events should be surfaced before other obligations.",
                priority="high",
            )
        )

    if "school_events_high_visibility" in preferences:
        itinerary.append(
            ItineraryBlock(
                time_block="08:30-09:00",
                event="School departure coordination",
                reason="Recurring school patterns suggest a protected school launch block.",
                priority="high",
            )
        )

    if "work_blocks_protected" in preferences:
        itinerary.append(
            ItineraryBlock(
                time_block="09:00-11:00",
                event="Work obligation focus window",
                reason="Work obligations appear repeatedly in artifact-derived household memory.",
                priority="high",
            )
        )

    itinerary.append(
        ItineraryBlock(
            time_block="12:30-13:00",
            event="Constraint review checkpoint",
            reason="Midday review reduces the chance of carrying forward unresolved schedule constraints.",
            priority="medium",
        )
    )
    itinerary.append(
        ItineraryBlock(
            time_block="18:00-18:30",
            event="Household coordination review",
            reason="Daily wrap-up supports routine stabilization and next-day planning.",
            priority="medium",
        )
    )

    ordered_blocks = sorted(itinerary, key=lambda item: (item.time_block.split("-", 1)[0], _priority_weight(item.priority), item.event))
    conflicts_detected = list(constraints)
    optimization_notes = [
        "Medical, school, and work obligations are ordered before lower-priority household review blocks.",
        "Conflict-derived constraints are surfaced for manual review instead of automatic schedule changes.",
    ]

    return ItineraryResponse(
        date=target_date or date.today().isoformat(),
        recommended_itinerary=ordered_blocks,
        conflicts_detected=conflicts_detected,
        optimization_notes=optimization_notes,
    )