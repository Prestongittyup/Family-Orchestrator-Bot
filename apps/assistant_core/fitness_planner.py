from __future__ import annotations

from apps.assistant_core.contracts import FitnessPlan, FitnessSession, TimelineBlock


def _focus_sequence(goal: str) -> list[str]:
    normalized = goal.lower()
    if "muscle" in normalized:
        return ["Upper hypertrophy", "Lower hypertrophy", "Accessory volume"]
    if "fat" in normalized:
        return ["Intervals", "Zone 2 cardio", "Full body circuit"]
    return ["Full body strength", "Pull emphasis", "Leg strength"]


def generate_fitness_plan(goal: str, available_windows: list[tuple[str, str, str]]) -> FitnessPlan:
    focus_items = _focus_sequence(goal)
    sessions: list[FitnessSession] = []
    insertions: list[TimelineBlock] = []

    for index, (day_label, time_block, rationale) in enumerate(available_windows[:3]):
        focus = focus_items[index % len(focus_items)]
        sessions.append(
            FitnessSession(
                day=day_label,
                time_block=time_block,
                focus=focus,
                duration_minutes=45,
                rationale=rationale,
            )
        )
        insertions.append(
            TimelineBlock(
                time_block=time_block,
                title=f"{focus} workout",
                rationale=f"Suggested for {day_label.lower()} because the window stays clear of household conflicts.",
                confidence=0.81,
            )
        )

    return FitnessPlan(
        goal=goal,
        weekly_summary=f"Three-session {goal} plan aligned to the clearest weekly schedule windows.",
        sessions=sessions,
        insertion_suggestions=insertions,
    )