from __future__ import annotations

from copy import deepcopy

from apps.api.integration_core.models.household_state import HouseholdState
from apps.assistant_core.fitness_planner import generate_fitness_plan
from apps.assistant_core.meal_planner import default_recipe_history
from apps.assistant_core.planning_engine import _find_available_windows, _resolve_reference_time, _state_events
from assistant.contracts.assistant_plan import (
    AssistantStateSnapshot,
    SnapshotCalendarEvent,
    SnapshotFitnessSession,
    SnapshotMealRecord,
)


class StateSnapshotService:
    def build(self, state: HouseholdState, *, fitness_goal: str | None = None) -> AssistantStateSnapshot:
        events = [
            SnapshotCalendarEvent(**deepcopy(event.as_dict()))
            for event in state.calendar_events
        ]
        recipe_history = [
            SnapshotMealRecord(
                recipe_name=str(item.get("recipe_name", "")),
                served_on=str(item.get("served_on", "")),
            )
            for item in deepcopy(default_recipe_history())
        ]

        reference_time = _resolve_reference_time(state)
        available_windows = _find_available_windows(_state_events(state), reference_time)
        fitness_plan = generate_fitness_plan(fitness_goal or "maintenance", available_windows)
        fitness_schedule = [
            SnapshotFitnessSession(day=session.day, time_block=session.time_block, focus=session.focus)
            for session in fitness_plan.sessions
        ]

        household_context = {
            "household_id": state.user_id,
            "reference_time": reference_time.isoformat().replace("+00:00", "Z"),
            "task_count": len(state.tasks),
            "alert_count": len(state.alerts),
            "integration_states": [item.as_dict() for item in state.integrations],
        }

        return AssistantStateSnapshot(
            calendar_events=events,
            recent_meals=recipe_history,
            fitness_schedule=fitness_schedule,
            household_context=deepcopy(household_context),
        )