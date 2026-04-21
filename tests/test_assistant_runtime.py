from __future__ import annotations

from copy import deepcopy

from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.integration_core.models.household_state import CalendarEvent, HouseholdState
from apps.assistant_core.planning_engine import _fallback_household_state
from assistant.runtime.assistant_runtime import AssistantRuntimeEngine
from assistant.state.state_snapshot import StateSnapshotService


def _dense_same_day_state() -> HouseholdState:
    state = _fallback_household_state("household-001")
    state.calendar_events.extend(
        [
            CalendarEvent(event_id="evt-early-1", title="Breakfast cleanup", start="2026-04-19T06:00:00Z", end="2026-04-19T06:45:00Z"),
            CalendarEvent(event_id="evt-early-2", title="Commute prep", start="2026-04-19T07:00:00Z", end="2026-04-19T07:45:00Z"),
            CalendarEvent(event_id="evt-early-3", title="Midday errand", start="2026-04-19T12:00:00Z", end="2026-04-19T12:45:00Z"),
            CalendarEvent(event_id="evt-early-4", title="After school buffer", start="2026-04-19T16:00:00Z", end="2026-04-19T16:45:00Z"),
            CalendarEvent(event_id="evt-early-5", title="Dinner commute", start="2026-04-19T18:10:00Z", end="2026-04-19T18:50:00Z"),
        ]
    )
    state.metadata["reference_time"] = "2026-04-19T08:00:00Z"
    return state


def test_runtime_output_is_deterministic() -> None:
    engine = AssistantRuntimeEngine()
    state = _fallback_household_state("household-001")

    left = engine.run(
        query="Plan today around dinner and a workout after school pickup",
        household_id="household-runtime-deterministic",
        repeat_window_days=10,
        fitness_goal="fat loss",
        state=state,
    ).decision_response.model_dump()
    right = engine.run(
        query="Plan today around dinner and a workout after school pickup",
        household_id="household-runtime-deterministic",
        repeat_window_days=10,
        fitness_goal="fat loss",
        state=state,
    ).decision_response.model_dump()

    assert left == right


def test_runtime_merges_multiple_domains() -> None:
    engine = AssistantRuntimeEngine()
    result = engine.run(
        query="Schedule a doctor appointment, dinner, and a workout around the family plan today",
        household_id="household-runtime-multi-domain",
        repeat_window_days=10,
        fitness_goal="fat loss",
        state=_fallback_household_state("household-001"),
    )

    assert result.decision_response.recommended_action.domain in {"appointment", "meal", "fitness", "general"}
    assert len(result.decision_response.grouped_approvals) == 1


def test_runtime_detects_cross_domain_conflicts() -> None:
    engine = AssistantRuntimeEngine()
    result = engine.run(
        query="Plan dinner and a workout for tonight around the household schedule",
        household_id="household-runtime-conflicts-v2",
        repeat_window_days=10,
        fitness_goal="fat loss",
        state=_dense_same_day_state(),
    )

    assert any(conflict.conflict_type in {"calendar_overlap", "meal_time_tradeoff", "evening_compression"} for conflict in result.decision_response.current_state_summary.conflicts)


def test_state_snapshot_is_read_only() -> None:
    state = _fallback_household_state("household-001")
    original = deepcopy(state.as_dict())
    snapshot = StateSnapshotService().build(state, fitness_goal="strength")

    snapshot.calendar_events[0].title = "Changed in snapshot only"
    snapshot.household_context["task_count"] = 999

    assert state.as_dict() == original
    assert state.calendar_events[0].title != snapshot.calendar_events[0].title


def test_runtime_does_not_execute_without_approval() -> None:
    client = TestClient(app)
    response = client.post(
        "/assistant/run",
        json={"query": "Plan today with dinner and a workout block after work", "fitness_goal": "fat loss", "household_id": "household-runtime-api"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["recommended_action"]["approval_required"] is True
    assert payload["recommended_action"]["approval_status"] == "pending"
    assert payload["grouped_approval_payload"]["approval_status"] == "pending"