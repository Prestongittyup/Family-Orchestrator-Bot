from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.integration_core.models.household_state import CalendarEvent
from apps.assistant_core.planning_engine import _fallback_household_state
from assistant.runtime.assistant_runtime import AssistantRuntimeEngine


def _overlapping_state():
    state = _fallback_household_state("household-state-conflict")
    state.calendar_events.extend(
        [
            CalendarEvent(event_id="evt-conflict-1", title="School drop-off", start="2026-04-19T08:00:00Z", end="2026-04-19T08:45:00Z"),
            CalendarEvent(event_id="evt-conflict-2", title="Doctor callback", start="2026-04-19T08:30:00Z", end="2026-04-19T09:00:00Z"),
            CalendarEvent(event_id="evt-conflict-3", title="Dinner prep", start="2026-04-19T18:00:00Z", end="2026-04-19T18:45:00Z"),
        ]
    )
    state.metadata["reference_time"] = "2026-04-19T07:30:00Z"
    return state


def test_state_convergence_after_multiple_queries() -> None:
    engine = AssistantRuntimeEngine()
    state = _fallback_household_state("household-state-convergence-v2")

    first = engine.run(
        query="I'm busy this week, need dentist appointment",
        household_id="household-state-convergence-v2",
        repeat_window_days=10,
        fitness_goal=None,
        state=state,
    ).decision_response
    second = engine.run(
        query="I'm busy this week, need dentist appointment",
        household_id="household-state-convergence-v2",
        repeat_window_days=10,
        fitness_goal=None,
        state=state,
    ).decision_response

    assert first.model_dump() == second.model_dump()
    assert second.current_state_summary.pending_approval_count >= 1


def test_no_module_leakage_in_response() -> None:
    client = TestClient(app)
    response = client.post("/assistant/run", json={"query": "What should I do next for dinner and groceries?", "household_id": "household-state-no-leak"})

    assert response.status_code == 200
    payload = response.json()
    # Check for new response schema with grouped_approval_payload and follow_ups
    assert set(payload.keys()) == {"request_id", "intent_interpretation", "current_state_summary", "recommended_action", "grouped_approval_payload", "follow_ups", "reasoning_trace"}
    assert "proposals" not in str(payload)
    assert "candidate_schedules" not in str(payload)


def test_conflict_detection_across_domains() -> None:
    engine = AssistantRuntimeEngine()
    result = engine.run(
        query="What's for dinner and do I need groceries?",
        household_id="household-state-conflict",
        repeat_window_days=10,
        fitness_goal=None,
        state=_overlapping_state(),
    ).decision_response

    assert any(conflict["conflict_type"] if isinstance(conflict, dict) else conflict.conflict_type for conflict in result.current_state_summary.conflicts)
    assert any(
        (conflict["conflict_type"] if isinstance(conflict, dict) else conflict.conflict_type) in {"calendar_overlap", "inventory_gap", "evening_compression"}
        for conflict in result.current_state_summary.conflicts
    )


def test_single_action_output_guarantee() -> None:
    client = TestClient(app)
    response = client.post("/assistant/query", json={"query": "I need to start working out consistently", "household_id": "household-state-single-action"})

    assert response.status_code == 200
    payload = response.json()
    # Verify single action output (not a list, but a single object)
    assert isinstance(payload["recommended_action"], dict)
    assert payload["recommended_action"]["action_id"] is not None
    assert len(payload["grouped_approval_payload"]["action_ids"]) == 1
    assert payload["recommended_action"]["approval_status"] == "pending"