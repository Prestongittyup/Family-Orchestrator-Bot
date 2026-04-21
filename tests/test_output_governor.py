from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from assistant.governance.output_governor import OutputGovernor
from apps.api.main import app
from apps.assistant_core.planning_engine import _fallback_household_state
from household_os.core.decision_engine import HouseholdOSDecisionEngine
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.runtime.orchestrator import HouseholdOSOrchestrator


def _runtime_response_for(query: str, household_id: str = "governor-household"):
    state = _fallback_household_state(household_id)
    store = HouseholdStateGraphStore()
    graph = store.refresh_graph(
        household_id=household_id,
        state=state,
        query=query,
        fitness_goal=None,
    )
    response = HouseholdOSDecisionEngine().run(
        household_id=household_id,
        query=query,
        graph=graph,
        request_id="governor-001",
    )
    return response, graph


def _test_client_with_temp_runtime_store(tmp_path):
    import apps.api.assistant_runtime_router as assistant_runtime_router

    store = HouseholdStateGraphStore(graph_path=Path(tmp_path) / "output_governor_graph.json")
    assistant_runtime_router.runtime_orchestrator = HouseholdOSOrchestrator(state_store=store)
    return TestClient(app)


def test_daily_focus_never_returns_booking_meal_or_fitness_actions():
    response, _graph = _runtime_response_for("I need to start working out")
    governor = OutputGovernor()

    payload = governor.govern(
        user_message="What should I focus on today?",
        payload={
            "action_id": response.recommended_action.action_id,
            "recommendation": "Schedule a 45-minute workout tomorrow morning at 6:00 AM before your day fills up.",
            "why": [
                "Your schedule is already pretty full",
                "You're trying to build consistency",
                "You haven't worked out recently",
            ],
            "impact": "This protects a repeatable workout slot.",
            "approval_required": True,
        },
        decision_response=response,
    )

    assert payload.recommendation == "Block 30 minutes this morning to review your schedule and prioritize your most important tasks before your day fills up."
    assert "workout" not in payload.recommendation.lower()
    assert "cook" not in payload.recommendation.lower()
    assert "appointment" not in payload.recommendation.lower()


def test_no_banned_system_phrases_appear_in_output():
    response, _graph = _runtime_response_for("Schedule a doctor appointment")
    governor = OutputGovernor()

    payload = governor.govern(
        user_message="Schedule a doctor appointment",
        payload={
            "action_id": response.recommended_action.action_id,
            "recommendation": "Book the appointment tomorrow morning because your day already has 5 planned commitments and 2 calendar events.",
            "why": [
                "A low-conflict window is available",
                "The execution pipeline found room",
                "The graph state is stable",
            ],
            "impact": "The decision engine picked a good slot.",
            "approval_required": True,
        },
        decision_response=response,
    )

    text = " ".join([payload.recommendation, payload.impact, *payload.why]).lower()
    for banned in ("low-conflict window", "planned commitments", "calendar events", "execution pipeline", "graph state", "decision engine"):
        assert banned not in text


def test_fallback_always_returns_schedule_review_recommendation():
    response, _graph = _runtime_response_for("What's for dinner tonight?")
    governor = OutputGovernor()

    payload = governor.govern(
        user_message="What should I focus on today?",
        payload={
            "action_id": response.recommended_action.action_id,
            "recommendation": "Cook dinner tonight.",
            "why": ["Meal planning helps"],
            "impact": "Dinner will be ready.",
            "approval_required": True,
        },
        decision_response=response,
    )

    assert payload.recommendation == "Block 30 minutes this morning to review your schedule and prioritize your most important tasks before your day fills up."


def test_daily_focus_api_response_uses_governed_fallback(tmp_path):
    client = _test_client_with_temp_runtime_store(tmp_path)

    response = client.post(
        "/assistant/run",
        json={
            "message": "What should I focus on today?",
            "household_id": "daily-focus-api-household",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["recommendation"] == "Block 30 minutes this morning to review your schedule and prioritize your most important tasks before your day fills up."
    assert all(banned not in payload["recommendation"].lower() for banned in ("calendar events", "planned commitments", "decision engine", "graph state"))
