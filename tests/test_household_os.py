from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from archive.apps.api.main import app
from archive.apps.api.integration_core.models.household_state import CalendarEvent
from archive.apps.assistant_core.planning_engine import _fallback_household_state
from household_os.core.decision_engine import HouseholdOSDecisionEngine
from household_os.core.household_state_graph import HouseholdStateGraphStore


def _state_with_conflicts():
    state = _fallback_household_state("household-os-conflict")
    state.calendar_events.extend(
        [
            CalendarEvent(event_id="evt-1", title="School drop-off", start="2026-04-19T08:00:00Z", end="2026-04-19T08:45:00Z"),
            CalendarEvent(event_id="evt-2", title="Doctor meeting", start="2026-04-19T08:30:00Z", end="2026-04-19T09:00:00Z"),
            CalendarEvent(event_id="evt-3", title="Dinner prep", start="2026-04-19T18:00:00Z", end="2026-04-19T18:45:00Z"),
        ]
    )
    state.metadata["reference_time"] = "2026-04-19T07:30:00Z"
    return state


def test_household_os_cross_domain_reasoning():
    """Household OS performs cross-domain reasoning from unified graph."""
    store = HouseholdStateGraphStore()
    state = _fallback_household_state("household-os-reasoning-1")
    
    graph = store.refresh_graph(
        household_id="household-os-reasoning-1",
        state=state,
        query="I'm overwhelmed this week",
        fitness_goal=None,
    )
    
    engine = HouseholdOSDecisionEngine()
    response = engine.run(
        household_id="household-os-reasoning-1",
        query="I'm overwhelmed this week",
        graph=graph,
        request_id="test-req-1",
    )
    
    assert response.request_id == "test-req-1"
    assert response.intent_interpretation.urgency in {"low", "medium", "high"}
    assert response.recommended_action.urgency in {"low", "medium", "high"}
    assert len(response.reasoning_trace) > 0


def test_household_os_single_action_output():
    """Household OS always outputs exactly ONE recommended action."""
    client = TestClient(app)
    
    response = client.post("/assistant/run", json={
        "query": "What should I cook tonight?",
        "household_id": "household-os-meal",
    })
    
    assert response.status_code == 200
    payload = response.json()
    
    # Verify response structure
    assert "request_id" in payload
    assert "intent_interpretation" in payload
    assert "current_state_summary" in payload
    assert "recommended_action" in payload
    assert "grouped_approval_payload" in payload
    
    # Verify single action
    assert isinstance(payload["recommended_action"], dict)
    assert payload["recommended_action"]["approval_required"] is True
    assert payload["recommended_action"]["approval_status"] == "pending"


def test_household_os_state_graph_consistency():
    """Household OS state graph persists and remains consistent."""
    store = HouseholdStateGraphStore()
    state1 = _fallback_household_state("household-os-consistency")
    
    graph1 = store.refresh_graph(
        household_id="household-os-consistency",
        state=state1,
        query="First query",
        fitness_goal="consistency",
    )
    
    graph2 = store.load_graph("household-os-consistency")
    
    assert graph1["state_version"] == graph2["state_version"]
    assert graph1["household_id"] == "household-os-consistency"
    assert len(graph2["event_history"]) > 0


def test_household_os_no_module_leakage():
    """Household OS response contains no module-specific internal details."""
    client = TestClient(app)
    
    response = client.post("/assistant/run", json={
        "query": "I need to start working out",
        "household_id": "household-os-no-leak",
    })
    
    assert response.status_code == 200
    payload = response.json()
    
    # Forbidden keywords that expose module structures
    response_str = str(payload)
    assert "proposals" not in response_str
    assert "candidate_schedules" not in response_str
    assert "fallback_options" not in response_str
    assert "planning_engine" not in response_str
    assert "module" not in response_str.lower()


def test_household_os_approval_recording():
    """Household OS records approvals without side effects."""
    client = TestClient(app)
    
    # Submit query
    run_response = client.post("/assistant/run", json={
        "query": "Schedule an appointment",
        "household_id": "household-os-approval",
    })
    assert run_response.status_code == 200
    
    request_id = run_response.json()["request_id"]
    action_id = run_response.json()["recommended_action"]["action_id"]
    
    # Record approval
    approval_response = client.post("/assistant/approve", json={
        "request_id": request_id,
        "action_ids": [action_id],
    })
    assert approval_response.status_code == 200
    
    payload = approval_response.json()
    assert payload["recommended_action"]["approval_status"] == "approved"
    assert payload["grouped_approval_payload"]["approval_status"] == "approved"


def test_household_os_deterministic_output():
    """Same query produces identical output structure across invocations."""
    store = HouseholdStateGraphStore()
    engine = HouseholdOSDecisionEngine()
    state = _fallback_household_state("household-os-deterministic")
    
    graph1 = store.refresh_graph(
        household_id="household-os-deterministic",
        state=state,
        query="What's for dinner?",
        fitness_goal=None,
    )
    
    response1 = engine.run(
        household_id="household-os-deterministic",
        query="What's for dinner?",
        graph=graph1,
        request_id="det-1",
    )
    
    graph2 = store.refresh_graph(
        household_id="household-os-deterministic",
        state=state,
        query="What's for dinner?",
        fitness_goal=None,
    )
    
    response2 = engine.run(
        household_id="household-os-deterministic",
        query="What's for dinner?",
        graph=graph2,
        request_id="det-1",
    )
    
    # Structure should be consistent (not necessarily identical ids/timestamps)
    assert response1.intent_interpretation.summary == response2.intent_interpretation.summary
    assert response1.recommended_action.urgency == response2.recommended_action.urgency


def test_household_os_preserves_explicit_requested_appointment_time():
    store = HouseholdStateGraphStore()
    state = _fallback_household_state("household-os-explicit-time")
    query = "lets create a calender event for 4/26/26 at 2:00pm"

    graph = store.refresh_graph(
        household_id="household-os-explicit-time",
        state=state,
        query=query,
        fitness_goal=None,
    )

    response = HouseholdOSDecisionEngine().run(
        household_id="household-os-explicit-time",
        query=query,
        graph=graph,
        request_id="explicit-time-req",
    )

    assert response.recommended_action.scheduled_for == "2026-04-26 14:00-14:45"
    assert response.recommended_action.title == "Schedule appointment for 2026-04-26 14:00-14:45"
    assert "requested time" in response.recommended_action.description.lower()


def test_household_os_preserves_explicit_requested_time_without_year():
    store = HouseholdStateGraphStore()
    state = _fallback_household_state("household-os-explicit-time-no-year")
    query = "lets create a calendar event for 4/26 at 2pm"

    graph = store.refresh_graph(
        household_id="household-os-explicit-time-no-year",
        state=state,
        query=query,
        fitness_goal=None,
    )

    response = HouseholdOSDecisionEngine().run(
        household_id="household-os-explicit-time-no-year",
        query=query,
        graph=graph,
        request_id="explicit-time-no-year-req",
    )

    assert response.recommended_action.scheduled_for == "2026-04-26 14:00-14:45"


def test_household_os_maps_relative_daypart_to_requested_slot():
    store = HouseholdStateGraphStore()
    state = _fallback_household_state("household-os-relative-daypart")
    query = "schedule a calendar event tomorrow afternoon"

    graph = store.refresh_graph(
        household_id="household-os-relative-daypart",
        state=state,
        query=query,
        fitness_goal=None,
    )

    response = HouseholdOSDecisionEngine().run(
        household_id="household-os-relative-daypart",
        query=query,
        graph=graph,
        request_id="relative-daypart-req",
    )

    assert response.recommended_action.scheduled_for == "2026-04-20 14:00-14:45"


def test_household_os_rejects_empty_allowed_domains() -> None:
    store = HouseholdStateGraphStore()
    state = _fallback_household_state("household-os-empty-domains")

    graph = store.refresh_graph(
        household_id="household-os-empty-domains",
        state=state,
        query="what should i cook tonight",
        fitness_goal=None,
    )

    with pytest.raises(ValueError, match="allowed_domains"):
        HouseholdOSDecisionEngine().run(
            household_id="household-os-empty-domains",
            query="what should i cook tonight",
            graph=graph,
            request_id="empty-domains-req",
            allowed_domains=[],
        )


def test_household_os_meal_schedule_uses_reference_date() -> None:
    store = HouseholdStateGraphStore()
    state = _fallback_household_state("household-os-meal-date")
    state.metadata["reference_time"] = "2026-04-23T09:00:00Z"

    graph = store.refresh_graph(
        household_id="household-os-meal-date",
        state=state,
        query="what should i cook tonight",
        fitness_goal=None,
    )

    response = HouseholdOSDecisionEngine().run(
        household_id="household-os-meal-date",
        query="what should i cook tonight",
        graph=graph,
        request_id="meal-date-req",
    )

    assert response.recommended_action.scheduled_for == "2026-04-23 18:30-19:15"
