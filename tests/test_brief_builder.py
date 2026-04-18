from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from apps.api import main
from apps.api.endpoints import brief_endpoint
from apps.api.integration_core.brief_builder import BriefBuilder
from apps.api.integration_core.decision_engine import DecisionEngine
from apps.api.integration_core.models.household_state import CalendarEvent, HouseholdState
from apps.api.integration_core.orchestrator import Orchestrator


class _MockStateBuilder:
    def __init__(self, state: HouseholdState) -> None:
        self._state = state
        self.fetch_calls = 0

    def build(self, user_id: str) -> HouseholdState:
        assert user_id == "brief-user"
        return self._state

    def fetch_events(self, *args, **kwargs):
        self.fetch_calls += 1
        raise AssertionError("Brief flow must not fetch calendar data directly")


def test_brief_uses_state_only(monkeypatch) -> None:
    reference_time = datetime(2026, 4, 17, 9, 0, tzinfo=UTC)
    today_event = CalendarEvent(
        event_id="evt-today",
        title="School pickup",
        start=(reference_time + timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
        end=(reference_time + timedelta(hours=3)).isoformat().replace("+00:00", "Z"),
    )
    later_event = CalendarEvent(
        event_id="evt-later",
        title="Dinner",
        start=(reference_time + timedelta(days=1)).isoformat().replace("+00:00", "Z"),
        end=(reference_time + timedelta(days=1, hours=1)).isoformat().replace("+00:00", "Z"),
    )
    state = HouseholdState(
        user_id="brief-user",
        calendar_events=[today_event, later_event],
        tasks=[],
        alerts=[],
        metadata={"reference_time": reference_time.isoformat().replace("+00:00", "Z")},
    )
    mock_state_builder = _MockStateBuilder(state)

    monkeypatch.setattr(
        brief_endpoint,
        "create_orchestrator",
        lambda **kwargs: Orchestrator(mock_state_builder),
    )

    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)
    response = client.get("/brief/hh-001", params={"user_id": "brief-user"})

    assert response.status_code == 200
    payload = response.json()["brief"]
    
    # Verify BOTH state events are reflected in brief
    assert len(state.calendar_events) == 2, "State has 2 events"
    assert payload["summary"]["calendar_event_count"] == 2, "Brief reflects state event count"
    assert payload["today_events"] == [today_event.as_dict()], "Today's event is present"
    assert payload["next_upcoming_event"] == today_event.as_dict(), "Next event is today's"
    
    # Verify calendar section extracted from state
    assert payload.get("calendar") is not None, "Calendar section must be present"
    assert payload["calendar"]["total_events"] == 2, "Calendar section reflects state count"
    
    # Verify no provider calls during brief generation
    assert mock_state_builder.fetch_calls == 0, "Brief must not call fetch_events()"


def test_brief_matches_state_projection(monkeypatch) -> None:
    """Enforce that Brief is a pure projection of HouseholdState.
    
    Verifies:
    - Brief contains ALL events from state via summary count
    - Brief contains NO additional events (summary count matches state)
    - Brief never calls provider.fetch_events()
    - Brief never mutates state
    - If state has events → brief MUST include them
    """
    reference_time = datetime(2026, 4, 18, 10, 0, tzinfo=UTC)

    # Create events exactly as specified in requirements
    event_a = CalendarEvent(
        event_id="1",
        title="Event A",
        start="2026-04-18T10:00:00Z",
        end="2026-04-18T11:00:00Z",
    )
    event_b = CalendarEvent(
        event_id="2",
        title="Event B",
        start="2026-04-19T12:00:00Z",
        end="2026-04-19T13:00:00Z",
    )

    # Build state with exactly 2 events
    state = HouseholdState(
        user_id="state-proj-user",
        calendar_events=[event_a, event_b],
        tasks=[],
        alerts=[],
        metadata={"reference_time": reference_time.isoformat().replace("+00:00", "Z")},
    )

    # Record original state for mutation detection
    original_event_count = len(state.calendar_events)
    original_event_ids = [e.event_id for e in state.calendar_events]

    mock_state_builder = _MockStateBuilder(state)
    mock_state_builder.build = lambda user_id: (
        state if user_id == "state-proj-user" else None
    )

    monkeypatch.setattr(
        brief_endpoint,
        "create_orchestrator",
        lambda **kwargs: Orchestrator(mock_state_builder),
    )

    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)
    response = client.get("/brief/hh-001", params={"user_id": "state-proj-user"})

    assert response.status_code == 200
    brief = response.json()["brief"]

    # REQUIREMENT: If state has events → brief MUST include them
    assert original_event_count > 0, "Test precondition: state must have events"
    assert brief["summary"]["calendar_event_count"] == original_event_count, \
        f"Brief event count {brief['summary']['calendar_event_count']} != state event count {original_event_count}"

    # Verify calendar section reflects state
    assert brief.get("calendar") is not None, "Calendar section must exist"
    assert brief["calendar"]["total_events"] == original_event_count, \
        f"Calendar section total_events {brief['calendar']['total_events']} != state count {original_event_count}"

    # Verify event IDs are present in today_events or summary context
    if brief["today_events"]:
        brief_event_ids = {e["event_id"] for e in brief["today_events"]}
        assert any(eid in brief_event_ids for eid in original_event_ids), \
            "At least one state event must appear in brief.today_events"

    # Verify no provider calls occurred during brief generation
    assert mock_state_builder.fetch_calls == 0, "Brief must not call fetch_events()"

    # Verify state was not mutated
    assert len(state.calendar_events) == original_event_count, \
        "State event count must not change"
    assert [e.event_id for e in state.calendar_events] == original_event_ids, \
        "State event IDs must not change"
    assert state.calendar_events[0].event_id == "1", \
        "State event order must be preserved"
    assert state.calendar_events[1].event_id == "2", \
        "State event order must be preserved"


def test_brief_includes_decision_when_available() -> None:
    state = HouseholdState(
        user_id="decision-user",
        calendar_events=[
            CalendarEvent(
                event_id="evt-1",
                title="Decision Event",
                start="2026-04-18T10:00:00+00:00",
                end="2026-04-18T11:00:00+00:00",
            )
        ],
        tasks=[],
        alerts=[],
        metadata={"reference_time": "2026-04-18T09:00:00+00:00"},
    )
    decision = DecisionEngine().process(state)

    brief = BriefBuilder().build(state, decision)

    assert "next_event" in brief
    assert "top_events" in brief
    assert "conflicts" in brief