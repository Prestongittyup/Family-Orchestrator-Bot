from __future__ import annotations

from datetime import UTC, datetime

from apps.api.integration_core.decision_engine import DecisionEngine
from apps.api.integration_core.models.household_state import CalendarEvent, HouseholdState
from apps.api.integration_core.orchestrator import Orchestrator


class _MockStateBuilder:
    def __init__(self, state: HouseholdState) -> None:
        self._state = state

    def build(self, user_id: str) -> HouseholdState:
        assert user_id == "test-user"
        return self._state


def test_decision_engine_prioritization() -> None:
    reference_time = datetime(2026, 4, 18, 9, 0, tzinfo=UTC)
    near_today = CalendarEvent(
        event_id="evt-near-today",
        title="Near Today",
        start="2026-04-18T09:30:00+00:00",
        end="2026-04-18T10:30:00+00:00",
    )
    far_future = CalendarEvent(
        event_id="evt-far-future",
        title="Far Future",
        start="2026-04-25T14:00:00+00:00",
        end="2026-04-25T15:00:00+00:00",
    )
    overlap = CalendarEvent(
        event_id="evt-overlap",
        title="Overlap",
        start="2026-04-18T09:45:00+00:00",
        end="2026-04-18T10:15:00+00:00",
    )

    state = HouseholdState(
        user_id="test-user",
        calendar_events=[far_future, overlap, near_today],
        tasks=[],
        alerts=[],
        metadata={"reference_time": reference_time.isoformat()},
    )

    orchestrator = Orchestrator(_MockStateBuilder(state), decision_engine=DecisionEngine())
    result = orchestrator.build_household_state("test-user")
    assert isinstance(result, tuple)
    _, decision = result

    # nearer/today events rank ahead of far-future
    assert decision.top_events[0]["event_id"] in {"evt-near-today", "evt-overlap"}
    assert decision.top_events[-1]["event_id"] == "evt-far-future"

    # conflicts detected
    assert len(decision.conflicts) == 1
    conflict_ids = {decision.conflicts[0][0]["event_id"], decision.conflicts[0][1]["event_id"]}
    assert conflict_ids == {"evt-near-today", "evt-overlap"}
