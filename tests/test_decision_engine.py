from __future__ import annotations

from dataclasses import dataclass

from apps.api.integration_core.decision_engine import DecisionEngine


@dataclass
class _State:
    calendar_events: list[dict]
    metadata: dict


def _state() -> _State:
    return _State(
        calendar_events=[
            {
                "event_id": "evt-1",
                "title": "Now",
                "start": "2026-04-18T09:30:00+00:00",
                "end": "2026-04-18T10:30:00+00:00",
            },
            {
                "event_id": "evt-2",
                "title": "Later",
                "start": "2026-04-20T09:30:00+00:00",
                "end": "2026-04-20T10:30:00+00:00",
            },
        ],
        metadata={"reference_time": "2026-04-18T09:00:00+00:00"},
    )


def test_process_returns_expected_shape() -> None:
    result = DecisionEngine().process(_state())
    assert isinstance(result.top_events, list)
    assert isinstance(result.conflicts, list)
    assert isinstance(result.summary, dict)
    assert "total_events" in result.summary
    assert "conflict_count" in result.summary


def test_nearer_event_prioritized() -> None:
    result = DecisionEngine().process(_state())
    assert result.top_events[0]["event_id"] == "evt-1"


def test_no_state_mutation() -> None:
    state = _state()
    before = list(state.calendar_events)
    DecisionEngine().process(state)
    assert state.calendar_events == before
