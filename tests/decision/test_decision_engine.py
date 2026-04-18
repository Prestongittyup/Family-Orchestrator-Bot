from __future__ import annotations

from dataclasses import dataclass

from apps.api.integration_core.decision_engine import DecisionEngine


@dataclass
class _MockState:
    calendar_events: list[dict]
    metadata: dict


def mock_state_with_events() -> _MockState:
    return _MockState(
        calendar_events=[
            {
                "event_id": "evt-1",
                "title": "Event 1",
                "start": "2026-04-18T10:00:00+00:00",
                "end": "2026-04-18T11:00:00+00:00",
            },
            {
                "event_id": "evt-2",
                "title": "Event 2",
                "start": "2026-04-18T12:00:00+00:00",
                "end": "2026-04-18T13:00:00+00:00",
            },
        ],
        metadata={"reference_time": "2026-04-18T09:00:00+00:00"},
    )


def mock_state_with_varied_times() -> _MockState:
    return _MockState(
        calendar_events=[
            {
                "event_id": "evt-late",
                "title": "Late",
                "start": "2026-04-19T15:00:00+00:00",
                "end": "2026-04-19T16:00:00+00:00",
            },
            {
                "event_id": "evt-near",
                "title": "Near",
                "start": "2026-04-18T09:30:00+00:00",
                "end": "2026-04-18T10:00:00+00:00",
            },
            {
                "event_id": "evt-today",
                "title": "Today",
                "start": "2026-04-18T11:00:00+00:00",
                "end": "2026-04-18T12:00:00+00:00",
            },
        ],
        metadata={"reference_time": "2026-04-18T09:00:00+00:00"},
    )


def mock_overlapping_events() -> _MockState:
    return _MockState(
        calendar_events=[
            {
                "event_id": "evt-a",
                "title": "A",
                "start": "2026-04-18T10:00:00+00:00",
                "end": "2026-04-18T11:00:00+00:00",
            },
            {
                "event_id": "evt-b",
                "title": "B",
                "start": "2026-04-18T10:30:00+00:00",
                "end": "2026-04-18T11:30:00+00:00",
            },
        ],
        metadata={"reference_time": "2026-04-18T09:00:00+00:00"},
    )


def mock_state() -> _MockState:
    return _MockState(
        calendar_events=[
            {
                "event_id": "evt-1",
                "title": "Stable",
                "start": "2026-04-18T10:00:00+00:00",
                "end": "2026-04-18T11:00:00+00:00",
            }
        ],
        metadata={"reference_time": "2026-04-18T09:00:00+00:00"},
    )


def test_decision_engine_is_deterministic() -> None:
    state = mock_state_with_events()

    engine = DecisionEngine()

    result1 = engine.process(state)
    result2 = engine.process(state)

    assert result1 == result2


def test_events_are_ranked_by_time() -> None:
    state = mock_state_with_varied_times()

    engine = DecisionEngine()
    result = engine.process(state)

    assert result.top_events[0]["start"] < result.top_events[-1]["start"]


def test_conflicts_detected() -> None:
    state = mock_overlapping_events()

    engine = DecisionEngine()
    result = engine.process(state)

    assert len(result.conflicts) > 0


def test_state_not_mutated() -> None:
    state = mock_state()

    original = list(state.calendar_events)

    engine = DecisionEngine()
    engine.process(state)

    assert state.calendar_events == original
