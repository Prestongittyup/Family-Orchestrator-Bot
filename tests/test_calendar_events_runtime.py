from __future__ import annotations

import pytest

from apps.api.schemas.event import SystemEvent
from apps.api.services import calendar_service


class _CaptureRouter:
    def __init__(self) -> None:
        self.events: list[SystemEvent] = []

    def emit(self, event: SystemEvent) -> None:
        self.events.append(event)


class _SuccessSession:
    def execute(self, *_args, **_kwargs):
        return None

    def commit(self) -> None:
        return None

    def close(self) -> None:
        return None


class _FailingSession(_SuccessSession):
    def commit(self) -> None:
        raise ValueError("invalid calendar payload")


def test_persist_calendar_event_success_emits_system_event(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _CaptureRouter()

    monkeypatch.setattr(calendar_service, "router", capture)
    monkeypatch.setattr(calendar_service, "SessionLocal", lambda: _SuccessSession())

    calendar_service._persist_calendar_event(
        event_id="evt-1",
        household_id="hh-1",
        title="Dinner",
        start_time="2026-04-23T18:00:00",
        end_time="2026-04-23T19:00:00",
        priority=3,
        metadata={"description": "Family dinner"},
    )

    assert len(capture.events) == 1
    emitted = capture.events[0]
    assert isinstance(emitted, SystemEvent)
    assert emitted.type == "calendar_event_created"


def test_persist_calendar_event_failure_emits_failure_event(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _CaptureRouter()

    monkeypatch.setattr(calendar_service, "router", capture)
    monkeypatch.setattr(calendar_service, "SessionLocal", lambda: _FailingSession())

    with pytest.raises(ValueError):
        calendar_service._persist_calendar_event(
            event_id="evt-2",
            household_id="hh-2",
            title="",
            start_time="2026-04-23T18:00:00",
            end_time="2026-04-23T19:00:00",
            priority=3,
            metadata={"description": "Bad payload"},
        )

    assert len(capture.events) == 1
    emitted = capture.events[0]
    assert isinstance(emitted, SystemEvent)
    assert emitted.type == "calendar_event_creation_failed"
    assert emitted.payload.get("reason") == "validation_error"
