from __future__ import annotations

import pytest

from archive.apps.api.modules.email import email_service
from archive.apps.api.schemas.events.email_events import EmailReceivedEvent


class _StubTil:
    def estimate_duration(self, task_type: str, payload: dict) -> int:
        del payload
        assert task_type == "email_received"
        return 15

    def suggest_time_slot(self, user_id: str, household_id: str, duration_minutes: int) -> dict:
        del user_id, household_id, duration_minutes
        return {
            "start_time": "2026-04-26T19:00:00Z",
            "end_time": "2026-04-26T19:15:00Z",
        }


def test_handle_email_received_uses_action_item_title_and_importance_priority(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_create_task(household_id: str, title: str):
        captured["household_id"] = household_id
        captured["title"] = title

        class _Task:
            id = "task-1"

        return _Task()

    def _fake_update_task_metadata(task_id: str, priority: str, category: str | None = None) -> None:
        captured["task_id"] = task_id
        captured["priority"] = priority
        captured["category"] = category

    def _fake_schedule_event(
        household_id: str,
        user_id: str,
        title: str,
        description: str | None = None,
        duration_minutes: int = 30,
        start_time: str | None = None,
    ) -> dict:
        captured["calendar_household_id"] = household_id
        captured["calendar_user_id"] = user_id
        captured["calendar_title"] = title
        captured["calendar_description"] = description
        captured["calendar_duration"] = duration_minutes
        captured["calendar_start_time"] = start_time
        return {
            "event_id": "evt-123",
            "household_id": household_id,
            "user_id": user_id,
            "title": title,
            "start_time": start_time,
        }

    monkeypatch.setattr(email_service, "get_til", lambda: _StubTil())
    monkeypatch.setattr(email_service, "create_task", _fake_create_task)
    monkeypatch.setattr(email_service, "update_task_metadata", _fake_update_task_metadata)
    monkeypatch.setattr(email_service, "schedule_event", _fake_schedule_event)
    monkeypatch.setattr(email_service, "evaluate_email_rules", lambda data: {"priority": "medium", "tags": []})

    payload = EmailReceivedEvent(
        subject="Weekly digest",
        sender="planner@example.test",
        summary="High priority follow-up needed.",
        importance_bucket="high",
        action_items=[
            {
                "title": "Confirm schedule changes",
                "details": "email_subject",
                "importance_score": 0.72,
                "importance_bucket": "high",
                "source_line": 0,
            }
        ],
        calendar_candidates=[
            {
                "title": "Schedule alignment call",
                "time_hint": "2026-04-27",
                "confidence": 0.77,
                "source_line": 1,
            }
        ],
    )

    result = email_service.handle_email_received("hh-123", payload)

    assert result["status"] == "email_processed"
    assert result["task_title"] == "Confirm schedule changes"
    assert result["priority"] == "high"
    assert result["calendar_event_id"] == "evt-123"

    assert captured["household_id"] == "hh-123"
    assert captured["title"] == "Confirm schedule changes"
    assert captured["task_id"] == "task-1"
    assert captured["priority"] == "high"
    assert "Calendar candidates: 1" in str(captured["category"])
    assert captured["calendar_household_id"] == "hh-123"
    assert captured["calendar_user_id"] == "system"
    assert captured["calendar_title"] == "Schedule alignment call"
    assert not str(captured["calendar_start_time"]).endswith("Z")


def test_handle_email_received_ignores_junk_emails(monkeypatch: pytest.MonkeyPatch) -> None:
    create_task_called = {"value": False}

    def _fake_create_task(household_id: str, title: str):
        del household_id, title
        create_task_called["value"] = True

        class _Task:
            id = "task-junk"

        return _Task()

    monkeypatch.setattr(email_service, "create_task", _fake_create_task)

    payload = EmailReceivedEvent(
        subject="Limited time sale",
        sender="no-reply@offers.test",
        summary="Promotional content",
        importance_bucket="low",
        is_junk=True,
        triage_decision="junk",
    )

    result = email_service.handle_email_received("hh-123", payload)

    assert result["status"] == "email_ignored_junk"
    assert create_task_called["value"] is False
