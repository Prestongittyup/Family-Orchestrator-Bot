from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from fastapi.testclient import TestClient

from apps.api.adapters.email_ingestion_adapter import EmailInput, convert_email_to_event, send_email_as_event
from apps.api.core.database import SessionLocal
from apps.api.endpoints import brief_endpoint
from apps.api.models.event_log import EventLog
from apps.api.models.idempotency_key import IdempotencyKey
from apps.api.models.task import Task
from apps.api.services import decision_engine as de
from apps.api.services import synthesis_engine as se


HOUSEHOLD_ID = "hh-email-adapter-001"
FIXED_NOW = datetime(2026, 4, 15, 9, 0, 0, tzinfo=UTC)


class _FrozenDateTime(datetime):
    @classmethod
    def utcnow(cls) -> datetime:
        return cls(2026, 4, 15, 9, 0, 0)


class _FrozenDate(se.date):
    @classmethod
    def today(cls):
        return cls(2026, 4, 15)


def _json_diff(expected: Any, actual: Any) -> str:
    return (
        "EXPECTED:\n"
        + json.dumps(expected, indent=2, sort_keys=True)
        + "\n\nACTUAL:\n"
        + json.dumps(actual, indent=2, sort_keys=True)
    )


def _wait_for_tasks(household_id: str, expected_count: int, timeout_seconds: float = 5.0) -> None:
    deadline = datetime.now(UTC).timestamp() + timeout_seconds
    while datetime.now(UTC).timestamp() < deadline:
        session = SessionLocal()
        try:
            count = session.query(Task).filter(Task.household_id == household_id).count()
        finally:
            session.close()

        if count >= expected_count:
            return

    raise AssertionError(
        f"Timed out waiting for tasks for household={household_id}; expected>={expected_count}"
    )


def _mock_emails() -> list[EmailInput]:
    return [
        EmailInput(
            email_id="email-mock-001",
            household_id=HOUSEHOLD_ID,
            sender="alerts@example.test",
            subject="Meeting reminder",
            body="Reminder for household sync meeting tomorrow morning.",
            received_at="2026-04-15T09:00:00Z",
        ),
        EmailInput(
            email_id="email-mock-002",
            household_id=HOUSEHOLD_ID,
            sender="family@example.test",
            subject="Grocery request",
            body="Please pick up milk, eggs, and fruit tonight.",
            received_at="2026-04-15T09:01:00Z",
        ),
        EmailInput(
            email_id="email-mock-003",
            household_id=HOUSEHOLD_ID,
            sender="calendar@example.test",
            subject="Calendar conflict notification",
            body="Possible overlap between dentist and school pickup windows.",
            received_at="2026-04-15T09:02:00Z",
        ),
    ]


def _brief_titles(brief: dict[str, Any]) -> set[str]:
    scheduled = [dict(row) for row in brief.get("suggested_actions", [])]
    unscheduled = [dict(row) for row in brief.get("suggestions", [])]
    return {
        str(row.get("title", ""))
        for row in scheduled + unscheduled
        if str(row.get("source_module", "")) == "task_module"
    }


def _without_generated_at(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result.pop("generated_at", None)
    return result


def test_email_ingestion_adapter_end_to_end(monkeypatch, test_client: TestClient) -> None:
    monkeypatch.setattr(de, "datetime", _FrozenDateTime)
    monkeypatch.setattr(se, "date", _FrozenDate)
    monkeypatch.setattr(brief_endpoint, "_now_utc", lambda: FIXED_NOW)

    brief_endpoint._clear_brief_cache()

    emails = _mock_emails()

    for email in emails:
        event_payload = convert_email_to_event(email)

        expected_event = {
            "household_id": email.household_id,
            "type": "email_received",
            "source": "email_ingestion_adapter",
            "timestamp": email.received_at,
            "severity": "info",
            "idempotency_key": email.email_id,
        }
        actual_event = {k: event_payload[k] for k in expected_event.keys()}
        assert actual_event == expected_event, (
            f"event payload mismatch for email_id={email.email_id}\n"
            + _json_diff(expected_event, actual_event)
        )

        response = send_email_as_event(test_client, email)
        assert response.status_code == 200, (
            f"/event failed for email_id={email.email_id}\n"
            + _json_diff(event_payload, {"status_code": response.status_code, "body": response.json()})
        )

    # Idempotency check for duplicate email_id.
    duplicate_response = send_email_as_event(test_client, emails[0])
    assert duplicate_response.status_code == 200
    duplicate_body = duplicate_response.json()
    assert duplicate_body.get("status") == "duplicate_ignored"
    assert duplicate_body.get("idempotency_key") == emails[0].email_id

    _wait_for_tasks(HOUSEHOLD_ID, expected_count=len(emails))

    session = SessionLocal()
    try:
        event_rows = session.query(EventLog).filter(EventLog.household_id == HOUSEHOLD_ID).all()
        key_rows = session.query(IdempotencyKey).filter(IdempotencyKey.household_id == HOUSEHOLD_ID).all()
        task_rows = session.query(Task).filter(Task.household_id == HOUSEHOLD_ID).all()
    finally:
        session.close()

    assert all(row.type == "email_received" for row in event_rows), "non-email event type persisted"
    assert len(event_rows) == len(emails), "unexpected event log count"
    assert len(key_rows) == len(emails), "unexpected idempotency key count"
    assert len(task_rows) >= len(emails), "email ingestion did not create expected tasks"

    brief_runs: list[dict[str, Any]] = []
    for _ in range(3):
        response = test_client.get(f"/brief/{HOUSEHOLD_ID}")
        assert response.status_code == 200
        brief_runs.append(_without_generated_at(response.json()))

    assert brief_runs[0] == brief_runs[1] == brief_runs[2], (
        "determinism failure in /brief after email ingestion\n"
        + _json_diff(brief_runs[0], brief_runs[1])
    )

    body = brief_runs[0]
    assert body.get("status") == "success"
    brief = body["brief"]

    output_titles = _brief_titles(brief)
    expected_titles = {email.subject for email in emails}
    missing = sorted(expected_titles - output_titles)
    assert not missing, (
        "email-derived actions missing from scheduled/unscheduled output\n"
        + json.dumps({"missing_subjects": missing, "available_titles": sorted(output_titles)}, indent=2)
    )

