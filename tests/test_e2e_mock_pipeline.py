from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import text

from apps.api.core.database import SessionLocal
from apps.api.endpoints import brief_endpoint
from apps.api.models.event_log import EventLog
from apps.api.models.idempotency_key import IdempotencyKey
from apps.api.models.task import Task
from apps.api.services import decision_engine as de
from apps.api.services import synthesis_engine as se
from modules.core.services.contract_registry import validate_brief_output_contract


HOUSEHOLD_ID = "hh-e2e-mock-pipeline-001"
FIXED_NOW = datetime(2026, 4, 15, 9, 0, 0, tzinfo=UTC)
CALENDAR_BLOCK_START = "2026-04-16T18:00:00"
CALENDAR_BLOCK_END = "2026-04-16T19:00:00"


class _FrozenDateTime(datetime):
    @classmethod
    def utcnow(cls) -> datetime:
        return cls(2026, 4, 15, 9, 0, 0)


class _FrozenDate(se.date):
    @classmethod
    def today(cls):
        return cls(2026, 4, 15)


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _overlaps(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    start_a = _dt(a_start)
    end_a = _dt(a_end)
    start_b = _dt(b_start)
    end_b = _dt(b_end)
    return start_a < end_b and start_b < end_a


def _hours_between(start: str, end: str) -> int:
    delta = _dt(end) - _dt(start)
    return int(delta.total_seconds() // 3600)


def _wait_for_tasks(household_id: str, expected_count: int, timeout_seconds: float = 5.0) -> None:
    deadline = datetime.now(UTC).timestamp() + timeout_seconds
    while datetime.now(UTC).timestamp() < deadline:
        session = SessionLocal()
        try:
            count = (
                session.query(Task)
                .filter(Task.household_id == household_id)
                .count()
            )
        finally:
            session.close()

        if count >= expected_count:
            return

    raise AssertionError(
        f"Timed out waiting for persisted tasks. Expected >= {expected_count}."
    )


def _json_diff(expected: Any, actual: Any) -> str:
    expected_text = json.dumps(expected, indent=2, sort_keys=True)
    actual_text = json.dumps(actual, indent=2, sort_keys=True)
    return f"EXPECTED:\n{expected_text}\n\nACTUAL:\n{actual_text}"


def _assert_deep_equal(expected: Any, actual: Any, message: str) -> None:
    if expected != actual:
        raise AssertionError(f"{message}\n{_json_diff(expected, actual)}")


def _build_mock_events() -> tuple[list[dict[str, Any]], list[str], dict[str, str]]:
    events = [
        {
            "household_id": HOUSEHOLD_ID,
            "type": "task_created",
            "source": "mock_e2e",
            "timestamp": "2026-04-15T09:00:00Z",
            "severity": "info",
            "idempotency_key": "e2e-task-001",
            "payload": {
                "title": "Grocery run"
            },
        },
        {
            "household_id": HOUSEHOLD_ID,
            "type": "email_received",
            "source": "mock_e2e",
            "timestamp": "2026-04-15T09:01:00Z",
            "severity": "info",
            "idempotency_key": "e2e-task-002",
            "payload": {
                "subject": "Household maintenance window",
                "sender": "noreply@example.test",
                "priority": "low",
                "category": "reference=maint-1; time_window=2026-04-16T10:00:00->2026-04-16T11:00:00",
            },
        },
        {
            "household_id": HOUSEHOLD_ID,
            "type": "email_received",
            "source": "mock_e2e",
            "timestamp": "2026-04-15T09:02:00Z",
            "severity": "info",
            "idempotency_key": "e2e-task-003",
            "payload": {
                "subject": "Appointment prep overlap candidate",
                "sender": "noreply@example.test",
                "priority": "medium",
                "category": "reference=appt-1; time_window=2026-04-16T10:00:00->2026-04-16T11:00:00",
            },
        },
        {
            "household_id": HOUSEHOLD_ID,
            "type": "calendar_event_scheduled",
            "source": "mock_e2e",
            "timestamp": "2026-04-15T09:03:00Z",
            "severity": "info",
            "idempotency_key": "e2e-calendar-001",
            "payload": {
                "event_id": "evt-fixed-001",
                "title": "Fixed appointment",
                "start_time": CALENDAR_BLOCK_START,
                "end_time": CALENDAR_BLOCK_END,
                "priority": 4,
            },
        },
    ]

    injected_task_titles = [
        "Grocery run",
        "Household maintenance window",
        "Appointment prep overlap candidate",
    ]

    calendar_block = {
        "start_time": CALENDAR_BLOCK_START,
        "end_time": CALENDAR_BLOCK_END,
    }

    return events, injected_task_titles, calendar_block


def _assert_no_scheduling_violations(
    scheduled_actions: list[dict[str, Any]],
    calendar_block: dict[str, str],
) -> list[str]:
    violations: list[str] = []

    for index, row in enumerate(scheduled_actions):
        start_time = row.get("start_time")
        end_time = row.get("end_time")
        duration_units = int(row.get("duration_units", 1))

        if not isinstance(start_time, str) or not isinstance(end_time, str):
            violations.append(f"scheduled_actions[{index}] missing concrete start/end time")
            continue

        computed_hours = _hours_between(start_time, end_time)
        if computed_hours != duration_units:
            violations.append(
                "duration mismatch for "
                f"proposal_id={row.get('proposal_id')}: duration_units={duration_units}, actual_hours={computed_hours}"
            )

        if _overlaps(start_time, end_time, calendar_block["start_time"], calendar_block["end_time"]):
            violations.append(
                "calendar overlap for "
                f"proposal_id={row.get('proposal_id')} with block "
                f"{calendar_block['start_time']}->{calendar_block['end_time']}"
            )

    for i in range(len(scheduled_actions)):
        for j in range(i + 1, len(scheduled_actions)):
            a = scheduled_actions[i]
            b = scheduled_actions[j]
            if _overlaps(str(a.get("start_time")), str(a.get("end_time")), str(b.get("start_time")), str(b.get("end_time"))):
                violations.append(
                    "scheduled overlap between "
                    f"proposal_id={a.get('proposal_id')} and proposal_id={b.get('proposal_id')}"
                )

    return violations


def test_e2e_mock_pipeline(monkeypatch, test_client: TestClient) -> None:
    monkeypatch.setattr(de, "datetime", _FrozenDateTime)
    monkeypatch.setattr(se, "date", _FrozenDate)
    monkeypatch.setattr(brief_endpoint, "_now_utc", lambda: FIXED_NOW)

    brief_endpoint._clear_brief_cache()

    events, injected_task_titles, calendar_block = _build_mock_events()

    for payload in events:
        response = test_client.post("/event", json=payload)
        assert response.status_code == 200, f"/event failed for key={payload['idempotency_key']}: {response.text}"

    # Duplicate-safe idempotency behavior check.
    duplicate_response = test_client.post("/event", json=events[0])
    assert duplicate_response.status_code == 200
    duplicate_body = duplicate_response.json()
    assert duplicate_body.get("status") == "duplicate_ignored"
    assert duplicate_body.get("idempotency_key") == events[0]["idempotency_key"]

    _wait_for_tasks(HOUSEHOLD_ID, expected_count=3)

    session = SessionLocal()
    try:
        persisted_event_count = (
            session.query(EventLog)
            .filter(EventLog.household_id == HOUSEHOLD_ID)
            .count()
        )
        persisted_idempotency_count = (
            session.query(IdempotencyKey)
            .filter(IdempotencyKey.household_id == HOUSEHOLD_ID)
            .count()
        )
        persisted_task_titles = {
            row.title
            for row in session.query(Task)
            .filter(Task.household_id == HOUSEHOLD_ID)
            .all()
        }
    finally:
        session.close()

    assert persisted_event_count == 4, "OS-1 persistence mismatch: expected 4 unique event log rows"
    assert persisted_idempotency_count == 4, "OS-1 persistence mismatch: expected 4 unique idempotency keys"
    assert set(injected_task_titles).issubset(persisted_task_titles), "OS-1 task persistence missing injected tasks"

    responses: list[dict[str, Any]] = []
    for _ in range(3):
        response = test_client.get(f"/brief/{HOUSEHOLD_ID}")
        assert response.status_code == 200, f"/brief failed: {response.text}"
        responses.append(response.json())

    # Strict deterministic deep equality across three consecutive runs.
    _assert_deep_equal(responses[0], responses[1], "Determinism failure between run1 and run2")
    _assert_deep_equal(responses[1], responses[2], "Determinism failure between run2 and run3")

    body = responses[0]
    assert body.get("status") == "success", _json_diff({"status": "success"}, body)
    assert set(body.keys()) == {"status", "brief", "generated_at"}, (
        "Schema drift at /brief envelope. "
        f"expected keys={{'status','brief','generated_at'}}, actual={set(body.keys())}"
    )

    brief = body["brief"]
    validate_brief_output_contract(brief)

    # Canonical validation shape required by this E2E harness.
    canonical = {
        "priorities": brief["priorities"],
        "scheduled_actions": brief["suggested_actions"],
        "unscheduled_actions": brief["suggestions"],
        "warnings": brief["warnings"],
        "risks": brief["risks"],
        "summary": brief["summary_text"],
    }
    assert set(canonical.keys()) == {
        "priorities",
        "scheduled_actions",
        "unscheduled_actions",
        "warnings",
        "risks",
        "summary",
    }

    scheduled_actions = [dict(row) for row in canonical["scheduled_actions"]]
    unscheduled_actions = [dict(row) for row in canonical["unscheduled_actions"]]

    violations = _assert_no_scheduling_violations(scheduled_actions, calendar_block)
    assert not violations, "Scheduling violations detected:\n" + "\n".join(violations)

    all_output_task_titles = {
        str(row.get("title", ""))
        for row in scheduled_actions + unscheduled_actions
        if row.get("source_module") == "task_module"
    }

    missing_titles = sorted(set(injected_task_titles) - all_output_task_titles)
    assert not missing_titles, (
        "Injected tasks missing from scheduled/unscheduled output: "
        + ", ".join(missing_titles)
    )

    summary = {
        "total_events_injected": len(events),
        "scheduled_count": len(scheduled_actions),
        "unscheduled_count": len(unscheduled_actions),
        "violations": violations,
        "determinism": "pass",
    }
    print(json.dumps(summary, indent=2, sort_keys=True))

