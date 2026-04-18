from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from fastapi.testclient import TestClient

from apps.api.core.database import SessionLocal
from apps.api.endpoints import brief_endpoint
from apps.api.models.event_log import EventLog
from apps.api.models.idempotency_key import IdempotencyKey
from apps.api.models.task import Task
from datetime import date as _date


HOUSEHOLD_ID = "hh-decision-trace-001"
FIXED_NOW = datetime(2026, 4, 15, 9, 0, 0, tzinfo=UTC)


class _FrozenDateTime(datetime):
    @classmethod
    def utcnow(cls) -> datetime:
        return cls(2026, 4, 15, 9, 0, 0)


class _FrozenDate(_date):
    @classmethod
    def today(cls):
        return cls(2026, 4, 15)


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
    raise AssertionError(f"Timed out waiting for tasks for household={household_id}")


def _build_dataset() -> list[dict[str, Any]]:
    return [
        {
            "household_id": HOUSEHOLD_ID,
            "type": "task_created",
            "source": "trace_test",
            "timestamp": "2026-04-15T09:00:00Z",
            "idempotency_key": "trace-task-001",
            "payload": {"title": "Trace grocery"},
        },
        {
            "household_id": HOUSEHOLD_ID,
            "type": "email_received",
            "source": "trace_test",
            "timestamp": "2026-04-15T09:01:00Z",
            "idempotency_key": "trace-task-002",
            "payload": {
                "subject": "Trace maintenance",
                "priority": "low",
                "category": "reference=tr-maint; time_window=2026-04-16T10:00:00->2026-04-16T11:00:00",
            },
        },
        {
            "household_id": HOUSEHOLD_ID,
            "type": "email_received",
            "source": "trace_test",
            "timestamp": "2026-04-15T09:02:00Z",
            "idempotency_key": "trace-task-003",
            "payload": {
                "subject": "Trace overlap candidate",
                "priority": "medium",
                "category": "reference=tr-overlap; time_window=2026-04-16T10:00:00->2026-04-16T11:00:00",
            },
        },
        {
            "household_id": HOUSEHOLD_ID,
            "type": "calendar_event_scheduled",
            "source": "trace_test",
            "timestamp": "2026-04-15T09:03:00Z",
            "idempotency_key": "trace-calendar-001",
            "payload": {
                "event_id": "trace-evt-1",
                "title": "Trace fixed appointment",
                "start_time": "2026-04-16T18:00:00",
                "end_time": "2026-04-16T19:00:00",
                "priority": 4,
            },
        },
    ]


def _response_without_generated_at(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(payload)
    cleaned.pop("generated_at", None)
    return cleaned


def test_decision_trace_debug_layer(monkeypatch, test_client: TestClient) -> None:
    monkeypatch.setattr(brief_endpoint, "_now_utc", lambda: FIXED_NOW)

    brief_endpoint._clear_brief_cache()

    dataset = _build_dataset()

    for event in dataset:
        response = test_client.post("/event", json=event)
        assert response.status_code == 200, response.text

    _wait_for_tasks(HOUSEHOLD_ID, expected_count=3)

    baseline_response = test_client.get(f"/brief/{HOUSEHOLD_ID}")
    assert baseline_response.status_code == 200
    baseline_body = baseline_response.json()

    trace_runs: list[dict[str, Any]] = []
    for _ in range(3):
        response = test_client.get(f"/brief/{HOUSEHOLD_ID}?include_trace=true")
        assert response.status_code == 200
        trace_runs.append(_response_without_generated_at(response.json()))

    # Deterministic trace output over repeated runs.
    assert trace_runs[0] == trace_runs[1] == trace_runs[2], (
        "Trace output is non-deterministic\n"
        + json.dumps(trace_runs, indent=2, sort_keys=True)
    )

    trace_body = trace_runs[0]
    assert trace_body.get("status") == "success"
    assert "brief" in trace_body

    brief = trace_body["brief"]
    assert "decision_trace" in brief

    # Ensure primary /brief output structure remains unchanged by default.
    assert "decision_trace" not in baseline_body["brief"]

    baseline_brief_without_trace = dict(brief)
    baseline_brief_without_trace.pop("decision_trace", None)
    assert baseline_brief_without_trace == baseline_body["brief"], (
        "Trace flag changed primary /brief structure or values\n"
        + json.dumps(
            {
                "baseline": baseline_body["brief"],
                "trace_without_decision_trace": baseline_brief_without_trace,
            },
            indent=2,
            sort_keys=True,
        )
    )

    decision_trace = brief["decision_trace"]
    assert set(decision_trace.keys()) == {"scheduled", "unscheduled", "summary"}

    scheduled_trace = decision_trace["scheduled"]
    unscheduled_trace = decision_trace["unscheduled"]
    summary = decision_trace["summary"]

    assert isinstance(scheduled_trace, list)
    assert isinstance(unscheduled_trace, list)
    assert isinstance(summary, dict)

    scheduled_actions = brief["suggested_actions"]
    unscheduled_actions = brief["suggestions"]

    scheduled_by_id = {str(row.get("proposal_id")): row for row in scheduled_actions}
    unscheduled_by_id = {str(row.get("proposal_id")): row for row in unscheduled_actions}

    scheduled_trace_by_id = {str(row.get("proposal_id")): row for row in scheduled_trace}
    unscheduled_trace_by_id = {str(row.get("proposal_id")): row for row in unscheduled_trace}

    all_output_ids = set(scheduled_by_id.keys()) | set(unscheduled_by_id.keys())
    all_trace_ids = set(scheduled_trace_by_id.keys()) | set(unscheduled_trace_by_id.keys())

    assert all_output_ids == all_trace_ids, (
        "Trace coverage mismatch\n"
        + json.dumps(
            {
                "output_ids": sorted(all_output_ids),
                "trace_ids": sorted(all_trace_ids),
            },
            indent=2,
        )
    )

    for proposal_id, row in scheduled_trace_by_id.items():
        assert "final_score" in row
        assert "reason_assigned" in row
        assert row["reason_assigned"]
        assert proposal_id in scheduled_by_id

    for proposal_id, row in unscheduled_trace_by_id.items():
        assert "rejection_reason" in row
        assert proposal_id in unscheduled_by_id

    assert summary["total_proposals_evaluated"] == len(all_output_ids)
    assert summary["total_scheduled"] == len(scheduled_actions)
    assert summary["total_unscheduled"] == len(unscheduled_actions)
    assert summary["scheduling_pass_count"] == 1

