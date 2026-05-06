from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import command as command_api
from app.api import schedule as schedule_api
from app.services.commands.runtime import CommandActor, CommandRuntimeService
from core.replay.event_replay_engine import replay, validate_replay
from household_os.runtime.event_router import CanonicalEventEnvelope


# FEATURE_INTAKE:
#   projection_impact: yes
#   read_model_impact: yes
#   kernel_interaction: none
FEATURE_INTAKE_DECLARATION = {
    "projection_impact": "yes",
    "read_model_impact": "yes",
    "kernel_interaction": "none",
}


@dataclass
class _FakeEventRow:
    event_id: str
    household_id: str
    timestamp: datetime
    type: str
    payload: dict[str, Any]


class _InMemoryEventLogService:
    def __init__(self) -> None:
        self.insert_order: list[_FakeEventRow] = []

    def append_envelope(self, envelope: CanonicalEventEnvelope) -> None:
        self.insert_order.append(
            _FakeEventRow(
                event_id=envelope.event_id,
                household_id=envelope.household_id,
                timestamp=envelope.timestamp,
                type=envelope.event_type,
                payload=dict(envelope.payload),
            )
        )

    def get_event_logs(
        self,
        *,
        household_id: str,
        user_id: str | None = None,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[_FakeEventRow]:
        rows = [row for row in self.insert_order if row.household_id == household_id]
        if event_type:
            rows = [row for row in rows if row.type == event_type]

        ordered = sorted(rows, key=lambda row: (row.timestamp, row.event_id), reverse=True)
        return ordered[: max(1, int(limit))]


class _InMemoryRouterService:
    def __init__(self, event_log: _InMemoryEventLogService) -> None:
        self._event_log = event_log

    def route(
        self,
        envelope: CanonicalEventEnvelope,
        *,
        persist: bool = True,
        dispatch: bool = True,
    ) -> dict[str, Any] | None:
        if persist:
            self._event_log.append_envelope(envelope)
        if not dispatch:
            return None
        return {"status": "persisted", "event_id": envelope.event_id}


def _build_runtime() -> tuple[CommandRuntimeService, _InMemoryEventLogService]:
    event_log = _InMemoryEventLogService()
    runtime = CommandRuntimeService(
        router_service=_InMemoryRouterService(event_log),
        event_log_service=event_log,
    )
    return runtime, event_log


def _build_test_client(runtime: CommandRuntimeService) -> TestClient:
    app = FastAPI()
    app.include_router(command_api.router)
    app.include_router(schedule_api.router)

    command_getter = command_api.get_command_runtime_service
    schedule_getter = schedule_api.get_command_runtime_service
    command_api.get_command_runtime_service = lambda: runtime
    schedule_api.get_command_runtime_service = lambda: runtime

    class _PatchedClient(TestClient):
        def __exit__(self, exc_type, exc_val, exc_tb):
            command_api.get_command_runtime_service = command_getter
            schedule_api.get_command_runtime_service = schedule_getter
            return super().__exit__(exc_type, exc_val, exc_tb)

    return _PatchedClient(app)


def _event_types(event_log: _InMemoryEventLogService) -> list[str]:
    return [row.type for row in event_log.insert_order]


def _create_schedule(
    client: TestClient,
    *,
    household_id: str,
    title: str,
    start_at: str,
    end_at: str,
) -> dict[str, Any]:
    response = client.post(
        "/command",
        json={
            "command_type": "schedule.create",
            "household_id": household_id,
            "payload": {
                "title": title,
                "start_at": start_at,
                "end_at": end_at,
            },
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "accepted"
    return body


def _cancel_schedule(client: TestClient, *, household_id: str, schedule_id: str) -> dict[str, Any]:
    response = client.post(
        "/command",
        json={
            "command_type": "schedule.cancel",
            "household_id": household_id,
            "payload": {"schedule_id": schedule_id},
        },
    )
    assert response.status_code == 200
    return response.json()


def test_schedule_create_command_emits_event_and_updates_projection() -> None:
    runtime, event_log = _build_runtime()

    result = runtime.handle_command(
        command_type="schedule.create",
        household_id="household-schedule-1",
        actor=CommandActor(actor_type="api_user", user_id="user-1", session_id="session-1"),
        payload={
            "title": "Piano lesson",
            "start_at": "2026-05-01T10:00:00Z",
            "end_at": "2026-05-01T11:00:00Z",
        },
        source="tests.schedule",
    )

    assert result["status"] == "accepted"
    schedule = result["response"]["schedule"]
    assert schedule["title"] == "Piano lesson"
    assert schedule["status"] == "scheduled"

    projection = result["projection"]
    assert schedule["schedule_id"] in projection["schedules"]

    assert _event_types(event_log) == [
        "command.received",
        "ScheduleCreated",
        "projection.snapshot",
    ]


def test_schedule_cancel_command_emits_event_and_updates_projection() -> None:
    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-1")

    created = runtime.handle_command(
        command_type="schedule.create",
        household_id="household-schedule-2",
        actor=actor,
        payload={
            "title": "Doctor visit",
            "start_at": "2026-05-02T08:30:00Z",
            "end_at": "2026-05-02T09:30:00Z",
        },
        source="tests.schedule",
    )
    schedule_id = str(created["response"]["schedule"]["schedule_id"])

    cancelled = runtime.handle_command(
        command_type="schedule.cancel",
        household_id="household-schedule-2",
        actor=actor,
        payload={"schedule_id": schedule_id},
        source="tests.schedule",
    )

    assert cancelled["status"] == "accepted"
    cancelled_schedule = cancelled["response"]["schedule"]
    assert cancelled_schedule["schedule_id"] == schedule_id
    assert cancelled_schedule["status"] == "cancelled"
    assert cancelled_schedule["cancelled_at"]

    projection = cancelled["projection"]
    assert projection["schedules"][schedule_id]["status"] == "cancelled"

    event_types = _event_types(event_log)
    assert event_types.count("ScheduleCreated") == 1
    assert event_types.count("ScheduleCancelled") == 1


def test_schedule_cancel_rejected_for_missing_or_already_cancelled() -> None:
    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-1")

    missing = runtime.handle_command(
        command_type="schedule.cancel",
        household_id="household-schedule-3",
        actor=actor,
        payload={"schedule_id": "missing-schedule"},
        source="tests.schedule",
    )
    assert missing["status"] == "rejected"
    assert missing["response"]["code"] == "schedule_not_found"
    assert _event_types(event_log) == []

    created = runtime.handle_command(
        command_type="schedule.create",
        household_id="household-schedule-3",
        actor=actor,
        payload={
            "title": "School pickup",
            "start_at": "2026-05-03T15:00:00Z",
            "end_at": "2026-05-03T16:00:00Z",
        },
        source="tests.schedule",
    )
    schedule_id = str(created["response"]["schedule"]["schedule_id"])

    first_cancel = runtime.handle_command(
        command_type="schedule.cancel",
        household_id="household-schedule-3",
        actor=actor,
        payload={"schedule_id": schedule_id},
        source="tests.schedule",
    )
    assert first_cancel["status"] == "accepted"

    second_cancel = runtime.handle_command(
        command_type="schedule.cancel",
        household_id="household-schedule-3",
        actor=actor,
        payload={"schedule_id": schedule_id},
        source="tests.schedule",
    )
    assert second_cancel["status"] == "rejected"
    assert second_cancel["response"]["code"] == "schedule_already_cancelled"

    event_types = _event_types(event_log)
    assert event_types.count("ScheduleCancelled") == 1


def test_schedule_read_model_supports_status_time_range_sort_and_pagination() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-schedule-read"
        first = _create_schedule(
            client,
            household_id=household_id,
            title="Morning run",
            start_at="2026-06-01T06:00:00Z",
            end_at="2026-06-01T07:00:00Z",
        )
        second = _create_schedule(
            client,
            household_id=household_id,
            title="Team sync",
            start_at="2026-06-01T09:00:00Z",
            end_at="2026-06-01T10:00:00Z",
        )
        third = _create_schedule(
            client,
            household_id=household_id,
            title="Dinner prep",
            start_at="2026-06-01T18:00:00Z",
            end_at="2026-06-01T19:00:00Z",
        )

        cancelled_schedule_id = str(second["response"]["schedule"]["schedule_id"])
        cancelled = _cancel_schedule(client, household_id=household_id, schedule_id=cancelled_schedule_id)
        assert cancelled["status"] == "accepted"

        paged = client.get(
            "/schedule",
            params={
                "household_id": household_id,
                "sort_by": "start_at",
                "order": "asc",
                "limit": 2,
                "offset": 0,
            },
        )
        cancelled_only = client.get(
            "/schedule",
            params={"household_id": household_id, "status": "cancelled"},
        )
        in_window = client.get(
            "/schedule",
            params={
                "household_id": household_id,
                "start_from": "2026-06-01T08:00:00Z",
                "end_to": "2026-06-01T20:00:00Z",
                "sort_by": "start_at",
                "order": "asc",
            },
        )

    assert paged.status_code == 200
    paged_body = paged.json()
    assert paged_body["pagination"] == {"limit": 2, "offset": 0, "returned": 2}
    assert paged_body["summary"] == {"total": 3, "scheduled": 2, "cancelled": 1}

    assert cancelled_only.status_code == 200
    cancelled_rows = cancelled_only.json()["schedules"]
    assert len(cancelled_rows) == 1
    assert cancelled_rows[0]["schedule_id"] == cancelled_schedule_id
    assert cancelled_rows[0]["status"] == "cancelled"

    assert in_window.status_code == 200
    in_window_rows = in_window.json()["schedules"]
    assert len(in_window_rows) == 2
    assert [row["title"] for row in in_window_rows] == ["Team sync", "Dinner prep"]

    _ = first
    _ = third


def test_schedule_read_model_remains_deterministic_with_replay() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-schedule-replay"
        created_1 = _create_schedule(
            client,
            household_id=household_id,
            title="Medication reminder",
            start_at="2026-07-01T08:00:00Z",
            end_at="2026-07-01T08:30:00Z",
        )
        _create_schedule(
            client,
            household_id=household_id,
            title="Parent-teacher call",
            start_at="2026-07-01T17:00:00Z",
            end_at="2026-07-01T17:30:00Z",
        )
        _cancel_schedule(
            client,
            household_id=household_id,
            schedule_id=str(created_1["response"]["schedule"]["schedule_id"]),
        )

        response = client.get(
            "/schedule",
            params={
                "household_id": household_id,
                "sort_by": "start_at",
                "order": "asc",
            },
        )

    assert response.status_code == 200
    runtime_payload = response.json()

    live_projection = runtime.get_projection(household_id, force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True

    expected = schedule_api._sorted_schedule_entries(
        [
            schedule_api._normalize_schedule_row(schedule)
            for schedule in list(live_projection.get("schedule_list") or [])
            if isinstance(schedule, dict)
        ],
        sort_by="start_at",
        order="asc",
    )
    assert runtime_payload["schedules"] == expected
    assert runtime_payload["summary"] == {
        "total": len(expected),
        "scheduled": sum(1 for item in expected if item.get("status") == "scheduled"),
        "cancelled": sum(1 for item in expected if item.get("status") == "cancelled"),
    }
