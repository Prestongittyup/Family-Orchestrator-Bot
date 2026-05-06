from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import command as command_api
from app.api import reminders as reminders_api
from app.api import schedule as schedule_api
from app.api import tasks as tasks_api
from app.services.commands.runtime import CommandRuntimeService
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
    app.include_router(tasks_api.router)
    app.include_router(schedule_api.router)
    app.include_router(reminders_api.router)

    command_getter = command_api.get_command_runtime_service
    tasks_getter = tasks_api.get_command_runtime_service
    schedule_getter = schedule_api.get_command_runtime_service
    reminders_getter = reminders_api.get_command_runtime_service

    command_api.get_command_runtime_service = lambda: runtime
    tasks_api.get_command_runtime_service = lambda: runtime
    schedule_api.get_command_runtime_service = lambda: runtime
    reminders_api.get_command_runtime_service = lambda: runtime

    class _PatchedClient(TestClient):
        def __exit__(self, exc_type, exc_val, exc_tb):
            command_api.get_command_runtime_service = command_getter
            tasks_api.get_command_runtime_service = tasks_getter
            schedule_api.get_command_runtime_service = schedule_getter
            reminders_api.get_command_runtime_service = reminders_getter
            return super().__exit__(exc_type, exc_val, exc_tb)

    return _PatchedClient(app)


def _post_command(
    client: TestClient,
    *,
    command_type: str,
    household_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    response = client.post(
        "/command",
        json={
            "command_type": command_type,
            "household_id": household_id,
            "payload": payload,
        },
    )
    assert response.status_code == 200
    return response.json()


def _create_reminder(
    client: TestClient,
    *,
    household_id: str,
    title: str,
    trigger_at: str,
) -> dict[str, Any]:
    body = _post_command(
        client,
        command_type="reminder.create",
        household_id=household_id,
        payload={
            "title": title,
            "message": f"message-{title}",
            "trigger_at": trigger_at,
        },
    )
    assert body["status"] == "accepted"
    return body


def _extract_reminder_ids(payload: dict[str, Any]) -> list[str]:
    return [str(item.get("reminder_id") or "") for item in payload.get("reminders") or []]


def _extract_task_ids(payload: dict[str, Any]) -> list[str]:
    return [str(item.get("task_id") or "") for item in payload.get("tasks") or []]


def _extract_schedule_ids(payload: dict[str, Any]) -> list[str]:
    return [str(item.get("schedule_id") or "") for item in payload.get("schedules") or []]


def test_system_trigger_governance_gates_manual_future_trigger() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-guardrails-system-trigger"
        future_trigger_at = (datetime.now(UTC) + timedelta(days=30)).isoformat().replace("+00:00", "Z")

        created = _create_reminder(
            client,
            household_id=household_id,
            title="Future-only trigger",
            trigger_at=future_trigger_at,
        )
        reminder_id = str(created["response"]["reminder"]["reminder_id"])
        events_after_create = len(event_log.insert_order)

        manual = _post_command(
            client,
            command_type="reminder.trigger",
            household_id=household_id,
            payload={"reminder_id": reminder_id, "system_trigger": False},
        )

        assert manual["status"] == "rejected"
        assert manual["response"]["code"] == "reminder_trigger_future_manual"
        assert len(event_log.insert_order) == events_after_create

        system_allowed = _post_command(
            client,
            command_type="reminder.trigger",
            household_id=household_id,
            payload={"reminder_id": reminder_id, "system_trigger": True},
        )

    assert system_allowed["status"] == "accepted"
    assert len(event_log.insert_order) == events_after_create + 3
    assert event_log.insert_order[-2].type == "ReminderTriggered"


def test_reminder_tie_break_replay_stability_and_pagination_slice_stability() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-guardrails-reminder-tie-break"
        shared_trigger = "2032-01-01T09:00:00Z"

        for index in range(6):
            _create_reminder(
                client,
                household_id=household_id,
                title=f"Tie break {index}",
                trigger_at=shared_trigger,
            )

        params = {
            "household_id": household_id,
            "sort_by": "trigger_at",
            "order": "asc",
            "limit": 3,
            "offset": 1,
        }

        first = client.get("/reminders", params=params)
        second = client.get("/reminders", params=params)
        replay(event_log.insert_order)
        third = client.get("/reminders", params=params)

    assert first.status_code == 200
    assert second.status_code == 200
    assert third.status_code == 200

    first_payload = first.json()
    second_payload = second.json()
    third_payload = third.json()

    assert first_payload == second_payload
    assert second_payload == third_payload

    first_ids = _extract_reminder_ids(first_payload)
    second_ids = _extract_reminder_ids(second_payload)
    third_ids = _extract_reminder_ids(third_payload)

    assert first_ids == second_ids == third_ids

    projection = runtime.get_projection(household_id, force_replay=True)
    projection_reminders = [
        row
        for row in projection.get("reminder_list") or []
        if isinstance(row, dict)
    ]
    normalized = [reminders_api._normalize_reminder_row(row) for row in projection_reminders]
    expected_sorted = reminders_api._sorted_reminders(
        normalized,
        sort_by="trigger_at",
        order="asc",
    )
    expected_slice = reminders_api._paginated_reminders(expected_sorted, limit=3, offset=1)
    expected_ids = [str(item.get("reminder_id") or "") for item in expected_slice]
    assert first_ids == expected_ids

    tie_rows = [
        {
            "reminder_id": "b-id",
            "title": "row-b",
            "message": "",
            "trigger_at": "2032-01-01T09:00:00Z",
            "status": "active",
            "created_at": "2032-01-01T09:30:00Z",
            "triggered_at": None,
        },
        {
            "reminder_id": "a-id",
            "title": "row-a",
            "message": "",
            "trigger_at": "2032-01-01T09:00:00Z",
            "status": "active",
            "created_at": "2032-01-01T09:30:00Z",
            "triggered_at": None,
        },
        {
            "reminder_id": "c-id",
            "title": "row-c",
            "message": "",
            "trigger_at": "2032-01-01T09:00:00Z",
            "status": "active",
            "created_at": "2032-01-01T09:31:00Z",
            "triggered_at": None,
        },
    ]
    tie_sorted = reminders_api._sorted_reminders(tie_rows, sort_by="trigger_at", order="asc")
    assert [row["reminder_id"] for row in tie_sorted] == ["a-id", "b-id", "c-id"]

    replayed = replay(event_log.insert_order)
    comparison = validate_replay(runtime.get_projection(household_id, force_replay=True), replayed["derived_state"])
    assert comparison["matches"] is True


def test_domain_pattern_consistency_between_tasks_schedule_reminders() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-guardrails-domain-pattern"

        task_created = _post_command(
            client,
            command_type="task.create",
            household_id=household_id,
            payload={"title": "Pattern task", "priority": "low"},
        )
        task_id = str(task_created["response"]["task"]["task_id"])
        _post_command(
            client,
            command_type="task_completed",
            household_id=household_id,
            payload={"task_id": task_id},
        )

        schedule_created = _post_command(
            client,
            command_type="schedule.create",
            household_id=household_id,
            payload={
                "title": "Pattern schedule",
                "start_at": "2031-01-01T08:00:00Z",
                "end_at": "2031-01-01T09:00:00Z",
            },
        )
        schedule_id = str(schedule_created["response"]["schedule"]["schedule_id"])
        _post_command(
            client,
            command_type="schedule.cancel",
            household_id=household_id,
            payload={"schedule_id": schedule_id},
        )

        reminder_created = _create_reminder(
            client,
            household_id=household_id,
            title="Pattern reminder",
            trigger_at="2031-01-01T10:00:00Z",
        )
        reminder_id = str(reminder_created["response"]["reminder"]["reminder_id"])
        _post_command(
            client,
            command_type="reminder.cancel",
            household_id=household_id,
            payload={"reminder_id": reminder_id},
        )

        tasks_payload = client.get("/tasks", params={"household_id": household_id}).json()
        schedule_payload = client.get("/schedule", params={"household_id": household_id}).json()
        reminders_payload = client.get("/reminders", params={"household_id": household_id}).json()

    assert set(tasks_payload.keys()) == {"tasks", "summary", "pagination"}
    assert set(schedule_payload.keys()) == {"schedules", "summary", "pagination"}
    assert set(reminders_payload.keys()) == {"reminders", "summary", "pagination"}

    assert set(tasks_payload["pagination"].keys()) == {"limit", "offset", "returned"}
    assert set(schedule_payload["pagination"].keys()) == {"limit", "offset", "returned"}
    assert set(reminders_payload["pagination"].keys()) == {"limit", "offset", "returned"}

    assert "total" in tasks_payload["summary"]
    assert "total" in schedule_payload["summary"]
    assert "total" in reminders_payload["summary"]

    assert all(isinstance(value, int) for value in tasks_payload["summary"].values())
    assert all(isinstance(value, int) for value in schedule_payload["summary"].values())
    assert all(isinstance(value, int) for value in reminders_payload["summary"].values())


def test_cache_non_authority_projection_updates_after_trigger_event() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-guardrails-cache-authority"
        trigger_at = (datetime.now(UTC) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")

        created = _create_reminder(
            client,
            household_id=household_id,
            title="Cache authority reminder",
            trigger_at=trigger_at,
        )
        reminder_id = str(created["response"]["reminder"]["reminder_id"])

        before = client.get(
            "/reminders",
            params={"household_id": household_id, "search": "cache authority"},
        ).json()

        _post_command(
            client,
            command_type="reminder.trigger",
            household_id=household_id,
            payload={"reminder_id": reminder_id, "system_trigger": True},
        )

        after = client.get(
            "/reminders",
            params={"household_id": household_id, "search": "cache authority"},
        ).json()

    assert before["reminders"][0]["status"] == "active"
    assert after["reminders"][0]["status"] == "triggered"
    assert after["reminders"][0]["triggered_at"]


def test_high_volume_cross_domain_determinism_with_replay_equivalence() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-guardrails-high-volume"

        for index in range(12):
            task_created = _post_command(
                client,
                command_type="task.create",
                household_id=household_id,
                payload={"title": f"HV task {index}", "priority": "medium"},
            )
            task_id = str(task_created["response"]["task"]["task_id"])
            if index % 3 == 0:
                _post_command(
                    client,
                    command_type="task_completed",
                    household_id=household_id,
                    payload={"task_id": task_id},
                )

        for index in range(12):
            schedule_created = _post_command(
                client,
                command_type="schedule.create",
                household_id=household_id,
                payload={
                    "title": f"HV schedule {index}",
                    "start_at": f"2033-01-{(index % 9) + 1:02d}T08:00:00Z",
                    "end_at": f"2033-01-{(index % 9) + 1:02d}T09:00:00Z",
                },
            )
            schedule_id = str(schedule_created["response"]["schedule"]["schedule_id"])
            if index % 4 == 0:
                _post_command(
                    client,
                    command_type="schedule.cancel",
                    household_id=household_id,
                    payload={"schedule_id": schedule_id},
                )

        for index in range(12):
            reminder_created = _create_reminder(
                client,
                household_id=household_id,
                title=f"HV reminder {index}",
                trigger_at=f"2033-02-{(index % 9) + 1:02d}T10:00:00Z",
            )
            reminder_id = str(reminder_created["response"]["reminder"]["reminder_id"])
            if index % 4 == 0:
                _post_command(
                    client,
                    command_type="reminder.cancel",
                    household_id=household_id,
                    payload={"reminder_id": reminder_id},
                )
            elif index % 4 == 1:
                _post_command(
                    client,
                    command_type="reminder.trigger",
                    household_id=household_id,
                    payload={"reminder_id": reminder_id, "system_trigger": True},
                )

        tasks_params = {
            "household_id": household_id,
            "sort_by": "created_at",
            "order": "desc",
            "limit": 7,
            "offset": 2,
        }
        schedule_params = {
            "household_id": household_id,
            "sort_by": "start_at",
            "order": "asc",
            "limit": 7,
            "offset": 2,
        }
        reminder_params = {
            "household_id": household_id,
            "sort_by": "trigger_at",
            "order": "asc",
            "limit": 7,
            "offset": 2,
        }

        tasks_first = client.get("/tasks", params=tasks_params).json()
        schedule_first = client.get("/schedule", params=schedule_params).json()
        reminders_first = client.get("/reminders", params=reminder_params).json()

        replay(event_log.insert_order)

        tasks_second = client.get("/tasks", params=tasks_params).json()
        schedule_second = client.get("/schedule", params=schedule_params).json()
        reminders_second = client.get("/reminders", params=reminder_params).json()

    assert tasks_first == tasks_second
    assert schedule_first == schedule_second
    assert reminders_first == reminders_second

    assert _extract_task_ids(tasks_first) == _extract_task_ids(tasks_second)
    assert _extract_schedule_ids(schedule_first) == _extract_schedule_ids(schedule_second)
    assert _extract_reminder_ids(reminders_first) == _extract_reminder_ids(reminders_second)

    live_projection = runtime.get_projection(household_id, force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True
