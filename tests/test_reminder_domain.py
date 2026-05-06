from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import command as command_api
from app.api import reminders as reminders_api
from app.api import schedule as schedule_api
from app.api import tasks as tasks_api
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


def _event_types(event_log: _InMemoryEventLogService) -> list[str]:
    return [row.type for row in event_log.insert_order]


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
    message: str = "",
) -> dict[str, Any]:
    body = _post_command(
        client,
        command_type="reminder.create",
        household_id=household_id,
        payload={
            "title": title,
            "message": message,
            "trigger_at": trigger_at,
        },
    )
    assert body["status"] == "accepted"
    return body


def _cancel_reminder(
    client: TestClient,
    *,
    household_id: str,
    reminder_id: str,
) -> dict[str, Any]:
    return _post_command(
        client,
        command_type="reminder.cancel",
        household_id=household_id,
        payload={"reminder_id": reminder_id},
    )


def _trigger_reminder(
    client: TestClient,
    *,
    household_id: str,
    reminder_id: str,
    system_trigger: bool = False,
) -> dict[str, Any]:
    return _post_command(
        client,
        command_type="reminder.trigger",
        household_id=household_id,
        payload={
            "reminder_id": reminder_id,
            "system_trigger": system_trigger,
        },
    )


def test_reminder_command_lifecycle_emits_events_and_updates_projection() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-reminder-lifecycle"

        created_cancelled = _create_reminder(
            client,
            household_id=household_id,
            title="Pay utilities",
            message="Electricity and water bills",
            trigger_at="2030-01-05T09:00:00Z",
        )
        created_triggered = _create_reminder(
            client,
            household_id=household_id,
            title="Take vitamins",
            message="Morning supplements",
            trigger_at="2020-01-01T08:00:00Z",
        )

        cancelled_id = str(created_cancelled["response"]["reminder"]["reminder_id"])
        triggered_id = str(created_triggered["response"]["reminder"]["reminder_id"])

        cancelled = _cancel_reminder(client, household_id=household_id, reminder_id=cancelled_id)
        triggered = _trigger_reminder(client, household_id=household_id, reminder_id=triggered_id)

    assert cancelled["status"] == "accepted"
    assert cancelled["response"]["reminder"]["status"] == "cancelled"
    assert triggered["status"] == "accepted"
    assert triggered["response"]["reminder"]["status"] == "triggered"
    assert triggered["response"]["reminder"]["triggered_at"]

    projection = runtime.get_projection("household-reminder-lifecycle", force_replay=True)
    assert projection["reminders"][cancelled_id]["status"] == "cancelled"
    assert projection["reminders"][triggered_id]["status"] == "triggered"

    event_types = _event_types(event_log)
    assert event_types.count("ReminderCreated") == 2
    assert event_types.count("ReminderCancelled") == 1
    assert event_types.count("ReminderTriggered") == 1


def test_reminder_invalid_transitions_are_rejected_before_event_emission() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-reminder-invalid"

        missing_cancel = _cancel_reminder(
            client,
            household_id=household_id,
            reminder_id="missing-reminder",
        )
        assert missing_cancel["status"] == "rejected"
        assert missing_cancel["response"]["code"] == "reminder_not_found"
        assert _event_types(event_log) == []

        created_for_cancel = _create_reminder(
            client,
            household_id=household_id,
            title="Call school",
            trigger_at="2020-02-01T10:00:00Z",
        )
        reminder_cancelled_id = str(created_for_cancel["response"]["reminder"]["reminder_id"])
        first_cancel = _cancel_reminder(
            client,
            household_id=household_id,
            reminder_id=reminder_cancelled_id,
        )
        assert first_cancel["status"] == "accepted"

        trigger_cancelled = _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=reminder_cancelled_id,
        )
        assert trigger_cancelled["status"] == "rejected"
        assert trigger_cancelled["response"]["code"] == "reminder_cancelled"

        created_for_trigger = _create_reminder(
            client,
            household_id=household_id,
            title="Replace air filter",
            trigger_at="2020-03-01T11:00:00Z",
        )
        reminder_triggered_id = str(created_for_trigger["response"]["reminder"]["reminder_id"])
        first_trigger = _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=reminder_triggered_id,
        )
        assert first_trigger["status"] == "accepted"

        cancel_triggered = _cancel_reminder(
            client,
            household_id=household_id,
            reminder_id=reminder_triggered_id,
        )
        assert cancel_triggered["status"] == "rejected"
        assert cancel_triggered["response"]["code"] == "reminder_already_triggered"

        created_future = _create_reminder(
            client,
            household_id=household_id,
            title="Prepare travel bag",
            trigger_at="2099-01-01T08:00:00Z",
        )
        future_reminder_id = str(created_future["response"]["reminder"]["reminder_id"])

        manual_future_trigger = _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=future_reminder_id,
        )
        assert manual_future_trigger["status"] == "rejected"
        assert manual_future_trigger["response"]["code"] == "reminder_trigger_future_manual"

        system_future_trigger = _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=future_reminder_id,
            system_trigger=True,
        )
        assert system_future_trigger["status"] == "accepted"

    event_types = _event_types(event_log)
    assert event_types.count("ReminderCreated") == 3
    assert event_types.count("ReminderCancelled") == 1
    assert event_types.count("ReminderTriggered") == 2


def test_reminder_projection_parity_between_runtime_and_replay() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-reminder-parity"
        created_one = _create_reminder(
            client,
            household_id=household_id,
            title="Send monthly report",
            trigger_at="2030-04-01T08:00:00Z",
        )
        created_two = _create_reminder(
            client,
            household_id=household_id,
            title="Take out trash",
            trigger_at="2020-04-01T20:00:00Z",
        )

        first_id = str(created_one["response"]["reminder"]["reminder_id"])
        second_id = str(created_two["response"]["reminder"]["reminder_id"])

        _cancel_reminder(client, household_id=household_id, reminder_id=first_id)
        _trigger_reminder(client, household_id=household_id, reminder_id=second_id)

    live_projection = runtime.get_projection("household-reminder-parity", force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])

    assert comparison["matches"] is True
    assert replayed["derived_state"]["reminders"][first_id]["status"] == "cancelled"
    assert replayed["derived_state"]["reminders"][second_id]["status"] == "triggered"


def test_reminders_read_model_is_deterministic_and_contract_stable() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-reminder-read-model"
        first = _create_reminder(
            client,
            household_id=household_id,
            title="Pay rent",
            message="Bank transfer before 5 PM",
            trigger_at="2030-08-01T09:00:00Z",
        )
        second = _create_reminder(
            client,
            household_id=household_id,
            title="Call dentist",
            message="Reschedule appointment",
            trigger_at="2020-08-01T09:30:00Z",
        )
        third = _create_reminder(
            client,
            household_id=household_id,
            title="School forms",
            message="Sign and return",
            trigger_at="2030-08-02T09:30:00Z",
        )

        _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=str(second["response"]["reminder"]["reminder_id"]),
        )
        _cancel_reminder(
            client,
            household_id=household_id,
            reminder_id=str(third["response"]["reminder"]["reminder_id"]),
        )

        params = {
            "household_id": household_id,
            "sort_by": "trigger_at",
            "order": "asc",
            "limit": 2,
            "offset": 0,
        }
        first_response = client.get("/reminders", params=params)
        second_response = client.get("/reminders", params=params)

        triggered_only = client.get(
            "/reminders",
            params={"household_id": household_id, "status": "triggered"},
        )
        searched = client.get(
            "/reminders",
            params={"household_id": household_id, "search": "bank"},
        )

    assert first_response.status_code == 200
    assert first_response.json() == second_response.json()
    assert first_response.json()["summary"] == {
        "total": 3,
        "active": 1,
        "triggered": 1,
        "cancelled": 1,
    }

    assert triggered_only.status_code == 200
    triggered_rows = triggered_only.json()["reminders"]
    assert len(triggered_rows) == 1
    assert triggered_rows[0]["status"] == "triggered"

    assert searched.status_code == 200
    searched_rows = searched.json()["reminders"]
    assert len(searched_rows) == 1
    assert searched_rows[0]["title"] == "Pay rent"

    _ = first


def test_reminders_pipeline_order_is_filter_then_search_then_sort_then_paginate() -> None:
    get_reminders_source = inspect.getsource(reminders_api.get_reminders)
    sorted_index = get_reminders_source.index("_get_or_build_sorted_view(")
    paginate_index = get_reminders_source.index("_paginated_reminders(")
    assert sorted_index < paginate_index

    filtered_source = inspect.getsource(reminders_api._filtered_records_with_summary)
    status_index = filtered_source.index("if status is not None")
    search_index = filtered_source.index("if search is not None")
    assert status_index < search_index


def test_cross_domain_determinism_with_mixed_task_schedule_reminder_stream() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-reminder-cross-domain"

        task_created = _post_command(
            client,
            command_type="task.create",
            household_id=household_id,
            payload={"title": "Pack lunches", "priority": "medium"},
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
                "title": "Swim lesson",
                "start_at": "2030-09-01T16:00:00Z",
                "end_at": "2030-09-01T17:00:00Z",
            },
        )
        schedule_id = str(schedule_created["response"]["schedule"]["schedule_id"])
        _post_command(
            client,
            command_type="schedule.cancel",
            household_id=household_id,
            payload={"schedule_id": schedule_id},
        )

        reminder_created = _post_command(
            client,
            command_type="reminder.create",
            household_id=household_id,
            payload={
                "title": "Water plants",
                "message": "Living room and balcony",
                "trigger_at": "2020-09-01T18:00:00Z",
            },
        )
        reminder_id = str(reminder_created["response"]["reminder"]["reminder_id"])
        _post_command(
            client,
            command_type="reminder.trigger",
            household_id=household_id,
            payload={"reminder_id": reminder_id},
        )

        tasks_first = client.get(
            "/tasks",
            params={"household_id": household_id, "sort_by": "created_at", "order": "desc"},
        )
        tasks_second = client.get(
            "/tasks",
            params={"household_id": household_id, "sort_by": "created_at", "order": "desc"},
        )
        schedule_first = client.get(
            "/schedule",
            params={"household_id": household_id, "sort_by": "start_at", "order": "asc"},
        )
        schedule_second = client.get(
            "/schedule",
            params={"household_id": household_id, "sort_by": "start_at", "order": "asc"},
        )
        reminders_first = client.get(
            "/reminders",
            params={"household_id": household_id, "sort_by": "trigger_at", "order": "asc"},
        )
        reminders_second = client.get(
            "/reminders",
            params={"household_id": household_id, "sort_by": "trigger_at", "order": "asc"},
        )

    assert tasks_first.status_code == 200
    assert schedule_first.status_code == 200
    assert reminders_first.status_code == 200

    assert tasks_first.json() == tasks_second.json()
    assert schedule_first.json() == schedule_second.json()
    assert reminders_first.json() == reminders_second.json()

    live_projection = runtime.get_projection("household-reminder-cross-domain", force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True
