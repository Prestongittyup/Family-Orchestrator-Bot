from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import command as command_api
from app.api import notifications as notifications_api
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
    app.include_router(notifications_api.router)

    command_getter = command_api.get_command_runtime_service
    tasks_getter = tasks_api.get_command_runtime_service
    schedule_getter = schedule_api.get_command_runtime_service
    reminders_getter = reminders_api.get_command_runtime_service
    notifications_getter = notifications_api.get_command_runtime_service

    command_api.get_command_runtime_service = lambda: runtime
    tasks_api.get_command_runtime_service = lambda: runtime
    schedule_api.get_command_runtime_service = lambda: runtime
    reminders_api.get_command_runtime_service = lambda: runtime
    notifications_api.get_command_runtime_service = lambda: runtime

    class _PatchedClient(TestClient):
        def __exit__(self, exc_type, exc_val, exc_tb):
            command_api.get_command_runtime_service = command_getter
            tasks_api.get_command_runtime_service = tasks_getter
            schedule_api.get_command_runtime_service = schedule_getter
            reminders_api.get_command_runtime_service = reminders_getter
            notifications_api.get_command_runtime_service = notifications_getter
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
    message: str,
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


def _event_types(event_log: _InMemoryEventLogService) -> list[str]:
    return [row.type for row in event_log.insert_order]


def test_event_to_notification_creation_from_reminder_triggered() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-notification-event-derivation"
        created = _create_reminder(
            client,
            household_id=household_id,
            title="Medication",
            message="Take evening dose",
            trigger_at="2020-01-01T20:00:00Z",
        )
        reminder_id = str(created["response"]["reminder"]["reminder_id"])

        triggered = _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=reminder_id,
        )
        assert triggered["status"] == "accepted"

        notifications_response = client.get(
            "/notifications",
            params={"household_id": household_id},
        )

    assert notifications_response.status_code == 200
    notifications_payload = notifications_response.json()
    assert len(notifications_payload["notifications"]) == 1

    notification = notifications_payload["notifications"][0]
    reminder_trigger_events = [
        row for row in event_log.insert_order if row.type == "ReminderTriggered"
    ]
    assert len(reminder_trigger_events) == 1
    assert notification["source_event_id"] == reminder_trigger_events[0].event_id
    assert notification["source_type"] == "reminder"
    assert notification["delivery_status"] == "pending"
    assert notification["message"] == "Take evening dose"

    # Notification is derived state, not a command/event mutation path.
    assert "NotificationCreated" not in _event_types(event_log)


def test_notification_projection_replay_parity() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-notification-replay"

        created_one = _create_reminder(
            client,
            household_id=household_id,
            title="Laundry",
            message="Move clothes to dryer",
            trigger_at="2020-01-02T10:00:00Z",
        )
        created_two = _create_reminder(
            client,
            household_id=household_id,
            title="Garbage",
            message="Take bins to curb",
            trigger_at="2020-01-02T11:00:00Z",
        )

        _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=str(created_one["response"]["reminder"]["reminder_id"]),
        )
        _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=str(created_two["response"]["reminder"]["reminder_id"]),
        )

    live_projection = runtime.get_projection(household_id, force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])

    assert comparison["matches"] is True
    assert live_projection["notifications"] == replayed["derived_state"]["notifications"]
    assert live_projection["notification_list"] == replayed["derived_state"]["notification_list"]


def test_notifications_read_model_is_deterministic_for_identical_query() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-notification-determinism"
        for index in range(4):
            created = _create_reminder(
                client,
                household_id=household_id,
                title=f"Reminder {index}",
                message=f"Message {index}",
                trigger_at="2020-01-03T10:00:00Z",
            )
            _trigger_reminder(
                client,
                household_id=household_id,
                reminder_id=str(created["response"]["reminder"]["reminder_id"]),
            )

        params = {
            "household_id": household_id,
            "delivery_status": "pending",
            "source_type": "reminder",
            "sort_by": "created_at",
            "order": "desc",
            "limit": 2,
            "offset": 1,
            "search": "message",
        }
        first = client.get("/notifications", params=params)
        second = client.get("/notifications", params=params)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()


def test_cross_domain_determinism_with_notifications_and_replay() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-notification-cross-domain"

        task_created = _post_command(
            client,
            command_type="task.create",
            household_id=household_id,
            payload={"title": "Pack snacks", "priority": "medium"},
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
                "title": "Piano class",
                "start_at": "2030-01-03T15:00:00Z",
                "end_at": "2030-01-03T16:00:00Z",
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
            title="Water plants",
            message="Balcony and kitchen",
            trigger_at="2020-01-03T19:00:00Z",
        )
        reminder_id = str(reminder_created["response"]["reminder"]["reminder_id"])
        _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=reminder_id,
        )

        tasks_params = {"household_id": household_id, "sort_by": "created_at", "order": "desc"}
        schedule_params = {"household_id": household_id, "sort_by": "start_at", "order": "asc"}
        reminders_params = {"household_id": household_id, "sort_by": "trigger_at", "order": "asc"}
        notifications_params = {"household_id": household_id, "sort_by": "created_at", "order": "desc"}

        tasks_first = client.get("/tasks", params=tasks_params)
        schedule_first = client.get("/schedule", params=schedule_params)
        reminders_first = client.get("/reminders", params=reminders_params)
        notifications_first = client.get("/notifications", params=notifications_params)

        replay(event_log.insert_order)

        tasks_second = client.get("/tasks", params=tasks_params)
        schedule_second = client.get("/schedule", params=schedule_params)
        reminders_second = client.get("/reminders", params=reminders_params)
        notifications_second = client.get("/notifications", params=notifications_params)

    assert tasks_first.status_code == 200
    assert schedule_first.status_code == 200
    assert reminders_first.status_code == 200
    assert notifications_first.status_code == 200

    assert tasks_first.json() == tasks_second.json()
    assert schedule_first.json() == schedule_second.json()
    assert reminders_first.json() == reminders_second.json()
    assert notifications_first.json() == notifications_second.json()

    live_projection = runtime.get_projection(household_id, force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True


def test_notifications_read_model_contract_and_pagination_sort_consistency() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-notification-contract"

        for index in range(5):
            created = _create_reminder(
                client,
                household_id=household_id,
                title=f"Contract Reminder {index}",
                message=f"Contract Message {index}",
                trigger_at="2020-01-04T09:00:00Z",
            )
            _trigger_reminder(
                client,
                household_id=household_id,
                reminder_id=str(created["response"]["reminder"]["reminder_id"]),
            )

        notifications_payload = client.get(
            "/notifications",
            params={
                "household_id": household_id,
                "sort_by": "created_at",
                "order": "desc",
                "limit": 2,
                "offset": 1,
            },
        ).json()

        tasks_payload = client.get("/tasks", params={"household_id": household_id}).json()
        schedule_payload = client.get("/schedule", params={"household_id": household_id}).json()
        reminders_payload = client.get("/reminders", params={"household_id": household_id}).json()

    assert set(notifications_payload.keys()) == {"notifications", "summary", "pagination"}
    assert set(tasks_payload.keys()) == {"tasks", "summary", "pagination"}
    assert set(schedule_payload.keys()) == {"schedules", "summary", "pagination"}
    assert set(reminders_payload.keys()) == {"reminders", "summary", "pagination"}

    assert set(notifications_payload["pagination"].keys()) == set(tasks_payload["pagination"].keys())
    assert set(notifications_payload["pagination"].keys()) == set(schedule_payload["pagination"].keys())
    assert set(notifications_payload["pagination"].keys()) == set(reminders_payload["pagination"].keys())

    assert set(notifications_payload["summary"].keys()) == {"total", "pending"}
    assert isinstance(notifications_payload["summary"]["total"], int)
    assert isinstance(notifications_payload["summary"]["pending"], int)

    projection = runtime.get_projection(household_id, force_replay=True)
    normalized = [
        notifications_api._normalize_notification_row(row)
        for row in list(projection.get("notification_list") or [])
        if isinstance(row, dict)
    ]
    expected_sorted = notifications_api._sorted_notifications(
        normalized,
        sort_by="created_at",
        order="desc",
    )
    expected_page = notifications_api._paginated_notifications(
        expected_sorted,
        limit=2,
        offset=1,
    )
    assert notifications_payload["notifications"] == expected_page


def test_notifications_cache_non_authority_after_new_event() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-notification-cache"

        created = _create_reminder(
            client,
            household_id=household_id,
            title="Cache visibility",
            message="Must appear after trigger",
            trigger_at="2020-01-05T07:00:00Z",
        )
        reminder_id = str(created["response"]["reminder"]["reminder_id"])

        before = client.get(
            "/notifications",
            params={"household_id": household_id, "search": "must appear"},
        )
        assert before.status_code == 200
        assert before.json()["notifications"] == []

        _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=reminder_id,
        )

        after = client.get(
            "/notifications",
            params={"household_id": household_id, "search": "must appear"},
        )

    assert after.status_code == 200
    after_payload = after.json()
    assert len(after_payload["notifications"]) == 1
    assert after_payload["notifications"][0]["source_type"] == "reminder"
    assert after_payload["notifications"][0]["delivery_status"] == "pending"
