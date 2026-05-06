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
from core.replay.domain_projection_helpers import (
    allowed_notification_event_sources,
    build_notification_from_reminder_triggered,
)
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
    message: str,
    trigger_at: str,
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


def _notifications_for_projection(projection: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        row
        for row in list(projection.get("notification_list") or [])
        if isinstance(row, dict)
    ]


def test_system_trigger_source_lock_only_reminder_triggered_creates_notifications() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-notification-guardrails-source-lock"

        task_created = _post_command(
            client,
            command_type="task.create",
            household_id=household_id,
            payload={"title": "Fan-out lock task", "priority": "low"},
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
                "title": "Fan-out lock schedule",
                "start_at": "2031-02-01T09:00:00Z",
                "end_at": "2031-02-01T10:00:00Z",
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
            title="Fan-out lock reminder",
            message="Only ReminderTriggered may fan out",
            trigger_at="2031-02-01T11:00:00Z",
        )
        reminder_id = str(reminder_created["response"]["reminder"]["reminder_id"])
        _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=reminder_id,
            system_trigger=True,
        )

    projection = runtime.get_projection(household_id, force_replay=True)
    notifications = _notifications_for_projection(projection)

    household_rows = [row for row in event_log.insert_order if row.household_id == household_id]
    reminder_trigger_rows = [row for row in household_rows if row.type == "ReminderTriggered"]
    non_reminder_event_ids = {row.event_id for row in household_rows if row.type != "ReminderTriggered"}

    assert allowed_notification_event_sources() == ("ReminderTriggered",)
    assert len(notifications) == len(reminder_trigger_rows) == 1

    notification = notifications[0]
    assert notification["source_type"] == "reminder"
    assert notification["source_event_id"] in {row.event_id for row in reminder_trigger_rows}
    assert notification["source_event_id"] not in non_reminder_event_ids
    assert notification["notification_id"] == f"notification-{notification['source_event_id']}"

    source_row = reminder_trigger_rows[0]
    expected = build_notification_from_reminder_triggered(
        payload=source_row.payload,
        source_event_id=source_row.event_id,
        recorded_at=source_row.timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    )
    assert expected is not None
    assert notification == expected


def test_tie_break_replay_stability_uses_timestamp_event_id_notification_id() -> None:
    tie_rows: list[dict[str, Any]] = [
        {
            "notification_id": "notification-event-b",
            "source_event_id": "event-b",
            "source_type": "reminder",
            "source_id": "reminder-b",
            "message": "row-b",
            "created_at": "2031-03-01T00:00:00Z",
            "delivery_status": "pending",
        },
        {
            "notification_id": "notification-event-a-2",
            "source_event_id": "event-a",
            "source_type": "reminder",
            "source_id": "reminder-a",
            "message": "row-a2",
            "created_at": "2031-03-01T00:00:00Z",
            "delivery_status": "pending",
        },
        {
            "notification_id": "notification-event-a-1",
            "source_event_id": "event-a",
            "source_type": "reminder",
            "source_id": "reminder-a",
            "message": "row-a1",
            "created_at": "2031-03-01T00:00:00Z",
            "delivery_status": "pending",
        },
    ]

    asc_sorted = notifications_api._sorted_notifications(tie_rows, sort_by="created_at", order="asc")
    desc_sorted = notifications_api._sorted_notifications(tie_rows, sort_by="created_at", order="desc")

    assert [row["notification_id"] for row in asc_sorted] == [
        "notification-event-a-1",
        "notification-event-a-2",
        "notification-event-b",
    ]
    assert [row["notification_id"] for row in desc_sorted] == [
        "notification-event-b",
        "notification-event-a-2",
        "notification-event-a-1",
    ]

    runtime, event_log = _build_runtime()
    with _build_test_client(runtime) as client:
        household_id = "household-notification-guardrails-tie-break"
        for index in range(5):
            created = _create_reminder(
                client,
                household_id=household_id,
                title=f"Tie {index}",
                message=f"Tie message {index}",
                trigger_at="2031-03-02T10:00:00Z",
            )
            reminder_id = str(created["response"]["reminder"]["reminder_id"])
            _trigger_reminder(
                client,
                household_id=household_id,
                reminder_id=reminder_id,
                system_trigger=True,
            )

        params = {
            "household_id": household_id,
            "sort_by": "created_at",
            "order": "asc",
            "limit": 4,
            "offset": 1,
        }
        first = client.get("/notifications", params=params)
        replay(event_log.insert_order)
        second = client.get("/notifications", params=params)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()


def test_domain_pattern_consistency_notification_matches_tasks_schedule_contract() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-notification-guardrails-domain-pattern"

        _post_command(
            client,
            command_type="task.create",
            household_id=household_id,
            payload={"title": "Pattern task", "priority": "medium"},
        )
        _post_command(
            client,
            command_type="schedule.create",
            household_id=household_id,
            payload={
                "title": "Pattern schedule",
                "start_at": "2031-04-01T08:00:00Z",
                "end_at": "2031-04-01T09:00:00Z",
            },
        )
        created = _create_reminder(
            client,
            household_id=household_id,
            title="Pattern reminder",
            message="Pattern reminder message",
            trigger_at="2031-04-01T10:00:00Z",
        )
        reminder_id = str(created["response"]["reminder"]["reminder_id"])
        _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=reminder_id,
            system_trigger=True,
        )

        notifications_payload = client.get(
            "/notifications",
            params={
                "household_id": household_id,
                "sort_by": "created_at",
                "order": "desc",
                "limit": 2,
                "offset": 0,
            },
        ).json()
        tasks_payload = client.get("/tasks", params={"household_id": household_id}).json()
        schedule_payload = client.get("/schedule", params={"household_id": household_id}).json()
        reminders_payload = client.get("/reminders", params={"household_id": household_id}).json()

    assert set(notifications_payload.keys()) == {"notifications", "summary", "pagination"}
    assert set(tasks_payload.keys()) == {"tasks", "summary", "pagination"}
    assert set(schedule_payload.keys()) == {"schedules", "summary", "pagination"}
    assert set(reminders_payload.keys()) == {"reminders", "summary", "pagination"}

    assert set(notifications_payload["pagination"].keys()) == {"limit", "offset", "returned"}
    assert set(notifications_payload["pagination"].keys()) == set(tasks_payload["pagination"].keys())
    assert set(notifications_payload["pagination"].keys()) == set(schedule_payload["pagination"].keys())

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
    expected_page = notifications_api._paginated_notifications(expected_sorted, limit=2, offset=0)
    assert notifications_payload["notifications"] == expected_page


def test_cache_vs_time_correctness_cache_is_not_authoritative() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-notification-guardrails-cache"
        created = _create_reminder(
            client,
            household_id=household_id,
            title="Cache correctness reminder",
            message="cache correctness event",
            trigger_at="2031-05-01T07:00:00Z",
        )
        reminder_id = str(created["response"]["reminder"]["reminder_id"])

        before_projection = runtime.get_projection(household_id, force_replay=True)
        before_notifications = _notifications_for_projection(before_projection)
        before_fingerprint = notifications_api._projection_fingerprint(
            household_id=household_id,
            projection=before_projection,
        )

        before = client.get(
            "/notifications",
            params={"household_id": household_id, "search": "cache correctness"},
        )

        _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=reminder_id,
            system_trigger=True,
        )

        after_projection = runtime.get_projection(household_id, force_replay=True)
        after_notifications = _notifications_for_projection(after_projection)
        after_fingerprint = notifications_api._projection_fingerprint(
            household_id=household_id,
            projection=after_projection,
        )

        after = client.get(
            "/notifications",
            params={"household_id": household_id, "search": "cache correctness"},
        )
        after_repeat = client.get(
            "/notifications",
            params={"household_id": household_id, "search": "cache correctness"},
        )

    assert before.status_code == 200
    assert after.status_code == 200
    assert after_repeat.status_code == 200

    assert before.json()["notifications"] == []
    assert len(after.json()["notifications"]) == 1
    assert after.json() == after_repeat.json()

    assert before_fingerprint != after_fingerprint
    assert before_fingerprint[1] == str(before_projection.get("last_event_id") or "").strip()
    assert before_fingerprint[2] == int(before_projection.get("state_version") or 0)
    assert before_fingerprint[3] == str(before_projection.get("checksum") or "").strip()

    assert after_fingerprint[1] == str(after_projection.get("last_event_id") or "").strip()
    assert after_fingerprint[2] == int(after_projection.get("state_version") or 0)
    assert after_fingerprint[3] == str(after_projection.get("checksum") or "").strip()


def test_high_volume_cross_domain_determinism_with_notification_replay_equivalence() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-notification-guardrails-high-volume"

        for index in range(14):
            created = _post_command(
                client,
                command_type="task.create",
                household_id=household_id,
                payload={"title": f"HV Task {index}", "priority": "medium"},
            )
            task_id = str(created["response"]["task"]["task_id"])
            if index % 3 == 0:
                _post_command(
                    client,
                    command_type="task_completed",
                    household_id=household_id,
                    payload={"task_id": task_id},
                )

        for index in range(14):
            created = _post_command(
                client,
                command_type="schedule.create",
                household_id=household_id,
                payload={
                    "title": f"HV Schedule {index}",
                    "start_at": f"2032-06-{(index % 9) + 1:02d}T08:00:00Z",
                    "end_at": f"2032-06-{(index % 9) + 1:02d}T09:00:00Z",
                },
            )
            schedule_id = str(created["response"]["schedule"]["schedule_id"])
            if index % 4 == 0:
                _post_command(
                    client,
                    command_type="schedule.cancel",
                    household_id=household_id,
                    payload={"schedule_id": schedule_id},
                )

        for index in range(14):
            created = _create_reminder(
                client,
                household_id=household_id,
                title=f"HV Reminder {index}",
                message=f"HV Message {index}",
                trigger_at=f"2032-07-{(index % 9) + 1:02d}T10:00:00Z",
            )
            reminder_id = str(created["response"]["reminder"]["reminder_id"])
            if index % 4 == 0:
                _post_command(
                    client,
                    command_type="reminder.cancel",
                    household_id=household_id,
                    payload={"reminder_id": reminder_id},
                )
            elif index % 4 == 1:
                _trigger_reminder(
                    client,
                    household_id=household_id,
                    reminder_id=reminder_id,
                    system_trigger=True,
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
        reminders_params = {
            "household_id": household_id,
            "sort_by": "trigger_at",
            "order": "asc",
            "limit": 7,
            "offset": 2,
        }
        notifications_params = {
            "household_id": household_id,
            "sort_by": "created_at",
            "order": "asc",
            "limit": 7,
            "offset": 0,
        }

        tasks_first = client.get("/tasks", params=tasks_params).json()
        schedule_first = client.get("/schedule", params=schedule_params).json()
        reminders_first = client.get("/reminders", params=reminders_params).json()
        notifications_first = client.get("/notifications", params=notifications_params).json()

        replay_first = replay(event_log.insert_order)
        replay_second = replay(event_log.insert_order)

        tasks_second = client.get("/tasks", params=tasks_params).json()
        schedule_second = client.get("/schedule", params=schedule_params).json()
        reminders_second = client.get("/reminders", params=reminders_params).json()
        notifications_second = client.get("/notifications", params=notifications_params).json()

    assert tasks_first == tasks_second
    assert schedule_first == schedule_second
    assert reminders_first == reminders_second
    assert notifications_first == notifications_second

    assert replay_first["replay_checksum"] == replay_second["replay_checksum"]
    assert replay_first["derived_state"] == replay_second["derived_state"]

    live_projection = runtime.get_projection(household_id, force_replay=True)
    comparison = validate_replay(live_projection, replay_first["derived_state"])
    assert comparison["matches"] is True

    for row in notifications_first["notifications"]:
        source_event_id = str(row.get("source_event_id") or "")
        notification_id = str(row.get("notification_id") or "")
        assert source_event_id
        assert notification_id == f"notification-{source_event_id}"
