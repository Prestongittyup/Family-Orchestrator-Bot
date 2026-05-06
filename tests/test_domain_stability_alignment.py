from __future__ import annotations

from copy import deepcopy
import inspect
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import command as command_api
from app.api import schedule as schedule_api
from app.api import tasks as tasks_api
from app.services.commands.runtime import CommandActor, CommandRuntimeService
from core.replay import event_replay_engine
from core.replay.domain_projection_helpers import (
    apply_schedule_cancelled_projection,
    apply_schedule_created_projection,
    apply_task_completed_projection,
    apply_task_created_projection,
)
from core.replay.event_replay_engine import replay, validate_replay
from household_os.runtime.event_router import CanonicalEventEnvelope


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

    command_getter = command_api.get_command_runtime_service
    tasks_getter = tasks_api.get_command_runtime_service
    schedule_getter = schedule_api.get_command_runtime_service

    command_api.get_command_runtime_service = lambda: runtime
    tasks_api.get_command_runtime_service = lambda: runtime
    schedule_api.get_command_runtime_service = lambda: runtime

    class _PatchedClient(TestClient):
        def __exit__(self, exc_type, exc_val, exc_tb):
            command_api.get_command_runtime_service = command_getter
            tasks_api.get_command_runtime_service = tasks_getter
            schedule_api.get_command_runtime_service = schedule_getter
            return super().__exit__(exc_type, exc_val, exc_tb)

    return _PatchedClient(app)


def _event_types(event_log: _InMemoryEventLogService) -> list[str]:
    return [row.type for row in event_log.insert_order]


def _create_task(client: TestClient, *, household_id: str, title: str) -> dict[str, Any]:
    response = client.post(
        "/command",
        json={
            "command_type": "task.create",
            "household_id": household_id,
            "payload": {
                "title": title,
                "priority": "medium",
            },
        },
    )
    assert response.status_code == 200
    return response.json()


def _complete_task(client: TestClient, *, household_id: str, task_id: str) -> dict[str, Any]:
    response = client.post(
        "/command",
        json={
            "command_type": "task_completed",
            "household_id": household_id,
            "payload": {"task_id": task_id},
        },
    )
    assert response.status_code == 200
    return response.json()


def _create_schedule(client: TestClient, *, household_id: str, title: str, start_at: str, end_at: str) -> dict[str, Any]:
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
    return response.json()


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


def test_projection_parity_holds_for_tasks_and_schedule_domains() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-alignment-projection"

        task = _create_task(client, household_id=household_id, title="Parity task")
        task_id = str(task["response"]["task"]["task_id"])
        _complete_task(client, household_id=household_id, task_id=task_id)

        schedule = _create_schedule(
            client,
            household_id=household_id,
            title="Parity schedule",
            start_at="2026-09-01T10:00:00Z",
            end_at="2026-09-01T11:00:00Z",
        )
        schedule_id = str(schedule["response"]["schedule"]["schedule_id"])
        _cancel_schedule(client, household_id=household_id, schedule_id=schedule_id)

    live_projection = runtime.get_projection(household_id, force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])

    assert comparison["matches"] is True
    assert task_id in live_projection["tasks"]
    assert schedule_id in live_projection["schedules"]


def test_duplicate_stateful_commands_are_rejected_consistently() -> None:
    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-align")
    household_id = "household-alignment-commands"

    created_task = runtime.handle_command(
        command_type="task.create",
        household_id=household_id,
        actor=actor,
        payload={"title": "Task duplicate check", "priority": "low"},
        source="tests.alignment",
    )
    task_id = str(created_task["response"]["task"]["task_id"])

    first_complete = runtime.handle_command(
        command_type="task_completed",
        household_id=household_id,
        actor=actor,
        payload={"task_id": task_id},
        source="tests.alignment",
    )
    second_complete = runtime.handle_command(
        command_type="task_completed",
        household_id=household_id,
        actor=actor,
        payload={"task_id": task_id},
        source="tests.alignment",
    )

    created_schedule = runtime.handle_command(
        command_type="schedule.create",
        household_id=household_id,
        actor=actor,
        payload={
            "title": "Schedule duplicate check",
            "start_at": "2026-09-02T10:00:00Z",
            "end_at": "2026-09-02T11:00:00Z",
        },
        source="tests.alignment",
    )
    schedule_id = str(created_schedule["response"]["schedule"]["schedule_id"])

    first_cancel = runtime.handle_command(
        command_type="schedule.cancel",
        household_id=household_id,
        actor=actor,
        payload={"schedule_id": schedule_id},
        source="tests.alignment",
    )
    second_cancel = runtime.handle_command(
        command_type="schedule.cancel",
        household_id=household_id,
        actor=actor,
        payload={"schedule_id": schedule_id},
        source="tests.alignment",
    )

    assert first_complete["status"] == "accepted"
    assert second_complete["status"] == "rejected"
    assert second_complete["response"]["code"] == "task_already_completed"

    assert first_cancel["status"] == "accepted"
    assert second_cancel["status"] == "rejected"
    assert second_cancel["response"]["code"] == "schedule_already_cancelled"

    event_types = _event_types(event_log)
    assert event_types.count("TaskCompleted") == 1
    assert event_types.count("ScheduleCancelled") == 1


def test_read_model_outputs_are_deterministic_and_uniformly_shaped() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-alignment-read"

        t1 = _create_task(client, household_id=household_id, title="Task Alpha")
        t2 = _create_task(client, household_id=household_id, title="Task Beta")
        _complete_task(client, household_id=household_id, task_id=str(t1["response"]["task"]["task_id"]))

        s1 = _create_schedule(
            client,
            household_id=household_id,
            title="Schedule Alpha",
            start_at="2026-09-03T07:00:00Z",
            end_at="2026-09-03T08:00:00Z",
        )
        _create_schedule(
            client,
            household_id=household_id,
            title="Schedule Beta",
            start_at="2026-09-03T09:00:00Z",
            end_at="2026-09-03T10:00:00Z",
        )
        _cancel_schedule(client, household_id=household_id, schedule_id=str(s1["response"]["schedule"]["schedule_id"]))

        task_params = {
            "household_id": household_id,
            "search": "task",
            "sort_by": "created_at",
            "order": "desc",
            "limit": 2,
            "offset": 0,
        }
        schedule_params = {
            "household_id": household_id,
            "search": "schedule",
            "sort_by": "start_at",
            "order": "asc",
            "limit": 2,
            "offset": 0,
        }

        tasks_first = client.get("/tasks", params=task_params)
        tasks_second = client.get("/tasks", params=task_params)
        schedule_first = client.get("/schedule", params=schedule_params)
        schedule_second = client.get("/schedule", params=schedule_params)

    assert tasks_first.status_code == 200
    assert tasks_second.status_code == 200
    assert schedule_first.status_code == 200
    assert schedule_second.status_code == 200

    tasks_payload = tasks_first.json()
    schedules_payload = schedule_first.json()

    assert tasks_payload == tasks_second.json()
    assert schedules_payload == schedule_second.json()

    assert list(tasks_payload.keys()) == ["tasks", "summary", "pagination"]
    assert list(schedules_payload.keys()) == ["schedules", "summary", "pagination"]
    assert list(tasks_payload["pagination"].keys()) == ["limit", "offset", "returned"]
    assert list(schedules_payload["pagination"].keys()) == ["limit", "offset", "returned"]

    task_rows = [
        {
            "task_id": "task-b",
            "created_at": "2026-01-01T00:00:00Z",
            "completed_at": "2026-02-01T00:00:00Z",
        },
        {
            "task_id": "task-a",
            "created_at": "2026-01-01T00:00:00Z",
            "completed_at": "2026-02-01T00:00:00Z",
        },
        {
            "task_id": "task-c",
            "created_at": "2026-01-02T00:00:00Z",
            "completed_at": "2026-02-01T00:00:00Z",
        },
    ]
    schedule_rows = [
        {
            "schedule_id": "schedule-b",
            "start_at": "2026-01-01T10:00:00Z",
            "created_at": "2026-01-01T00:00:00Z",
        },
        {
            "schedule_id": "schedule-a",
            "start_at": "2026-01-01T10:00:00Z",
            "created_at": "2026-01-01T00:00:00Z",
        },
        {
            "schedule_id": "schedule-c",
            "start_at": "2026-01-02T10:00:00Z",
            "created_at": "2026-01-01T00:00:00Z",
        },
    ]

    sorted_tasks = tasks_api._sorted_tasks(task_rows, sort_by="completed_at", order="asc")
    sorted_schedules = schedule_api._sorted_schedule_entries(
        schedule_rows,
        sort_by="start_at",
        order="asc",
    )

    assert [row["task_id"] for row in sorted_tasks] == ["task-a", "task-b", "task-c"]
    assert [row["schedule_id"] for row in sorted_schedules] == ["schedule-a", "schedule-b", "schedule-c"]


def test_tasks_and_schedule_read_models_use_shared_pipeline_helpers() -> None:
    tasks_sort_source = inspect.getsource(tasks_api._sorted_tasks)
    schedule_sort_source = inspect.getsource(schedule_api._sorted_schedule_entries)
    tasks_page_source = inspect.getsource(tasks_api._paginated_tasks)
    schedule_page_source = inspect.getsource(schedule_api._paginated_schedule_entries)

    assert "_shared_sort_records_with_tie_break" in tasks_sort_source
    assert "_shared_sort_records_with_tie_break" in schedule_sort_source
    assert "_shared_paginate_records" in tasks_page_source
    assert "_shared_paginate_records" in schedule_page_source


def test_projection_helpers_are_deterministic_and_payload_pure() -> None:
    created_payload = {
        "request_id": "req-created",
        "task": {
            "task_id": "task-1",
            "title": "Helper determinism",
            "status": "pending",
        },
        "response": {
            "request_id": "req-created",
            "task": {
                "task_id": "task-1",
            },
        },
    }

    def _run_task_created() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any] | None, dict[str, Any]]:
        tasks: dict[str, dict[str, Any]] = {}
        actions: dict[str, dict[str, Any]] = {}
        responses: dict[str, dict[str, Any]] = {}
        payload = deepcopy(created_payload)
        response = apply_task_created_projection(
            payload=payload,
            recorded_at="2026-10-01T00:00:00Z",
            tasks=tasks,
            actions=actions,
            responses=responses,
        )
        return tasks, actions, responses, response, payload

    first_task_created = _run_task_created()
    second_task_created = _run_task_created()
    assert first_task_created == second_task_created
    assert first_task_created[-1] == created_payload

    completed_payload = {
        "request_id": "req-completed",
        "task_id": "task-1",
        "response": {
            "request_id": "req-completed",
            "task": {
                "task_id": "task-1",
                "status": "completed",
            },
        },
    }

    def _run_task_completed() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any] | None, dict[str, Any]]:
        tasks = {
            "task-1": {
                "task_id": "task-1",
                "title": "Helper determinism",
                "status": "pending",
                "created_at": "2026-10-01T00:00:00Z",
            }
        }
        actions: dict[str, dict[str, Any]] = {}
        responses: dict[str, dict[str, Any]] = {}
        payload = deepcopy(completed_payload)
        response = apply_task_completed_projection(
            payload=payload,
            recorded_at="2026-10-01T01:00:00Z",
            tasks=tasks,
            actions=actions,
            responses=responses,
        )
        return tasks, actions, responses, response, payload

    first_task_completed = _run_task_completed()
    second_task_completed = _run_task_completed()
    assert first_task_completed == second_task_completed
    assert first_task_completed[-1] == completed_payload

    schedule_created_payload = {
        "request_id": "req-schedule-created",
        "schedule": {
            "schedule_id": "schedule-1",
            "title": "Schedule helper",
            "status": "scheduled",
        },
        "response": {
            "request_id": "req-schedule-created",
            "schedule": {
                "schedule_id": "schedule-1",
            },
        },
    }

    def _run_schedule_created() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        schedules: dict[str, dict[str, Any]] = {}
        responses: dict[str, dict[str, Any]] = {}
        payload = deepcopy(schedule_created_payload)
        apply_schedule_created_projection(
            payload=payload,
            recorded_at="2026-10-01T02:00:00Z",
            schedules=schedules,
            responses=responses,
        )
        return schedules, responses, payload

    first_schedule_created = _run_schedule_created()
    second_schedule_created = _run_schedule_created()
    assert first_schedule_created == second_schedule_created
    assert first_schedule_created[-1] == schedule_created_payload

    schedule_cancelled_payload = {
        "request_id": "req-schedule-cancelled",
        "schedule_id": "schedule-1",
        "response": {
            "request_id": "req-schedule-cancelled",
            "schedule": {
                "schedule_id": "schedule-1",
                "status": "cancelled",
            },
        },
    }

    def _run_schedule_cancelled() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        schedules = {
            "schedule-1": {
                "schedule_id": "schedule-1",
                "title": "Schedule helper",
                "status": "scheduled",
                "created_at": "2026-10-01T02:00:00Z",
            }
        }
        responses: dict[str, dict[str, Any]] = {}
        payload = deepcopy(schedule_cancelled_payload)
        apply_schedule_cancelled_projection(
            payload=payload,
            recorded_at="2026-10-01T03:00:00Z",
            schedules=schedules,
            responses=responses,
        )
        return schedules, responses, payload

    first_schedule_cancelled = _run_schedule_cancelled()
    second_schedule_cancelled = _run_schedule_cancelled()
    assert first_schedule_cancelled == second_schedule_cancelled
    assert first_schedule_cancelled[-1] == schedule_cancelled_payload


def test_runtime_and_replay_projection_paths_use_shared_domain_helpers() -> None:
    runtime_source = inspect.getsource(CommandRuntimeService._replay_projection)
    replay_source = inspect.getsource(event_replay_engine._project_from_normalized)

    helper_names = [
        "apply_task_created_projection",
        "apply_task_completed_projection",
        "apply_schedule_created_projection",
        "apply_schedule_cancelled_projection",
    ]
    for helper_name in helper_names:
        assert helper_name in runtime_source
        assert helper_name in replay_source


def test_command_normalization_is_whitespace_trim_only_and_stable() -> None:
    source = inspect.getsource(CommandRuntimeService.handle_command)
    assert "normalized_command_type = command_type.strip()" in source

    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="normalization-user")

    first = runtime.handle_command(
        command_type="task.create",
        household_id="household-normalization",
        actor=actor,
        payload={
            "title": "Normalization contract",
            "priority": "medium",
        },
        source="tests.normalization",
    )
    second = runtime.handle_command(
        command_type="  task.create  ",
        household_id="household-normalization",
        actor=actor,
        payload={
            "title": "Normalization contract",
            "priority": "medium",
        },
        source="tests.normalization",
    )

    assert first["status"] == "accepted"
    assert second["status"] == "duplicate"
    assert first["request_id"] == second["request_id"]

    event_types = _event_types(event_log)
    assert event_types.count("command.received") == 1
    assert event_types.count("task.created") == 1


def test_command_variant_aliases_are_stable_for_task_creation() -> None:
    runtime, _ = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="alias-user")
    household_id = "household-command-aliases"

    variants = ["task.create", "create_task", "task_created"]
    titles: list[str] = []
    for index, variant in enumerate(variants, start=1):
        result = runtime.handle_command(
            command_type=variant,
            household_id=household_id,
            actor=actor,
            payload={
                "title": f"Alias {index}",
                "priority": "low",
            },
            source="tests.normalization",
        )
        assert result["status"] in {"accepted", "pending_approval"}
        titles.append(str(result["response"]["task"]["title"]))

    assert titles == ["Alias 1", "Alias 2", "Alias 3"]


def test_read_model_pipeline_ordering_invariants_are_locked() -> None:
    tasks_endpoint_source = inspect.getsource(tasks_api.get_tasks)
    schedule_endpoint_source = inspect.getsource(schedule_api.get_schedule)

    pipeline_markers = [
        "get_projection",
        "_get_or_build_materialized_records",
        "_get_or_build_sorted_view",
        "_paginated_",
        "paginated_payload",
    ]

    for endpoint_source in (tasks_endpoint_source, schedule_endpoint_source):
        previous_index = -1
        for marker in pipeline_markers:
            current_index = endpoint_source.find(marker)
            assert current_index > previous_index
            previous_index = current_index

    tasks_filter_source = inspect.getsource(tasks_api._filtered_records_with_summary)
    schedule_filter_source = inspect.getsource(schedule_api._filtered_records_with_summary)

    assert tasks_filter_source.find("if status is not None") < tasks_filter_source.find("if search is not None")
    assert schedule_filter_source.find("if status is not None") < schedule_filter_source.find("if search is not None")


def test_schedule_tie_break_and_pagination_stability_after_dataset_change() -> None:
    schedule_rows = [
        {
            "schedule_id": "schedule-b",
            "start_at": "2026-01-01T10:00:00Z",
            "created_at": "2026-01-01T00:00:00Z",
        },
        {
            "schedule_id": "schedule-a",
            "start_at": "2026-01-01T10:00:00Z",
            "created_at": "2026-01-01T00:00:00Z",
        },
        {
            "schedule_id": "schedule-c",
            "start_at": "2026-01-02T10:00:00Z",
            "created_at": "2026-01-01T00:00:00Z",
        },
    ]
    ascending = schedule_api._sorted_schedule_entries(schedule_rows, sort_by="start_at", order="asc")
    descending = schedule_api._sorted_schedule_entries(schedule_rows, sort_by="start_at", order="desc")

    assert [row["schedule_id"] for row in ascending] == ["schedule-a", "schedule-b", "schedule-c"]
    assert [row["schedule_id"] for row in descending] == ["schedule-c", "schedule-b", "schedule-a"]

    runtime, _ = _build_runtime()
    with _build_test_client(runtime) as client:
        household_id = "household-schedule-pagination-stability"
        for index in range(6):
            _create_schedule(
                client,
                household_id=household_id,
                title=f"Stable schedule {index}",
                start_at=f"2026-11-01T{index:02d}:00:00Z",
                end_at=f"2026-11-01T{index + 1:02d}:00:00Z",
            )

        params = {
            "household_id": household_id,
            "sort_by": "start_at",
            "order": "asc",
            "limit": 2,
            "offset": 1,
        }
        _ = client.get("/schedule", params=params)

        _create_schedule(
            client,
            household_id=household_id,
            title="New schedule event",
            start_at="2026-11-01T23:00:00Z",
            end_at="2026-11-02T00:00:00Z",
        )

        first = client.get("/schedule", params=params)
        second = client.get("/schedule", params=params)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert first.json()["pagination"] == {"limit": 2, "offset": 1, "returned": 2}


def test_cross_domain_determinism_repeated_queries_and_replay_match() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-cross-domain-determinism"
        task = _create_task(client, household_id=household_id, title="Cross Task")
        _complete_task(client, household_id=household_id, task_id=str(task["response"]["task"]["task_id"]))

        schedule = _create_schedule(
            client,
            household_id=household_id,
            title="Cross Schedule",
            start_at="2026-12-01T10:00:00Z",
            end_at="2026-12-01T11:00:00Z",
        )
        _cancel_schedule(
            client,
            household_id=household_id,
            schedule_id=str(schedule["response"]["schedule"]["schedule_id"]),
        )

        task_params = {
            "household_id": household_id,
            "sort_by": "created_at",
            "order": "desc",
            "limit": 5,
            "offset": 0,
        }
        schedule_params = {
            "household_id": household_id,
            "sort_by": "start_at",
            "order": "asc",
            "limit": 5,
            "offset": 0,
        }

        tasks_first = client.get("/tasks", params=task_params)
        tasks_second = client.get("/tasks", params=task_params)
        schedule_first = client.get("/schedule", params=schedule_params)
        schedule_second = client.get("/schedule", params=schedule_params)

    assert tasks_first.status_code == 200
    assert tasks_second.status_code == 200
    assert schedule_first.status_code == 200
    assert schedule_second.status_code == 200

    assert tasks_first.json() == tasks_second.json()
    assert schedule_first.json() == schedule_second.json()

    live_projection = runtime.get_projection(household_id, force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True
