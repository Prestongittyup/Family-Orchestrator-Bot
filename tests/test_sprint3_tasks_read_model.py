from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import command as command_api
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

    command_getter = command_api.get_command_runtime_service
    tasks_getter = tasks_api.get_command_runtime_service
    command_api.get_command_runtime_service = lambda: runtime
    tasks_api.get_command_runtime_service = lambda: runtime

    class _PatchedClient(TestClient):
        def __exit__(self, exc_type, exc_val, exc_tb):
            command_api.get_command_runtime_service = command_getter
            tasks_api.get_command_runtime_service = tasks_getter
            return super().__exit__(exc_type, exc_val, exc_tb)

    return _PatchedClient(app)


def _create_task(client: TestClient, *, household_id: str, title: str) -> str:
    response = client.post(
        "/command",
        json={
            "command_type": "task_created",
            "household_id": household_id,
            "payload": {
                "title": title,
                "priority": "medium",
            },
        },
    )
    assert response.status_code == 200
    return str(response.json()["response"]["task"]["task_id"])


def _complete_task(client: TestClient, *, household_id: str, task_id: str) -> None:
    response = client.post(
        "/command",
        json={
            "command_type": "task_completed",
            "household_id": household_id,
            "payload": {
                "task_id": task_id,
            },
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "accepted"


def test_tasks_filtering_pending_only() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s3-filter"
        first = _create_task(client, household_id=household_id, title="Pack lunch")
        second = _create_task(client, household_id=household_id, title="Submit field-trip form")
        _complete_task(client, household_id=household_id, task_id=first)

        response = client.get("/tasks", params={"household_id": household_id, "status": "pending"})

    assert response.status_code == 200
    body = response.json()
    tasks = body["tasks"]
    assert len(tasks) == 1
    assert tasks[0]["task_id"] == second
    assert all(task["status"] == "pending" for task in tasks)


def test_tasks_sorting_created_at_ascending() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s3-sort"
        _create_task(client, household_id=household_id, title="Task one")
        _create_task(client, household_id=household_id, title="Task two")

        response = client.get(
            "/tasks",
            params={
                "household_id": household_id,
                "sort_by": "created_at",
                "order": "asc",
            },
        )

    assert response.status_code == 200
    tasks = response.json()["tasks"]
    created_values = [str(task.get("created_at") or "") for task in tasks]
    assert created_values == sorted(created_values)


def test_tasks_summary_counts_match_projection() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s3-summary"
        complete_me = _create_task(client, household_id=household_id, title="Complete this")
        _create_task(client, household_id=household_id, title="Keep pending")
        _complete_task(client, household_id=household_id, task_id=complete_me)

        response = client.get("/tasks", params={"household_id": household_id})

    assert response.status_code == 200
    summary = response.json()["summary"]
    assert summary == {
        "total": 2,
        "pending": 1,
        "completed": 1,
    }


def test_tasks_read_model_deterministic_with_replay() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s3-determinism"
        first = _create_task(client, household_id=household_id, title="Reserve piano lesson")
        _create_task(client, household_id=household_id, title="Schedule vaccine")
        _complete_task(client, household_id=household_id, task_id=first)

        tasks_response = client.get("/tasks", params={"household_id": household_id})

    assert tasks_response.status_code == 200
    tasks_body = tasks_response.json()

    live_projection = runtime.get_projection(household_id, force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True

    expected_tasks = tasks_api._sorted_tasks(
        [
            tasks_api._normalize_task_row(task)
            for task in list(live_projection.get("tasks_list") or [])
            if isinstance(task, dict)
        ],
        sort_by="created_at",
        order="desc",
    )

    assert tasks_body["tasks"] == expected_tasks
    assert tasks_body["summary"] == {
        "total": len(expected_tasks),
        "pending": sum(1 for task in expected_tasks if task.get("status") == "pending"),
        "completed": sum(1 for task in expected_tasks if task.get("status") == "completed"),
    }
