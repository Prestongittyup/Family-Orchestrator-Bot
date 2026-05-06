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


def _create_task(
    client: TestClient,
    *,
    household_id: str,
    title: str,
    description: str = "",
) -> str:
    response = client.post(
        "/command",
        json={
            "command_type": "task_created",
            "household_id": household_id,
            "payload": {
                "title": title,
                "description": description,
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
            "payload": {"task_id": task_id},
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "accepted"


def test_tasks_pagination_returns_expected_windows() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s4-pagination"
        for index in range(5):
            _create_task(
                client,
                household_id=household_id,
                title=f"Task {index}",
            )

        first_page = client.get(
            "/tasks",
            params={"household_id": household_id, "limit": 2, "offset": 0},
        )
        second_page = client.get(
            "/tasks",
            params={"household_id": household_id, "limit": 2, "offset": 2},
        )

    assert first_page.status_code == 200
    assert second_page.status_code == 200

    first_payload = first_page.json()
    second_payload = second_page.json()

    assert len(first_payload["tasks"]) == 2
    assert len(second_payload["tasks"]) == 2
    assert first_payload["tasks"] != second_payload["tasks"]

    assert first_payload["pagination"] == {"limit": 2, "offset": 0, "returned": 2}
    assert second_payload["pagination"] == {"limit": 2, "offset": 2, "returned": 2}


def test_tasks_search_title_and_description_case_insensitive() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s4-search"
        _create_task(
            client,
            household_id=household_id,
            title="Book Dentist Visit",
            description="Call clinic for next-week slot",
        )
        _create_task(
            client,
            household_id=household_id,
            title="School Supplies",
            description="Buy pens and notebooks",
        )

        title_match = client.get(
            "/tasks",
            params={"household_id": household_id, "search": "dentist"},
        )
        description_match = client.get(
            "/tasks",
            params={"household_id": household_id, "search": "NOTEBOOKS"},
        )

    assert title_match.status_code == 200
    assert description_match.status_code == 200

    title_tasks = title_match.json()["tasks"]
    description_tasks = description_match.json()["tasks"]

    assert len(title_tasks) == 1
    assert "dentist" in title_tasks[0]["title"].lower()

    assert len(description_tasks) == 1
    assert "notebooks" in description_tasks[0]["description"].lower()


def test_tasks_combined_search_filter_sort_and_pagination() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s4-combined"

        completed_1 = _create_task(
            client,
            household_id=household_id,
            title="Alpha cleanup",
            description="cleanup backlog alpha",
        )
        completed_2 = _create_task(
            client,
            household_id=household_id,
            title="Beta cleanup",
            description="cleanup backlog beta",
        )
        pending = _create_task(
            client,
            household_id=household_id,
            title="Gamma cleanup",
            description="cleanup backlog gamma",
        )

        _complete_task(client, household_id=household_id, task_id=completed_1)
        _complete_task(client, household_id=household_id, task_id=completed_2)

        response = client.get(
            "/tasks",
            params={
                "household_id": household_id,
                "status": "completed",
                "search": "cleanup",
                "sort_by": "completed_at",
                "order": "asc",
                "limit": 1,
                "offset": 1,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    tasks = payload["tasks"]

    assert len(tasks) == 1
    assert tasks[0]["task_id"] in {completed_1, completed_2}
    assert tasks[0]["task_id"] != pending
    assert tasks[0]["status"] == "completed"

    assert payload["summary"]["total"] == 2
    assert payload["summary"]["completed"] == 2
    assert payload["summary"]["pending"] == 0
    assert payload["pagination"] == {"limit": 1, "offset": 1, "returned": 1}


def test_tasks_deterministic_with_replay_under_search_and_pagination() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s4-determinism"
        first = _create_task(
            client,
            household_id=household_id,
            title="Plan camping trip",
            description="trip logistics checklist",
        )
        _create_task(
            client,
            household_id=household_id,
            title="Trip budget",
            description="trip cost planning",
        )
        _complete_task(client, household_id=household_id, task_id=first)

        response = client.get(
            "/tasks",
            params={
                "household_id": household_id,
                "search": "trip",
                "sort_by": "created_at",
                "order": "desc",
                "limit": 1,
                "offset": 0,
            },
        )

    assert response.status_code == 200
    runtime_payload = response.json()

    live_projection = runtime.get_projection(household_id, force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True

    projection_tasks = [
        task
        for task in list(live_projection.get("tasks_list") or [])
        if isinstance(task, dict)
    ]
    filtered = tasks_api._status_filtered_tasks(projection_tasks, status=None)
    searched = tasks_api._searched_tasks(filtered, search="trip")
    sorted_tasks = tasks_api._sorted_tasks(searched, sort_by="created_at", order="desc")
    paged_tasks = tasks_api._paginated_tasks(sorted_tasks, limit=1, offset=0)
    expected_tasks = [tasks_api._normalize_task_row(task) for task in paged_tasks]

    assert runtime_payload["tasks"] == expected_tasks
    assert runtime_payload["pagination"] == {"limit": 1, "offset": 0, "returned": len(expected_tasks)}
