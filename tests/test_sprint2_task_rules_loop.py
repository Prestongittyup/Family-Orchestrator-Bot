from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
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


def _event_types(event_log: _InMemoryEventLogService) -> list[str]:
    return [row.type for row in event_log.insert_order]


def test_rules_reject_task_completion_for_nonexistent_task() -> None:
    runtime, event_log = _build_runtime()

    result = runtime.handle_command(
        command_type="task_completed",
        household_id="household-s2-rules",
        actor=CommandActor(actor_type="api_user", user_id="user-s2"),
        payload={"task_id": "task-missing"},
        source="tests.sprint2",
    )

    assert result["status"] == "rejected"
    assert result["response"]["code"] == "task_not_found"
    assert _event_types(event_log) == []


def test_rules_reject_already_completed_task() -> None:
    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-s2")

    created = runtime.handle_command(
        command_type="task_created",
        household_id="household-s2-rules",
        actor=actor,
        payload={"title": "Pay school fees", "priority": "medium"},
        source="tests.sprint2",
    )
    task_id = str(created["response"]["task"]["task_id"])

    first_completion = runtime.handle_command(
        command_type="task_completed",
        household_id="household-s2-rules",
        actor=actor,
        payload={"task_id": task_id},
        source="tests.sprint2",
    )
    second_completion = runtime.handle_command(
        command_type="task_completed",
        household_id="household-s2-rules",
        actor=actor,
        payload={"task_id": task_id},
        source="tests.sprint2",
    )

    assert first_completion["status"] == "accepted"
    assert second_completion["status"] == "rejected"
    assert second_completion["response"]["code"] == "task_already_completed"
    assert _event_types(event_log).count("TaskCompleted") == 1


def test_task_completion_emits_event_and_updates_projection() -> None:
    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-s2")

    created = runtime.handle_command(
        command_type="task_created",
        household_id="household-s2-flow",
        actor=actor,
        payload={"title": "Book pediatric visit", "priority": "low"},
        source="tests.sprint2",
    )
    task_id = str(created["response"]["task"]["task_id"])

    completed = runtime.handle_command(
        command_type="task_completed",
        household_id="household-s2-flow",
        actor=actor,
        payload={"task_id": task_id},
        source="tests.sprint2",
    )

    assert completed["status"] == "accepted"
    assert "TaskCompleted" in _event_types(event_log)
    completed_task = next(
        task
        for task in completed["projection"]["tasks_list"]
        if str(task.get("task_id") or "") == task_id
    )
    assert completed_task["status"] == "completed"
    assert isinstance(completed_task.get("completed_at"), str) and completed_task["completed_at"].strip()


def test_end_to_end_task_loop_with_tasks_read_model(monkeypatch) -> None:
    runtime, _ = _build_runtime()
    monkeypatch.setattr(command_api, "get_command_runtime_service", lambda: runtime)
    monkeypatch.setattr(tasks_api, "get_command_runtime_service", lambda: runtime)

    app = FastAPI()
    app.include_router(command_api.router)
    app.include_router(tasks_api.router)

    with TestClient(app) as client:
        create_response = client.post(
            "/command",
            json={
                "command_type": "task_created",
                "household_id": "household-s2-e2e",
                "payload": {"title": "Finalize camp registration", "priority": "medium"},
            },
        )
        assert create_response.status_code == 200
        created_task_id = str(create_response.json()["response"]["task"]["task_id"])

        complete_response = client.post(
            "/command",
            json={
                "command_type": "task_completed",
                "household_id": "household-s2-e2e",
                "payload": {"task_id": created_task_id},
            },
        )
        assert complete_response.status_code == 200
        assert complete_response.json()["status"] == "accepted"

        tasks_response = client.get("/tasks", params={"household_id": "household-s2-e2e"})
        assert tasks_response.status_code == 200
        tasks = tasks_response.json()["tasks"]
        task_row = next(task for task in tasks if str(task.get("task_id") or "") == created_task_id)
        assert task_row["status"] == "completed"


def test_replay_integrity_after_task_completion() -> None:
    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-s2")

    created = runtime.handle_command(
        command_type="task_created",
        household_id="household-s2-replay",
        actor=actor,
        payload={"title": "Order science supplies", "priority": "medium"},
        source="tests.sprint2",
    )
    task_id = str(created["response"]["task"]["task_id"])

    runtime.handle_command(
        command_type="task_completed",
        household_id="household-s2-replay",
        actor=actor,
        payload={"task_id": task_id},
        source="tests.sprint2",
    )

    live_projection = runtime.get_projection("household-s2-replay", force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])

    assert comparison["matches"] is True
    assert comparison["drift"] == {
        "structural": False,
        "integrity": False,
        "causal": False,
    }
