from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import command as command_api
from app.services.commands.runtime import CommandActor, CommandRuntimeService
from core.replay.event_replay_engine import replay, validate_replay
from household_os.runtime.event_router import CanonicalEventEnvelope


# FEATURE_INTAKE:
#   projection_impact: yes
#   read_model_impact: no
#   kernel_interaction: none
FEATURE_INTAKE_DECLARATION = {
    "projection_impact": "yes",
    "read_model_impact": "no",
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


def test_end_to_end_command_flow() -> None:
    runtime, event_log = _build_runtime()

    app = FastAPI()
    app.include_router(command_api.router)

    command_api_getter = command_api.get_command_runtime_service
    command_api.get_command_runtime_service = lambda: runtime
    try:
        with TestClient(app) as client:
            response = client.post(
                "/command",
                json={
                    "command_type": "task.create",
                    "household_id": "sprint-household-0",
                    "payload": {
                        "title": "Sprint zero task",
                        "priority": "low",
                    },
                },
            )
    finally:
        command_api.get_command_runtime_service = command_api_getter

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "accepted"
    assert isinstance(body.get("event_id"), str) and body["event_id"].strip()
    assert body["projection"]["state_version"] >= 1
    assert body["projection"]["tasks"]
    assert "task.created" in _event_types(event_log)


def test_task_creation_flow_task_created_command() -> None:
    runtime, event_log = _build_runtime()

    result = runtime.handle_command(
        command_type="task_created",
        household_id="sprint-household-1",
        actor=CommandActor(actor_type="api_user", user_id="user-s1"),
        payload={
            "title": "Sprint one canonical task",
            "priority": "medium",
        },
        source="tests.sprint",
    )

    assert result["status"] == "accepted"
    assert "TaskCreated" in _event_types(event_log)
    task_id = str(result["response"]["task"]["task_id"])
    assert task_id
    tasks_list = result["projection"].get("tasks_list") or []
    assert any(str(task.get("task_id") or "") == task_id for task in tasks_list)


def test_idempotency_same_command_no_duplicate_events() -> None:
    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-s1")

    payload = {
        "title": "Deduplicated task",
        "priority": "low",
    }

    first = runtime.handle_command(
        command_type="task_created",
        household_id="sprint-household-idem",
        actor=actor,
        payload=payload,
        source="tests.sprint",
        idempotency_key="idem-sprint-01",
    )
    second = runtime.handle_command(
        command_type="task_created",
        household_id="sprint-household-idem",
        actor=actor,
        payload=payload,
        source="tests.sprint",
        idempotency_key="idem-sprint-01",
    )

    assert first["status"] == "accepted"
    assert second["status"] == "duplicate"
    event_types = _event_types(event_log)
    assert event_types.count("command.received") == 1
    assert event_types.count("TaskCreated") == 1


def test_projection_integrity_replay_matches_runtime() -> None:
    runtime, event_log = _build_runtime()

    runtime.handle_command(
        command_type="task_created",
        household_id="sprint-household-replay",
        actor=CommandActor(actor_type="api_user", user_id="user-s1"),
        payload={
            "title": "Replay integrity task",
            "priority": "high",
            "requires_financial_approval": True,
        },
        source="tests.sprint",
    )

    live_projection = runtime.get_projection("sprint-household-replay", force_replay=True)
    replayed = replay(event_log.insert_order)

    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True
    assert comparison["drift"] == {
        "structural": False,
        "integrity": False,
        "causal": False,
    }
