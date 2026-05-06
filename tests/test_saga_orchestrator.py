from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.services.commands.runtime import CommandActor, CommandRuntimeService
from core.sagas import saga_orchestrator
from household_os.runtime.event_router import CanonicalEventEnvelope


@dataclass
class _FakeEventRow:
    event_id: str
    timestamp: datetime
    type: str
    payload: dict[str, Any]
    household_id: str
    source: str


class _InMemoryEventLogService:
    def __init__(self) -> None:
        self.insert_order: list[_FakeEventRow] = []

    def append_envelope(self, envelope: CanonicalEventEnvelope) -> None:
        self.insert_order.append(
            _FakeEventRow(
                event_id=envelope.event_id,
                timestamp=envelope.timestamp,
                type=envelope.event_type,
                payload=dict(envelope.payload),
                household_id=envelope.household_id,
                source=envelope.source,
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
        if user_id:
            rows = [row for row in rows if str(row.payload.get("user_id") or "") == user_id]
        if event_type:
            rows = [row for row in rows if row.type == event_type]

        ordered = sorted(rows, key=lambda row: (row.timestamp, row.event_id), reverse=True)
        return ordered[: max(1, int(limit))]


class _InMemoryRouterService:
    def __init__(self, event_log: _InMemoryEventLogService) -> None:
        self._event_log = event_log
        self._idempotency_keys: set[str] = set()

    def route(
        self,
        envelope: CanonicalEventEnvelope,
        *,
        persist: bool = True,
        dispatch: bool = True,
    ) -> dict[str, Any] | None:
        if envelope.idempotency_key and envelope.idempotency_key in self._idempotency_keys:
            return {
                "status": "duplicate",
                "event_id": envelope.event_id,
            }

        if envelope.idempotency_key:
            self._idempotency_keys.add(envelope.idempotency_key)

        if persist:
            self._event_log.append_envelope(envelope)
        if not dispatch:
            return None
        return {
            "status": "persisted",
            "event_id": envelope.event_id,
        }


def _build_runtime() -> tuple[CommandRuntimeService, _InMemoryEventLogService]:
    event_log = _InMemoryEventLogService()
    runtime = CommandRuntimeService(
        router_service=_InMemoryRouterService(event_log),
        event_log_service=event_log,
    )
    return runtime, event_log


def _event_types(event_log: _InMemoryEventLogService) -> list[str]:
    return [row.type for row in event_log.insert_order]


def _actor() -> CommandActor:
    return CommandActor(actor_type="api_user", user_id="user-1", session_id="session-1")


def _happy_path_payload() -> dict[str, Any]:
    return {
        "saga_id": "morning-routine-saga",
        "steps": [
            {
                "step_id": "prepare-task",
                "event_emitted": "workflow.task.prepare",
                "success_condition": {"status": "prepared"},
                "failure_condition": {},
                "compensation_event": "workflow.task.prepare.rollback",
            },
            {
                "step_id": "reserve-calendar",
                "event_emitted": "workflow.calendar.reserve",
                "success_condition": {"status": "reserved"},
                "failure_condition": {},
                "compensation_event": "workflow.calendar.reserve.rollback",
            },
        ],
    }


def test_saga_happy_path_execution() -> None:
    runtime, event_log = _build_runtime()

    result = runtime.handle_command(
        command_type="saga.execute",
        household_id="household-saga",
        actor=_actor(),
        payload=_happy_path_payload(),
        source="test",
    )

    assert result["status"] == "committed"
    saga_response = result["response"]["saga"]
    assert saga_response["status"] == "completed"
    assert saga_response["executed_steps"] == ["prepare-task", "reserve-calendar"]
    assert saga_response["failed_step"] is None
    assert saga_response["compensated_steps"] == []

    assert _event_types(event_log) == [
        "command.received",
        "saga.started",
        "saga.step_started",
        "workflow.task.prepare",
        "saga.step_succeeded",
        "saga.step_started",
        "workflow.calendar.reserve",
        "saga.step_succeeded",
        "saga.completed",
        "projection.snapshot",
    ]


def test_saga_partial_failure_triggers_compensation() -> None:
    runtime, event_log = _build_runtime()

    payload = _happy_path_payload()
    payload["steps"][1]["failure_condition"] = {"force_fail": True}

    result = runtime.handle_command(
        command_type="saga.execute",
        household_id="household-saga",
        actor=_actor(),
        payload=payload,
        source="test",
    )

    assert result["status"] == "compensated"
    saga_response = result["response"]["saga"]
    assert saga_response["status"] == "compensated"
    assert saga_response["failed_step"] == "reserve-calendar"
    assert saga_response["compensated_steps"] == ["prepare-task"]

    event_types = _event_types(event_log)
    assert "workflow.task.prepare.rollback" in event_types
    assert "saga.compensation_applied" in event_types
    assert "saga.compensated" in event_types


def test_saga_replay_matches_execution() -> None:
    runtime, _ = _build_runtime()

    result = runtime.handle_command(
        command_type="saga.execute",
        household_id="household-saga",
        actor=_actor(),
        payload=_happy_path_payload(),
        source="test",
    )

    replay_validation = result["response"]["saga"]["replay_validation"]
    assert replay_validation["matches"] is True
    assert replay_validation["drift"] == {
        "structural": False,
        "integrity": False,
        "causal": False,
    }


def test_saga_idempotency() -> None:
    runtime, event_log = _build_runtime()
    payload = _happy_path_payload()

    first = runtime.handle_command(
        command_type="saga.execute",
        household_id="household-saga",
        actor=_actor(),
        payload=payload,
        source="test",
    )
    second = runtime.handle_command(
        command_type="saga.execute",
        household_id="household-saga",
        actor=_actor(),
        payload=payload,
        source="test",
    )

    assert first["status"] == "committed"
    assert second["status"] == "duplicate"

    event_types = _event_types(event_log)
    assert event_types.count("command.received") == 1
    assert event_types.count("saga.started") == 1
    assert event_types.count("saga.completed") == 1


def test_saga_event_chain_ordering() -> None:
    runtime, event_log = _build_runtime()

    runtime.handle_command(
        command_type="saga.execute",
        household_id="household-saga",
        actor=_actor(),
        payload=_happy_path_payload(),
        source="test",
    )

    event_types = _event_types(event_log)
    filtered = [
        event_type
        for event_type in event_types
        if event_type
        in {
            "saga.started",
            "saga.step_started",
            "workflow.task.prepare",
            "saga.step_succeeded",
            "workflow.calendar.reserve",
            "saga.completed",
        }
    ]

    assert filtered == [
        "saga.started",
        "saga.step_started",
        "workflow.task.prepare",
        "saga.step_succeeded",
        "saga.step_started",
        "workflow.calendar.reserve",
        "saga.step_succeeded",
        "saga.completed",
    ]


def test_saga_fsm_is_not_directly_mutated() -> None:
    source = inspect.getsource(saga_orchestrator)

    assert "apps.api.core.state_machine" not in source
    assert "household_os.runtime.state_machine" not in source
    assert "validate_transition" not in source
