from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import command as command_api
from app.services.commands.runtime import CommandActor, CommandRuntimeService
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
        rows = [
            row
            for row in self.insert_order
            if (row.payload.get("household_id") or household_id) == household_id
        ]
        if user_id:
            rows = [row for row in rows if str(row.payload.get("user_id") or "") == user_id]
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


def _bootstrap_decision_card_to_acknowledged(
    *,
    runtime: CommandRuntimeService,
    household_id: str,
    actor: CommandActor,
    decision_id: str,
    root_cause_key: str,
    title: str,
) -> None:
    runtime.handle_command(
        command_type="decision.card.create",
        household_id=household_id,
        actor=actor,
        payload={
            "decision_card_id": decision_id,
            "root_cause_key": root_cause_key,
            "title": title,
        },
        source="test",
    )
    runtime.handle_command(
        command_type="decision.card.surface",
        household_id=household_id,
        actor=actor,
        payload={"decision_card_id": decision_id},
        source="test",
    )
    runtime.handle_command(
        command_type="decision.card.acknowledge",
        household_id=household_id,
        actor=actor,
        payload={"decision_card_id": decision_id},
        source="test",
    )


def test_task_create_command_records_rules_risk_fsm_and_event_log() -> None:
    runtime, event_log = _build_runtime()

    result = runtime.handle_command(
        command_type="task.create",
        household_id="household-1",
        actor=CommandActor(actor_type="api_user", user_id="user-1", session_id="session-1"),
        payload={
            "title": "Buy groceries",
            "description": "Weekly restock",
            "priority": "medium",
        },
        source="test",
    )

    assert result["status"] == "accepted"
    task = result["response"]["task"]
    assert task["title"] == "Buy groceries"
    assert task["lifecycle_state"] == "created"
    assert task["risk_level"] == "low"

    projection = result["projection"]
    assert task["task_id"] in projection["tasks"]

    assert _event_types(event_log) == [
        "command.received",
        "task.rules_evaluated",
        "task.risk_assessed",
        "task.fsm_transitioned",
        "task.created",
        "projection.snapshot",
    ]


def test_task_create_rejects_invalid_payload_without_event_emission() -> None:
    runtime, event_log = _build_runtime()

    result = runtime.handle_command(
        command_type="task.create",
        household_id="household-1",
        actor=CommandActor(actor_type="api_user", user_id="user-1"),
        payload={"priority": "medium"},
        source="test",
    )

    assert result["status"] == "rejected"
    assert result["response"]["code"] == "task_title_required"
    assert _event_types(event_log) == []


def test_task_create_duplicate_returns_cached_response_without_new_events() -> None:
    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-1")
    payload = {
        "title": "Call plumber",
        "priority": "low",
    }

    first = runtime.handle_command(
        command_type="task.create",
        household_id="household-1",
        actor=actor,
        payload=payload,
        source="test",
    )
    second = runtime.handle_command(
        command_type="task.create",
        household_id="household-1",
        actor=actor,
        payload=payload,
        source="test",
    )

    assert first["status"] == "accepted"
    assert second["status"] == "duplicate"
    assert second["response"]["task"]["title"] == "Call plumber"

    event_types = _event_types(event_log)
    assert event_types.count("command.received") == 1
    assert event_types.count("task.created") == 1


def test_task_create_high_risk_uses_pending_approval_fsm_state() -> None:
    runtime, _ = _build_runtime()

    result = runtime.handle_command(
        command_type="create_task",
        household_id="household-2",
        actor=CommandActor(actor_type="api_user", user_id="user-2"),
        payload={
            "title": "Pay utility bill",
            "priority": "high",
            "requires_financial_approval": True,
        },
        source="test",
    )

    assert result["status"] == "pending_approval"
    task = result["response"]["task"]
    assert task["lifecycle_state"] == "pending_approval"
    assert task["risk_level"] == "high"
    assert any(item["action_id"] == task["task_id"] for item in result["projection"]["pending_actions"])


def test_command_gateway_routes_task_creation_into_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime, event_log = _build_runtime()
    monkeypatch.setattr(command_api, "get_command_runtime_service", lambda: runtime)

    app = FastAPI()
    app.include_router(command_api.router)

    with TestClient(app) as client:
        response = client.post(
            "/command",
            json={
                "command_type": "task.create",
                "household_id": "household-gateway",
                "payload": {
                    "title": "Prepare school bags",
                    "priority": "low",
                },
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "accepted"
    assert body["response"]["task"]["title"] == "Prepare school bags"
    assert "task.created" in _event_types(event_log)


def test_decision_complete_command_emits_event_and_updates_projection() -> None:
    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-2")

    created = runtime.handle_command(
        command_type="task.create",
        household_id="household-decision-complete",
        actor=actor,
        payload={
            "title": "Decision complete target",
            "priority": "medium",
            "due_at": "2036-01-03T10:00:00Z",
        },
        source="test",
    )
    decision_id = str(created["response"]["task"]["task_id"])

    _bootstrap_decision_card_to_acknowledged(
        runtime=runtime,
        household_id="household-decision-complete",
        actor=actor,
        decision_id=decision_id,
        root_cause_key=f"task:{decision_id}:completion",
        title="Decision complete target",
    )

    result = runtime.handle_command(
        command_type="decision.complete",
        household_id="household-decision-complete",
        actor=actor,
        payload={"decision_id": decision_id},
        source="test",
    )

    assert result["status"] == "accepted"
    assert result["response"]["decision_id"] == decision_id
    assert result["response"]["event_type"] == "DecisionCompleted"
    assert result["projection"]["tasks"][decision_id]["status"] == "completed"
    assert result["projection"]["tasks"][decision_id]["completed_at"]

    event_types = _event_types(event_log)
    assert event_types.count("DecisionCompleted") == 1


def test_decision_defer_command_requires_defer_to_date_and_updates_projection() -> None:
    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-3")

    created = runtime.handle_command(
        command_type="task.create",
        household_id="household-decision-defer",
        actor=actor,
        payload={
            "title": "Decision defer target",
            "priority": "low",
            "due_at": "2036-01-04T09:00:00Z",
        },
        source="test",
    )
    decision_id = str(created["response"]["task"]["task_id"])

    rejected = runtime.handle_command(
        command_type="decision.defer",
        household_id="household-decision-defer",
        actor=actor,
        payload={"decision_id": decision_id},
        source="test",
    )
    assert rejected["status"] == "rejected"
    assert rejected["response"]["code"] == "decision_defer_to_date_required"

    _bootstrap_decision_card_to_acknowledged(
        runtime=runtime,
        household_id="household-decision-defer",
        actor=actor,
        decision_id=decision_id,
        root_cause_key=f"task:{decision_id}:defer",
        title="Decision defer target",
    )

    accepted = runtime.handle_command(
        command_type="decision.defer",
        household_id="household-decision-defer",
        actor=actor,
        payload={
            "decision_id": decision_id,
            "defer_to_date": "2036-01-08",
        },
        source="test",
    )

    assert accepted["status"] == "accepted"
    assert accepted["response"]["event_type"] == "DecisionDeferred"
    assert accepted["projection"]["tasks"][decision_id]["lifecycle_state"] == "deferred"
    assert accepted["projection"]["tasks"][decision_id]["due_at"] == "2036-01-08"

    event_types = _event_types(event_log)
    assert event_types.count("DecisionDeferred") == 1


def test_decision_ignore_command_emits_event_and_marks_task_ignored() -> None:
    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-4")

    created = runtime.handle_command(
        command_type="task.create",
        household_id="household-decision-ignore",
        actor=actor,
        payload={
            "title": "Decision ignore target",
            "priority": "low",
        },
        source="test",
    )
    decision_id = str(created["response"]["task"]["task_id"])

    _bootstrap_decision_card_to_acknowledged(
        runtime=runtime,
        household_id="household-decision-ignore",
        actor=actor,
        decision_id=decision_id,
        root_cause_key=f"task:{decision_id}:ignore",
        title="Decision ignore target",
    )

    ignored = runtime.handle_command(
        command_type="decision.ignore",
        household_id="household-decision-ignore",
        actor=actor,
        payload={"decision_id": decision_id},
        source="test",
    )

    assert ignored["status"] == "accepted"
    assert ignored["response"]["event_type"] == "DecisionIgnored"
    assert ignored["projection"]["tasks"][decision_id]["status"] == "ignored"
    assert ignored["projection"]["tasks"][decision_id]["ignored_at"]

    event_types = _event_types(event_log)
    assert event_types.count("DecisionIgnored") == 1


def test_decision_card_lifecycle_commands_gate_decision_completion() -> None:
    runtime, event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-card-1")

    created = runtime.handle_command(
        command_type="task.create",
        household_id="household-decision-card",
        actor=actor,
        payload={
            "title": "Decision card runtime target",
            "priority": "medium",
        },
        source="test",
    )
    decision_id = str(created["response"]["task"]["task_id"])

    generated = runtime.handle_command(
        command_type="decision.card.create",
        household_id="household-decision-card",
        actor=actor,
        payload={
            "decision_card_id": decision_id,
            "root_cause_key": "calendar_overlap:dentist",
            "title": "Resolve dentist overlap",
        },
        source="test",
    )
    assert generated["status"] == "accepted"

    surfaced = runtime.handle_command(
        command_type="decision.card.surface",
        household_id="household-decision-card",
        actor=actor,
        payload={"decision_card_id": decision_id},
        source="test",
    )
    assert surfaced["status"] == "accepted"

    acknowledged = runtime.handle_command(
        command_type="decision.card.acknowledge",
        household_id="household-decision-card",
        actor=actor,
        payload={"decision_card_id": decision_id},
        source="test",
    )
    assert acknowledged["status"] == "accepted"

    completed = runtime.handle_command(
        command_type="decision.complete",
        household_id="household-decision-card",
        actor=actor,
        payload={"decision_id": decision_id},
        source="test",
    )
    assert completed["status"] == "accepted"
    assert completed["projection"]["tasks"][decision_id]["status"] == "completed"
    assert completed["projection"]["decision_cards"][decision_id]["state"] == "applied"

    event_types = _event_types(event_log)
    assert event_types.count("DecisionCardGenerated") == 1
    assert event_types.count("DecisionCardSurfaced") == 1
    assert event_types.count("DecisionCardAcknowledged") == 1
    assert event_types.count("DecisionCardResolved") == 1
    assert event_types.count("DecisionCardApplied") == 1
    assert event_types.count("DecisionCompleted") == 1


def test_decision_complete_rejected_when_card_not_acknowledged() -> None:
    runtime, _event_log = _build_runtime()
    actor = CommandActor(actor_type="api_user", user_id="user-card-2")

    created = runtime.handle_command(
        command_type="task.create",
        household_id="household-decision-card-reject",
        actor=actor,
        payload={
            "title": "Decision card reject target",
            "priority": "low",
        },
        source="test",
    )
    decision_id = str(created["response"]["task"]["task_id"])

    runtime.handle_command(
        command_type="decision.card.create",
        household_id="household-decision-card-reject",
        actor=actor,
        payload={
            "decision_card_id": decision_id,
            "root_cause_key": "inventory_gap:dinner",
            "title": "Resolve dinner inventory gap",
        },
        source="test",
    )

    rejected = runtime.handle_command(
        command_type="decision.complete",
        household_id="household-decision-card-reject",
        actor=actor,
        payload={"decision_id": decision_id},
        source="test",
    )

    assert rejected["status"] == "rejected"
    assert rejected["response"]["code"] == "decision_card_transition_invalid"
