from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from app.services.commands.runtime import CommandActor, CommandRuntimeService
from core.control import CircuitBreakerRule, ControlPlane
from core.policy import PolicyVersionRegistry
from core.replay import replay, validate_replay
from core.sagas import SagaDefinition, SagaStepDefinition
from household_os.runtime.event_router import CanonicalEventEnvelope


@dataclass
class _FakeEventRow:
    event_id: str
    household_id: str
    timestamp: datetime
    type: str
    payload: dict[str, Any]
    source: str


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
                source=envelope.source,
            )
        )

    def idempotency_key_exists(self, key: str) -> bool:
        for row in self.insert_order:
            if str(row.payload.get("idempotency_key") or "") == key:
                return True
        return False

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
            persisted_payload = dict(envelope.payload)
            if envelope.idempotency_key:
                persisted_payload.setdefault("idempotency_key", envelope.idempotency_key)

            self._event_log.append_envelope(
                CanonicalEventEnvelope(
                    event_id=envelope.event_id,
                    event_type=envelope.event_type,
                    user_id=envelope.user_id,
                    household_id=envelope.household_id,
                    source=envelope.source,
                    payload=persisted_payload,
                    version=envelope.version,
                    severity=envelope.severity,
                    idempotency_key=envelope.idempotency_key,
                    actor_type=envelope.actor_type,
                    timestamp=envelope.timestamp,
                )
            )
        if not dispatch:
            return None
        return {
            "status": "persisted",
            "event_id": envelope.event_id,
        }


def _build_policy_registry(
    *,
    breaker_threshold: int = 1,
    affected_saga_types: list[str] | None = None,
) -> PolicyVersionRegistry:
    registry = PolicyVersionRegistry()
    registry.register_policy_version(
        version_id="policy.control.test",
        rules_snapshot={
            "task_title_max_length": 160,
            "task_priority_values": ["low", "medium", "high"],
        },
        risk_thresholds_snapshot={
            "high_risk_keywords": ["bank", "wire", "transfer", "payment", "pay", "password", "security"],
            "financial_approval_is_high": True,
            "high_priority_promotes_to_medium": True,
            "due_date_promotes_to_medium": True,
        },
        control_plane_thresholds_snapshot={
            "policy_version_id": "policy.control.test",
            "max_concurrent_high_risk": 1,
            "breaker_rules": [
                {
                    "breaker_id": "saga_failure_burst",
                    "threshold": breaker_threshold,
                    "lookback_events": 20,
                    "affected_saga_types": list(affected_saga_types or ["*"]),
                    "failure_event_types": ["saga.failed", "saga.step_failed", "saga.compensated"],
                    "recovery_event_types": ["saga.completed"],
                    "recovery_threshold": 2,
                }
            ],
        },
        activation_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
    )
    return registry


def _build_runtime(
    *,
    control_plane: ControlPlane | None = None,
    policy_registry: PolicyVersionRegistry | None = None,
) -> tuple[CommandRuntimeService, _InMemoryEventLogService]:
    event_log = _InMemoryEventLogService()
    runtime = CommandRuntimeService(
        router_service=_InMemoryRouterService(event_log),
        event_log_service=event_log,
        control_plane=control_plane,
        policy_registry=policy_registry or _build_policy_registry(),
    )
    return runtime, event_log


def _actor() -> CommandActor:
    return CommandActor(actor_type="api_user", user_id="control-user", session_id="control-session")


def _event(
    *,
    event_id: str,
    event_type: str,
    timestamp: datetime,
    payload: dict[str, Any],
    household_id: str = "household-control",
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "household_id": household_id,
        "payload": payload,
        "source": "tests.control_plane",
    }


def _minimal_definition(
    *,
    saga_id: str,
    saga_type: str,
    resource_keys: list[str] | None = None,
    risk_level: str = "low",
) -> SagaDefinition:
    return SagaDefinition(
        id=saga_id,
        steps=(
            SagaStepDefinition(step_id="step-1", event_emitted="workflow.step.one"),
        ),
        compensation_steps=(),
        metadata={
            "saga_type": saga_type,
            "resource_keys": list(resource_keys or []),
            "risk_level": risk_level,
        },
        idempotency_key=f"control:{saga_id}",
    )


def _saga_payload(
    *,
    saga_id: str,
    saga_type: str,
    resource_keys: list[str] | None = None,
    risk_level: str = "low",
    force_fail: bool = False,
) -> dict[str, Any]:
    return {
        "saga_id": saga_id,
        "metadata": {
            "saga_type": saga_type,
            "resource_keys": list(resource_keys or []),
            "risk_level": risk_level,
        },
        "steps": [
            {
                "step_id": "reserve-slot",
                "event_emitted": "workflow.reserve.slot",
                "success_condition": {"status": "reserved"},
                "failure_condition": {},
                "compensation_event": "workflow.reserve.slot.rollback",
                "metadata": {
                    "resource_keys": list(resource_keys or []),
                },
            },
            {
                "step_id": "finalize",
                "event_emitted": "workflow.finalize",
                "success_condition": {"status": "finalized"},
                "failure_condition": {"force_fail": True} if force_fail else {},
                "metadata": {
                    "resource_keys": list(resource_keys or []),
                },
            },
        ],
    }


@pytest.mark.ci_gate
def test_control_plane_detects_failure_patterns() -> None:
    t0 = datetime(2026, 4, 29, 9, 0, tzinfo=UTC)
    events = [
        _event(
            event_id="e1",
            event_type="saga.started",
            timestamp=t0,
            payload={"saga_id": "billing-1", "request_id": "req-1", "metadata": {"saga_type": "billing"}},
        ),
        _event(
            event_id="e2",
            event_type="saga.step_failed",
            timestamp=t0 + timedelta(seconds=1),
            payload={"saga_id": "billing-1", "request_id": "req-1", "step_id": "step-1"},
        ),
        _event(
            event_id="e3",
            event_type="saga.compensated",
            timestamp=t0 + timedelta(seconds=2),
            payload={"saga_id": "billing-1", "request_id": "req-1"},
        ),
        _event(
            event_id="e4",
            event_type="saga.started",
            timestamp=t0 + timedelta(seconds=3),
            payload={"saga_id": "billing-2", "request_id": "req-2", "metadata": {"saga_type": "billing"}},
        ),
        _event(
            event_id="e5",
            event_type="saga.step_failed",
            timestamp=t0 + timedelta(seconds=4),
            payload={"saga_id": "billing-2", "request_id": "req-2", "step_id": "step-1"},
        ),
    ]

    plane = ControlPlane(
        breaker_rules=(
            CircuitBreakerRule(
                breaker_id="billing-failure-burst",
                threshold=2,
                lookback_events=10,
                affected_saga_types=("billing",),
                recovery_threshold=2,
            ),
        ),
    )

    emitted: list[dict[str, Any]] = []

    def emit_event(*, event_type: str, payload: dict[str, Any], idempotency_key: str | None = None) -> None:
        emitted.append({"event_type": event_type, "payload": dict(payload), "idempotency_key": idempotency_key})

    decision = plane.evaluate_execution(
        definition=_minimal_definition(saga_id="billing-3", saga_type="billing"),
        household_id="household-control",
        request_id="req-3",
        emit_event=emit_event,
        read_events=lambda: list(events),
    )

    assert decision.failure_snapshot["recent_failures"] >= 2
    assert decision.circuit_state["is_open"] is True
    assert decision.status == "halted"
    assert any(item["event_type"] == "system.circuit_opened" for item in emitted)


@pytest.mark.ci_gate
def test_circuit_breaker_triggers_deterministically() -> None:
    t0 = datetime(2026, 4, 29, 10, 0, tzinfo=UTC)
    events: list[dict[str, Any]] = [
        _event(
            event_id="e1",
            event_type="saga.started",
            timestamp=t0,
            payload={"saga_id": "finance-1", "request_id": "req-1", "metadata": {"saga_type": "finance"}},
        ),
        _event(
            event_id="e2",
            event_type="saga.step_failed",
            timestamp=t0 + timedelta(seconds=1),
            payload={"saga_id": "finance-1", "request_id": "req-1", "step_id": "step-1"},
        ),
    ]

    plane = ControlPlane(
        breaker_rules=(
            CircuitBreakerRule(
                breaker_id="finance-failure-burst",
                threshold=1,
                lookback_events=10,
                affected_saga_types=("finance",),
                recovery_threshold=2,
            ),
        ),
    )

    event_counter = 100

    def emit_event(*, event_type: str, payload: dict[str, Any], idempotency_key: str | None = None) -> None:
        nonlocal event_counter
        event_counter += 1
        events.append(
            _event(
                event_id=f"e{event_counter}",
                event_type=event_type,
                timestamp=t0 + timedelta(seconds=event_counter),
                payload=dict(payload),
            )
        )

    first = plane.evaluate_execution(
        definition=_minimal_definition(saga_id="finance-2", saga_type="finance"),
        household_id="household-control",
        request_id="req-2",
        emit_event=emit_event,
        read_events=lambda: list(events),
    )
    second = plane.evaluate_execution(
        definition=_minimal_definition(saga_id="finance-3", saga_type="finance"),
        household_id="household-control",
        request_id="req-3",
        emit_event=emit_event,
        read_events=lambda: list(events),
    )

    opened_count = sum(1 for row in events if row["event_type"] == "system.circuit_opened")
    assert first.status == "halted"
    assert second.status == "halted"
    assert opened_count == 1


@pytest.mark.migration
def test_cross_saga_conflict_detection() -> None:
    t0 = datetime(2026, 4, 29, 11, 0, tzinfo=UTC)
    events = [
        _event(
            event_id="e1",
            event_type="saga.started",
            timestamp=t0,
            payload={
                "saga_id": "calendar-1",
                "request_id": "req-1",
                "metadata": {"saga_type": "calendar", "resource_keys": ["calendar:family"]},
            },
        ),
    ]

    plane = ControlPlane()
    emitted: list[str] = []

    def emit_event(*, event_type: str, payload: dict[str, Any], idempotency_key: str | None = None) -> None:
        emitted.append(event_type)

    decision = plane.evaluate_execution(
        definition=_minimal_definition(
            saga_id="calendar-2",
            saga_type="calendar",
            resource_keys=["calendar:family"],
        ),
        household_id="household-control",
        request_id="req-2",
        emit_event=emit_event,
        read_events=lambda: list(events),
    )

    assert decision.status == "halted"
    assert decision.reason == "cross_saga_conflict"
    assert decision.conflict_snapshot["has_resource_conflict"] is True
    assert "saga.halted" in emitted


@pytest.mark.ci_gate
def test_control_events_are_event_sourced_only() -> None:
    control_plane = ControlPlane(
        breaker_rules=(
            CircuitBreakerRule(
                breaker_id="billing-failure-burst",
                threshold=1,
                lookback_events=10,
                affected_saga_types=("billing",),
                recovery_threshold=2,
            ),
        ),
    )
    runtime, event_log = _build_runtime(control_plane=control_plane)

    first = runtime.handle_command(
        command_type="saga.execute",
        household_id="household-control",
        actor=_actor(),
        payload=_saga_payload(
            saga_id="billing-1",
            saga_type="billing",
            resource_keys=["payments:ledger"],
            risk_level="high",
            force_fail=True,
        ),
        source="tests.control_plane",
    )
    second = runtime.handle_command(
        command_type="saga.execute",
        household_id="household-control",
        actor=_actor(),
        payload=_saga_payload(
            saga_id="billing-2",
            saga_type="billing",
            resource_keys=["payments:ledger"],
            risk_level="high",
            force_fail=False,
        ),
        source="tests.control_plane",
    )

    assert first["status"] == "compensated"
    assert second["status"] == "halted"

    second_request_id = second["request_id"]
    second_events = [
        row.type
        for row in event_log.insert_order
        if str(row.payload.get("request_id") or "") == second_request_id
    ]

    assert "system.circuit_opened" in second_events
    assert "saga.halted" in second_events
    assert "saga.step_started" not in second_events
    assert "workflow.reserve.slot" not in second_events


@pytest.mark.reliability
def test_saga_halt_on_circuit_open() -> None:
    control_plane = ControlPlane(
        breaker_rules=(
            CircuitBreakerRule(
                breaker_id="ops-failure-burst",
                threshold=1,
                lookback_events=10,
                affected_saga_types=("ops",),
                recovery_threshold=2,
            ),
        ),
    )
    runtime, event_log = _build_runtime(control_plane=control_plane)

    runtime.handle_command(
        command_type="saga.execute",
        household_id="household-control",
        actor=_actor(),
        payload=_saga_payload(
            saga_id="ops-1",
            saga_type="ops",
            resource_keys=["resource:a"],
            force_fail=True,
        ),
        source="tests.control_plane",
    )

    halted = runtime.handle_command(
        command_type="saga.execute",
        household_id="household-control",
        actor=_actor(),
        payload=_saga_payload(
            saga_id="ops-2",
            saga_type="ops",
            resource_keys=["resource:a"],
            force_fail=False,
        ),
        source="tests.control_plane",
    )

    assert halted["status"] == "halted"
    assert halted["response"]["control"]["reason"] == "circuit_open"

    halted_request_id = halted["request_id"]
    started_for_halted_request = [
        row.type
        for row in event_log.insert_order
        if row.type == "saga.started" and str(row.payload.get("request_id") or "") == halted_request_id
    ]
    assert started_for_halted_request == []


@pytest.mark.reliability
def test_replay_matches_control_plane_decisions() -> None:
    control_plane = ControlPlane(
        breaker_rules=(
            CircuitBreakerRule(
                breaker_id="coordination-failure-burst",
                threshold=1,
                lookback_events=10,
                affected_saga_types=("coordination",),
                recovery_threshold=2,
            ),
        ),
    )
    runtime, event_log = _build_runtime(control_plane=control_plane)

    runtime.handle_command(
        command_type="saga.execute",
        household_id="household-control",
        actor=_actor(),
        payload=_saga_payload(
            saga_id="coordination-1",
            saga_type="coordination",
            resource_keys=["shared:calendar"],
            force_fail=True,
        ),
        source="tests.control_plane",
    )

    halted = runtime.handle_command(
        command_type="saga.execute",
        household_id="household-control",
        actor=_actor(),
        payload=_saga_payload(
            saga_id="coordination-2",
            saga_type="coordination",
            resource_keys=["shared:calendar"],
            force_fail=False,
        ),
        source="tests.control_plane",
    )

    replayed = replay(event_log.insert_order)
    projection = runtime.get_projection("household-control", force_replay=True)

    control_compare = validate_replay(
        projection["control_plane"],
        replayed["derived_state"]["control_plane"],
    )

    assert halted["response"]["control"]["replay_validation"]["matches"] is True
    assert control_compare["matches"] is True

    circuit_states = replayed["derived_state"]["control_plane"]["circuits"]
    assert any(row.get("status") == "open" for row in circuit_states.values())
    assert "coordination-2" in replayed["derived_state"]["control_plane"]["halted_sagas"]
