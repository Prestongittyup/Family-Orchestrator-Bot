from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from app.services.commands.runtime import CommandActor, CommandRuntimeService
from core.control import ControlPlane
from core.policy import PolicyVersionRegistry
from core.replay import replay_with_policy_context
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
            self._event_log.append_envelope(envelope)
        if not dispatch:
            return None
        return {
            "status": "persisted",
            "event_id": envelope.event_id,
        }


def _build_registry() -> PolicyVersionRegistry:
    registry = PolicyVersionRegistry()
    registry.register_policy_version(
        version_id="policy.v1",
        rules_snapshot={
            "task_title_max_length": 160,
            "task_priority_values": ["low", "medium", "high"],
        },
        risk_thresholds_snapshot={
            "high_risk_keywords": ["wire", "payment"],
            "financial_approval_is_high": True,
            "high_priority_promotes_to_medium": True,
            "due_date_promotes_to_medium": True,
        },
        control_plane_thresholds_snapshot={
            "policy_version_id": "policy.v1",
            "max_concurrent_high_risk": 1,
            "breaker_rules": [
                {
                    "breaker_id": "billing-breaker",
                    "threshold": 1,
                    "lookback_events": 10,
                    "affected_saga_types": ["billing"],
                    "recovery_threshold": 2,
                }
            ],
        },
        activation_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        deprecation_timestamp=datetime(2026, 4, 1, tzinfo=UTC),
    )
    registry.register_policy_version(
        version_id="policy.v2",
        rules_snapshot={
            "task_title_max_length": 200,
            "task_priority_values": ["low", "medium", "high"],
        },
        risk_thresholds_snapshot={
            "high_risk_keywords": ["wire", "payment", "transfer"],
            "financial_approval_is_high": True,
            "high_priority_promotes_to_medium": True,
            "due_date_promotes_to_medium": True,
        },
        control_plane_thresholds_snapshot={
            "policy_version_id": "policy.v2",
            "max_concurrent_high_risk": 2,
            "breaker_rules": [
                {
                    "breaker_id": "billing-breaker",
                    "threshold": 3,
                    "lookback_events": 10,
                    "affected_saga_types": ["billing"],
                    "recovery_threshold": 2,
                }
            ],
        },
        activation_timestamp=datetime(2026, 4, 1, tzinfo=UTC),
        deprecation_timestamp=None,
    )
    return registry


def _build_runtime(registry: PolicyVersionRegistry) -> tuple[CommandRuntimeService, _InMemoryEventLogService]:
    event_log = _InMemoryEventLogService()
    runtime = CommandRuntimeService(
        router_service=_InMemoryRouterService(event_log),
        event_log_service=event_log,
        control_plane=ControlPlane(),
        policy_registry=registry,
    )
    return runtime, event_log


def _actor() -> CommandActor:
    return CommandActor(actor_type="api_user", user_id="policy-user", session_id="policy-session")


@pytest.mark.ci_gate
def test_policy_version_creation_and_immutability() -> None:
    registry = PolicyVersionRegistry()
    version = registry.register_policy_version(
        version_id="policy.immutable.v1",
        rules_snapshot={"task_title_max_length": 160},
        risk_thresholds_snapshot={"high_risk_keywords": ["wire"]},
        control_plane_thresholds_snapshot={"max_concurrent_high_risk": 1, "breaker_rules": []},
        activation_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert len(registry.versions) == 1
    assert version.version_id == "policy.immutable.v1"
    with pytest.raises(TypeError):
        version.rules_snapshot["task_title_max_length"] = 999


@pytest.mark.ci_gate
def test_policy_resolution_by_event_timestamp() -> None:
    registry = _build_registry()

    resolved_v1 = registry.resolve_policy(datetime(2026, 3, 15, tzinfo=UTC))
    resolved_v2 = registry.resolve_policy(datetime(2026, 4, 15, tzinfo=UTC))

    assert resolved_v1.version_id == "policy.v1"
    assert resolved_v2.version_id == "policy.v2"
    assert resolved_v1.evaluation_context_hash != resolved_v2.evaluation_context_hash


@pytest.mark.ci_gate
def test_event_carries_policy_version() -> None:
    registry = _build_registry()
    runtime, event_log = _build_runtime(registry)

    runtime.handle_command(
        command_type="task.create",
        household_id="household-policy",
        actor=_actor(),
        payload={"title": "Prepare documents", "priority": "low"},
        source="tests.policy",
    )

    assert event_log.insert_order
    assert all(str(row.payload.get("policy_version_id") or "").strip() for row in event_log.insert_order)
    assert all(str(row.payload.get("evaluation_context_hash") or "").strip() for row in event_log.insert_order)


@pytest.mark.migration
def test_replay_under_historical_policy_state() -> None:
    registry = _build_registry()
    event_time = datetime(2026, 3, 10, 9, 0, tzinfo=UTC)
    policy = registry.resolve_policy(event_time)

    events = [
        {
            "event_id": "e1",
            "event_type": "task.rules_evaluated",
            "timestamp": event_time,
            "household_id": "household-policy",
            "payload": {
                "request_id": "req-1",
                "rules_passed": True,
                "policy_version_id": policy.version_id,
                "evaluation_context_hash": policy.evaluation_context_hash,
            },
            "source": "tests.policy",
        },
        {
            "event_id": "e2",
            "event_type": "task.risk_assessed",
            "timestamp": event_time + timedelta(seconds=1),
            "household_id": "household-policy",
            "payload": {
                "request_id": "req-1",
                "risk": {"level": "low"},
                "policy_version_id": policy.version_id,
                "evaluation_context_hash": policy.evaluation_context_hash,
            },
            "source": "tests.policy",
        },
        {
            "event_id": "e3",
            "event_type": "task.fsm_transitioned",
            "timestamp": event_time + timedelta(seconds=2),
            "household_id": "household-policy",
            "payload": {
                "request_id": "req-1",
                "task_id": "task-1",
                "current_state": "created",
                "transitions": [
                    {"index": 1, "from_state": "received", "to_state": "rules_passed"},
                    {"index": 2, "from_state": "rules_passed", "to_state": "risk_assessed"},
                    {"index": 3, "from_state": "risk_assessed", "to_state": "created"},
                ],
                "policy_version_id": policy.version_id,
                "evaluation_context_hash": policy.evaluation_context_hash,
            },
            "source": "tests.policy",
        },
        {
            "event_id": "e4",
            "event_type": "task.created",
            "timestamp": event_time + timedelta(seconds=3),
            "household_id": "household-policy",
            "payload": {
                "request_id": "req-1",
                "task": {
                    "task_id": "task-1",
                    "request_id": "req-1",
                    "title": "Prepare documents",
                    "lifecycle_state": "created",
                },
                "policy_version_id": policy.version_id,
                "evaluation_context_hash": policy.evaluation_context_hash,
            },
            "source": "tests.policy",
        },
    ]

    result = replay_with_policy_context(events, policy_registry=registry)

    assert result["policy_reconstruction"]["matches"] is True
    assert result["derived_state"]["tasks"]["task-1"]["title"] == "Prepare documents"


@pytest.mark.reliability
def test_policy_drift_detection_across_versions() -> None:
    registry = _build_registry()
    event_time = datetime(2026, 3, 20, 9, 0, tzinfo=UTC)
    policy = registry.resolve_policy(event_time)

    events = [
        {
            "event_id": "e1",
            "event_type": "task.rules_evaluated",
            "timestamp": event_time,
            "household_id": "household-policy",
            "payload": {
                "request_id": "req-drift",
                "rules_passed": True,
                "policy_version_id": policy.version_id,
                "evaluation_context_hash": policy.evaluation_context_hash,
            },
            "source": "tests.policy",
        },
        {
            "event_id": "e2",
            "event_type": "task.risk_assessed",
            "timestamp": event_time + timedelta(seconds=1),
            "household_id": "household-policy",
            "payload": {
                "request_id": "req-drift",
                "risk": {"level": "high"},
                "policy_version_id": policy.version_id,
                "evaluation_context_hash": policy.evaluation_context_hash,
            },
            "source": "tests.policy",
        },
    ]

    result = replay_with_policy_context(
        events,
        policy_registry=registry,
        compare_with_current=True,
        current_timestamp=datetime(2026, 4, 15, tzinfo=UTC),
    )

    assert result["policy_reconstruction"]["matches"] is True
    assert result["policy_drift"]["drift"] == {
        "structural": True,
        "integrity": False,
        "causal": False,
    }
    assert "policy_evolved" in result["policy_drift"]["drift_reasons"]["structural"]
    assert result["policy_drift"]["evolved_event_ids"]


@pytest.mark.migration
def test_control_plane_respects_policy_version() -> None:
    control_plane = ControlPlane()
    t0 = datetime(2026, 4, 29, 9, 0, tzinfo=UTC)

    historical_events = [
        {
            "event_id": "e1",
            "event_type": "saga.started",
            "timestamp": t0,
            "household_id": "household-policy",
            "payload": {
                "saga_id": "billing-1",
                "request_id": "req-1",
                "metadata": {"saga_type": "billing"},
                "policy_version_id": "policy.v1",
                "evaluation_context_hash": "hash-v1",
            },
            "source": "tests.policy",
        },
        {
            "event_id": "e2",
            "event_type": "saga.step_started",
            "timestamp": t0 + timedelta(seconds=1),
            "household_id": "household-policy",
            "payload": {
                "saga_id": "billing-1",
                "request_id": "req-1",
                "step_id": "step-1",
                "policy_version_id": "policy.v1",
                "evaluation_context_hash": "hash-v1",
            },
            "source": "tests.policy",
        },
        {
            "event_id": "e3",
            "event_type": "workflow.billing.step",
            "timestamp": t0 + timedelta(seconds=2),
            "household_id": "household-policy",
            "payload": {
                "saga_id": "billing-1",
                "request_id": "req-1",
                "step_id": "step-1",
                "policy_version_id": "policy.v1",
                "evaluation_context_hash": "hash-v1",
            },
            "source": "tests.policy",
        },
        {
            "event_id": "e4",
            "event_type": "saga.step_failed",
            "timestamp": t0 + timedelta(seconds=3),
            "household_id": "household-policy",
            "payload": {
                "saga_id": "billing-1",
                "request_id": "req-1",
                "step_id": "step-1",
                "policy_version_id": "policy.v1",
                "evaluation_context_hash": "hash-v1",
            },
            "source": "tests.policy",
        },
    ]

    emitted_events: list[str] = []

    def emit_event(*, event_type: str, payload: dict[str, Any], idempotency_key: str | None = None) -> None:
        del payload, idempotency_key
        emitted_events.append(event_type)

    definition = SagaDefinition(
        id="billing-2",
        steps=(SagaStepDefinition(step_id="step-a", event_emitted="workflow.billing.step"),),
        compensation_steps=(),
        metadata={"saga_type": "billing"},
        idempotency_key="policy-control:billing-2",
    )

    strict = control_plane.evaluate_execution(
        definition=definition,
        household_id="household-policy",
        request_id="req-strict",
        emit_event=emit_event,
        read_events=lambda: list(historical_events),
        policy_snapshot={
            "policy_version_id": "policy.v1",
            "max_concurrent_high_risk": 1,
            "breaker_rules": [
                {
                    "breaker_id": "billing-breaker",
                    "threshold": 1,
                    "lookback_events": 10,
                    "affected_saga_types": ["billing"],
                    "recovery_threshold": 2,
                }
            ],
        },
    )
    lenient = control_plane.evaluate_execution(
        definition=definition,
        household_id="household-policy",
        request_id="req-lenient",
        emit_event=emit_event,
        read_events=lambda: list(historical_events),
        policy_snapshot={
            "policy_version_id": "policy.v2",
            "max_concurrent_high_risk": 1,
            "breaker_rules": [
                {
                    "breaker_id": "billing-breaker",
                    "threshold": 5,
                    "lookback_events": 10,
                    "affected_saga_types": ["billing"],
                    "recovery_threshold": 2,
                }
            ],
        },
    )

    assert strict.status == "halted"
    assert strict.policy_version_id == "policy.v1"
    assert lenient.status == "allowed"
    assert lenient.policy_version_id == "policy.v2"
