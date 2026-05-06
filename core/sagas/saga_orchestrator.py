from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Protocol

from core.replay import replay, validate_replay


class SagaExecutionError(ValueError):
    """Raised when saga definition or execution constraints are invalid."""


class SagaEventEmitter(Protocol):
    def __call__(self, *, event_type: str, payload: dict[str, Any], idempotency_key: str | None = None) -> object | None:
        ...


class SagaEventReader(Protocol):
    def __call__(self) -> Iterable[Mapping[str, Any] | Any]:
        ...


@dataclass(frozen=True)
class SagaStepDefinition:
    step_id: str
    event_emitted: str
    success_condition: dict[str, Any] = field(default_factory=dict)
    failure_condition: dict[str, Any] = field(default_factory=dict)
    compensation_event: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SagaDefinition:
    id: str
    steps: tuple[SagaStepDefinition, ...]
    compensation_steps: tuple[SagaStepDefinition, ...]
    metadata: dict[str, Any] = field(default_factory=dict)
    idempotency_key: str = ""


@dataclass(frozen=True)
class SagaExecutionResult:
    saga_id: str
    status: str
    executed_steps: list[str]
    failed_step: str | None
    compensated_steps: list[str]
    replay_validation: dict[str, Any]
    live_state: dict[str, Any]


class SagaOrchestrator:
    """Deterministic event-driven saga execution with compensation and replay validation."""

    def execute(
        self,
        *,
        definition: SagaDefinition,
        emit_event: SagaEventEmitter,
        read_events: SagaEventReader,
        request_id: str,
        household_id: str,
    ) -> SagaExecutionResult:
        self._validate_definition(definition)

        saga_state = {
            "saga_id": definition.id,
            "request_id": request_id,
            "status": "running",
            "executed_steps": [],
            "failed_step": None,
            "compensated_steps": [],
            "metadata": dict(definition.metadata),
            "idempotency_key": definition.idempotency_key,
        }

        emit_event(
            event_type="saga.started",
            payload={
                "saga_id": definition.id,
                "request_id": request_id,
                "household_id": household_id,
                "step_count": len(definition.steps),
                "metadata": dict(definition.metadata),
            },
            idempotency_key=f"{definition.idempotency_key}:saga.started",
        )

        completed: list[SagaStepDefinition] = []

        for index, step in enumerate(definition.steps, start=1):
            emit_event(
                event_type="saga.step_started",
                payload={
                    "saga_id": definition.id,
                    "request_id": request_id,
                    "step_id": step.step_id,
                    "step_index": index,
                    "event_emitted": step.event_emitted,
                },
                idempotency_key=f"{definition.idempotency_key}:step:{step.step_id}:started",
            )

            emit_event(
                event_type=step.event_emitted,
                payload={
                    "saga_id": definition.id,
                    "request_id": request_id,
                    "step_id": step.step_id,
                    "step_index": index,
                    "metadata": dict(step.metadata),
                },
                idempotency_key=f"{definition.idempotency_key}:step:{step.step_id}:event",
            )

            force_fail = bool(step.failure_condition.get("force_fail", False))
            if force_fail:
                saga_state["failed_step"] = step.step_id
                saga_state["status"] = "failed"
                emit_event(
                    event_type="saga.step_failed",
                    payload={
                        "saga_id": definition.id,
                        "request_id": request_id,
                        "step_id": step.step_id,
                        "step_index": index,
                        "failure_condition": dict(step.failure_condition),
                    },
                    idempotency_key=f"{definition.idempotency_key}:step:{step.step_id}:failed",
                )

                compensated = self._run_compensation(
                    definition=definition,
                    completed=completed,
                    emit_event=emit_event,
                    request_id=request_id,
                )
                saga_state["compensated_steps"] = compensated
                saga_state["status"] = "compensated"
                emit_event(
                    event_type="saga.compensated",
                    payload={
                        "saga_id": definition.id,
                        "request_id": request_id,
                        "failed_step": step.step_id,
                        "compensated_steps": compensated,
                    },
                    idempotency_key=f"{definition.idempotency_key}:saga.compensated",
                )
                return self._finalize_result(
                    definition=definition,
                    saga_state=saga_state,
                    read_events=read_events,
                )

            completed.append(step)
            saga_state["executed_steps"].append(step.step_id)
            emit_event(
                event_type="saga.step_succeeded",
                payload={
                    "saga_id": definition.id,
                    "request_id": request_id,
                    "step_id": step.step_id,
                    "step_index": index,
                    "success_condition": dict(step.success_condition),
                },
                idempotency_key=f"{definition.idempotency_key}:step:{step.step_id}:succeeded",
            )

        saga_state["status"] = "completed"
        emit_event(
            event_type="saga.completed",
            payload={
                "saga_id": definition.id,
                "request_id": request_id,
                "executed_steps": list(saga_state["executed_steps"]),
            },
            idempotency_key=f"{definition.idempotency_key}:saga.completed",
        )

        return self._finalize_result(
            definition=definition,
            saga_state=saga_state,
            read_events=read_events,
        )

    def _run_compensation(
        self,
        *,
        definition: SagaDefinition,
        completed: list[SagaStepDefinition],
        emit_event: SagaEventEmitter,
        request_id: str,
    ) -> list[str]:
        compensation_by_step = {step.step_id: step.compensation_event for step in definition.compensation_steps}
        compensated_steps: list[str] = []

        for step in reversed(completed):
            compensation_event = compensation_by_step.get(step.step_id) or step.compensation_event
            if not compensation_event:
                continue

            emit_event(
                event_type=compensation_event,
                payload={
                    "saga_id": definition.id,
                    "request_id": request_id,
                    "step_id": step.step_id,
                },
                idempotency_key=f"{definition.idempotency_key}:step:{step.step_id}:compensation:event",
            )
            emit_event(
                event_type="saga.compensation_applied",
                payload={
                    "saga_id": definition.id,
                    "request_id": request_id,
                    "step_id": step.step_id,
                    "compensation_event": compensation_event,
                },
                idempotency_key=f"{definition.idempotency_key}:step:{step.step_id}:compensation:applied",
            )
            compensated_steps.append(step.step_id)

        return compensated_steps

    def _finalize_result(
        self,
        *,
        definition: SagaDefinition,
        saga_state: dict[str, Any],
        read_events: SagaEventReader,
    ) -> SagaExecutionResult:
        all_events = list(read_events())
        replayed = replay(all_events)
        replayed_sagas = replayed.get("derived_state", {}).get("sagas", {})
        canonical_live_state = {
            "status": str(saga_state.get("status") or ""),
            "request_id": str(saga_state.get("request_id") or ""),
            "executed_steps": list(saga_state.get("executed_steps") or []),
            "failed_step": saga_state.get("failed_step") or None,
            "compensated_steps": list(saga_state.get("compensated_steps") or []),
        }
        replay_validation = validate_replay(
            {"sagas": {definition.id: canonical_live_state}},
            {"sagas": replayed_sagas},
        )

        return SagaExecutionResult(
            saga_id=definition.id,
            status=str(saga_state["status"]),
            executed_steps=list(saga_state["executed_steps"]),
            failed_step=(str(saga_state["failed_step"]) if saga_state.get("failed_step") else None),
            compensated_steps=list(saga_state["compensated_steps"]),
            replay_validation=replay_validation,
            live_state=saga_state,
        )

    def _validate_definition(self, definition: SagaDefinition) -> None:
        if not definition.id.strip():
            raise SagaExecutionError("Saga id is required")
        if not definition.idempotency_key.strip():
            raise SagaExecutionError("Saga idempotency_key is required")
        if not definition.steps:
            raise SagaExecutionError("Saga steps are required")

        seen_steps: set[str] = set()
        for step in definition.steps:
            if not step.step_id.strip():
                raise SagaExecutionError("Each saga step must include step_id")
            if step.step_id in seen_steps:
                raise SagaExecutionError(f"Duplicate saga step_id: {step.step_id}")
            seen_steps.add(step.step_id)

            if not step.event_emitted.strip():
                raise SagaExecutionError(f"Saga step {step.step_id} missing event_emitted")
            if not isinstance(step.success_condition, dict):
                raise SagaExecutionError(f"Saga step {step.step_id} success_condition must be an object")
            if not isinstance(step.failure_condition, dict):
                raise SagaExecutionError(f"Saga step {step.step_id} failure_condition must be an object")

        for comp_step in definition.compensation_steps:
            if comp_step.step_id not in seen_steps:
                raise SagaExecutionError(
                    f"Compensation step references unknown step_id: {comp_step.step_id}"
                )
