from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Iterable, Mapping

from core.health import empty_drift_classification, empty_drift_reasons
from core.policy import PolicyVersionRegistry
from decision_card_system.registry import (
    DecisionCardInvariantError,
    reduce_decision_card_projection,
)
from core.replay.domain_projection_helpers import (
    apply_decision_completed_projection,
    apply_decision_deferred_projection,
    apply_decision_ignored_projection,
    apply_reminder_cancelled_projection,
    apply_reminder_created_projection,
    apply_reminder_triggered_projection,
    apply_schedule_cancelled_projection,
    apply_schedule_created_projection,
    apply_task_completed_projection,
    apply_task_created_projection,
    derive_notification_from_reminder_triggered,
)
from core.replay.surface_registry import (
    iter_surfaces,
    resolve_projection,
    resolve_reducer,
    resolve_surface_id_for_event,
)


class ReplayValidationError(ValueError):
    """Raised when event replay cannot deterministically reconstruct state."""


class ActionState(str, Enum):
    PROPOSED = "proposed"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    COMMITTED = "committed"
    REJECTED = "rejected"
    FAILED = "failed"


class TransitionError(ValueError):
    """Raised when replay detects an invalid transition sequence."""


_ALLOWED_TRANSITIONS: dict[ActionState, frozenset[ActionState]] = {
    ActionState.PROPOSED: frozenset(
        {
            ActionState.PENDING_APPROVAL,
            ActionState.APPROVED,
            ActionState.REJECTED,
            ActionState.FAILED,
        }
    ),
    ActionState.PENDING_APPROVAL: frozenset(
        {
            ActionState.APPROVED,
            ActionState.REJECTED,
            ActionState.FAILED,
        }
    ),
    ActionState.APPROVED: frozenset({ActionState.COMMITTED, ActionState.FAILED}),
    ActionState.COMMITTED: frozenset(),
    ActionState.REJECTED: frozenset(),
    ActionState.FAILED: frozenset({ActionState.PROPOSED}),
}


def _validate_action_transition(
    *,
    from_state: ActionState,
    to_state: ActionState,
    context: Mapping[str, Any] | None = None,
) -> None:
    context = context or {}
    if from_state == to_state:
        raise TransitionError(f"No-op transition not allowed: {from_state.value} -> {to_state.value}")

    allowed = _ALLOWED_TRANSITIONS.get(from_state, frozenset())
    if to_state not in allowed:
        raise TransitionError(
            f"Transition not allowed: {from_state.value} -> {to_state.value}. "
            f"Allowed targets: {sorted(state.value for state in allowed)}"
        )

    if to_state == ActionState.APPROVED and context.get("actor_type") == "assistant":
        raise TransitionError("Assistant cannot approve actions")

    if (
        from_state == ActionState.PROPOSED
        and to_state == ActionState.APPROVED
        and context.get("requires_approval") is True
    ):
        raise TransitionError("Action requires approval before approved state")


@dataclass(frozen=True)
class ReplayEvent:
    event_id: str
    event_type: str
    timestamp: datetime
    household_id: str
    payload: dict[str, Any]
    source: str


def replay(events: Iterable[Mapping[str, Any] | Any]) -> dict[str, Any]:
    """Reconstruct replay state from authoritative event history."""
    normalized = _normalize_event_stream(events)
    _validate_required_predecessors(normalized)
    derived_state = _project_from_normalized(normalized)
    fsm_state = _rebuild_fsm_from_normalized(normalized)

    replay_checksum = _stable_hash(
        {
            "fsm_state": fsm_state,
            "derived_state": derived_state,
            "event_ids": [event.event_id for event in normalized],
        }
    )

    return {
        "event_count": len(normalized),
        "last_event_id": normalized[-1].event_id if normalized else "",
        "derived_state": derived_state,
        "fsm_state": fsm_state,
        "replay_checksum": replay_checksum,
    }


def replay_partitioned(
    events: Iterable[Mapping[str, Any] | Any],
    *,
    max_workers: int | None = None,
) -> dict[str, dict[str, Any]]:
    normalized = [_coerce_event(event) for event in events]
    if not normalized:
        raise ReplayValidationError("Event stream is empty")

    partitions: dict[str, list[ReplayEvent]] = {}
    for event in normalized:
        partitions.setdefault(event.household_id, []).append(event)

    partition_items = list(partitions.items())
    if len(partition_items) == 1:
        partition_id, partition_events = partition_items[0]
        return {partition_id: replay(partition_events)}

    worker_count = max_workers or len(partition_items)
    resolved_workers = max(2, min(len(partition_items), int(worker_count)))
    replayed_by_partition: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=resolved_workers, thread_name_prefix="replay-partition") as executor:
        submitted = {
            executor.submit(replay, partition_events): partition_id
            for partition_id, partition_events in partition_items
        }

        for future in as_completed(submitted):
            partition_id = submitted[future]
            replayed_by_partition[partition_id] = future.result()
    return replayed_by_partition


def project_state_partitioned(
    events: Iterable[Mapping[str, Any] | Any],
    *,
    max_workers: int | None = None,
) -> dict[str, dict[str, Any]]:
    replayed = replay_partitioned(events, max_workers=max_workers)
    return {
        partition_id: dict(result.get("derived_state") or {})
        for partition_id, result in replayed.items()
    }


def rebuild_fsm(events: Iterable[Mapping[str, Any] | Any]) -> dict[str, Any]:
    """Rebuild FSM state from event stream only."""
    normalized = _normalize_event_stream(events)
    _validate_required_predecessors(normalized)
    return _rebuild_fsm_from_normalized(normalized)


def project_state(events: Iterable[Mapping[str, Any] | Any]) -> dict[str, Any]:
    """Project deterministic read state from canonical events only."""
    normalized = _normalize_event_stream(events)
    _validate_required_predecessors(normalized)
    return _project_from_normalized(normalized)


def validate_replay(live_state: Mapping[str, Any], replayed_state: Mapping[str, Any]) -> dict[str, Any]:
    """Detect drift between live runtime state and replayed event-sourced state."""
    live_checksum = _stable_hash(live_state)
    replayed_checksum = _stable_hash(replayed_state)

    drift = empty_drift_classification()
    drift_reasons = empty_drift_reasons()
    if live_checksum != replayed_checksum:
        drift["integrity"] = True
        drift_reasons["integrity"].append("checksum_mismatch")

    if isinstance(live_state, Mapping) and isinstance(replayed_state, Mapping):
        for key in sorted(set(live_state.keys()) | set(replayed_state.keys())):
            if live_state.get(key) != replayed_state.get(key):
                drift["integrity"] = True
                drift_reasons["integrity"].append(f"field_mismatch:{key}")

    normalized_reasons = {
        key: list(dict.fromkeys(value))
        for key, value in drift_reasons.items()
    }

    return {
        "matches": not (drift["integrity"] or drift["causal"]),
        "drift": drift,
        "drift_reasons": normalized_reasons,
        "live_checksum": live_checksum,
        "replayed_checksum": replayed_checksum,
    }


def replay_with_policy_context(
    events: Iterable[Mapping[str, Any] | Any],
    *,
    policy_registry: PolicyVersionRegistry,
    compare_with_current: bool = False,
    current_timestamp: datetime | None = None,
) -> dict[str, Any]:
    normalized = _normalize_event_stream(events)
    serializable_events = [
        {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "timestamp": event.timestamp,
            "household_id": event.household_id,
            "payload": dict(event.payload),
            "source": event.source,
        }
        for event in normalized
    ]

    replayed = replay(serializable_events)
    policy_reconstruction = policy_registry.reconstruct_event_policy_bindings(serializable_events)

    policy_drift: dict[str, Any] = {
        "drift": empty_drift_classification(),
        "drift_reasons": empty_drift_reasons(),
    }
    if compare_with_current:
        policy_drift = policy_registry.detect_policy_drift(
            serializable_events,
            current_timestamp=current_timestamp,
        )

    replayed["policy_reconstruction"] = policy_reconstruction
    replayed["policy_drift"] = policy_drift
    return replayed


def _normalize_event_stream(events: Iterable[Mapping[str, Any] | Any]) -> list[ReplayEvent]:
    normalized = [_coerce_event(event) for event in events]
    if not normalized:
        raise ReplayValidationError("Event stream is empty")

    previous_timestamp: datetime | None = None
    previous_sequence: int | None = None

    for index, event in enumerate(normalized, start=1):
        sequence_value = event.payload.get("sequence")
        if sequence_value is not None:
            try:
                sequence = int(sequence_value)
            except (TypeError, ValueError) as exc:
                raise ReplayValidationError(
                    f"Invalid sequence value for event {event.event_id}: {sequence_value!r}"
                ) from exc

            if previous_sequence is not None and sequence <= previous_sequence:
                raise ReplayValidationError(
                    f"Out-of-order sequence detected at index {index}: {sequence} <= {previous_sequence}"
                )
            previous_sequence = sequence

        if previous_timestamp is not None and event.timestamp < previous_timestamp:
            raise ReplayValidationError(
                f"Out-of-order timestamp detected at index {index}: {event.timestamp.isoformat()}"
            )
        previous_timestamp = event.timestamp

    return normalized


def _coerce_event(event: Mapping[str, Any] | Any) -> ReplayEvent:
    if isinstance(event, Mapping):
        raw = dict(event)
        event_id = str(raw.get("event_id") or raw.get("id") or "").strip()
        event_type = str(raw.get("event_type") or raw.get("type") or "").strip()
        timestamp = _coerce_timestamp(raw.get("timestamp"))
        household_id = str(raw.get("household_id") or "").strip()
        payload_raw = raw.get("payload")
        source = str(raw.get("source") or "unknown")
    else:
        event_id = str(getattr(event, "event_id", getattr(event, "id", "")) or "").strip()
        event_type = str(getattr(event, "event_type", getattr(event, "type", "")) or "").strip()
        timestamp = _coerce_timestamp(getattr(event, "timestamp", None))
        household_id = str(getattr(event, "household_id", "") or "").strip()
        payload_raw = getattr(event, "payload", None)
        source = str(getattr(event, "source", "unknown") or "unknown")

    if not event_id:
        raise ReplayValidationError("Event missing event_id")
    if not event_type:
        raise ReplayValidationError(f"Event {event_id} missing event_type")
    if not household_id:
        raise ReplayValidationError(f"Event {event_id} missing household_id")
    if not isinstance(payload_raw, Mapping):
        raise ReplayValidationError(f"Event {event_id} payload must be an object")

    return ReplayEvent(
        event_id=event_id,
        event_type=event_type,
        timestamp=timestamp,
        household_id=household_id,
        payload=dict(payload_raw),
        source=source,
    )


def _coerce_timestamp(raw_timestamp: Any) -> datetime:
    if isinstance(raw_timestamp, datetime):
        if raw_timestamp.tzinfo is None:
            return raw_timestamp.replace(tzinfo=UTC)
        return raw_timestamp.astimezone(UTC)

    if isinstance(raw_timestamp, str) and raw_timestamp.strip():
        normalized = raw_timestamp.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ReplayValidationError(f"Invalid timestamp format: {raw_timestamp!r}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    raise ReplayValidationError("Event missing timestamp")


def _validate_required_predecessors(events: list[ReplayEvent]) -> None:
    request_events: dict[str, set[str]] = {}
    approved_actions: set[str] = set()
    created_tasks: set[str] = set()
    completed_tasks: set[str] = set()
    created_schedules: set[str] = set()
    cancelled_schedules: set[str] = set()
    created_reminders: set[str] = set()
    cancelled_reminders: set[str] = set()
    triggered_reminders: set[str] = set()
    completed_decisions: set[str] = set()
    ignored_decisions: set[str] = set()
    deferred_decision_events: set[str] = set()
    ingested_message_ids: set[str] = set()
    promoted_message_ids: set[str] = set()
    started_sagas: set[str] = set()
    started_saga_steps: set[tuple[str, str]] = set()
    open_circuits: set[tuple[str, str]] = set()
    decision_cards_state: dict[str, dict[str, Any]] = {}
    surface_validation_state: dict[str, Any] = {}

    for event in events:
        request_id = _request_id(event)
        try:
            decision_cards_state = reduce_decision_card_projection(
                event_type=event.event_type,
                payload=event.payload,
                recorded_at=event.timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
                decision_cards=decision_cards_state,
                strict=True,
            )
        except DecisionCardInvariantError as exc:
            raise ReplayValidationError(
                f"Event {event.event_id} ({event.event_type}) {str(exc)}"
            ) from exc

        if request_id:
            request_events.setdefault(request_id, set()).add(event.event_type)

        saga_id = str(event.payload.get("saga_id") or "").strip()
        step_id = str(event.payload.get("step_id") or "").strip()

        if event.event_type == "saga.started":
            if saga_id:
                started_sagas.add(saga_id)

        if event.event_type == "saga.step_started":
            if not saga_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing saga_id"
                )
            if saga_id not in started_sagas:
                raise ReplayValidationError(
                    f"Missing prerequisite saga.started for saga_id={saga_id}"
                )
            if not step_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing step_id"
                )
            started_saga_steps.add((saga_id, step_id))

        if event.event_type in {"saga.step_succeeded", "saga.step_failed"}:
            if not saga_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing saga_id"
                )
            if saga_id not in started_sagas:
                raise ReplayValidationError(
                    f"Missing prerequisite saga.started for saga_id={saga_id}"
                )
            if not step_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing step_id"
                )
            if (saga_id, step_id) not in started_saga_steps:
                raise ReplayValidationError(
                    f"Missing prerequisite saga.step_started for saga_id={saga_id}, step_id={step_id}"
                )

        if event.event_type in {"saga.compensation_applied", "saga.completed", "saga.compensated"}:
            if not saga_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing saga_id"
                )
            if saga_id not in started_sagas:
                raise ReplayValidationError(
                    f"Missing prerequisite saga.started for saga_id={saga_id}"
                )

        if event.event_type == "system.circuit_opened":
            breaker_id = str(event.payload.get("breaker_id") or "").strip()
            circuit_saga_type = str(event.payload.get("saga_type") or "").strip()
            if not breaker_id or not circuit_saga_type:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing breaker_id or saga_type"
                )
            open_circuits.add((breaker_id, circuit_saga_type))

        if event.event_type == "system.circuit_closed":
            breaker_id = str(event.payload.get("breaker_id") or "").strip()
            circuit_saga_type = str(event.payload.get("saga_type") or "").strip()
            if not breaker_id or not circuit_saga_type:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing breaker_id or saga_type"
                )
            circuit_key = (breaker_id, circuit_saga_type)
            if circuit_key not in open_circuits:
                raise ReplayValidationError(
                    f"Missing prerequisite system.circuit_opened for breaker_id={breaker_id}, saga_type={circuit_saga_type}"
                )
            open_circuits.remove(circuit_key)

        if event.event_type == "task.risk_assessed":
            _require_request_event(request_events, request_id, "task.rules_evaluated", event)

        if event.event_type == "task.fsm_transitioned":
            _require_request_event(request_events, request_id, "task.risk_assessed", event)

        if event.event_type in {"task.created", "TaskCreated"}:
            _require_request_event(request_events, request_id, "task.fsm_transitioned", event)
            task_payload = event.payload.get("task")
            task_id = ""
            if isinstance(task_payload, Mapping):
                task_id = str(task_payload.get("task_id") or "").strip()
            if not task_id:
                task_id = str(event.payload.get("task_id") or "").strip()
            if not task_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing task_id"
                )
            created_tasks.add(task_id)

        if event.event_type == "TaskCompleted":
            task_id = str(event.payload.get("task_id") or "").strip()
            if not task_id:
                task_payload = event.payload.get("task")
                if isinstance(task_payload, Mapping):
                    task_id = str(task_payload.get("task_id") or "").strip()
            if not task_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing task_id"
                )
            if task_id not in created_tasks:
                raise ReplayValidationError(
                    f"Missing prerequisite task.created for task_id={task_id}"
                )
            if task_id in completed_tasks:
                raise ReplayValidationError(
                    f"Duplicate TaskCompleted for task_id={task_id}"
                )
            completed_tasks.add(task_id)

        if event.event_type == "ScheduleCreated":
            schedule_id = str(event.payload.get("schedule_id") or "").strip()
            if not schedule_id:
                schedule_payload = event.payload.get("schedule")
                if isinstance(schedule_payload, Mapping):
                    schedule_id = str(schedule_payload.get("schedule_id") or "").strip()
            if not schedule_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing schedule_id"
                )
            if schedule_id in created_schedules:
                raise ReplayValidationError(
                    f"Duplicate ScheduleCreated for schedule_id={schedule_id}"
                )
            created_schedules.add(schedule_id)

        if event.event_type == "ScheduleCancelled":
            schedule_id = str(event.payload.get("schedule_id") or "").strip()
            if not schedule_id:
                schedule_payload = event.payload.get("schedule")
                if isinstance(schedule_payload, Mapping):
                    schedule_id = str(schedule_payload.get("schedule_id") or "").strip()
            if not schedule_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing schedule_id"
                )
            if schedule_id not in created_schedules:
                raise ReplayValidationError(
                    f"Missing prerequisite ScheduleCreated for schedule_id={schedule_id}"
                )
            if schedule_id in cancelled_schedules:
                raise ReplayValidationError(
                    f"Duplicate ScheduleCancelled for schedule_id={schedule_id}"
                )
            cancelled_schedules.add(schedule_id)

        if event.event_type == "ReminderCreated":
            reminder_id = str(event.payload.get("reminder_id") or "").strip()
            if not reminder_id:
                reminder_payload = event.payload.get("reminder")
                if isinstance(reminder_payload, Mapping):
                    reminder_id = str(reminder_payload.get("reminder_id") or "").strip()
            if not reminder_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing reminder_id"
                )
            if reminder_id in created_reminders:
                raise ReplayValidationError(
                    f"Duplicate ReminderCreated for reminder_id={reminder_id}"
                )
            created_reminders.add(reminder_id)

        if event.event_type == "ReminderCancelled":
            reminder_id = str(event.payload.get("reminder_id") or "").strip()
            if not reminder_id:
                reminder_payload = event.payload.get("reminder")
                if isinstance(reminder_payload, Mapping):
                    reminder_id = str(reminder_payload.get("reminder_id") or "").strip()
            if not reminder_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing reminder_id"
                )
            if reminder_id not in created_reminders:
                raise ReplayValidationError(
                    f"Missing prerequisite ReminderCreated for reminder_id={reminder_id}"
                )
            if reminder_id in triggered_reminders:
                raise ReplayValidationError(
                    f"Cannot cancel triggered reminder_id={reminder_id}"
                )
            if reminder_id in cancelled_reminders:
                raise ReplayValidationError(
                    f"Duplicate ReminderCancelled for reminder_id={reminder_id}"
                )
            cancelled_reminders.add(reminder_id)

        if event.event_type == "ReminderTriggered":
            reminder_id = str(event.payload.get("reminder_id") or "").strip()
            if not reminder_id:
                reminder_payload = event.payload.get("reminder")
                if isinstance(reminder_payload, Mapping):
                    reminder_id = str(reminder_payload.get("reminder_id") or "").strip()
            if not reminder_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing reminder_id"
                )
            if reminder_id not in created_reminders:
                raise ReplayValidationError(
                    f"Missing prerequisite ReminderCreated for reminder_id={reminder_id}"
                )
            if reminder_id in cancelled_reminders:
                raise ReplayValidationError(
                    f"Cannot trigger cancelled reminder_id={reminder_id}"
                )
            if reminder_id in triggered_reminders:
                raise ReplayValidationError(
                    f"Duplicate ReminderTriggered for reminder_id={reminder_id}"
                )
            triggered_reminders.add(reminder_id)

        if event.event_type == "DecisionCompleted":
            decision_id = str(event.payload.get("decision_id") or "").strip()
            if not decision_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing decision_id"
                )
            if decision_id in completed_decisions:
                raise ReplayValidationError(
                    f"Duplicate DecisionCompleted for decision_id={decision_id}"
                )
            completed_decisions.add(decision_id)

        if event.event_type == "DecisionDeferred":
            decision_id = str(event.payload.get("decision_id") or "").strip()
            defer_to_date = str(event.payload.get("defer_to_date") or "").strip()
            if not decision_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing decision_id"
                )
            if not defer_to_date:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing defer_to_date"
                )
            duplicate_key = f"{decision_id}:{defer_to_date}"
            if duplicate_key in deferred_decision_events:
                raise ReplayValidationError(
                    f"Duplicate DecisionDeferred for decision_id={decision_id}, defer_to_date={defer_to_date}"
                )
            deferred_decision_events.add(duplicate_key)

        if event.event_type == "DecisionIgnored":
            decision_id = str(event.payload.get("decision_id") or "").strip()
            if not decision_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing decision_id"
                )
            if decision_id in ignored_decisions:
                raise ReplayValidationError(
                    f"Duplicate DecisionIgnored for decision_id={decision_id}"
                )
            ignored_decisions.add(decision_id)

        if event.event_type == "household_message_ingested":
            message_id = str(event.payload.get("message_id") or "").strip()
            if not message_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing message_id"
                )
            if message_id in ingested_message_ids:
                raise ReplayValidationError(
                    f"Duplicate household_message_ingested for message_id={message_id}"
                )
            ingested_message_ids.add(message_id)

        if event.event_type == "household_item_promoted":
            source_message_id = str(event.payload.get("source_message_id") or "").strip()
            interpretation_type = str(event.payload.get("interpretation_type") or "").strip()
            promotion_reason = str(event.payload.get("promotion_reason") or "").strip()
            promotion_target = str(event.payload.get("promotion_target") or "").strip()
            if not source_message_id:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing source_message_id"
                )
            if not interpretation_type:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing interpretation_type"
                )
            if not promotion_reason:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing promotion_reason"
                )
            if not promotion_target:
                raise ReplayValidationError(
                    f"Event {event.event_id} ({event.event_type}) missing promotion_target"
                )
            if source_message_id not in ingested_message_ids:
                raise ReplayValidationError(
                    f"Missing prerequisite household_message_ingested for source_message_id={source_message_id}"
                )
            if source_message_id in promoted_message_ids:
                raise ReplayValidationError(
                    f"Duplicate household_item_promoted for source_message_id={source_message_id}"
                )
            promoted_message_ids.add(source_message_id)

        surface_reducer_resolution = _resolve_registered_surface_reducer(event_type=event.event_type)
        if surface_reducer_resolution is not None:
            surface_id, reducer_registration = surface_reducer_resolution
            validator = reducer_registration.validator
            if callable(validator):
                try:
                    surface_validation_state[surface_id] = validator(
                        event_payload=event.payload,
                        validation_state=surface_validation_state.get(surface_id),
                    )
                except ValueError as exc:
                    raise ReplayValidationError(
                        f"Event {event.event_id} ({event.event_type}) {str(exc)}"
                    ) from exc

        if event.event_type == "assistant.action_approved":
            _require_request_event(request_events, request_id, "assistant.response_proposed", event)
            action_ids = [str(item) for item in event.payload.get("action_ids") or [] if str(item).strip()]
            approved_actions.update(action_ids)

        if event.event_type == "assistant.action_executed":
            action_id = str(event.payload.get("action_id") or "").strip()
            if action_id and action_id not in approved_actions:
                raise ReplayValidationError(
                    f"Missing prerequisite assistant.action_approved for action_id={action_id}"
                )


def _require_request_event(
    request_events: Mapping[str, set[str]],
    request_id: str,
    required_event_type: str,
    current_event: ReplayEvent,
) -> None:
    if not request_id:
        raise ReplayValidationError(
            f"Event {current_event.event_id} ({current_event.event_type}) missing request_id"
        )

    seen = request_events.get(request_id, set())
    if required_event_type not in seen:
        raise ReplayValidationError(
            f"Missing prerequisite {required_event_type} for request_id={request_id}"
        )


def _request_id(event: ReplayEvent) -> str:
    raw = event.payload.get("request_id")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    nested = event.payload.get("response")
    if isinstance(nested, Mapping):
        nested_request = nested.get("request_id")
        if isinstance(nested_request, str) and nested_request.strip():
            return nested_request.strip()
    return ""


def _resolve_registered_surface_reducer(
    *,
    event_type: str,
) -> tuple[str, Any] | None:
    surface_id = resolve_surface_id_for_event(event_type)
    if not surface_id:
        return None

    registration = resolve_reducer(surface_id, event_type)
    if registration is not None:
        return surface_id, registration
    return None


def _iter_registered_surface_projection_builders() -> tuple[tuple[str, str, Any], ...]:
    projection_builders: list[tuple[str, str, Any]] = []
    for surface_descriptor in iter_surfaces():
        projection_map = resolve_projection(surface_descriptor.id)
        for projection_name, projection_builder in projection_map.items():
            projection_builders.append((surface_descriptor.id, projection_name, projection_builder))
    return tuple(projection_builders)


def _project_from_normalized(events: list[ReplayEvent]) -> dict[str, Any]:
    household_id = events[-1].household_id if events else ""
    projection: dict[str, Any] = {
        "household_id": household_id,
        "responses": {},
        "actions": {},
        "tasks": {},
        "tasks_list": [],
        "schedules": {},
        "schedule_list": [],
        "reminders": {},
        "reminder_list": [],
        "notifications": {},
        "notification_list": [],
        "ingested_emails": [],
        "email_actions": [],
        "calendar_events": [],
        "calendar_conflicts": [],
        "household_messages": [],
        "household_promotions": [],
        "decisions": {},
        "decision_cards": {},
        "task_transition_log": {},
        "sagas": {},
        "control_plane": {
            "circuits": {},
            "throttled_sagas": {},
            "halted_sagas": {},
        },
        "policy_bindings": {
            "policy_versions": {},
            "missing_policy_reference": [],
            "event_count_with_policy": 0,
        },
        "events": [],
        "pending_actions": [],
        "last_recommendation": None,
        "state_version": len(events),
        "last_event_id": events[-1].event_id if events else "",
        "checksum": "",
        "drift": empty_drift_classification(),
        "drift_reasons": empty_drift_reasons(),
    }

    responses: dict[str, dict[str, Any]] = {}
    actions: dict[str, dict[str, Any]] = {}
    tasks: dict[str, dict[str, Any]] = {}
    schedules: dict[str, dict[str, Any]] = {}
    reminders: dict[str, dict[str, Any]] = {}
    notifications: dict[str, dict[str, Any]] = {}
    ingested_emails: list[dict[str, Any]] = []
    email_actions: list[dict[str, Any]] = []
    calendar_events: list[dict[str, Any]] = []
    calendar_conflicts: list[dict[str, Any]] = []
    household_messages: list[dict[str, Any]] = []
    household_promotions: list[dict[str, Any]] = []
    decisions: dict[str, dict[str, Any]] = {}
    decision_cards: dict[str, dict[str, Any]] = {}
    task_transition_log: dict[str, list[dict[str, Any]]] = {}
    sagas: dict[str, dict[str, Any]] = {}
    control_plane: dict[str, Any] = {
        "circuits": {},
        "throttled_sagas": {},
        "halted_sagas": {},
    }
    policy_versions: dict[str, dict[str, Any]] = {}
    missing_policy_reference: list[str] = []
    runtime_events: list[dict[str, Any]] = []
    latest_projection_checksum: str | None = None
    surface_state_by_id: dict[str, Any] = {}

    for event in events:
        payload = event.payload
        recorded_at = event.timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z")

        try:
            decision_cards = reduce_decision_card_projection(
                event_type=event.event_type,
                payload=payload,
                recorded_at=recorded_at,
                decision_cards=decision_cards,
                strict=True,
            )
        except DecisionCardInvariantError as exc:
            raise ReplayValidationError(
                f"Event {event.event_id} ({event.event_type}) {str(exc)}"
            ) from exc

        policy_version_id = str(payload.get("policy_version_id") or "").strip()
        evaluation_context_hash = str(payload.get("evaluation_context_hash") or "").strip()
        if policy_version_id and evaluation_context_hash:
            version_row = policy_versions.setdefault(
                policy_version_id,
                {
                    "event_count": 0,
                    "evaluation_context_hashes": set(),
                },
            )
            version_row["event_count"] += 1
            version_row["evaluation_context_hashes"].add(evaluation_context_hash)
        else:
            missing_policy_reference.append(event.event_id)

        if event.event_type == "assistant.response_proposed":
            response = payload.get("response")
            if isinstance(response, Mapping):
                request_id = str(response.get("request_id") or payload.get("request_id") or "").strip()
                if request_id:
                    response_dict = dict(response)
                    responses[request_id] = response_dict
                    recommended = response_dict.get("recommended_action")
                    if isinstance(recommended, Mapping):
                        action_id = str(recommended.get("action_id") or "").strip()
                        if action_id:
                            actions[action_id] = {
                                "request_id": request_id,
                                "approval_status": str(recommended.get("approval_status") or "pending"),
                                "approval_required": bool(recommended.get("approval_required", True)),
                                "title": str(recommended.get("title") or ""),
                            }
                    projection["last_recommendation"] = response_dict

        elif event.event_type == "assistant.action_approved":
            request_id = str(payload.get("request_id") or "").strip()
            for action_id in [str(item) for item in payload.get("action_ids") or [] if str(item).strip()]:
                action = actions.setdefault(action_id, {"request_id": request_id})
                action["approval_status"] = "approved"
                if request_id and request_id in responses:
                    response_row = responses[request_id]
                    recommended = dict(response_row.get("recommended_action") or {})
                    if str(recommended.get("action_id") or "") == action_id:
                        recommended["approval_status"] = "approved"
                        response_row["recommended_action"] = recommended
                        grouped = dict(response_row.get("grouped_approval_payload") or {})
                        grouped["approval_status"] = "approved"
                        response_row["grouped_approval_payload"] = grouped
                        response_row["reasoning_trace"] = list(response_row.get("reasoning_trace") or []) + [
                            "Approval recorded via command pipeline.",
                        ]

        elif event.event_type == "assistant.action_rejected":
            action_id = str(payload.get("action_id") or "").strip()
            request_id = str(payload.get("request_id") or "").strip()
            if action_id:
                action = actions.setdefault(action_id, {"request_id": request_id})
                action["approval_status"] = "rejected"
            if request_id and request_id in responses:
                response_row = responses[request_id]
                response_row["reasoning_trace"] = list(response_row.get("reasoning_trace") or []) + [
                    "Action rejected by user decision.",
                ]

        elif event.event_type == "assistant.action_executed":
            action_id = str(payload.get("action_id") or "").strip()
            if action_id:
                action = actions.setdefault(action_id, {"request_id": str(payload.get("request_id") or "")})
                action["approval_status"] = "committed"
                effect = payload.get("effect")
                if isinstance(effect, Mapping):
                    action["effect"] = dict(effect)
            event_row = payload.get("event")
            if isinstance(event_row, Mapping):
                runtime_events.append(dict(event_row))

        elif event.event_type == "email.ingested":
            email_payload = payload.get("email")
            if isinstance(email_payload, Mapping):
                ingested_emails.append(
                    {
                        "email_id": str(email_payload.get("email_id") or email_payload.get("id") or "").strip(),
                        "subject": str(email_payload.get("subject") or "").strip(),
                        "from": str(email_payload.get("from") or email_payload.get("sender") or "").strip(),
                        "received_at": str(email_payload.get("received_at") or "").strip(),
                    }
                )

            raw_actions = payload.get("action_items")
            if isinstance(raw_actions, list):
                for item in raw_actions:
                    if not isinstance(item, Mapping):
                        continue
                    email_actions.append(
                        {
                            "action_id": str(item.get("action_id") or "").strip(),
                            "title": str(item.get("title") or "").strip(),
                            "due_date": str(item.get("due_date") or "").strip(),
                            "email_id": str(item.get("email_id") or "").strip(),
                        }
                    )

        elif event.event_type == "calendar.ingested":
            raw_events = payload.get("events")
            if isinstance(raw_events, list):
                for item in raw_events:
                    if not isinstance(item, Mapping):
                        continue
                    calendar_events.append(
                        {
                            "event_id": str(item.get("event_id") or item.get("id") or "").strip(),
                            "title": str(item.get("title") or "").strip(),
                            "start_at": str(item.get("start_at") or "").strip(),
                            "end_at": str(item.get("end_at") or "").strip(),
                            "source": str(item.get("source") or "").strip(),
                        }
                    )

            raw_conflicts = payload.get("conflicts")
            if isinstance(raw_conflicts, list):
                for item in raw_conflicts:
                    if not isinstance(item, Mapping):
                        continue
                    calendar_conflicts.append(
                        {
                            "conflict_id": str(item.get("conflict_id") or "").strip(),
                            "left_event_id": str(item.get("left_event_id") or "").strip(),
                            "right_event_id": str(item.get("right_event_id") or "").strip(),
                            "left_title": str(item.get("left_title") or "").strip(),
                            "right_title": str(item.get("right_title") or "").strip(),
                            "start_at": str(item.get("start_at") or "").strip(),
                            "end_at": str(item.get("end_at") or "").strip(),
                        }
                    )

        elif event.event_type == "household_message_ingested":
            household_messages.append(
                {
                    "message_id": str(payload.get("message_id") or "").strip(),
                    "source": str(payload.get("source") or "").strip(),
                    "raw_content": str(payload.get("raw_content") or "").strip(),
                    "created_at": str(payload.get("created_at") or "").strip(),
                    "member_id": str(payload.get("member_id") or "").strip(),
                    "classification": payload.get("classification"),
                }
            )

        elif event.event_type == "household_item_promoted":
            household_promotions.append(
                {
                    "source_message_id": str(payload.get("source_message_id") or "").strip(),
                    "classification": str(payload.get("classification") or "").strip(),
                    "interpretation_type": str(payload.get("interpretation_type") or "").strip(),
                    "interpretation_confidence": payload.get("interpretation_confidence"),
                    "promotion_target": str(payload.get("promotion_target") or "").strip(),
                    "promotion_reason": str(payload.get("promotion_reason") or "").strip(),
                    "promotion_status": str(payload.get("promotion_status") or "").strip(),
                    "promoted_entity_type": str(payload.get("promoted_entity_type") or "").strip(),
                    "promoted_entity_id": str(payload.get("promoted_entity_id") or "").strip(),
                    "dependency_schedule_id": str(payload.get("dependency_schedule_id") or "").strip(),
                    "conflict_schedule_id": str(payload.get("conflict_schedule_id") or "").strip(),
                    "secondary_entity_type": str(payload.get("secondary_entity_type") or "").strip(),
                    "secondary_entity_id": str(payload.get("secondary_entity_id") or "").strip(),
                    "member_id": str(payload.get("member_id") or "").strip(),
                    "promoted_at": str(payload.get("promoted_at") or "").strip(),
                }
            )

        elif event.event_type == "task.fsm_transitioned":
            task_id = str(payload.get("task_id") or "").strip()
            transitions = payload.get("transitions")
            if task_id and isinstance(transitions, list):
                task_transition_log[task_id] = [dict(item) for item in transitions if isinstance(item, Mapping)]

        elif event.event_type in {"task.created", "TaskCreated"}:
            recorded_at = event.timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z")
            tasks, actions, responses, response = apply_task_created_projection(
                payload=payload,
                recorded_at=recorded_at,
                tasks=tasks,
                actions=actions,
                responses=responses,
            )
            if response is not None:
                projection["last_recommendation"] = response

        elif event.event_type == "TaskCompleted":
            tasks, actions, responses, response = apply_task_completed_projection(
                payload=payload,
                recorded_at=recorded_at,
                tasks=tasks,
                actions=actions,
                responses=responses,
            )
            if response is not None:
                projection["last_recommendation"] = response

        elif event.event_type == "ScheduleCreated":
            schedules, responses = apply_schedule_created_projection(
                payload=payload,
                recorded_at=recorded_at,
                schedules=schedules,
                responses=responses,
            )

        elif event.event_type == "ScheduleCancelled":
            schedules, responses = apply_schedule_cancelled_projection(
                payload=payload,
                recorded_at=recorded_at,
                schedules=schedules,
                responses=responses,
            )

        elif event.event_type == "ReminderCreated":
            reminders, responses = apply_reminder_created_projection(
                payload=payload,
                recorded_at=recorded_at,
                reminders=reminders,
                responses=responses,
            )

        elif event.event_type == "ReminderCancelled":
            reminders, responses = apply_reminder_cancelled_projection(
                payload=payload,
                reminders=reminders,
                responses=responses,
            )

        elif event.event_type == "ReminderTriggered":
            reminders, responses = apply_reminder_triggered_projection(
                payload=payload,
                recorded_at=recorded_at,
                reminders=reminders,
                responses=responses,
            )
            notifications, _ = derive_notification_from_reminder_triggered(
                payload=payload,
                source_event_id=event.event_id,
                recorded_at=recorded_at,
                notifications=notifications,
            )

        elif event.event_type == "DecisionCompleted":
            decisions, tasks, schedules, reminders, actions = apply_decision_completed_projection(
                payload=payload,
                recorded_at=recorded_at,
                decisions=decisions,
                tasks=tasks,
                schedules=schedules,
                reminders=reminders,
                actions=actions,
            )

        elif event.event_type == "DecisionDeferred":
            decisions, tasks, schedules, reminders, actions = apply_decision_deferred_projection(
                payload=payload,
                recorded_at=recorded_at,
                decisions=decisions,
                tasks=tasks,
                schedules=schedules,
                reminders=reminders,
                actions=actions,
            )

        elif event.event_type == "DecisionIgnored":
            decisions, tasks, schedules, reminders, actions = apply_decision_ignored_projection(
                payload=payload,
                recorded_at=recorded_at,
                decisions=decisions,
                tasks=tasks,
                schedules=schedules,
                reminders=reminders,
                actions=actions,
            )

        elif (surface_reducer_resolution := _resolve_registered_surface_reducer(event_type=event.event_type)) is not None:
            surface_id, reducer_registration = surface_reducer_resolution
            reduced_surface_state, responses = reducer_registration.reducer(
                payload=payload,
                recorded_at=recorded_at,
                surface_state=surface_state_by_id.get(surface_id),
                responses=responses,
            )
            surface_state_by_id[surface_id] = reduced_surface_state

        elif event.event_type == "projection.snapshot":
            checksum = payload.get("checksum")
            if isinstance(checksum, str) and checksum.strip():
                latest_projection_checksum = checksum.strip()

        elif event.event_type == "saga.started":
            saga_id = str(payload.get("saga_id") or "").strip()
            if saga_id:
                sagas[saga_id] = {
                    "status": "running",
                    "request_id": str(payload.get("request_id") or ""),
                    "executed_steps": [],
                    "failed_step": None,
                    "compensated_steps": [],
                }

        elif event.event_type == "saga.step_succeeded":
            saga_id = str(payload.get("saga_id") or "").strip()
            step_id = str(payload.get("step_id") or "").strip()
            if saga_id:
                saga_row = sagas.setdefault(
                    saga_id,
                    {
                        "status": "running",
                        "request_id": str(payload.get("request_id") or ""),
                        "executed_steps": [],
                        "failed_step": None,
                        "compensated_steps": [],
                    },
                )
                if step_id and step_id not in saga_row["executed_steps"]:
                    saga_row["executed_steps"].append(step_id)

        elif event.event_type == "saga.step_failed":
            saga_id = str(payload.get("saga_id") or "").strip()
            step_id = str(payload.get("step_id") or "").strip()
            if saga_id:
                saga_row = sagas.setdefault(
                    saga_id,
                    {
                        "status": "failed",
                        "request_id": str(payload.get("request_id") or ""),
                        "executed_steps": [],
                        "failed_step": None,
                        "compensated_steps": [],
                    },
                )
                saga_row["status"] = "failed"
                saga_row["failed_step"] = step_id or None

        elif event.event_type == "saga.compensation_applied":
            saga_id = str(payload.get("saga_id") or "").strip()
            step_id = str(payload.get("step_id") or "").strip()
            if saga_id:
                saga_row = sagas.setdefault(
                    saga_id,
                    {
                        "status": "compensating",
                        "request_id": str(payload.get("request_id") or ""),
                        "executed_steps": [],
                        "failed_step": None,
                        "compensated_steps": [],
                    },
                )
                if step_id and step_id not in saga_row["compensated_steps"]:
                    saga_row["compensated_steps"].append(step_id)

        elif event.event_type == "saga.completed":
            saga_id = str(payload.get("saga_id") or "").strip()
            if saga_id:
                saga_row = sagas.setdefault(
                    saga_id,
                    {
                        "status": "completed",
                        "request_id": str(payload.get("request_id") or ""),
                        "executed_steps": [],
                        "failed_step": None,
                        "compensated_steps": [],
                    },
                )
                saga_row["status"] = "completed"

        elif event.event_type == "saga.compensated":
            saga_id = str(payload.get("saga_id") or "").strip()
            if saga_id:
                saga_row = sagas.setdefault(
                    saga_id,
                    {
                        "status": "compensated",
                        "request_id": str(payload.get("request_id") or ""),
                        "executed_steps": [],
                        "failed_step": None,
                        "compensated_steps": [],
                    },
                )
                saga_row["status"] = "compensated"

        elif event.event_type == "saga.halted":
            saga_id = str(payload.get("saga_id") or "").strip()
            if saga_id:
                control_plane["halted_sagas"][saga_id] = {
                    "saga_id": saga_id,
                    "request_id": str(payload.get("request_id") or ""),
                    "reason": str(payload.get("reason") or ""),
                }

        elif event.event_type == "saga.throttled":
            saga_id = str(payload.get("saga_id") or "").strip()
            if saga_id:
                control_plane["throttled_sagas"][saga_id] = {
                    "saga_id": saga_id,
                    "request_id": str(payload.get("request_id") or ""),
                    "reason": str(payload.get("reason") or ""),
                }

        elif event.event_type == "system.circuit_opened":
            breaker_id = str(payload.get("breaker_id") or "").strip()
            saga_type = str(payload.get("saga_type") or "").strip()
            if breaker_id and saga_type:
                circuit_key = f"{breaker_id}:{saga_type}"
                control_plane["circuits"][circuit_key] = {
                    "breaker_id": breaker_id,
                    "saga_type": saga_type,
                    "status": "open",
                    "request_id": str(payload.get("request_id") or ""),
                    "reason": str(payload.get("reason") or ""),
                    "failure_count_recent": int(payload.get("failure_count_recent") or 0),
                }

        elif event.event_type == "system.circuit_closed":
            breaker_id = str(payload.get("breaker_id") or "").strip()
            saga_type = str(payload.get("saga_type") or "").strip()
            if breaker_id and saga_type:
                circuit_key = f"{breaker_id}:{saga_type}"
                control_plane["circuits"][circuit_key] = {
                    "breaker_id": breaker_id,
                    "saga_type": saga_type,
                    "status": "closed",
                    "request_id": str(payload.get("request_id") or ""),
                    "reason": str(payload.get("reason") or ""),
                    "recovery_success_count_recent": int(payload.get("recovery_success_count_recent") or 0),
                }

    pending_actions = [
        {
            "action_id": action_id,
            "request_id": str(action_payload.get("request_id") or ""),
            "approval_status": str(action_payload.get("approval_status") or "pending"),
            "title": str(action_payload.get("title") or ""),
        }
        for action_id, action_payload in actions.items()
        if str(action_payload.get("approval_status") or "pending") == "pending"
    ]
    tasks_list = [
        dict(task_payload)
        for _task_id, task_payload in sorted(tasks.items(), key=lambda item: item[0])
    ]
    schedule_list = [
        dict(schedule_payload)
        for _schedule_id, schedule_payload in sorted(schedules.items(), key=lambda item: item[0])
    ]
    reminder_list = [
        dict(reminder_payload)
        for _reminder_id, reminder_payload in sorted(reminders.items(), key=lambda item: item[0])
    ]
    notification_list = [
        dict(notification_payload)
        for _notification_id, notification_payload in sorted(notifications.items(), key=lambda item: item[0])
    ]

    projection.update(
        {
            "responses": responses,
            "actions": actions,
            "tasks": tasks,
            "tasks_list": tasks_list,
            "schedules": schedules,
            "schedule_list": schedule_list,
            "reminders": reminders,
            "reminder_list": reminder_list,
            "notifications": notifications,
            "notification_list": notification_list,
            "ingested_emails": sorted(ingested_emails, key=lambda row: (str(row.get("received_at") or ""), str(row.get("email_id") or ""))),
            "email_actions": sorted(email_actions, key=lambda row: (str(row.get("due_date") or ""), str(row.get("action_id") or ""), str(row.get("title") or ""))),
            "calendar_events": sorted(calendar_events, key=lambda row: (str(row.get("start_at") or ""), str(row.get("end_at") or ""), str(row.get("event_id") or ""))),
            "calendar_conflicts": sorted(calendar_conflicts, key=lambda row: (str(row.get("start_at") or ""), str(row.get("end_at") or ""), str(row.get("conflict_id") or ""))),
            "household_messages": sorted(
                household_messages,
                key=lambda row: (
                    str(row.get("created_at") or ""),
                    str(row.get("message_id") or ""),
                ),
            ),
            "household_promotions": sorted(
                household_promotions,
                key=lambda row: (
                    str(row.get("promoted_at") or ""),
                    str(row.get("source_message_id") or ""),
                ),
            ),
            "decisions": decisions,
            "decision_cards": decision_cards,
            "task_transition_log": task_transition_log,
            "sagas": sagas,
            "control_plane": control_plane,
            "policy_bindings": {
                "policy_versions": {
                    version_id: {
                        "event_count": int(version_payload.get("event_count") or 0),
                        "evaluation_context_hashes": sorted(
                            str(item)
                            for item in version_payload.get("evaluation_context_hashes") or set()
                            if str(item).strip()
                        ),
                    }
                    for version_id, version_payload in policy_versions.items()
                },
                "missing_policy_reference": sorted(set(missing_policy_reference)),
                "event_count_with_policy": sum(
                    int(version_payload.get("event_count") or 0)
                    for version_payload in policy_versions.values()
                ),
            },
            "events": runtime_events,
            "pending_actions": sorted(pending_actions, key=lambda item: item["action_id"]),
        }
    )

    reference_timestamp = (
        events[-1].timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z") if events else ""
    )
    for surface_id, _projection_name, projection_builder in _iter_registered_surface_projection_builders():
        if surface_id not in surface_state_by_id:
            continue

        resolved_projection = projection_builder(
            partition_id=household_id,
            surface_state=surface_state_by_id[surface_id],
            reference_timestamp=reference_timestamp,
        )
        if not isinstance(resolved_projection, Mapping):
            continue

        for projection_key, projection_value in resolved_projection.items():
            projection[str(projection_key)] = projection_value

    checksum_payload = {
        "responses": projection["responses"],
        "actions": projection["actions"],
        "tasks": projection["tasks"],
        "tasks_list": projection["tasks_list"],
        "schedules": projection["schedules"],
        "schedule_list": projection["schedule_list"],
        "reminders": projection["reminders"],
        "reminder_list": projection["reminder_list"],
        "notifications": projection["notifications"],
        "notification_list": projection["notification_list"],
        "ingested_emails": projection["ingested_emails"],
        "email_actions": projection["email_actions"],
        "calendar_events": projection["calendar_events"],
        "calendar_conflicts": projection["calendar_conflicts"],
        "household_messages": projection["household_messages"],
        "household_promotions": projection["household_promotions"],
        "decisions": projection["decisions"],
        "task_transition_log": projection["task_transition_log"],
        "sagas": projection["sagas"],
        "control_plane": projection["control_plane"],
        "policy_bindings": projection["policy_bindings"],
        "events": projection["events"],
        "state_version": projection["state_version"],
    }
    checksum_excluded_keys = {
        "household_id",
        "last_recommendation",
        "last_event_id",
        "checksum",
        "drift",
        "drift_reasons",
    }
    for projection_key in sorted(
        key
        for key in projection.keys()
        if key not in checksum_payload and key not in checksum_excluded_keys
    ):
        checksum_payload[projection_key] = projection[projection_key]

    computed_checksum = _stable_hash(checksum_payload)
    projection["checksum"] = computed_checksum
    projection_drift = empty_drift_classification()
    projection_drift_reasons = empty_drift_reasons()
    if latest_projection_checksum and latest_projection_checksum != computed_checksum:
        projection_drift["integrity"] = True
        projection_drift_reasons["integrity"].append("projection_checksum_mismatch")

    projection["drift"] = projection_drift
    projection["drift_reasons"] = projection_drift_reasons

    return projection


def _rebuild_fsm_from_normalized(events: list[ReplayEvent]) -> dict[str, Any]:
    action_states: dict[str, dict[str, Any]] = {}
    task_states: dict[str, dict[str, Any]] = {}

    for event in events:
        payload = event.payload

        if event.event_type == "assistant.response_proposed":
            response = payload.get("response")
            if isinstance(response, Mapping):
                recommended = response.get("recommended_action")
                if isinstance(recommended, Mapping):
                    action_id = str(recommended.get("action_id") or "").strip()
                    if action_id:
                        action_states[action_id] = {
                            "current_state": ActionState.PENDING_APPROVAL.value,
                            "request_id": str(response.get("request_id") or payload.get("request_id") or ""),
                            "transition_log": [
                                {
                                    "from_state": ActionState.PROPOSED.value,
                                    "to_state": ActionState.PENDING_APPROVAL.value,
                                    "event_type": event.event_type,
                                    "event_id": event.event_id,
                                }
                            ],
                        }

        elif event.event_type == "assistant.action_approved":
            for action_id in [str(item) for item in payload.get("action_ids") or [] if str(item).strip()]:
                _transition_action_state(action_states, action_id, ActionState.APPROVED, event)

        elif event.event_type == "assistant.action_executed":
            action_id = str(payload.get("action_id") or "").strip()
            if action_id:
                _transition_action_state(action_states, action_id, ActionState.COMMITTED, event)

        elif event.event_type == "assistant.action_rejected":
            action_id = str(payload.get("action_id") or "").strip()
            if action_id:
                _transition_action_state(action_states, action_id, ActionState.REJECTED, event)

        elif event.event_type == "task.fsm_transitioned":
            task_id = str(payload.get("task_id") or "").strip()
            transitions = payload.get("transitions")
            current_state = str(payload.get("current_state") or "").strip()
            if not task_id or not isinstance(transitions, list):
                raise ReplayValidationError(
                    f"Invalid task.fsm_transitioned payload for event_id={event.event_id}"
                )

            normalized_transitions = [dict(item) for item in transitions if isinstance(item, Mapping)]
            if not normalized_transitions:
                raise ReplayValidationError(
                    f"task.fsm_transitioned transitions missing for task_id={task_id}"
                )

            for index in range(1, len(normalized_transitions)):
                previous = normalized_transitions[index - 1]
                current = normalized_transitions[index]
                if str(previous.get("to_state") or "") != str(current.get("from_state") or ""):
                    raise ReplayValidationError(
                        f"Non-contiguous task transition sequence for task_id={task_id}"
                    )

            terminal_state = str(normalized_transitions[-1].get("to_state") or "").strip()
            if current_state and terminal_state != current_state:
                raise ReplayValidationError(
                    f"Task FSM current_state mismatch for task_id={task_id}: {current_state} != {terminal_state}"
                )

            task_states[task_id] = {
                "current_state": terminal_state,
                "transition_log": normalized_transitions,
                "request_id": str(payload.get("request_id") or ""),
            }

        elif event.event_type == "TaskCompleted":
            task_id = str(payload.get("task_id") or "").strip()
            if task_id:
                state_row = task_states.setdefault(
                    task_id,
                    {
                        "current_state": "created",
                        "transition_log": [],
                        "request_id": str(payload.get("request_id") or ""),
                    },
                )
                prior_state = str(state_row.get("current_state") or "created")
                transition_log = state_row.setdefault("transition_log", [])
                if isinstance(transition_log, list):
                    transition_log.append(
                        {
                            "from_state": prior_state,
                            "to_state": "completed",
                            "event_type": event.event_type,
                            "event_id": event.event_id,
                        }
                    )
                state_row["current_state"] = "completed"

    return {
        "actions": action_states,
        "tasks": task_states,
    }


def _transition_action_state(
    action_states: dict[str, dict[str, Any]],
    action_id: str,
    target_state: ActionState,
    event: ReplayEvent,
) -> None:
    state_row = action_states.setdefault(
        action_id,
        {
            "current_state": ActionState.PENDING_APPROVAL.value,
            "request_id": str(event.payload.get("request_id") or ""),
            "transition_log": [
                {
                    "from_state": ActionState.PROPOSED.value,
                    "to_state": ActionState.PENDING_APPROVAL.value,
                    "event_type": "synthetic.bootstrap",
                    "event_id": "",
                }
            ],
        },
    )

    current_state = ActionState(str(state_row.get("current_state") or ActionState.PENDING_APPROVAL.value))
    try:
        _validate_action_transition(
            from_state=current_state,
            to_state=target_state,
            context={"actor_type": "api_user", "requires_approval": current_state == ActionState.PROPOSED},
        )
    except (TransitionError, ValueError) as exc:
        raise ReplayValidationError(
            f"Invalid FSM transition for action_id={action_id}: {current_state.value} -> {target_state.value}"
        ) from exc

    state_row["transition_log"].append(
        {
            "from_state": current_state.value,
            "to_state": target_state.value,
            "event_type": event.event_type,
            "event_id": event.event_id,
        }
    )
    state_row["current_state"] = target_state.value


def _stable_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
