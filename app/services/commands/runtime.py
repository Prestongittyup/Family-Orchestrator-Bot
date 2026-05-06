from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date as date_value, datetime, timedelta
from threading import Lock
from typing import Any, Mapping, Sequence, cast
from uuid import uuid4

from app.services.agents import HeadAgent
from app.services.events import CanonicalRouterService, EventLogService
from app.services.rules_engine import evaluate_command
from app.services.commands.decision_actions import (
    handle_decision_complete,
    handle_decision_defer,
    handle_decision_ignore,
)
from core.control import ControlPlane
from core.health import empty_drift_classification, empty_drift_reasons
from core.policy import PolicyResolution, PolicyVersionRegistry, build_default_policy_registry
from decision_card_system.registry import (
    DECISION_CARD_CANONICAL_ORIGIN_API,
    DECISION_CARD_ACKNOWLEDGED_EVENT_CANONICAL,
    DECISION_CARD_APPLIED_EVENT_CANONICAL,
    DECISION_CARD_CONTRACT_VERSION,
    DECISION_CARD_GENERATED_EVENT_CANONICAL,
    DECISION_CARD_RESOLVED_EVENT_CANONICAL,
    DECISION_CARD_STATE_ACKNOWLEDGED,
    DECISION_CARD_STATE_APPLIED,
    DECISION_CARD_STATE_RESOLVED,
    DECISION_CARD_SURFACED_EVENT_CANONICAL,
    DecisionCardInvariantError,
    createDecisionCard,
    reduce_decision_card_projection,
)
from core.replay.event_replay_engine import project_state
from core.replay.surface_registry import resolve_command
from core.sagas import SagaDefinition, SagaOrchestrator, SagaStepDefinition
from household_os.core.contracts import (
    CurrentStateSummary,
    GroupedApprovalPayload,
    HouseholdOSRunResponse,
    IntentInterpretation,
    RecommendedNextAction,
)
from household_os.runtime.event_router import CanonicalEventEnvelope


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_iso(value: datetime | None = None) -> str:
    resolved = value or _utc_now()
    return resolved.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _clip01(value: float) -> float:
    if value <= 0.0:
        return 0.0
    if value >= 1.0:
        return 1.0
    return float(value)


TASK_CREATE_COMMAND_TYPES = frozenset({"task.create", "create_task", "task_created"})
TASK_COMPLETE_COMMAND_TYPES = frozenset({"task_completed"})
TASK_CREATED_EVENT_LEGACY = "task.created"
TASK_CREATED_EVENT_CANONICAL = "TaskCreated"
TASK_COMPLETED_EVENT_CANONICAL = "TaskCompleted"
TASK_CREATED_EVENT_TYPES = frozenset({TASK_CREATED_EVENT_LEGACY, TASK_CREATED_EVENT_CANONICAL})
SCHEDULE_CREATE_COMMAND_TYPES = frozenset({"schedule.create"})
SCHEDULE_CANCEL_COMMAND_TYPES = frozenset({"schedule.cancel"})
SCHEDULE_CREATED_EVENT_CANONICAL = "ScheduleCreated"
SCHEDULE_CANCELLED_EVENT_CANONICAL = "ScheduleCancelled"
REMINDER_CREATE_COMMAND_TYPES = frozenset({"reminder.create"})
REMINDER_CANCEL_COMMAND_TYPES = frozenset({"reminder.cancel"})
REMINDER_TRIGGER_COMMAND_TYPES = frozenset({"reminder.trigger"})
REMINDER_CREATED_EVENT_CANONICAL = "ReminderCreated"
REMINDER_CANCELLED_EVENT_CANONICAL = "ReminderCancelled"
REMINDER_TRIGGERED_EVENT_CANONICAL = "ReminderTriggered"
DECISION_COMPLETE_COMMAND_TYPES = frozenset({"decision.complete"})
DECISION_DEFER_COMMAND_TYPES = frozenset({"decision.defer"})
DECISION_IGNORE_COMMAND_TYPES = frozenset({"decision.ignore"})
HOUSEHOLD_MESSAGE_INGEST_COMMAND_TYPES = frozenset({"household.message.ingest", "household_message.ingest"})
DECISION_CARD_CREATE_COMMAND_TYPES = frozenset({"decision.card.create"})
DECISION_CARD_SURFACE_COMMAND_TYPES = frozenset({"decision.card.surface"})
DECISION_CARD_ACKNOWLEDGE_COMMAND_TYPES = frozenset({"decision.card.acknowledge"})
DECISION_CARD_RESOLVE_COMMAND_TYPES = frozenset({"decision.card.resolve"})
DECISION_COMPLETED_EVENT_CANONICAL = "DecisionCompleted"
DECISION_DEFERRED_EVENT_CANONICAL = "DecisionDeferred"
DECISION_IGNORED_EVENT_CANONICAL = "DecisionIgnored"
SAGA_EXECUTE_COMMAND_TYPES = frozenset({"saga.execute", "workflow.saga.execute"})
TASK_PRIORITY_VALUES = frozenset({"low", "medium", "high"})
DUPLICATE_SHORT_CIRCUIT_BYPASS_COMMAND_TYPES = frozenset(
    {
        "assistant.run",
        "assistant.query",
        "assistant.approve",
        "assistant.reject",
    }
)
RULE_FIRST_DOMAIN_COMMAND_TYPES = frozenset(
    {
        *TASK_COMPLETE_COMMAND_TYPES,
        *SCHEDULE_CANCEL_COMMAND_TYPES,
        *REMINDER_CANCEL_COMMAND_TYPES,
        *REMINDER_TRIGGER_COMMAND_TYPES,
        *DECISION_COMPLETE_COMMAND_TYPES,
        *DECISION_DEFER_COMMAND_TYPES,
        *DECISION_IGNORE_COMMAND_TYPES,
        *DECISION_CARD_CREATE_COMMAND_TYPES,
        *DECISION_CARD_SURFACE_COMMAND_TYPES,
        *DECISION_CARD_ACKNOWLEDGE_COMMAND_TYPES,
        *DECISION_CARD_RESOLVE_COMMAND_TYPES,
    }
)
DUPLICATE_SHORT_CIRCUIT_EXEMPT_COMMAND_TYPES = frozenset(
    {
        *DUPLICATE_SHORT_CIRCUIT_BYPASS_COMMAND_TYPES,
        *RULE_FIRST_DOMAIN_COMMAND_TYPES,
    }
)
TASK_HIGH_RISK_KEYWORDS = (
    "bank",
    "wire",
    "transfer",
    "payment",
    "pay",
    "password",
    "security",
)
HOUSEHOLD_MESSAGE_SOURCE_VALUES = frozenset({"manual", "test"})
HOUSEHOLD_MESSAGE_CLASSIFICATION_VALUES = frozenset({"schedule", "todo", "action", "fyi"})
HOUSEHOLD_MESSAGE_INTERPRETATION_VALUES = frozenset(
    {
        "schedule_create",
        "cancellation",
        "time_change",
        "conflict_reported",
        "deadline",
        "obligation",
        "ambiguity",
        "informational",
    }
)
HOUSEHOLD_MESSAGE_INTERPRETATION_CONFIDENCE_THRESHOLD = 0.7
HOUSEHOLD_MAX_ACTIVE_PROMOTION_DECISIONS_VISIBLE = 3
HOUSEHOLD_OPEN_DECISION_CARD_STATES = frozenset({"generated", "surfaced", "acknowledged"})
HOUSEHOLD_LOW_PRIORITY_DECISION_REASONS = frozenset(
    {
        "explicit_decision_target",
        "ambiguity_requires_clarification",
        "low_confidence_merge",
    }
)
HOUSEHOLD_UPM_PRIORITY_CRITICAL = "critical"
HOUSEHOLD_UPM_PRIORITY_HIGH = "high"
HOUSEHOLD_UPM_PRIORITY_MEDIUM = "medium"
HOUSEHOLD_UPM_PRIORITY_LOW = "low"
HOUSEHOLD_UPM_PRIORITY_NOISE = "noise"
HOUSEHOLD_UPM_PRIORITY_CLASSES = frozenset(
    {
        HOUSEHOLD_UPM_PRIORITY_CRITICAL,
        HOUSEHOLD_UPM_PRIORITY_HIGH,
        HOUSEHOLD_UPM_PRIORITY_MEDIUM,
        HOUSEHOLD_UPM_PRIORITY_LOW,
        HOUSEHOLD_UPM_PRIORITY_NOISE,
    }
)
HOUSEHOLD_UPM_ACTIONABILITY_THRESHOLD = 0.56
HOUSEHOLD_UPM_CONFIDENCE_MIN = 0.58
HOUSEHOLD_UPM_BORDERLINE_MARGIN = 0.08
HOUSEHOLD_UPM_DENSITY_WINDOW_MAX = 25
HOUSEHOLD_UPM_DECISION_QUEUE_MAX = 8
HOUSEHOLD_UPM_CONFLICT_BACKLOG_MAX = 6
HOUSEHOLD_UPM_DECISION_CAP_MIN = 3
HOUSEHOLD_UPM_DECISION_CAP_MAX = 8
HOUSEHOLD_UPM_HISTORY_WINDOW = 25
HOUSEHOLD_UPM_RECALL_SAFETY_CONFIDENCE = 0.6
HOUSEHOLD_UPM_RECALL_SAFETY_ACTIONABILITY = 0.58
HOUSEHOLD_UPM_COMPRESSION_GAP_GUARD = 2
HOUSEHOLD_UPM_RESOLUTION_ACK_STATES = frozenset({"generated", "surfaced", "acknowledged", "resolved", "applied"})


@dataclass(frozen=True)
class CommandActor:
    actor_type: str
    user_id: str
    session_id: str | None = None


@dataclass(frozen=True)
class ProjectionCacheEntry:
    latest_event_id: str
    projection: Mapping[str, Any]


ProjectionPartitionKey = tuple[str]


class _FrozenDict(dict[str, Any]):
    def __setitem__(self, key: Any, value: Any) -> None:
        return None

    def __delitem__(self, key: Any) -> None:
        return None

    def clear(self) -> None:
        return None

    def pop(self, key: Any, default: Any = None) -> Any:
        if key in self:
            return super().get(key)
        return default

    def popitem(self) -> tuple[Any, Any]:
        if not self:
            raise KeyError("popitem(): dictionary is empty")
        first_key = next(iter(self.keys()))
        return first_key, super().get(first_key)

    def setdefault(self, key: Any, default: Any = None) -> Any:
        if key in self:
            return super().get(key)
        return default

    def update(self, *args: Any, **kwargs: Any) -> None:
        return None

    def __ior__(self, other: Any) -> _FrozenDict:
        _ = other
        return self

    def __or__(self, other: Any) -> _FrozenDict:
        merged = dict(self)
        if isinstance(other, Mapping):
            merged.update(dict(other))
        return _FrozenDict(merged)

    def __ror__(self, other: Any) -> _FrozenDict:
        merged: dict[str, Any] = {}
        if isinstance(other, Mapping):
            merged.update(dict(other))
        merged.update(dict(self))
        return _FrozenDict(merged)

    def copy(self) -> _FrozenDict:
        return _FrozenDict(dict(self))


class _FrozenList(list[Any]):
    def __setitem__(self, key: Any, value: Any) -> None:
        return None

    def __delitem__(self, key: Any) -> None:
        return None

    def append(self, value: Any) -> None:
        return None

    def clear(self) -> None:
        return None

    def extend(self, iterable: Any) -> None:
        _ = iterable
        return None

    def insert(self, index: int, value: Any) -> None:
        _ = (index, value)
        return None

    def pop(self, index: int = -1) -> Any:
        return super().__getitem__(index)

    def remove(self, value: Any) -> None:
        _ = value
        return None

    def reverse(self) -> None:
        return None

    def sort(self, *args: Any, **kwargs: Any) -> None:
        _ = (args, kwargs)
        return None

    def __iadd__(self, other: Any) -> _FrozenList:
        _ = other
        return self

    def __imul__(self, count: Any) -> _FrozenList:
        _ = count
        return self

    def __add__(self, other: Any) -> _FrozenList:
        merged = list(self)
        if isinstance(other, list):
            merged.extend(other)
        return _FrozenList(merged)

    def __mul__(self, count: Any) -> _FrozenList:
        try:
            factor = int(count)
        except (TypeError, ValueError):
            factor = 1
        return _FrozenList(list(self) * factor)

    def __rmul__(self, count: Any) -> _FrozenList:
        return self.__mul__(count)

    def copy(self) -> _FrozenList:
        return _FrozenList(list(self))


def _wrap_projection_view_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _ProjectionViewDict(value)
    if isinstance(value, list):
        return _ProjectionViewList(value)
    return value


class _ProjectionViewDict(dict[str, Any]):
    def __init__(self, snapshot: Mapping[str, Any]) -> None:
        super().__init__(snapshot)
        self._snapshot = snapshot
        self._view_cache: dict[str, Any] = {}

    def _view_for_key(self, key: str) -> Any:
        if key in self._view_cache:
            return self._view_cache[key]
        wrapped = _wrap_projection_view_value(self._snapshot[key])
        self._view_cache[key] = wrapped
        return wrapped

    def __getitem__(self, key: str) -> Any:
        if key not in self._snapshot:
            raise KeyError(key)
        return self._view_for_key(key)

    def get(self, key: str, default: Any = None) -> Any:
        if key in self._snapshot:
            return self._view_for_key(key)
        return default

    def __contains__(self, key: object) -> bool:
        return key in self._snapshot

    def __iter__(self):
        return iter(self._snapshot)

    def __len__(self) -> int:
        return len(self._snapshot)

    def keys(self):
        return self._snapshot.keys()

    def items(self):
        for key in self._snapshot:
            yield key, self._view_for_key(key)

    def values(self):
        for key in self._snapshot:
            yield self._view_for_key(key)

    def __setitem__(self, key: Any, value: Any) -> None:
        _ = (key, value)
        return None

    def __delitem__(self, key: Any) -> None:
        _ = key
        return None

    def clear(self) -> None:
        return None

    def pop(self, key: Any, default: Any = None) -> Any:
        _ = key
        return default

    def popitem(self) -> tuple[Any, Any]:
        if not self._snapshot:
            raise KeyError("popitem(): dictionary is empty")
        first_key = next(iter(self._snapshot.keys()))
        return first_key, self._view_for_key(first_key)

    def setdefault(self, key: Any, default: Any = None) -> Any:
        if key in self._snapshot:
            return self._view_for_key(cast(str, key))
        return default

    def update(self, *args: Any, **kwargs: Any) -> None:
        _ = (args, kwargs)
        return None

    def __ior__(self, other: Any) -> _ProjectionViewDict:
        _ = other
        return self

    def copy(self) -> _ProjectionViewDict:
        return _ProjectionViewDict(self._snapshot)

    def __repr__(self) -> str:
        return repr(dict(self.items()))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Mapping):
            return dict(self.items()) == dict(other.items())
        return False


class _ProjectionViewList(list[Any]):
    def __init__(self, snapshot: Sequence[Any]) -> None:
        super().__init__(snapshot)
        self._snapshot = snapshot
        self._view_cache: dict[int, Any] = {}

    def _normalize_index(self, index: int) -> int:
        resolved = index
        if resolved < 0:
            resolved += len(self._snapshot)
        return resolved

    def _view_for_index(self, index: int) -> Any:
        resolved = self._normalize_index(index)
        if resolved in self._view_cache:
            return self._view_cache[resolved]
        wrapped = _wrap_projection_view_value(self._snapshot[resolved])
        self._view_cache[resolved] = wrapped
        return wrapped

    def __getitem__(self, index: Any) -> Any:
        if isinstance(index, slice):
            return [_wrap_projection_view_value(item) for item in self._snapshot[index]]
        return self._view_for_index(cast(int, index))

    def __iter__(self):
        for idx in range(len(self._snapshot)):
            yield self._view_for_index(idx)

    def __len__(self) -> int:
        return len(self._snapshot)

    def __contains__(self, value: object) -> bool:
        for item in self:
            if item == value:
                return True
        return False

    def __setitem__(self, key: Any, value: Any) -> None:
        _ = (key, value)
        return None

    def __delitem__(self, key: Any) -> None:
        _ = key
        return None

    def append(self, value: Any) -> None:
        _ = value
        return None

    def clear(self) -> None:
        return None

    def extend(self, iterable: Any) -> None:
        _ = iterable
        return None

    def insert(self, index: int, value: Any) -> None:
        _ = (index, value)
        return None

    def pop(self, index: int = -1) -> Any:
        return self._view_for_index(index)

    def remove(self, value: Any) -> None:
        _ = value
        return None

    def reverse(self) -> None:
        return None

    def sort(self, *args: Any, **kwargs: Any) -> None:
        _ = (args, kwargs)
        return None

    def __iadd__(self, other: Any) -> _ProjectionViewList:
        _ = other
        return self

    def __imul__(self, count: Any) -> _ProjectionViewList:
        _ = count
        return self

    def copy(self) -> _ProjectionViewList:
        return _ProjectionViewList(self._snapshot)

    def __repr__(self) -> str:
        return repr(list(self))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Sequence):
            return list(self) == list(other)
        return False


class CommandRuntimeService:
    """Single mutation runtime enforcing command -> event router -> event log -> projection."""

    def __init__(
        self,
        *,
        router_service: CanonicalRouterService | None = None,
        event_log_service: EventLogService | None = None,
        control_plane: ControlPlane | None = None,
        policy_registry: PolicyVersionRegistry | None = None,
        head_agent: HeadAgent | None = None,
    ) -> None:
        self._router = router_service or CanonicalRouterService()
        self._event_log = event_log_service or EventLogService()
        self._policy_registry = policy_registry or build_default_policy_registry()
        self._control_plane = control_plane or ControlPlane()
        self._head_agent = head_agent or HeadAgent()
        self._projection_cache: dict[ProjectionPartitionKey, ProjectionCacheEntry] = {}

    def _projection_partition_key(self, household_id: str) -> ProjectionPartitionKey:
        return (household_id,)

    def _freeze_projection_value(self, value: Any) -> Any:
        if isinstance(value, Mapping):
            return _FrozenDict({key: self._freeze_projection_value(inner) for key, inner in value.items()})
        if isinstance(value, list):
            return _FrozenList([self._freeze_projection_value(inner) for inner in value])
        return value

    def _freeze_projection_snapshot(self, projection: Mapping[str, Any]) -> Mapping[str, Any]:
        frozen = self._freeze_projection_value(projection)
        if isinstance(frozen, Mapping):
            return frozen
        return _FrozenDict({})

    def _projection_view(self, projection: Mapping[str, Any]) -> dict[str, Any]:
        return cast(dict[str, Any], _ProjectionViewDict(projection))

    def handle_command(
        self,
        *,
        command_type: str,
        household_id: str,
        actor: CommandActor,
        payload: dict[str, Any],
        source: str,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        normalized_command_type = command_type.strip()
        if not normalized_command_type:
            raise ValueError("command_type is required")
        if not household_id.strip():
            raise ValueError("household_id is required")

        return self._execute_partition_command(
            command_type=normalized_command_type,
            household_id=household_id,
            actor=actor,
            payload=payload,
            source=source,
            idempotency_key=idempotency_key,
        )

    def _execute_partition_command(
        self,
        *,
        command_type: str,
        household_id: str,
        actor: CommandActor,
        payload: dict[str, Any],
        source: str,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        normalized_payload = dict(payload)
        semantic_fingerprint = self._semantic_fingerprint(
            command_type=command_type,
            household_id=household_id,
            actor=actor,
            payload=normalized_payload,
        )
        dedupe_key = (
            idempotency_key
            or f"command:{household_id}:{command_type}:{semantic_fingerprint}"
        )

        duplicate = self._find_duplicate_command(
            household_id=household_id,
            command_type=command_type,
            semantic_fingerprint=semantic_fingerprint,
        )
        if (
            duplicate is not None
            and command_type not in DUPLICATE_SHORT_CIRCUIT_EXEMPT_COMMAND_TYPES
        ):
            projection = self.get_projection(household_id)
            request_id = str(duplicate.get("request_id") or "")
            nested_payload = duplicate.get("payload")
            if (
                isinstance(nested_payload, dict)
                and command_type in {"assistant.approve", "assistant.reject"}
            ):
                target_request_id = str(nested_payload.get("request_id") or "")
                if not target_request_id:
                    action_id = str(nested_payload.get("action_id") or "")
                    action_ids = nested_payload.get("action_ids")
                    if not action_id and isinstance(action_ids, list) and action_ids:
                        action_id = str(action_ids[0])
                    if action_id:
                        target_request_id = self._request_id_from_action(projection, action_id) or ""
                if target_request_id:
                    request_id = target_request_id
            return {
                "status": "duplicate",
                "request_id": request_id,
                "event_id": str(projection.get("last_event_id") or ""),
                "response": self._response_for_request(projection, request_id),
                "projection": projection,
                "effects": [],
            }

        request_id = str(
            normalized_payload.get("request_id")
            or self._request_id(command_type, semantic_fingerprint)
        )
        projection_for_rules = self.get_projection(household_id)
        rules_result = evaluate_command(
            command_type=command_type,
            payload=normalized_payload,
            current_state=projection_for_rules,
        )
        if not rules_result.allowed:
            return {
                "status": "rejected",
                "request_id": request_id,
                "event_id": str(projection_for_rules.get("last_event_id") or ""),
                "response": {
                    "request_id": request_id,
                    "command_type": command_type,
                    "reason": rules_result.reason,
                    "code": rules_result.code,
                },
                "projection": projection_for_rules,
                "effects": [],
            }

        policy_resolution = self._policy_registry.resolve_policy(_utc_now())

        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type="command.received",
            source=source,
            idempotency_key=dedupe_key,
            policy_resolution=policy_resolution,
            payload={
                "command_type": command_type,
                "request_id": request_id,
                "semantic_fingerprint": semantic_fingerprint,
                "actor_type": actor.actor_type,
                "payload": normalized_payload,
                "received_at": _utc_iso(),
            },
        )

        try:
            result = self._apply_action_pipeline(
                command_type=command_type,
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=normalized_payload,
                source=source,
                policy_resolution=policy_resolution,
                projection_hint=projection_for_rules,
            )
        except Exception as exc:
            self._emit_event(
                household_id=household_id,
                actor=actor,
                event_type="saga.failed",
                source="runtime.action_pipeline",
                policy_resolution=policy_resolution,
                payload={
                    "command_type": command_type,
                    "request_id": request_id,
                    "error": str(exc),
                    "failed_at": _utc_iso(),
                },
            )
            raise

        projection_hint = result.get("_projection_hint")
        if isinstance(projection_hint, dict):
            projection = projection_hint
        else:
            projection = self.get_projection(household_id, force_replay=True)

        self._emit_projection_snapshot(household_id=household_id, actor=actor, projection=projection)

        result_payload = {key: value for key, value in result.items() if key != "_projection_hint"}

        return {
            **result_payload,
            "request_id": request_id,
            "event_id": str(projection.get("last_event_id") or ""),
            "projection": projection,
        }

    def get_projection(self, household_id: str, *, force_replay: bool = False) -> dict[str, Any]:
        partition_key = self._projection_partition_key(household_id)
        rows = self._event_log.get_event_logs(household_id=household_id, limit=5000)
        if not rows:
            empty_snapshot = self._freeze_projection_snapshot(self._empty_projection(household_id))
            return self._projection_view(empty_snapshot)

        latest_event_id = rows[0].event_id
        cached = self._projection_cache.get(partition_key)
        if not force_replay and cached is not None and cached.latest_event_id == latest_event_id:
            return self._projection_view(cached.projection)

        ordered_rows = sorted(rows, key=lambda row: (row.timestamp, row.event_id))
        projection = self._replay_projection(household_id=household_id, rows=ordered_rows)
        frozen_snapshot = self._freeze_projection_snapshot(projection)
        self._projection_cache[partition_key] = ProjectionCacheEntry(
            latest_event_id=latest_event_id,
            projection=frozen_snapshot,
        )
        return self._projection_view(frozen_snapshot)

    def _apply_action_pipeline(
        self,
        *,
        command_type: str,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        source: str,
        policy_resolution: PolicyResolution,
        projection_hint: dict[str, Any],
    ) -> dict[str, Any]:
        if command_type in {"assistant.run", "assistant.query"}:
            query = str(payload.get("query") or payload.get("message") or "").strip()
            response = self._build_household_response(
                household_id=household_id,
                request_id=request_id,
                query=query,
            )
            self._emit_event(
                household_id=household_id,
                actor=actor,
                event_type="assistant.response_proposed",
                source="runtime.action_pipeline",
                idempotency_key=f"{request_id}:assistant.response_proposed",
                payload={
                    "request_id": request_id,
                    "response": response.model_dump(),
                    "query": query,
                    "proposed_at": _utc_iso(),
                },
            )
            return {
                "status": "accepted",
                "response": response.model_dump(),
                "effects": [],
            }

        if command_type == "assistant.approve":
            projection = projection_hint
            action_ids = [str(item) for item in payload.get("action_ids") or [] if str(item).strip()]
            action_id = str(payload.get("action_id") or "").strip()
            if action_id and action_id not in action_ids:
                action_ids.append(action_id)

            resolved_request_id = str(payload.get("request_id") or "").strip()
            if not resolved_request_id and action_ids:
                resolved_request_id = self._request_id_from_action(projection, action_ids[0]) or ""
            if resolved_request_id and not action_ids:
                resolved_action = self._action_id_from_request(projection, resolved_request_id)
                if resolved_action:
                    action_ids = [resolved_action]

            if not resolved_request_id or not action_ids:
                raise ValueError("assistant.approve requires request_id or action_id/action_ids")

            current_response = self._response_for_request(projection, resolved_request_id) or {}
            approval_epoch = _stable_hash(current_response)[:12] if current_response else "baseline"

            self._emit_event(
                household_id=household_id,
                actor=actor,
                event_type="assistant.action_approved",
                source="runtime.action_pipeline",
                idempotency_key=f"{resolved_request_id}:assistant.action_approved:{approval_epoch}",
                payload={
                    "request_id": resolved_request_id,
                    "action_ids": action_ids,
                    "approved_at": _utc_iso(),
                },
            )

            effects: list[dict[str, Any]] = []
            for approved_action_id in action_ids:
                effect = {
                    "action_id": approved_action_id,
                    "handler": "calendar_update",
                    "result": {
                        "status": "committed",
                        "event_id": f"runtime-{approved_action_id}",
                    },
                }
                effects.append(effect)
                self._emit_event(
                    household_id=household_id,
                    actor=actor,
                    event_type="assistant.action_executed",
                    source="runtime.action_pipeline",
                    idempotency_key=f"{approved_action_id}:assistant.action_executed:{approval_epoch}",
                    payload={
                        "request_id": resolved_request_id,
                        "action_id": approved_action_id,
                        "effect": effect,
                        "event": {
                            "event_id": f"runtime-{approved_action_id}",
                            "title": "Approved assistant action",
                            "source": "assistant_runtime",
                            "start": _utc_iso(),
                            "end": _utc_iso(_utc_now()),
                        },
                        "executed_at": _utc_iso(),
                    },
                )

            updated_projection = self.get_projection(household_id, force_replay=True)
            updated_response = self._response_for_request(updated_projection, resolved_request_id)
            return {
                "status": "committed",
                "response": updated_response,
                "effects": effects,
                "_projection_hint": updated_projection,
            }

        if command_type == "assistant.reject":
            projection = projection_hint
            action_id = str(payload.get("action_id") or "").strip()
            request_id_override = str(payload.get("request_id") or "").strip()
            resolved_request_id = request_id_override or self._request_id_from_action(projection, action_id) or ""
            if not resolved_request_id or not action_id:
                raise ValueError("assistant.reject requires request_id and action_id")

            self._emit_event(
                household_id=household_id,
                actor=actor,
                event_type="assistant.action_rejected",
                source="runtime.action_pipeline",
                idempotency_key=f"{action_id}:assistant.action_rejected",
                payload={
                    "request_id": resolved_request_id,
                    "action_id": action_id,
                    "rejected_at": _utc_iso(),
                },
            )
            updated_projection = self.get_projection(household_id, force_replay=True)
            updated_response = self._response_for_request(updated_projection, resolved_request_id)
            return {
                "status": "rejected",
                "response": updated_response,
                "effects": [],
                "_projection_hint": updated_projection,
            }

        if command_type in TASK_CREATE_COMMAND_TYPES:
            return self._handle_task_create_command(
                command_type=command_type,
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                policy_resolution=policy_resolution,
            )

        if command_type in SAGA_EXECUTE_COMMAND_TYPES:
            return self._handle_saga_execute_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                policy_resolution=policy_resolution,
            )

        if command_type in TASK_COMPLETE_COMMAND_TYPES:
            return self._handle_task_completed_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        if command_type in SCHEDULE_CREATE_COMMAND_TYPES:
            return self._handle_schedule_create_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        if command_type in SCHEDULE_CANCEL_COMMAND_TYPES:
            return self._handle_schedule_cancel_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        if command_type in REMINDER_CREATE_COMMAND_TYPES:
            return self._handle_reminder_create_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        if command_type in REMINDER_CANCEL_COMMAND_TYPES:
            return self._handle_reminder_cancel_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        if command_type in REMINDER_TRIGGER_COMMAND_TYPES:
            return self._handle_reminder_trigger_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        if command_type in DECISION_CARD_CREATE_COMMAND_TYPES:
            return self._handle_decision_card_create_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        if command_type in DECISION_CARD_SURFACE_COMMAND_TYPES:
            return self._handle_decision_card_surface_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        if command_type in DECISION_CARD_ACKNOWLEDGE_COMMAND_TYPES:
            return self._handle_decision_card_acknowledge_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        if command_type in DECISION_CARD_RESOLVE_COMMAND_TYPES:
            return self._handle_decision_card_resolve_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        if command_type in DECISION_COMPLETE_COMMAND_TYPES:
            return self._handle_decision_complete_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        if command_type in DECISION_DEFER_COMMAND_TYPES:
            return self._handle_decision_defer_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        if command_type in DECISION_IGNORE_COMMAND_TYPES:
            return self._handle_decision_ignore_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
            )

        resolved_surface_command = resolve_command(command_type)
        if resolved_surface_command is not None:
            dispatch = resolved_surface_command.command_handler(
                command_type=command_type,
                event_type=resolved_surface_command.event_type,
                household_id=household_id,
                actor_user_id=actor.user_id,
                request_id=request_id,
                payload=payload,
            )

            response_payload = dict(dispatch.response_payload)
            event_payload = dict(dispatch.event_payload)
            event_type = str(dispatch.event_type or resolved_surface_command.event_type)

            self._emit_event(
                household_id=household_id,
                actor=actor,
                event_type=event_type,
                source="runtime.action_pipeline",
                idempotency_key=dispatch.idempotency_key or f"{request_id}:{event_type}",
                payload={
                    **event_payload,
                    "request_id": request_id,
                    "response": response_payload,
                },
            )

            return {
                "status": str(dispatch.status or "accepted"),
                "response": response_payload,
                "effects": [dict(effect) for effect in dispatch.effects],
            }

        if command_type == "email.ingest":
            parsed = self._head_agent.process_email_ingest(payload, request_id=request_id)
            self._emit_event(
                household_id=household_id,
                actor=actor,
                event_type="email.ingested",
                source="runtime.action_pipeline",
                payload={
                    "request_id": request_id,
                    "email": parsed.get("email") or {},
                    "action_items": parsed.get("action_items") or [],
                    "ingested_at": _utc_iso(),
                },
                idempotency_key=f"{request_id}:email.ingested",
            )
            return {
                "status": "accepted",
                "response": {
                    "request_id": request_id,
                    "ingested": True,
                    "action_items": list(parsed.get("action_items") or []),
                },
                "effects": [],
            }

        if command_type == "calendar.ingest":
            parsed = self._head_agent.process_calendar_ingest(payload, request_id=request_id)
            self._emit_event(
                household_id=household_id,
                actor=actor,
                event_type="calendar.ingested",
                source="runtime.action_pipeline",
                payload={
                    "request_id": request_id,
                    "calendar_id": str(parsed.get("calendar_id") or ""),
                    "events": list(parsed.get("events") or []),
                    "conflicts": list(parsed.get("conflicts") or []),
                    "ingested_at": _utc_iso(),
                },
                idempotency_key=f"{request_id}:calendar.ingested",
            )
            return {
                "status": "accepted",
                "response": {
                    "request_id": request_id,
                    "ingested": True,
                    "event_count": len(list(parsed.get("events") or [])),
                    "conflict_count": len(list(parsed.get("conflicts") or [])),
                },
                "effects": [],
            }

        if command_type in HOUSEHOLD_MESSAGE_INGEST_COMMAND_TYPES:
            return self._handle_household_message_ingest_command(
                household_id=household_id,
                actor=actor,
                request_id=request_id,
                payload=payload,
                precomputed_projection=projection_hint,
                policy_resolution=policy_resolution,
            )

        raise ValueError(f"Unsupported command_type: {command_type}")

    def _handle_household_message_ingest_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
        policy_resolution: PolicyResolution,
    ) -> dict[str, Any]:
        projection = (
            precomputed_projection
            if isinstance(precomputed_projection, dict)
            else self.get_projection(household_id)
        )
        normalized = self._normalize_household_message_payload(
            payload=payload,
            request_id=request_id,
            default_member_id=actor.user_id,
        )

        message_id = str(normalized["message_id"])
        raw_content = str(normalized["raw_content"])
        created_at = str(normalized["created_at"])
        member_id = str(normalized["member_id"])
        source = str(normalized["source"])

        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type="household_message_ingested",
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:household_message_ingested",
            payload={
                "request_id": request_id,
                "message_id": message_id,
                "source": source,
                "raw_content": raw_content,
                "created_at": created_at,
                "member_id": member_id,
                "classification": None,
                "ingested_at": _utc_iso(),
            },
        )

        interpretation = self._interpret_household_message(
            raw_content=raw_content,
            created_at=created_at,
            projection=projection,
            member_id=member_id,
        )
        classification = str(interpretation.get("classification") or "fyi")
        if classification not in HOUSEHOLD_MESSAGE_CLASSIFICATION_VALUES:
            classification = "fyi"

        promotion_target = self._resolve_household_message_promotion_target(
            classification=classification,
            raw_content=raw_content,
            interpretation=interpretation,
        )
        interpretation_type = str(interpretation.get("interpretation_type") or "informational")
        if interpretation_type not in HOUSEHOLD_MESSAGE_INTERPRETATION_VALUES:
            interpretation_type = "informational"
        interpretation_confidence = float(interpretation.get("confidence") or 0.0)
        promotion_reason = str(interpretation.get("promotion_reason") or "informational.no_promotion")
        dependency_schedule_id = str(interpretation.get("dependency_schedule_id") or "")
        conflict_schedule_id = str(interpretation.get("conflict_schedule_id") or "")
        conflict_type = str(interpretation.get("conflict_type") or "")
        conflict_severity = str(interpretation.get("conflict_severity") or "")
        conflict_events_involved = [
            str(item).strip()
            for item in list(interpretation.get("conflict_events_involved") or [])
            if str(item).strip()
        ]
        upm_profile_input = interpretation.get("upm")
        if isinstance(upm_profile_input, Mapping):
            upm_profile = {
                "priority_score": int(upm_profile_input.get("priority_score") or 0),
                "priority_class": str(upm_profile_input.get("priority_class") or HOUSEHOLD_UPM_PRIORITY_NOISE),
                "requires_decision": bool(upm_profile_input.get("requires_decision")),
                "conflict_risk": bool(upm_profile_input.get("conflict_risk")),
                "state_dependency": bool(upm_profile_input.get("state_dependency")),
                "decision_score": self._coerce_upm_score(upm_profile_input.get("decision_score"), default=0.0),
                "actionability_score": self._coerce_upm_score(
                    upm_profile_input.get("actionability_score"),
                    default=0.0,
                ),
                "confidence_score": self._coerce_upm_score(upm_profile_input.get("confidence_score"), default=0.0),
                "actionability_threshold": self._coerce_upm_score(
                    upm_profile_input.get("actionability_threshold"),
                    default=HOUSEHOLD_UPM_ACTIONABILITY_THRESHOLD,
                ),
                "confidence_min": self._coerce_upm_score(
                    upm_profile_input.get("confidence_min"),
                    default=HOUSEHOLD_UPM_CONFIDENCE_MIN,
                ),
                "borderline_event": bool(upm_profile_input.get("borderline_event")),
                "event_density_score": self._coerce_upm_score(
                    upm_profile_input.get("event_density_score"),
                    default=0.0,
                ),
                "decision_queue_score": self._coerce_upm_score(
                    upm_profile_input.get("decision_queue_score"),
                    default=0.0,
                ),
                "conflict_backlog_score": self._coerce_upm_score(
                    upm_profile_input.get("conflict_backlog_score"),
                    default=0.0,
                ),
                "household_state_load": self._coerce_upm_score(
                    upm_profile_input.get("household_state_load"),
                    default=0.0,
                ),
                "conflict_forced_eligibility": bool(upm_profile_input.get("conflict_forced_eligibility")),
                "dependency_forced_eligibility": bool(upm_profile_input.get("dependency_forced_eligibility")),
                "ambiguity_forced_eligibility": bool(upm_profile_input.get("ambiguity_forced_eligibility")),
                "priority_signals": [
                    str(item).strip()
                    for item in list(upm_profile_input.get("priority_signals") or [])
                    if str(item).strip()
                ],
            }
        else:
            upm_profile = self._upm_priority_profile_for_message(
                projection=projection,
                raw_content=raw_content,
                interpretation_type=interpretation_type,
                interpretation=interpretation,
                member_id=member_id,
                context={
                    "dependency_schedule_id": dependency_schedule_id,
                    "conflict_schedule_id": conflict_schedule_id,
                    "conflict_type": conflict_type,
                    "conflict_events_involved": conflict_events_involved,
                    "due_at": str(interpretation.get("due_at") or ""),
                    "derived_start_at": str(interpretation.get("derived_start_at") or ""),
                    "derived_end_at": str(interpretation.get("derived_end_at") or ""),
                },
            )
        upm_priority_class = str(upm_profile.get("priority_class") or HOUSEHOLD_UPM_PRIORITY_NOISE)
        if upm_priority_class not in HOUSEHOLD_UPM_PRIORITY_CLASSES:
            upm_priority_class = HOUSEHOLD_UPM_PRIORITY_NOISE
        upm_requires_decision = bool(upm_profile.get("requires_decision"))
        decision_resolutions: list[dict[str, Any]] = []

        promoted_entity_type = ""
        promoted_entity_id = ""
        promotion_status = "ignored"
        secondary_entity_type = ""
        secondary_entity_id = ""
        decision_generated = False

        if promotion_target == "action":
            task_payload = self._task_payload_from_household_message(
                normalized,
                interpretation=interpretation,
            )
            task_payload["task_id"] = f"task-msg-{message_id}"
            task_payload["source_message_id"] = message_id
            task_payload["promotion_reason"] = promotion_reason
            task_payload["interpretation_type"] = interpretation_type
            task_result = self._handle_task_create_command(
                command_type="task.create",
                household_id=household_id,
                actor=actor,
                request_id=f"{request_id}:promote:task:{message_id}",
                payload=task_payload,
                policy_resolution=policy_resolution,
            )
            task_response = task_result.get("response")
            task_row = task_response.get("task") if isinstance(task_response, Mapping) else None
            promoted_entity_id = (
                str(task_row.get("task_id") or "")
                if isinstance(task_row, Mapping)
                else f"task-msg-{message_id}"
            )
            promoted_entity_type = "task"
            promotion_status = str(task_result.get("status") or "accepted")
            if upm_requires_decision:
                decision_resolution = self._maybe_create_household_message_decision_card(
                    household_id=household_id,
                    actor=actor,
                    request_id=f"{request_id}:promote:task:decision:{message_id}",
                    message_id=message_id,
                    raw_content=raw_content,
                    source=source,
                    member_id=member_id,
                    classification=classification,
                    interpretation_type=interpretation_type,
                    promotion_reason=promotion_reason,
                    promotion_target=promotion_target,
                    interpretation=interpretation,
                    precomputed_projection=projection,
                    context={
                        "decision_reason": "task_conflict_or_uncertainty",
                        "conflict_schedule_id": conflict_schedule_id,
                        "conflict_type": conflict_type,
                        "conflict_severity": conflict_severity,
                    },
                    decision_card_id=f"decision-msg-{message_id}-task",
                )
                decision_resolutions.append(decision_resolution)
                resolved_decision_card_id = str(decision_resolution.get("decision_card_id") or "")
                if resolved_decision_card_id:
                    secondary_entity_type = "decision_card"
                    secondary_entity_id = resolved_decision_card_id
                decision_generated = decision_generated or bool(decision_resolution.get("created"))

        elif promotion_target == "calendar":
            schedule_payload = self._schedule_payload_from_household_message(
                normalized,
                interpretation=interpretation,
                projection=projection,
            )
            schedule_payload["schedule_id"] = f"schedule-msg-{message_id}"
            schedule_payload["source_message_id"] = message_id
            schedule_payload["promotion_reason"] = promotion_reason
            schedule_payload["interpretation_type"] = interpretation_type
            schedule_payload["member_id"] = member_id
            schedule_result = self._handle_schedule_create_command(
                household_id=household_id,
                actor=actor,
                request_id=f"{request_id}:promote:schedule:{message_id}",
                payload=schedule_payload,
                precomputed_projection=projection,
            )
            schedule_response = schedule_result.get("response")
            schedule_row = schedule_response.get("schedule") if isinstance(schedule_response, Mapping) else None
            promoted_entity_id = (
                str(schedule_row.get("schedule_id") or "")
                if isinstance(schedule_row, Mapping)
                else f"schedule-msg-{message_id}"
            )
            promoted_entity_type = "schedule"
            promotion_status = str(schedule_result.get("status") or "accepted")
            if upm_requires_decision:
                decision_resolution = self._maybe_create_household_message_decision_card(
                    household_id=household_id,
                    actor=actor,
                    request_id=f"{request_id}:promote:schedule:decision:{message_id}",
                    message_id=message_id,
                    raw_content=raw_content,
                    source=source,
                    member_id=member_id,
                    classification=classification,
                    interpretation_type=interpretation_type,
                    promotion_reason=promotion_reason,
                    promotion_target=promotion_target,
                    interpretation=interpretation,
                    precomputed_projection=projection,
                    context={
                        "decision_reason": "calendar_conflict_detected",
                        "conflict_schedule_id": conflict_schedule_id,
                        "conflict_type": conflict_type,
                        "conflict_severity": conflict_severity,
                    },
                    decision_card_id=f"decision-msg-{message_id}-schedule",
                )
                decision_resolutions.append(decision_resolution)
                resolved_decision_card_id = str(decision_resolution.get("decision_card_id") or "")
                if resolved_decision_card_id:
                    secondary_entity_type = "decision_card"
                    secondary_entity_id = resolved_decision_card_id
                decision_generated = decision_generated or bool(decision_resolution.get("created"))

        elif promotion_target == "calendar_update":
            promoted_entity_type = "schedule"
            if interpretation_type == "cancellation" and dependency_schedule_id:
                cancel_result = self._handle_schedule_cancel_command(
                    household_id=household_id,
                    actor=actor,
                    request_id=f"{request_id}:promote:cancel:{message_id}",
                    payload={"schedule_id": dependency_schedule_id},
                    precomputed_projection=projection,
                )
                promoted_entity_id = dependency_schedule_id
                promotion_status = str(cancel_result.get("status") or "accepted")
            elif interpretation_type == "time_change":
                if dependency_schedule_id:
                    self._handle_schedule_cancel_command(
                        household_id=household_id,
                        actor=actor,
                        request_id=f"{request_id}:promote:timechange:cancel:{message_id}",
                        payload={"schedule_id": dependency_schedule_id},
                        precomputed_projection=projection,
                    )
                schedule_payload = self._schedule_payload_from_household_message(
                    normalized,
                    interpretation=interpretation,
                    projection=projection,
                )
                schedule_payload["schedule_id"] = f"schedule-msg-{message_id}-update"
                schedule_payload["source_message_id"] = message_id
                schedule_payload["promotion_reason"] = promotion_reason
                schedule_payload["interpretation_type"] = interpretation_type
                schedule_payload["member_id"] = member_id
                schedule_result = self._handle_schedule_create_command(
                    household_id=household_id,
                    actor=actor,
                    request_id=f"{request_id}:promote:timechange:create:{message_id}",
                    payload=schedule_payload,
                    precomputed_projection=projection,
                )
                schedule_response = schedule_result.get("response")
                schedule_row = schedule_response.get("schedule") if isinstance(schedule_response, Mapping) else None
                promoted_entity_id = (
                    str(schedule_row.get("schedule_id") or "")
                    if isinstance(schedule_row, Mapping)
                    else f"schedule-msg-{message_id}-update"
                )
                promotion_status = str(schedule_result.get("status") or "accepted")
            else:
                decision_resolution = self._maybe_create_household_message_decision_card(
                    household_id=household_id,
                    actor=actor,
                    request_id=f"{request_id}:promote:update:fallback:{message_id}",
                    message_id=message_id,
                    raw_content=raw_content,
                    source=source,
                    member_id=member_id,
                    classification=classification,
                    interpretation_type=interpretation_type,
                    promotion_reason=promotion_reason,
                    promotion_target=promotion_target,
                    interpretation=interpretation,
                    precomputed_projection=projection,
                    context={
                        "decision_reason": "calendar_update_requires_confirmation",
                        "conflict_schedule_id": conflict_schedule_id,
                        "conflict_type": conflict_type,
                        "conflict_severity": conflict_severity,
                    },
                    decision_card_id=f"decision-msg-{message_id}-update",
                )
                decision_resolutions.append(decision_resolution)
                resolved_decision_card_id = str(decision_resolution.get("decision_card_id") or "")
                if resolved_decision_card_id:
                    promoted_entity_type = "decision_card"
                    promoted_entity_id = resolved_decision_card_id
                    promotion_status = str(decision_resolution.get("status") or "accepted")
                    promotion_target = "decision"
                else:
                    promoted_entity_type = ""
                    promoted_entity_id = ""
                    promotion_status = f"suppressed:{str(decision_resolution.get('suppressed_reason') or 'decision')}"
                    promotion_target = "ignore"
                decision_generated = decision_generated or bool(decision_resolution.get("created"))

            if promotion_target == "calendar_update" and upm_requires_decision:
                decision_resolution = self._maybe_create_household_message_decision_card(
                    household_id=household_id,
                    actor=actor,
                    request_id=f"{request_id}:promote:update:decision:{message_id}",
                    message_id=message_id,
                    raw_content=raw_content,
                    source=source,
                    member_id=member_id,
                    classification=classification,
                    interpretation_type=interpretation_type,
                    promotion_reason=promotion_reason,
                    promotion_target=promotion_target,
                    interpretation=interpretation,
                    precomputed_projection=projection,
                    context={
                        "decision_reason": "calendar_change_conflict_or_uncertainty",
                        "conflict_schedule_id": conflict_schedule_id,
                        "conflict_type": conflict_type,
                        "conflict_severity": conflict_severity,
                    },
                    decision_card_id=f"decision-msg-{message_id}-calendar-update",
                )
                decision_resolutions.append(decision_resolution)
                resolved_decision_card_id = str(decision_resolution.get("decision_card_id") or "")
                if resolved_decision_card_id:
                    secondary_entity_type = "decision_card"
                    secondary_entity_id = resolved_decision_card_id
                decision_generated = decision_generated or bool(decision_resolution.get("created"))

        elif promotion_target == "decision":
            decision_resolution = self._maybe_create_household_message_decision_card(
                household_id=household_id,
                actor=actor,
                request_id=f"{request_id}:promote:decision:{message_id}",
                message_id=message_id,
                raw_content=raw_content,
                source=source,
                member_id=member_id,
                classification=classification,
                interpretation_type=interpretation_type,
                promotion_reason=promotion_reason,
                promotion_target=promotion_target,
                interpretation=interpretation,
                precomputed_projection=projection,
                context={
                    "decision_reason": "explicit_decision_target",
                    "conflict_schedule_id": conflict_schedule_id,
                    "conflict_type": conflict_type,
                    "conflict_severity": conflict_severity,
                },
                decision_card_id=f"decision-msg-{message_id}",
            )
            decision_resolutions.append(decision_resolution)
            resolved_decision_card_id = str(decision_resolution.get("decision_card_id") or "")
            if resolved_decision_card_id:
                promoted_entity_id = resolved_decision_card_id
                promoted_entity_type = "decision_card"
                promotion_status = str(decision_resolution.get("status") or "accepted")
            else:
                promoted_entity_id = ""
                promoted_entity_type = ""
                promotion_status = f"suppressed:{str(decision_resolution.get('suppressed_reason') or 'decision')}"
                promotion_target = "ignore"
            decision_generated = decision_generated or bool(decision_resolution.get("created"))

        critical_event_detected = upm_priority_class == HOUSEHOLD_UPM_PRIORITY_CRITICAL
        if critical_event_detected and not decision_generated:
            upm_critical_decision_resolution = self._maybe_create_household_message_decision_card(
                household_id=household_id,
                actor=actor,
            request_id=f"{request_id}:promote:upm-critical-override:{message_id}",
                message_id=message_id,
                raw_content=raw_content,
                source=source,
                member_id=member_id,
                classification=classification,
                interpretation_type=interpretation_type,
                promotion_reason=promotion_reason,
                promotion_target="decision",
                interpretation=interpretation,
                precomputed_projection=projection,
                context={
                    "decision_reason": "upm_critical_recall_override",
                    "conflict_schedule_id": conflict_schedule_id,
                    "conflict_type": conflict_type,
                    "conflict_severity": conflict_severity,
                    "upm_priority_class": upm_priority_class,
                    "upm_requires_decision": upm_requires_decision,
                    "upm_conflict_risk": bool(upm_profile.get("conflict_risk")),
                    "upm_state_dependency": bool(upm_profile.get("state_dependency")),
                },
                decision_card_id=f"decision-msg-{message_id}-upm-override",
            )
            decision_resolutions.append(upm_critical_decision_resolution)
            upm_critical_decision_id = str(upm_critical_decision_resolution.get("decision_card_id") or "").strip()
            if upm_critical_decision_id:
                if not promoted_entity_id or promotion_target == "ignore":
                    promoted_entity_type = "decision_card"
                    promoted_entity_id = upm_critical_decision_id
                    promotion_target = "decision"
                    promotion_status = str(upm_critical_decision_resolution.get("status") or "accepted")
                elif not secondary_entity_id:
                    secondary_entity_type = "decision_card"
                    secondary_entity_id = upm_critical_decision_id
            decision_generated = decision_generated or bool(upm_critical_decision_resolution.get("created"))

        if critical_event_detected and not decision_generated:
            raise RuntimeError(
                "system_validation_failed:critical_event_detected_without_decision"
            )

        decision_audit_payload = self._decision_audit_payload_for_message(
            input_id=message_id,
            upm_profile=upm_profile,
            decision_generated=decision_generated,
            decision_resolutions=decision_resolutions,
        )
        self._emit_household_message_suppression_audit_trace(payload=decision_audit_payload)
        decision_routing_model = str(decision_audit_payload.get("decision_routing_model") or "upm_unified")
        decision_priority_class = str(decision_audit_payload.get("upm_priority_class") or upm_priority_class)
        decision_blocked = bool(decision_audit_payload.get("decision_blocked"))
        decision_suppression_reason = str(decision_audit_payload.get("suppression_reason") or "")
        decision_suppressed_score_delta = self._coerce_upm_score(
            decision_audit_payload.get("suppressed_score_delta"),
            default=0.0,
        )
        decision_alternative_path = str(decision_audit_payload.get("alternative_path") or "")
        decision_block_root_cause = str(decision_audit_payload.get("root_cause") or "")
        effective_upm_requires_decision = bool(upm_requires_decision)
        if (
            not decision_generated
            and decision_suppression_reason in {
                "merged_into_existing",
                "collapsed_into_low_priority_decision",
                "decision_not_required",
                "upm_no_decision_required",
                "low_impact_actionable_noise",
            }
        ):
            effective_upm_requires_decision = False
        if not decision_generated and decision_alternative_path == "decision.merge_existing":
            effective_upm_requires_decision = False

        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type="household_item_promoted",
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:household_item_promoted",
            payload={
                "request_id": request_id,
                "source_message_id": message_id,
                "classification": classification,
                "interpretation_type": interpretation_type,
                "interpretation_confidence": interpretation_confidence,
                "promotion_target": promotion_target,
                "promotion_reason": promotion_reason,
                "promotion_status": promotion_status,
                "promoted_entity_type": promoted_entity_type,
                "promoted_entity_id": promoted_entity_id,
                "dependency_schedule_id": dependency_schedule_id,
                "conflict_schedule_id": conflict_schedule_id,
                "conflict_type": conflict_type,
                "conflict_severity": conflict_severity,
                "conflict_events_involved": conflict_events_involved,
                "secondary_entity_type": secondary_entity_type,
                "secondary_entity_id": secondary_entity_id,
                "member_id": member_id,
                "promoted_at": _utc_iso(),
            },
        )

        return {
            "status": "accepted",
            "response": {
                "request_id": request_id,
                "message_id": message_id,
                "classification": classification,
                "interpretation_type": interpretation_type,
                "interpretation_confidence": interpretation_confidence,
                "promotion_target": promotion_target,
                "promotion_reason": promotion_reason,
                "promotion_status": promotion_status,
                "promoted_entity_type": promoted_entity_type,
                "promoted_entity_id": promoted_entity_id,
                "dependency_schedule_id": dependency_schedule_id,
                "conflict_schedule_id": conflict_schedule_id,
                "conflict_type": conflict_type,
                "conflict_severity": conflict_severity,
                "conflict_events_involved": conflict_events_involved,
                "secondary_entity_type": secondary_entity_type,
                "secondary_entity_id": secondary_entity_id,
                "decision_generated": decision_generated,
                "decision_routing_model": decision_routing_model,
                "upm_priority_class": decision_priority_class,
                "upm_requires_decision": effective_upm_requires_decision,
                "upm_conflict_risk": bool(upm_profile.get("conflict_risk")),
                "upm_state_dependency": bool(upm_profile.get("state_dependency")),
                "upm_decision_score": self._coerce_upm_score(upm_profile.get("decision_score"), default=0.0),
                "upm_actionability_score": self._coerce_upm_score(upm_profile.get("actionability_score"), default=0.0),
                "upm_confidence_score": self._coerce_upm_score(upm_profile.get("confidence_score"), default=0.0),
                "upm_actionability_threshold": self._coerce_upm_score(
                    upm_profile.get("actionability_threshold"),
                    default=HOUSEHOLD_UPM_ACTIONABILITY_THRESHOLD,
                ),
                "upm_confidence_min": self._coerce_upm_score(
                    upm_profile.get("confidence_min"),
                    default=HOUSEHOLD_UPM_CONFIDENCE_MIN,
                ),
                "suppression_reason": decision_suppression_reason,
                "suppressed_score_delta": decision_suppressed_score_delta,
                "alternative_path": decision_alternative_path,
                "critical_event_detected": critical_event_detected,
                "decision_blocked": decision_blocked,
                "decision_block_root_cause": decision_block_root_cause,
            },
            "effects": [
                {
                    "source_message_id": message_id,
                    "classification": classification,
                    "interpretation_type": interpretation_type,
                    "interpretation_confidence": interpretation_confidence,
                    "promotion_target": promotion_target,
                    "promotion_reason": promotion_reason,
                    "promotion_status": promotion_status,
                    "promoted_entity_type": promoted_entity_type,
                    "promoted_entity_id": promoted_entity_id,
                    "dependency_schedule_id": dependency_schedule_id,
                    "conflict_schedule_id": conflict_schedule_id,
                    "conflict_type": conflict_type,
                    "conflict_severity": conflict_severity,
                    "conflict_events_involved": conflict_events_involved,
                    "secondary_entity_type": secondary_entity_type,
                    "secondary_entity_id": secondary_entity_id,
                    "decision_generated": decision_generated,
                    "decision_routing_model": decision_routing_model,
                    "upm_priority_class": decision_priority_class,
                    "upm_requires_decision": effective_upm_requires_decision,
                    "upm_conflict_risk": bool(upm_profile.get("conflict_risk")),
                    "upm_state_dependency": bool(upm_profile.get("state_dependency")),
                    "upm_decision_score": self._coerce_upm_score(upm_profile.get("decision_score"), default=0.0),
                    "upm_actionability_score": self._coerce_upm_score(upm_profile.get("actionability_score"), default=0.0),
                    "upm_confidence_score": self._coerce_upm_score(upm_profile.get("confidence_score"), default=0.0),
                    "upm_actionability_threshold": self._coerce_upm_score(
                        upm_profile.get("actionability_threshold"),
                        default=HOUSEHOLD_UPM_ACTIONABILITY_THRESHOLD,
                    ),
                    "upm_confidence_min": self._coerce_upm_score(
                        upm_profile.get("confidence_min"),
                        default=HOUSEHOLD_UPM_CONFIDENCE_MIN,
                    ),
                    "suppression_reason": decision_suppression_reason,
                    "suppressed_score_delta": decision_suppressed_score_delta,
                    "alternative_path": decision_alternative_path,
                    "critical_event_detected": critical_event_detected,
                    "decision_blocked": decision_blocked,
                    "decision_block_root_cause": decision_block_root_cause,
                }
            ],
        }

    def _handle_task_create_command(
        self,
        *,
        command_type: str,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        policy_resolution: PolicyResolution,
    ) -> dict[str, Any]:
        try:
            normalized = self._normalize_task_create_payload(
                payload=payload,
                request_id=request_id,
                default_owner_user_id=actor.user_id,
                rules_snapshot=policy_resolution.rules_snapshot,
            )
        except ValueError as exc:
            self._emit_event(
                household_id=household_id,
                actor=actor,
                event_type="task.rules_evaluated",
                source="runtime.action_pipeline",
                idempotency_key=f"{request_id}:task.rules_evaluated",
                payload={
                    "request_id": request_id,
                    "rules_passed": False,
                    "error": str(exc),
                    "evaluated_at": _utc_iso(),
                },
            )
            raise

        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type="task.rules_evaluated",
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:task.rules_evaluated",
            payload={
                "request_id": request_id,
                "task_candidate": {
                    "title": normalized["title"],
                    "priority": normalized["priority"],
                    "due_at": normalized["due_at"],
                },
                "rules_passed": True,
                "evaluated_at": _utc_iso(),
            },
        )

        risk = self._classify_task_risk(
            normalized,
            risk_thresholds=policy_resolution.risk_thresholds_snapshot,
        )
        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type="task.risk_assessed",
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:task.risk_assessed",
            payload={
                "request_id": request_id,
                "risk": risk,
                "assessed_at": _utc_iso(),
            },
        )

        transitions = self._task_fsm_transitions(risk_level=str(risk["level"]))
        final_state = str(transitions[-1].get("to_state") or "created")

        resolved_task_id = str(payload.get("task_id") or "").strip() or f"task-{request_id}"
        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type="task.fsm_transitioned",
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:task.fsm_transitioned",
            payload={
                "request_id": request_id,
                "task_id": resolved_task_id,
                "current_state": final_state,
                "transitions": transitions,
                "transitioned_at": _utc_iso(),
            },
        )

        task_record = {
            "task_id": resolved_task_id,
            "request_id": request_id,
            "title": normalized["title"],
            "description": normalized["description"],
            "priority": normalized["priority"],
            "owner_user_id": normalized["owner_user_id"],
            "due_at": normalized["due_at"],
            "source_message_id": str(payload.get("source_message_id") or "").strip() or None,
            "promotion_reason": str(payload.get("promotion_reason") or "").strip() or None,
            "interpretation_type": str(payload.get("interpretation_type") or "").strip() or None,
            "risk_level": risk["level"],
            "status": "pending",
            "lifecycle_state": final_state,
            "created_at": _utc_iso(),
            "completed_at": None,
        }
        response_payload = {
            "request_id": request_id,
            "task": dict(task_record),
            "risk": dict(risk),
            "fsm": {
                "current_state": final_state,
                "transitions": transitions,
            },
        }

        created_event_type = (
            TASK_CREATED_EVENT_CANONICAL
            if command_type == "task_created"
            else TASK_CREATED_EVENT_LEGACY
        )

        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type=created_event_type,
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:{created_event_type}",
            payload={
                "request_id": request_id,
                "task": task_record,
                "risk": risk,
                "response": response_payload,
                "recorded_at": _utc_iso(),
            },
        )

        status = "pending_approval" if final_state == "pending_approval" else "accepted"
        return {
            "status": status,
            "response": response_payload,
            "effects": [
                {
                    "task_id": resolved_task_id,
                    "lifecycle_state": final_state,
                }
            ],
        }

    def _handle_task_completed_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        projection = (
            precomputed_projection
            if isinstance(precomputed_projection, dict)
            else self.get_projection(household_id)
        )
        task_id = str(payload.get("task_id") or "").strip()
        tasks = projection.get("tasks") if isinstance(projection.get("tasks"), dict) else {}
        task_row = tasks.get(task_id) if isinstance(tasks, dict) else None
        if not task_id or not isinstance(task_row, dict):
            raise ValueError("task_completed requires an existing task_id")

        completed_at = _utc_iso()
        updated_task = dict(task_row)
        updated_task["status"] = "completed"
        updated_task["completed_at"] = completed_at
        updated_task["lifecycle_state"] = "completed"

        response_payload = {
            "request_id": request_id,
            "task": dict(updated_task),
        }
        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type=TASK_COMPLETED_EVENT_CANONICAL,
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:{TASK_COMPLETED_EVENT_CANONICAL}",
            payload={
                "request_id": request_id,
                "task_id": task_id,
                "completed_at": completed_at,
                "task": dict(updated_task),
                "response": response_payload,
            },
        )

        return {
            "status": "accepted",
            "response": response_payload,
            "effects": [
                {
                    "task_id": task_id,
                    "status": "completed",
                }
            ],
        }

    def _handle_schedule_create_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        _ = precomputed_projection
        normalized = self._normalize_schedule_create_payload(payload=payload, request_id=request_id)

        schedule_record = {
            "schedule_id": normalized["schedule_id"],
            "request_id": request_id,
            "title": normalized["title"],
            "start_at": normalized["start_at"],
            "end_at": normalized["end_at"],
            "member_id": str(payload.get("member_id") or actor.user_id).strip() or actor.user_id,
            "source_message_id": str(payload.get("source_message_id") or "").strip() or None,
            "promotion_reason": str(payload.get("promotion_reason") or "").strip() or None,
            "interpretation_type": str(payload.get("interpretation_type") or "").strip() or None,
            "status": "scheduled",
            "created_at": _utc_iso(),
            "cancelled_at": None,
        }
        response_payload = {
            "request_id": request_id,
            "schedule": dict(schedule_record),
        }
        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type=SCHEDULE_CREATED_EVENT_CANONICAL,
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:{SCHEDULE_CREATED_EVENT_CANONICAL}",
            payload={
                "request_id": request_id,
                "schedule": dict(schedule_record),
                "response": response_payload,
                "recorded_at": _utc_iso(),
            },
        )

        return {
            "status": "accepted",
            "response": response_payload,
            "effects": [
                {
                    "schedule_id": normalized["schedule_id"],
                    "status": "scheduled",
                }
            ],
        }

    def _handle_schedule_cancel_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        projection = (
            precomputed_projection
            if isinstance(precomputed_projection, dict)
            else self.get_projection(household_id)
        )
        schedule_id = str(payload.get("schedule_id") or "").strip()
        schedules = projection.get("schedules") if isinstance(projection.get("schedules"), dict) else {}
        schedule_row = schedules.get(schedule_id) if isinstance(schedules, dict) else None
        if not schedule_id or not isinstance(schedule_row, dict):
            raise ValueError("schedule.cancel requires an existing schedule_id")

        cancelled_at = _utc_iso()
        updated_schedule = dict(schedule_row)
        updated_schedule["status"] = "cancelled"
        updated_schedule["cancelled_at"] = cancelled_at

        response_payload = {
            "request_id": request_id,
            "schedule": dict(updated_schedule),
        }
        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type=SCHEDULE_CANCELLED_EVENT_CANONICAL,
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:{SCHEDULE_CANCELLED_EVENT_CANONICAL}",
            payload={
                "request_id": request_id,
                "schedule_id": schedule_id,
                "cancelled_at": cancelled_at,
                "schedule": dict(updated_schedule),
                "response": response_payload,
            },
        )

        return {
            "status": "accepted",
            "response": response_payload,
            "effects": [
                {
                    "schedule_id": schedule_id,
                    "status": "cancelled",
                }
            ],
        }

    def _handle_reminder_create_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        _ = precomputed_projection
        normalized = self._normalize_reminder_create_payload(payload=payload, request_id=request_id)

        reminder_record = {
            "reminder_id": normalized["reminder_id"],
            "request_id": request_id,
            "title": normalized["title"],
            "message": normalized["message"],
            "trigger_at": normalized["trigger_at"],
            "status": "active",
            "created_at": _utc_iso(),
            "triggered_at": None,
        }
        response_payload = {
            "request_id": request_id,
            "reminder": dict(reminder_record),
        }
        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type=REMINDER_CREATED_EVENT_CANONICAL,
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:{REMINDER_CREATED_EVENT_CANONICAL}",
            payload={
                "request_id": request_id,
                "reminder": dict(reminder_record),
                "response": response_payload,
                "recorded_at": _utc_iso(),
            },
        )

        return {
            "status": "accepted",
            "response": response_payload,
            "effects": [
                {
                    "reminder_id": normalized["reminder_id"],
                    "status": "active",
                }
            ],
        }

    def _handle_reminder_cancel_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        projection = (
            precomputed_projection
            if isinstance(precomputed_projection, dict)
            else self.get_projection(household_id)
        )
        reminder_id = str(payload.get("reminder_id") or "").strip()
        reminders = projection.get("reminders") if isinstance(projection.get("reminders"), dict) else {}
        reminder_row = reminders.get(reminder_id) if isinstance(reminders, dict) else None
        if not reminder_id or not isinstance(reminder_row, dict):
            raise ValueError("reminder.cancel requires an existing reminder_id")

        updated_reminder = dict(reminder_row)
        updated_reminder["status"] = "cancelled"

        response_payload = {
            "request_id": request_id,
            "reminder": dict(updated_reminder),
        }
        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type=REMINDER_CANCELLED_EVENT_CANONICAL,
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:{REMINDER_CANCELLED_EVENT_CANONICAL}",
            payload={
                "request_id": request_id,
                "reminder_id": reminder_id,
                "reminder": dict(updated_reminder),
                "response": response_payload,
            },
        )

        return {
            "status": "accepted",
            "response": response_payload,
            "effects": [
                {
                    "reminder_id": reminder_id,
                    "status": "cancelled",
                }
            ],
        }

    def _handle_reminder_trigger_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        projection = (
            precomputed_projection
            if isinstance(precomputed_projection, dict)
            else self.get_projection(household_id)
        )
        reminder_id = str(payload.get("reminder_id") or "").strip()
        reminders = projection.get("reminders") if isinstance(projection.get("reminders"), dict) else {}
        reminder_row = reminders.get(reminder_id) if isinstance(reminders, dict) else None
        if not reminder_id or not isinstance(reminder_row, dict):
            raise ValueError("reminder.trigger requires an existing reminder_id")

        triggered_at_raw = str(payload.get("triggered_at") or "").strip()
        triggered_at = (
            self._normalize_reminder_timestamp(triggered_at_raw)
            if triggered_at_raw
            else _utc_iso()
        )

        updated_reminder = dict(reminder_row)
        updated_reminder["status"] = "triggered"
        updated_reminder["triggered_at"] = triggered_at

        response_payload = {
            "request_id": request_id,
            "reminder": dict(updated_reminder),
        }
        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type=REMINDER_TRIGGERED_EVENT_CANONICAL,
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:{REMINDER_TRIGGERED_EVENT_CANONICAL}",
            payload={
                "request_id": request_id,
                "reminder_id": reminder_id,
                "triggered_at": triggered_at,
                "reminder": dict(updated_reminder),
                "response": response_payload,
            },
        )

        return {
            "status": "accepted",
            "response": response_payload,
            "effects": [
                {
                    "reminder_id": reminder_id,
                    "status": "triggered",
                }
            ],
        }

    def _decision_cards_from_projection(
        self,
        projection: Mapping[str, Any],
    ) -> dict[str, dict[str, Any]]:
        decision_cards_payload = projection.get("decision_cards")
        if not isinstance(decision_cards_payload, Mapping):
            return {}
        return {
            str(decision_card_id): dict(row)
            for decision_card_id, row in decision_cards_payload.items()
            if isinstance(row, Mapping)
        }

    def _require_decision_card_authority(
        self,
        *,
        decision_cards: Mapping[str, Mapping[str, Any]],
        decision_id: str,
        command_type: str,
        allowed_states: frozenset[str],
    ) -> dict[str, Any]:
        normalized_decision_id = decision_id.strip()
        if not normalized_decision_id:
            raise ValueError(f"{command_type} requires decision_id")

        decision_card_row = decision_cards.get(normalized_decision_id)
        if not isinstance(decision_card_row, Mapping):
            raise ValueError(
                f"{command_type} requires canonical decision card for decision_id={normalized_decision_id}"
            )

        decision_card = dict(decision_card_row)
        origin_api = str(decision_card.get("origin_api") or "").strip()
        if origin_api != DECISION_CARD_CANONICAL_ORIGIN_API:
            raise ValueError(
                f"{command_type} rejected non-canonical decision card origin for decision_id={normalized_decision_id}"
            )

        card_id = str(decision_card.get("decision_card_id") or normalized_decision_id).strip()
        if card_id != normalized_decision_id:
            raise ValueError(
                f"{command_type} requires decision_card_id alignment for decision_id={normalized_decision_id}"
            )

        card_state = str(decision_card.get("state") or "").strip().lower()
        if card_state not in allowed_states:
            allowed = ", ".join(sorted(allowed_states))
            raise ValueError(
                f"{command_type} requires decision card state in [{allowed}] for decision_id={normalized_decision_id}; got={card_state or 'missing'}"
            )

        return decision_card

    def _emit_validated_decision_card_event(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        event_type: str,
        event_payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
        decision_cards_override: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, dict[str, Any]]:
        source_cards: Mapping[str, Mapping[str, Any]]
        if decision_cards_override is not None:
            source_cards = decision_cards_override
        else:
            projection = (
                precomputed_projection
                if isinstance(precomputed_projection, dict)
                else self.get_projection(household_id)
            )
            source_cards = self._decision_cards_from_projection(projection)

        normalized_event_payload = dict(event_payload)
        normalized_event_payload.setdefault("request_id", request_id)
        normalized_event_payload["timestamp"] = str(
            normalized_event_payload.get("timestamp") or _utc_iso()
        )

        try:
            next_cards = reduce_decision_card_projection(
                event_type=event_type,
                payload=normalized_event_payload,
                recorded_at=str(normalized_event_payload["timestamp"]),
                decision_cards=source_cards,
                strict=True,
            )
        except DecisionCardInvariantError as exc:
            raise ValueError(str(exc)) from exc

        decision_card_id = str(normalized_event_payload.get("decision_card_id") or "").strip()
        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type=event_type,
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:{event_type}:{decision_card_id}",
            payload=normalized_event_payload,
        )
        return next_cards

    def _handle_decision_card_create_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        root_cause_key = str(payload.get("root_cause_key") or "").strip()
        title = str(payload.get("title") or "").strip()
        contract_version = str(payload.get("contract_version") or "").strip() or DECISION_CARD_CONTRACT_VERSION
        metadata = payload.get("metadata")

        decision_card_event = createDecisionCard(
            household_id=household_id,
            root_cause_key=root_cause_key,
            title=title,
            actor_id=actor.user_id,
            timestamp=_utc_now(),
            contract_version=contract_version,
            metadata=metadata if isinstance(metadata, Mapping) else None,
            decision_card_id=str(payload.get("decision_card_id") or "").strip() or None,
        )
        event_payload = dict(decision_card_event)
        event_type = str(event_payload.pop("event_type", DECISION_CARD_GENERATED_EVENT_CANONICAL))

        next_cards = self._emit_validated_decision_card_event(
            household_id=household_id,
            actor=actor,
            request_id=request_id,
            event_type=event_type,
            event_payload=event_payload,
            precomputed_projection=precomputed_projection,
        )

        decision_card_id = str(event_payload.get("decision_card_id") or "").strip()
        return {
            "status": "accepted",
            "response": {
                "request_id": request_id,
                "decision_card_id": decision_card_id,
                "event_type": event_type,
                "decision_card": dict(next_cards.get(decision_card_id) or {}),
            },
            "effects": [
                {
                    "decision_card_id": decision_card_id,
                    "action": "generated",
                }
            ],
        }

    def _handle_decision_card_surface_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        decision_card_id = str(payload.get("decision_card_id") or "").strip()
        if not decision_card_id:
            raise ValueError("decision.card.surface requires decision_card_id")

        next_cards = self._emit_validated_decision_card_event(
            household_id=household_id,
            actor=actor,
            request_id=request_id,
            event_type=DECISION_CARD_SURFACED_EVENT_CANONICAL,
            event_payload={
                "decision_card_id": decision_card_id,
                "actor_id": actor.user_id,
                "timestamp": _utc_iso(),
            },
            precomputed_projection=precomputed_projection,
        )

        return {
            "status": "accepted",
            "response": {
                "request_id": request_id,
                "decision_card_id": decision_card_id,
                "event_type": DECISION_CARD_SURFACED_EVENT_CANONICAL,
                "decision_card": dict(next_cards.get(decision_card_id) or {}),
            },
            "effects": [
                {
                    "decision_card_id": decision_card_id,
                    "action": "surfaced",
                }
            ],
        }

    def _handle_decision_card_acknowledge_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        decision_card_id = str(payload.get("decision_card_id") or "").strip()
        if not decision_card_id:
            raise ValueError("decision.card.acknowledge requires decision_card_id")

        next_cards = self._emit_validated_decision_card_event(
            household_id=household_id,
            actor=actor,
            request_id=request_id,
            event_type=DECISION_CARD_ACKNOWLEDGED_EVENT_CANONICAL,
            event_payload={
                "decision_card_id": decision_card_id,
                "actor_id": actor.user_id,
                "timestamp": _utc_iso(),
            },
            precomputed_projection=precomputed_projection,
        )

        return {
            "status": "accepted",
            "response": {
                "request_id": request_id,
                "decision_card_id": decision_card_id,
                "event_type": DECISION_CARD_ACKNOWLEDGED_EVENT_CANONICAL,
                "decision_card": dict(next_cards.get(decision_card_id) or {}),
            },
            "effects": [
                {
                    "decision_card_id": decision_card_id,
                    "action": "acknowledged",
                }
            ],
        }

    def _handle_decision_card_resolve_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        decision_card_id = str(payload.get("decision_card_id") or "").strip()
        if not decision_card_id:
            raise ValueError("decision.card.resolve requires decision_card_id")

        next_cards = self._emit_validated_decision_card_event(
            household_id=household_id,
            actor=actor,
            request_id=request_id,
            event_type=DECISION_CARD_RESOLVED_EVENT_CANONICAL,
            event_payload={
                "decision_card_id": decision_card_id,
                "actor_id": actor.user_id,
                "timestamp": _utc_iso(),
                "resolution_kind": str(payload.get("resolution_kind") or "manual").strip() or "manual",
            },
            precomputed_projection=precomputed_projection,
        )

        return {
            "status": "accepted",
            "response": {
                "request_id": request_id,
                "decision_card_id": decision_card_id,
                "event_type": DECISION_CARD_RESOLVED_EVENT_CANONICAL,
                "decision_card": dict(next_cards.get(decision_card_id) or {}),
            },
            "effects": [
                {
                    "decision_card_id": decision_card_id,
                    "action": "resolved",
                }
            ],
        }

    def _handle_decision_complete_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        decision_id = str(payload.get("decision_id") or "").strip()
        if not decision_id:
            raise ValueError("decision.complete requires decision_id")

        projection = (
            precomputed_projection
            if isinstance(precomputed_projection, dict)
            else self.get_projection(household_id)
        )
        decision_cards = self._decision_cards_from_projection(projection)

        decision_card: Mapping[str, Any] | None = None
        try:
            decision_card = self._require_decision_card_authority(
                decision_cards=decision_cards,
                decision_id=decision_id,
                command_type="decision.complete",
                allowed_states=frozenset({DECISION_CARD_STATE_ACKNOWLEDGED, DECISION_CARD_STATE_RESOLVED}),
            )
        except ValueError:
            current_card = decision_cards.get(decision_id) if isinstance(decision_cards, Mapping) else None
            current_state = str(current_card.get("state") or "").strip().lower() if isinstance(current_card, Mapping) else ""

            if current_state == DECISION_CARD_STATE_APPLIED:
                raise ValueError(f"decision card already applied for decision_card_id={decision_id}")

            if current_state == "generated":
                self._handle_decision_card_surface_command(
                    household_id=household_id,
                    actor=actor,
                    request_id=f"{request_id}:legacy:surface:{decision_id}",
                    payload={"decision_card_id": decision_id},
                    precomputed_projection=None,
                )
                self._handle_decision_card_acknowledge_command(
                    household_id=household_id,
                    actor=actor,
                    request_id=f"{request_id}:legacy:ack:{decision_id}",
                    payload={"decision_card_id": decision_id},
                    precomputed_projection=None,
                )
            elif current_state == "surfaced":
                self._handle_decision_card_acknowledge_command(
                    household_id=household_id,
                    actor=actor,
                    request_id=f"{request_id}:legacy:ack:{decision_id}",
                    payload={"decision_card_id": decision_id},
                    precomputed_projection=None,
                )
            elif current_state in {"acknowledged", "resolved"}:
                pass
            else:
                tasks_payload = projection.get("tasks") if isinstance(projection.get("tasks"), Mapping) else {}
                task_row = tasks_payload.get(decision_id) if isinstance(tasks_payload, Mapping) else None
                task_title = str(task_row.get("title") or "").strip() if isinstance(task_row, Mapping) else ""
                synthesized_title = task_title or f"Resolve legacy decision {decision_id}"

                self._handle_decision_card_create_command(
                    household_id=household_id,
                    actor=actor,
                    request_id=f"{request_id}:legacy:create:{decision_id}",
                    payload={
                        "decision_card_id": decision_id,
                        "root_cause_key": f"legacy:{decision_id}:decision-complete",
                        "title": synthesized_title,
                    },
                    precomputed_projection=None,
                )
                self._handle_decision_card_surface_command(
                    household_id=household_id,
                    actor=actor,
                    request_id=f"{request_id}:legacy:surface:{decision_id}",
                    payload={"decision_card_id": decision_id},
                    precomputed_projection=None,
                )
                self._handle_decision_card_acknowledge_command(
                    household_id=household_id,
                    actor=actor,
                    request_id=f"{request_id}:legacy:ack:{decision_id}",
                    payload={"decision_card_id": decision_id},
                    precomputed_projection=None,
                )

            projection = self.get_projection(household_id)
            decision_cards = self._decision_cards_from_projection(projection)
            decision_card = self._require_decision_card_authority(
                decision_cards=decision_cards,
                decision_id=decision_id,
                command_type="decision.complete",
                allowed_states=frozenset({DECISION_CARD_STATE_ACKNOWLEDGED, DECISION_CARD_STATE_RESOLVED}),
            )

        if decision_card is not None:
            current_state = str(decision_card.get("state") or "").strip().lower()
            if current_state == DECISION_CARD_STATE_APPLIED:
                raise ValueError(f"decision card already applied for decision_card_id={decision_id}")

            if current_state == DECISION_CARD_STATE_RESOLVED:
                decision_cards = self._emit_validated_decision_card_event(
                    household_id=household_id,
                    actor=actor,
                    request_id=request_id,
                    event_type=DECISION_CARD_APPLIED_EVENT_CANONICAL,
                    event_payload={
                        "decision_card_id": decision_id,
                        "actor_id": actor.user_id,
                        "timestamp": _utc_iso(),
                        "resolution_kind": "completed",
                    },
                    precomputed_projection=precomputed_projection,
                    decision_cards_override=decision_cards,
                )
            else:
                decision_cards = self._emit_validated_decision_card_event(
                    household_id=household_id,
                    actor=actor,
                    request_id=request_id,
                    event_type=DECISION_CARD_RESOLVED_EVENT_CANONICAL,
                    event_payload={
                        "decision_card_id": decision_id,
                        "actor_id": actor.user_id,
                        "timestamp": _utc_iso(),
                        "resolution_kind": "completed",
                    },
                    precomputed_projection=precomputed_projection,
                    decision_cards_override=decision_cards,
                )
                self._emit_validated_decision_card_event(
                    household_id=household_id,
                    actor=actor,
                    request_id=request_id,
                    event_type=DECISION_CARD_APPLIED_EVENT_CANONICAL,
                    event_payload={
                        "decision_card_id": decision_id,
                        "actor_id": actor.user_id,
                        "timestamp": _utc_iso(),
                        "resolution_kind": "completed",
                    },
                    precomputed_projection=precomputed_projection,
                    decision_cards_override=decision_cards,
                )

        decision_events = handle_decision_complete(
            household_id=household_id,
            decision_id=decision_id,
            actor_id=actor.user_id,
            timestamp=_utc_now(),
        )
        event_payload = dict(decision_events[0])
        event_type = str(event_payload.pop("event_type", DECISION_COMPLETED_EVENT_CANONICAL))

        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type=event_type,
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:{event_type}:{decision_id}",
            payload=event_payload,
        )

        return {
            "status": "accepted",
            "response": {
                "request_id": request_id,
                "decision_id": decision_id,
                "event_type": event_type,
            },
            "effects": [
                {
                    "decision_id": decision_id,
                    "action": "complete",
                }
            ],
        }

    def _handle_decision_defer_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        decision_id = str(payload.get("decision_id") or "").strip()
        defer_to_date_raw = str(payload.get("defer_to_date") or "").strip()
        if not defer_to_date_raw:
            raise ValueError("decision.defer requires defer_to_date")

        try:
            defer_to_date = date_value.fromisoformat(defer_to_date_raw)
        except ValueError as exc:
            raise ValueError("decision.defer requires ISO-8601 defer_to_date") from exc

        projection = (
            precomputed_projection
            if isinstance(precomputed_projection, dict)
            else self.get_projection(household_id)
        )
        decision_cards = self._decision_cards_from_projection(projection)
        self._require_decision_card_authority(
            decision_cards=decision_cards,
            decision_id=decision_id,
            command_type="decision.defer",
            allowed_states=frozenset({DECISION_CARD_STATE_ACKNOWLEDGED}),
        )
        self._emit_validated_decision_card_event(
            household_id=household_id,
            actor=actor,
            request_id=request_id,
            event_type=DECISION_CARD_RESOLVED_EVENT_CANONICAL,
            event_payload={
                "decision_card_id": decision_id,
                "actor_id": actor.user_id,
                "timestamp": _utc_iso(),
                "defer_to_date": defer_to_date.isoformat(),
                "resolution_kind": "deferred",
            },
            precomputed_projection=precomputed_projection,
            decision_cards_override=decision_cards,
        )

        decision_events = handle_decision_defer(
            household_id=household_id,
            decision_id=decision_id,
            actor_id=actor.user_id,
            timestamp=_utc_now(),
            defer_to_date=defer_to_date,
        )
        event_payload = dict(decision_events[0])
        event_type = str(event_payload.pop("event_type", DECISION_DEFERRED_EVENT_CANONICAL))

        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type=event_type,
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:{event_type}:{decision_id}",
            payload=event_payload,
        )

        return {
            "status": "accepted",
            "response": {
                "request_id": request_id,
                "decision_id": decision_id,
                "defer_to_date": defer_to_date.isoformat(),
                "event_type": event_type,
            },
            "effects": [
                {
                    "decision_id": decision_id,
                    "action": "defer",
                    "defer_to_date": defer_to_date.isoformat(),
                }
            ],
        }

    def _handle_decision_ignore_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        precomputed_projection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        decision_id = str(payload.get("decision_id") or "").strip()
        projection = (
            precomputed_projection
            if isinstance(precomputed_projection, dict)
            else self.get_projection(household_id)
        )
        decision_cards = self._decision_cards_from_projection(projection)
        self._require_decision_card_authority(
            decision_cards=decision_cards,
            decision_id=decision_id,
            command_type="decision.ignore",
            allowed_states=frozenset({DECISION_CARD_STATE_ACKNOWLEDGED}),
        )
        self._emit_validated_decision_card_event(
            household_id=household_id,
            actor=actor,
            request_id=request_id,
            event_type=DECISION_CARD_RESOLVED_EVENT_CANONICAL,
            event_payload={
                "decision_card_id": decision_id,
                "actor_id": actor.user_id,
                "timestamp": _utc_iso(),
                "resolution_kind": "ignored",
            },
            precomputed_projection=precomputed_projection,
            decision_cards_override=decision_cards,
        )

        decision_events = handle_decision_ignore(
            household_id=household_id,
            decision_id=decision_id,
            actor_id=actor.user_id,
            timestamp=_utc_now(),
        )
        event_payload = dict(decision_events[0])
        event_type = str(event_payload.pop("event_type", DECISION_IGNORED_EVENT_CANONICAL))

        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type=event_type,
            source="runtime.action_pipeline",
            idempotency_key=f"{request_id}:{event_type}:{decision_id}",
            payload=event_payload,
        )

        return {
            "status": "accepted",
            "response": {
                "request_id": request_id,
                "decision_id": decision_id,
                "event_type": event_type,
            },
            "effects": [
                {
                    "decision_id": decision_id,
                    "action": "ignore",
                }
            ],
        }

    def _handle_saga_execute_command(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        payload: dict[str, Any],
        policy_resolution: PolicyResolution,
    ) -> dict[str, Any]:
        definition = self._build_saga_definition(payload=payload, request_id=request_id)
        orchestrator = SagaOrchestrator()

        def emit_saga_event(*, event_type: str, payload: dict[str, Any], idempotency_key: str | None = None) -> object | None:
            return self._emit_event(
                household_id=household_id,
                actor=actor,
                event_type=event_type,
                source="runtime.action_pipeline",
                payload=payload,
                idempotency_key=idempotency_key,
            )

        def read_saga_events() -> list[dict[str, Any]]:
            rows = self._event_log.get_event_logs(household_id=household_id, limit=5000)
            ordered_rows = sorted(rows, key=lambda row: (row.timestamp, row.event_id))
            return [
                {
                    "event_id": row.event_id,
                    "event_type": row.type,
                    "timestamp": row.timestamp,
                    "household_id": str(getattr(row, "household_id", "") or household_id),
                    "payload": dict(row.payload or {}) if isinstance(row.payload, dict) else {},
                    "source": str(getattr(row, "source", "runtime.action_pipeline")),
                }
                for row in ordered_rows
            ]

        control_decision = self._control_plane.evaluate_execution(
            definition=definition,
            household_id=household_id,
            request_id=request_id,
            emit_event=emit_saga_event,
            read_events=read_saga_events,
            policy_snapshot={
                **dict(policy_resolution.control_plane_thresholds_snapshot),
                "policy_version_id": policy_resolution.version_id,
            },
        )

        if not control_decision.allowed:
            return {
                "status": control_decision.status,
                "response": {
                    "request_id": request_id,
                    "saga": {
                        "saga_id": definition.id,
                        "status": control_decision.status,
                        "executed_steps": [],
                        "failed_step": None,
                        "compensated_steps": [],
                        "replay_validation": control_decision.replay_validation,
                    },
                    "control": {
                        "policy_version_id": policy_resolution.version_id,
                        "status": control_decision.status,
                        "reason": control_decision.reason,
                        "emitted_events": control_decision.emitted_events,
                        "circuit_state": control_decision.circuit_state,
                        "failure_snapshot": control_decision.failure_snapshot,
                        "conflict_snapshot": control_decision.conflict_snapshot,
                        "replay_validation": control_decision.replay_validation,
                    },
                },
                "effects": [
                    {
                        "saga_id": definition.id,
                        "status": control_decision.status,
                        "reason": control_decision.reason,
                    }
                ],
            }

        result = orchestrator.execute(
            definition=definition,
            emit_event=emit_saga_event,
            read_events=read_saga_events,
            request_id=request_id,
            household_id=household_id,
        )

        runtime_status = "committed" if result.status == "completed" else result.status
        return {
            "status": runtime_status,
            "response": {
                "request_id": request_id,
                "saga": {
                    "saga_id": result.saga_id,
                    "status": result.status,
                    "executed_steps": result.executed_steps,
                    "failed_step": result.failed_step,
                    "compensated_steps": result.compensated_steps,
                    "replay_validation": result.replay_validation,
                },
                "control": {
                    "policy_version_id": policy_resolution.version_id,
                    "status": control_decision.status,
                    "reason": control_decision.reason,
                    "emitted_events": control_decision.emitted_events,
                    "circuit_state": control_decision.circuit_state,
                    "failure_snapshot": control_decision.failure_snapshot,
                    "conflict_snapshot": control_decision.conflict_snapshot,
                    "replay_validation": control_decision.replay_validation,
                },
            },
            "effects": [
                {
                    "saga_id": result.saga_id,
                    "status": result.status,
                }
            ],
        }

    def _build_saga_definition(self, *, payload: dict[str, Any], request_id: str) -> SagaDefinition:
        saga_id = str(payload.get("saga_id") or "").strip()
        if not saga_id:
            raise ValueError("saga.execute requires saga_id")

        raw_steps = payload.get("steps")
        if not isinstance(raw_steps, list) or not raw_steps:
            raise ValueError("saga.execute requires a non-empty steps array")

        steps: list[SagaStepDefinition] = []
        for index, raw_step in enumerate(raw_steps, start=1):
            if not isinstance(raw_step, dict):
                raise ValueError(f"saga.execute step at index {index} must be an object")

            event_emitted = str(raw_step.get("event_emitted") or raw_step.get("event_type") or "").strip()
            step_id = str(raw_step.get("step_id") or f"step_{index}").strip()
            success_condition = raw_step.get("success_condition")
            failure_condition = raw_step.get("failure_condition")
            metadata = raw_step.get("metadata")

            if success_condition is None:
                success_condition = {}
            if failure_condition is None:
                failure_condition = {}
            if metadata is None:
                metadata = {}

            if not isinstance(success_condition, dict):
                raise ValueError(f"saga.execute step {step_id} success_condition must be an object")
            if not isinstance(failure_condition, dict):
                raise ValueError(f"saga.execute step {step_id} failure_condition must be an object")
            if not isinstance(metadata, dict):
                raise ValueError(f"saga.execute step {step_id} metadata must be an object")

            compensation_event = str(raw_step.get("compensation_event") or "").strip() or None
            steps.append(
                SagaStepDefinition(
                    step_id=step_id,
                    event_emitted=event_emitted,
                    success_condition=dict(success_condition),
                    failure_condition=dict(failure_condition),
                    compensation_event=compensation_event,
                    metadata=dict(metadata),
                )
            )

        compensation_steps_raw = payload.get("compensation_steps")
        compensation_steps: list[SagaStepDefinition] = []
        if isinstance(compensation_steps_raw, list):
            step_index = {step.step_id: step for step in steps}
            for entry in compensation_steps_raw:
                if not isinstance(entry, dict):
                    raise ValueError("saga.execute compensation_steps entries must be objects")

                step_id = str(entry.get("step_id") or "").strip()
                if not step_id:
                    raise ValueError("saga.execute compensation_steps requires step_id")

                base_step = step_index.get(step_id)
                if base_step is None:
                    raise ValueError(f"saga.execute compensation_steps references unknown step_id {step_id}")

                compensation_event = str(entry.get("compensation_event") or base_step.compensation_event or "").strip()
                compensation_steps.append(
                    SagaStepDefinition(
                        step_id=base_step.step_id,
                        event_emitted=base_step.event_emitted,
                        success_condition=dict(base_step.success_condition),
                        failure_condition=dict(base_step.failure_condition),
                        compensation_event=compensation_event or None,
                        metadata=dict(base_step.metadata),
                    )
                )
        else:
            compensation_steps = [step for step in steps if step.compensation_event]

        metadata = payload.get("metadata")
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, dict):
            raise ValueError("saga.execute metadata must be an object")

        idempotency_key = str(payload.get("idempotency_key") or f"saga:{saga_id}:{request_id}").strip()
        return SagaDefinition(
            id=saga_id,
            steps=tuple(steps),
            compensation_steps=tuple(compensation_steps),
            metadata=dict(metadata),
            idempotency_key=idempotency_key,
        )

    def _normalize_household_message_payload(
        self,
        *,
        payload: dict[str, Any],
        request_id: str,
        default_member_id: str,
    ) -> dict[str, str]:
        raw_content = str(payload.get("raw_content") or payload.get("content") or "").strip()
        if not raw_content:
            raise ValueError("household.message.ingest raw_content is required")

        raw_source = str(payload.get("source") or "manual").strip().lower()
        source = raw_source if raw_source in HOUSEHOLD_MESSAGE_SOURCE_VALUES else "manual"

        created_at_raw = str(payload.get("created_at") or "").strip()
        created_at = self._normalize_schedule_timestamp(created_at_raw) if created_at_raw else _utc_iso()

        member_id = str(payload.get("member_id") or payload.get("user_id") or default_member_id).strip()
        if not member_id:
            member_id = "system"

        message_id = str(payload.get("message_id") or "").strip() or f"message-{request_id}"
        return {
            "message_id": message_id,
            "source": source,
            "raw_content": raw_content,
            "created_at": created_at,
            "member_id": member_id,
        }

    def _create_household_message_promotion_decision_card(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        message_id: str,
        raw_content: str,
        source: str,
        member_id: str,
        classification: str,
        interpretation_type: str,
        promotion_reason: str,
        precomputed_projection: dict[str, Any] | None,
        context: Mapping[str, Any] | None = None,
        decision_card_id: str | None = None,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "source_message_id": message_id,
            "classification": classification,
            "interpretation_type": interpretation_type,
            "promotion_reason": promotion_reason,
            "source": source,
            "member_id": member_id,
            "raw_content": raw_content,
        }
        if isinstance(context, Mapping):
            for key, value in context.items():
                if value is None:
                    continue
                normalized_value = str(value).strip() if isinstance(value, str) else value
                if normalized_value in ("", []):
                    continue
                metadata[str(key)] = normalized_value

        resolved_decision_card_id = str(decision_card_id or "").strip() or f"decision-msg-{message_id}"
        return self._handle_decision_card_create_command(
            household_id=household_id,
            actor=actor,
            request_id=request_id,
            payload={
                "decision_card_id": resolved_decision_card_id,
                "root_cause_key": f"message:{message_id}:promotion:{interpretation_type}",
                "title": self._decision_title_from_household_message(raw_content),
                "metadata": metadata,
            },
            precomputed_projection=precomputed_projection,
        )

    def _open_decision_cards_from_projection(
        self,
        *,
        projection: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        rows = self._decision_cards_from_projection(projection)
        open_rows: list[dict[str, Any]] = []
        for decision_card_id, row in rows.items():
            state = str(row.get("state") or "").strip().lower()
            if state not in HOUSEHOLD_OPEN_DECISION_CARD_STATES:
                continue
            normalized = dict(row)
            normalized["decision_card_id"] = str(row.get("decision_card_id") or decision_card_id)
            open_rows.append(normalized)
        open_rows.sort(
            key=lambda row: (
                str(row.get("updated_at") or row.get("created_at") or ""),
                str(row.get("decision_card_id") or ""),
            )
        )
        return open_rows

    def _decision_priority_for_message(
        self,
        *,
        interpretation_type: str,
        interpretation: Mapping[str, Any],
        context: Mapping[str, Any] | None,
    ) -> str:
        upm_profile = interpretation.get("upm")
        if isinstance(upm_profile, Mapping):
            upm_class = str(upm_profile.get("priority_class") or "").strip().lower()
            if upm_class in {HOUSEHOLD_UPM_PRIORITY_CRITICAL, HOUSEHOLD_UPM_PRIORITY_HIGH}:
                return "high"
            if upm_class == HOUSEHOLD_UPM_PRIORITY_MEDIUM:
                return "medium"
        conflict_type = str((context or {}).get("conflict_type") or interpretation.get("conflict_type") or "").strip().lower()
        conflict_schedule_id = str(
            (context or {}).get("conflict_schedule_id") or interpretation.get("conflict_schedule_id") or ""
        ).strip()
        requires_decision = bool(interpretation.get("requires_decision"))
        if conflict_schedule_id or conflict_type in {"direct", "cross_member", "derived", "cascade"}:
            return "high"
        if interpretation_type in {"conflict_reported", "time_change", "cancellation", "deadline"} and requires_decision:
            return "high"
        if interpretation_type in {"schedule_create", "obligation"}:
            return "medium"
        return "low"

    def _decision_merge_key_for_message(
        self,
        *,
        interpretation_type: str,
        interpretation: Mapping[str, Any],
        promotion_target: str,
        context: Mapping[str, Any] | None,
    ) -> str:
        explicit_merge_key = str((context or {}).get("decision_merge_key") or "").strip().lower()
        if explicit_merge_key:
            return explicit_merge_key

        conflict_schedule_id = str(
            (context or {}).get("conflict_schedule_id") or interpretation.get("conflict_schedule_id") or ""
        ).strip()
        if conflict_schedule_id:
            return f"conflict:{conflict_schedule_id}"

        dependency_schedule_id = str(interpretation.get("dependency_schedule_id") or "").strip()
        if dependency_schedule_id and interpretation_type in {"time_change", "cancellation"}:
            return f"schedule-change:{dependency_schedule_id}"

        if interpretation_type == "ambiguity":
            return "ambiguity:household"

        confidence = float(interpretation.get("confidence") or 0.0)
        if promotion_target == "decision" and confidence < HOUSEHOLD_MESSAGE_INTERPRETATION_CONFIDENCE_THRESHOLD:
            return "low-confidence:household"
        return ""

    def _find_open_decision_card_for_merge_key(
        self,
        *,
        projection: Mapping[str, Any],
        merge_key: str,
    ) -> str:
        normalized_merge_key = str(merge_key or "").strip().lower()
        if not normalized_merge_key:
            return ""

        for row in self._open_decision_cards_from_projection(projection=projection):
            metadata = row.get("metadata")
            if not isinstance(metadata, Mapping):
                continue
            existing_merge_key = str(metadata.get("decision_merge_key") or "").strip().lower()
            if existing_merge_key != normalized_merge_key:
                continue
            return str(row.get("decision_card_id") or "").strip()
        return ""

    def _upm_resolution_acknowledgment_for_suppression(
        self,
        *,
        projection: Mapping[str, Any],
        suppression_reason: str,
        merged_decision_card_id: str,
        decision_merge_key: str,
        interpretation: Mapping[str, Any],
        interpretation_type: str,
        upm_profile: Mapping[str, Any],
    ) -> tuple[bool, str]:
        normalized_reason = str(suppression_reason or "").strip().lower()
        if normalized_reason not in {"merged_into_existing", "collapsed_into_low_priority_decision"}:
            return True, ""

        normalized_decision_id = str(merged_decision_card_id or "").strip()
        if not normalized_decision_id:
            return False, "resolution_unacknowledged"

        decision_cards = self._decision_cards_from_projection(projection)
        decision_card_row = decision_cards.get(normalized_decision_id)
        if not isinstance(decision_card_row, Mapping):
            return False, "resolution_unacknowledged"

        decision_state = str(decision_card_row.get("state") or "").strip().lower()
        if decision_state not in HOUSEHOLD_UPM_RESOLUTION_ACK_STATES:
            return False, "resolution_unacknowledged"

        metadata = decision_card_row.get("metadata")
        if not isinstance(metadata, Mapping):
            return False, "resolution_unacknowledged"

        normalized_merge_key = str(decision_merge_key or "").strip().lower()
        if normalized_merge_key:
            existing_merge_key = str(metadata.get("decision_merge_key") or "").strip().lower()
            if normalized_merge_key == existing_merge_key:
                return True, ""

        conflict_schedule_id = str(
            upm_profile.get("conflict_schedule_id") or interpretation.get("conflict_schedule_id") or ""
        ).strip()
        if conflict_schedule_id:
            existing_conflict_schedule_id = str(metadata.get("conflict_schedule_id") or "").strip()
            if conflict_schedule_id == existing_conflict_schedule_id:
                return True, ""

        dependency_schedule_id = str(interpretation.get("dependency_schedule_id") or "").strip()
        if dependency_schedule_id:
            existing_dependency_schedule_id = str(metadata.get("dependency_schedule_id") or "").strip()
            if dependency_schedule_id == existing_dependency_schedule_id:
                return True, ""

        normalized_interpretation_type = str(interpretation_type or "").strip().lower()
        existing_interpretation_type = str(metadata.get("interpretation_type") or "").strip().lower()
        if normalized_reason == "merged_into_existing" and normalized_interpretation_type:
            if normalized_interpretation_type == existing_interpretation_type:
                return True, ""

        if normalized_reason == "collapsed_into_low_priority_decision" and normalized_interpretation_type:
            if normalized_interpretation_type == existing_interpretation_type:
                decision_reason = str(metadata.get("decision_reason") or "").strip().lower()
                if decision_reason in HOUSEHOLD_LOW_PRIORITY_DECISION_REASONS:
                    return True, ""

        return False, "resolution_unacknowledged"

    def _decision_quality_filter(
        self,
        *,
        raw_content: str,
        interpretation: Mapping[str, Any],
        interpretation_type: str,
    ) -> dict[str, bool]:
        lowered = raw_content.lower()
        requires_decision = bool(interpretation.get("requires_decision"))
        due_at = str(interpretation.get("due_at") or "").strip()
        derived_start_at = str(interpretation.get("derived_start_at") or "").strip()
        conflict_schedule_id = str(interpretation.get("conflict_schedule_id") or "").strip()
        dependency_schedule_id = str(interpretation.get("dependency_schedule_id") or "").strip()

        is_actionable = interpretation_type != "informational" and (
            requires_decision
            or bool(conflict_schedule_id)
            or bool(due_at)
            or bool(derived_start_at)
            or "?" in lowered
        )
        is_time_sensitive = bool(due_at) or bool(derived_start_at) or bool(
            re.search(r"\b(today|tonight|tomorrow|asap|urgent|before|by|deadline|late)\b", lowered)
        )
        has_clear_options = (
            interpretation_type
            in {"conflict_reported", "time_change", "cancellation", "deadline", "obligation", "schedule_create", "ambiguity"}
            or bool(conflict_schedule_id)
            or bool(dependency_schedule_id)
        )
        can_be_auto_resolved = False
        if interpretation_type in {"cancellation", "time_change"}:
            can_be_auto_resolved = bool(dependency_schedule_id) and not bool(conflict_schedule_id)
        elif interpretation_type in {"deadline", "obligation", "schedule_create"}:
            can_be_auto_resolved = not requires_decision and not bool(conflict_schedule_id)
        elif interpretation_type == "ambiguity":
            can_be_auto_resolved = not is_time_sensitive

        return {
            "is_actionable": is_actionable,
            "is_time_sensitive": is_time_sensitive,
            "has_clear_options": has_clear_options,
            "can_be_auto_resolved": can_be_auto_resolved,
        }

    def _upm_priority_profile_for_message(
        self,
        *,
        projection: Mapping[str, Any],
        raw_content: str,
        interpretation_type: str,
        interpretation: Mapping[str, Any],
        member_id: str,
        context: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        lowered = raw_content.lower()
        requires_decision_hint = bool(interpretation.get("requires_decision"))
        confidence = _clip01(float(interpretation.get("confidence") or 0.0))
        dependency_schedule_id = str(
            (context or {}).get("dependency_schedule_id") or interpretation.get("dependency_schedule_id") or ""
        ).strip()
        due_at = str((context or {}).get("due_at") or interpretation.get("due_at") or "").strip()
        derived_start_at = str(
            (context or {}).get("derived_start_at") or interpretation.get("derived_start_at") or ""
        ).strip()
        derived_end_at = str(
            (context or {}).get("derived_end_at") or interpretation.get("derived_end_at") or ""
        ).strip()
        conflict_schedule_id = str(
            (context or {}).get("conflict_schedule_id") or interpretation.get("conflict_schedule_id") or ""
        ).strip()
        conflict_type = str(
            (context or {}).get("conflict_type") or interpretation.get("conflict_type") or ""
        ).strip().lower()
        conflict_events_involved = [
            str(item).strip()
            for item in list(
                (context or {}).get("conflict_events_involved")
                or interpretation.get("conflict_events_involved")
                or []
            )
            if str(item).strip()
        ]

        conflict_risk = bool(
            conflict_schedule_id
            or conflict_events_involved
            or conflict_type in {"direct", "cross_member", "derived", "cascade"}
        )

        if not conflict_risk and derived_start_at and derived_end_at:
            derived_conflict = self._detect_schedule_conflict_details(
                projection=projection,
                start_at=derived_start_at,
                end_at=derived_end_at,
                exclude_schedule_id=dependency_schedule_id or None,
                candidate_member_id=member_id,
                conflict_type_hint="derived",
            )
            detected_schedule_id = str(derived_conflict.get("conflict_schedule_id") or "").strip()
            if detected_schedule_id:
                conflict_risk = True
                conflict_schedule_id = detected_schedule_id
                conflict_type = str(derived_conflict.get("conflict_type") or "derived")
                conflict_events_involved = [
                    str(item).strip()
                    for item in list(derived_conflict.get("events_involved") or [])
                    if str(item).strip()
                ]

        if not conflict_risk and due_at:
            deadline_conflict = self._deadline_conflict_details(
                projection=projection,
                due_at=due_at,
            )
            detected_schedule_id = str(deadline_conflict.get("conflict_schedule_id") or "").strip()
            if detected_schedule_id:
                conflict_risk = True
                conflict_schedule_id = detected_schedule_id
                conflict_type = str(deadline_conflict.get("conflict_type") or "derived")
                conflict_events_involved = [
                    str(item).strip()
                    for item in list(deadline_conflict.get("events_involved") or [])
                    if str(item).strip()
                ]

        state_dependency = bool(
            dependency_schedule_id
            or conflict_schedule_id
            or conflict_events_involved
            or due_at
            or derived_start_at
        )
        if interpretation_type in {"cancellation", "time_change", "conflict_reported"}:
            state_dependency = True
        if re.search(r"\b(parent|child|family|pickup|dropoff|carpool|dinner|practice)\b", lowered):
            state_dependency = True

        priority_score = 0
        priority_signals: list[str] = []
        if interpretation_type == "conflict_reported":
            priority_score += 88
            priority_signals.append("reported_conflict")
        if interpretation_type in {"cancellation", "time_change"} and dependency_schedule_id:
            priority_score += 95
            priority_signals.append("schedule_modification_existing_event")
        elif interpretation_type in {"cancellation", "time_change"}:
            priority_score += 72
            priority_signals.append("schedule_modification_unresolved_anchor")
        if interpretation_type == "deadline":
            priority_score += 85
            priority_signals.append("deadline_change")
        if interpretation_type == "obligation":
            priority_score += 78
            priority_signals.append("obligation_introduction")
        if interpretation_type == "schedule_create":
            priority_score += 58
            priority_signals.append("schedule_create")
        if interpretation_type == "ambiguity":
            priority_score += 25
            priority_signals.append("ambiguity")
        if conflict_risk:
            priority_score += 26
            priority_signals.append("conflict_risk")
        if conflict_type in {"cross_member", "cascade"}:
            priority_score += 20
            priority_signals.append("state_dependency_overlap")
        if state_dependency:
            priority_score += 14
        if requires_decision_hint:
            priority_score += 18
        if confidence < HOUSEHOLD_MESSAGE_INTERPRETATION_CONFIDENCE_THRESHOLD and interpretation_type != "informational":
            priority_score += 5

        if bool(interpretation.get("promotional_noise")) and interpretation_type == "informational":
            priority_score -= 100
            priority_signals.append("noise")
        if interpretation_type == "informational" and not conflict_risk and not state_dependency:
            priority_score -= 35

        household_messages = projection.get("household_messages")
        recent_event_count = len(household_messages) if isinstance(household_messages, list) else 0
        if recent_event_count == 0:
            recent_event_count = len(self._schedule_rows_from_projection(projection=projection))

        event_density_score = _clip01(recent_event_count / float(HOUSEHOLD_UPM_DENSITY_WINDOW_MAX))
        decision_queue_size = len(self._open_decision_cards_from_projection(projection=projection))
        decision_queue_score = _clip01(decision_queue_size / float(HOUSEHOLD_UPM_DECISION_QUEUE_MAX))
        conflict_backlog_rows = projection.get("calendar_conflicts")
        conflict_backlog_count = len(conflict_backlog_rows) if isinstance(conflict_backlog_rows, list) else 0
        conflict_backlog_score = _clip01(conflict_backlog_count / float(HOUSEHOLD_UPM_CONFLICT_BACKLOG_MAX))
        household_state_load = _clip01(
            (event_density_score * 0.5)
            + (decision_queue_score * 0.3)
            + (conflict_backlog_score * 0.2)
        )
        suppression_profile = self._upm_historical_suppression_profile(projection=projection)
        suppression_delta = self._coerce_upm_score(
            suppression_profile.get("suppression_delta"),
            default=0.0,
        )
        conflict_frequency = self._coerce_upm_score(
            suppression_profile.get("conflict_frequency"),
            default=0.0,
        )
        decision_surface_gap = int(suppression_profile.get("decision_surface_gap") or 0)
        decision_surface_ratio = self._coerce_upm_score(
            suppression_profile.get("decision_surface_ratio"),
            default=1.0,
        )

        normalization_penalty = int(round((event_density_score * 12.0) + (decision_queue_score * 10.0)))
        conflict_backlog_boost = int(round(conflict_backlog_score * 8.0)) if conflict_risk else 0
        priority_score = max(0, priority_score - normalization_penalty + conflict_backlog_boost)

        if conflict_risk and priority_score < 72:
            priority_score = 72
        if state_dependency and interpretation_type in {"time_change", "cancellation", "deadline"} and priority_score < 48:
            priority_score = 48

        if priority_score >= 110:
            priority_class = HOUSEHOLD_UPM_PRIORITY_CRITICAL
        elif priority_score >= 84:
            priority_class = HOUSEHOLD_UPM_PRIORITY_HIGH
        elif priority_score >= 58:
            priority_class = HOUSEHOLD_UPM_PRIORITY_MEDIUM
        elif priority_score >= 34:
            priority_class = HOUSEHOLD_UPM_PRIORITY_LOW
        else:
            priority_class = HOUSEHOLD_UPM_PRIORITY_NOISE

        if priority_class in {HOUSEHOLD_UPM_PRIORITY_CRITICAL, HOUSEHOLD_UPM_PRIORITY_HIGH}:
            requires_decision = True
        elif priority_class == HOUSEHOLD_UPM_PRIORITY_MEDIUM:
            requires_decision = bool(
                requires_decision_hint
                or state_dependency
                or "?" in lowered
            )
        elif priority_class == HOUSEHOLD_UPM_PRIORITY_LOW:
            requires_decision = bool(requires_decision_hint and (conflict_risk or state_dependency))
        else:
            requires_decision = False

        if conflict_risk:
            requires_decision = True
        if state_dependency and interpretation_type in {"time_change", "cancellation", "deadline", "conflict_reported"}:
            requires_decision = True

        is_actionable_hint = interpretation_type != "informational"
        if interpretation_type == "ambiguity" and not conflict_risk and not state_dependency:
            is_actionable_hint = bool(requires_decision_hint or "?" in lowered)
        is_time_sensitive_hint = bool(
            due_at
            or derived_start_at
            or re.search(r"\b(today|tonight|tomorrow|asap|urgent|before|by|deadline|late)\b", lowered)
        )
        has_clear_options_hint = bool(
            conflict_risk
            or state_dependency
            or interpretation_type
            in {
                "conflict_reported",
                "time_change",
                "cancellation",
                "deadline",
                "obligation",
                "schedule_create",
                "ambiguity",
            }
        )

        decision_score = _clip01(priority_score / 120.0)
        actionability_score = _clip01(
            (decision_score * 0.42)
            + (0.2 if is_actionable_hint else 0.0)
            + (0.14 if is_time_sensitive_hint else 0.0)
            + (0.14 if has_clear_options_hint else 0.0)
            + (0.1 if requires_decision_hint else 0.0)
            - (0.1 if interpretation_type == "informational" else 0.0)
            - (0.08 if interpretation_type == "ambiguity" and not state_dependency else 0.0)
        )
        confidence_score = _clip01(
            confidence
            - (event_density_score * 0.12)
            - (decision_queue_score * 0.08)
            + (conflict_backlog_score * 0.06)
        )

        adaptive_recall_relief = min(
            0.08,
            (suppression_delta * 0.12)
            + (conflict_frequency * 0.03),
        )
        if priority_class in {HOUSEHOLD_UPM_PRIORITY_LOW, HOUSEHOLD_UPM_PRIORITY_NOISE}:
            adaptive_recall_relief *= 0.4

        actionability_threshold = _clip01(
            HOUSEHOLD_UPM_ACTIONABILITY_THRESHOLD
            + (event_density_score * 0.12)
            + (decision_queue_score * 0.08)
            - (0.08 if conflict_risk else 0.0)
            - (0.06 if state_dependency else 0.0)
            - adaptive_recall_relief
        )
        confidence_min = _clip01(
            HOUSEHOLD_UPM_CONFIDENCE_MIN
            + (event_density_score * 0.06)
            + (decision_queue_score * 0.04)
            - (0.05 if conflict_risk else 0.0)
            - min(0.09, adaptive_recall_relief * 0.72)
        )

        conflict_forced_eligibility = bool(
            conflict_risk
            and (
                state_dependency
                or conflict_type in {"cross_member", "cascade", "direct", "derived"}
            )
        )
        dependency_forced_eligibility = bool(
            state_dependency
            and interpretation_type in {"cancellation", "time_change", "deadline", "conflict_reported"}
        )
        ambiguity_forced_eligibility = bool(
            interpretation_type == "ambiguity"
            and (
                (
                    0.45 <= confidence_score <= 0.82
                    and (
                        bool(due_at)
                        or bool(derived_start_at)
                        or bool(dependency_schedule_id)
                        or bool(conflict_risk)
                        or bool(
                            re.search(
                                r"\b(tomorrow|tonight|next week|after|before|schedule|practice|meeting|deadline|changed)\b",
                                lowered,
                            )
                        )
                    )
                )
                or (
                    requires_decision_hint
                    and bool(
                        re.search(
                            r"\b(tomorrow|tonight|next week|after|before|schedule|practice|meeting|deadline|changed)\b",
                            lowered,
                        )
                    )
                )
            )
        )
        eligible_by_threshold = bool(
            actionability_score >= actionability_threshold
            and confidence_score >= confidence_min
        )
        if priority_class == HOUSEHOLD_UPM_PRIORITY_CRITICAL:
            eligible_by_threshold = True
        if conflict_forced_eligibility or dependency_forced_eligibility or ambiguity_forced_eligibility:
            eligible_by_threshold = True
        if interpretation_type == "informational" and not conflict_risk and not state_dependency:
            eligible_by_threshold = False

        requires_decision = bool(
            (requires_decision and eligible_by_threshold)
            or conflict_forced_eligibility
            or dependency_forced_eligibility
            or ambiguity_forced_eligibility
            or priority_class == HOUSEHOLD_UPM_PRIORITY_CRITICAL
        )

        borderline_event = bool(
            priority_class in {
                HOUSEHOLD_UPM_PRIORITY_HIGH,
                HOUSEHOLD_UPM_PRIORITY_MEDIUM,
                HOUSEHOLD_UPM_PRIORITY_LOW,
            }
            and abs(actionability_score - actionability_threshold) <= HOUSEHOLD_UPM_BORDERLINE_MARGIN
            and confidence_score >= (confidence_min - 0.06)
        )

        return {
            "priority_score": int(priority_score),
            "priority_class": priority_class,
            "requires_decision": bool(requires_decision),
            "conflict_risk": bool(conflict_risk),
            "state_dependency": bool(state_dependency),
            "conflict_schedule_id": conflict_schedule_id,
            "conflict_type": conflict_type,
            "conflict_events_involved": conflict_events_involved,
            "priority_signals": priority_signals,
            "decision_score": round(decision_score, 4),
            "actionability_score": round(actionability_score, 4),
            "confidence_score": round(confidence_score, 4),
            "actionability_threshold": round(actionability_threshold, 4),
            "confidence_min": round(confidence_min, 4),
            "borderline_event": bool(borderline_event),
            "event_density_score": round(event_density_score, 4),
            "decision_queue_score": round(decision_queue_score, 4),
            "conflict_backlog_score": round(conflict_backlog_score, 4),
            "household_state_load": round(household_state_load, 4),
            "suppression_delta": round(suppression_delta, 4),
            "conflict_frequency": round(conflict_frequency, 4),
            "decision_surface_gap": int(decision_surface_gap),
            "decision_surface_ratio": round(decision_surface_ratio, 4),
            "conflict_forced_eligibility": bool(conflict_forced_eligibility),
            "dependency_forced_eligibility": bool(dependency_forced_eligibility),
            "ambiguity_forced_eligibility": bool(ambiguity_forced_eligibility),
        }

    def _upm_pre_suppression_guard(
        self,
        *,
        projection: Mapping[str, Any],
        raw_content: str,
        interpretation_type: str,
        interpretation: Mapping[str, Any],
        member_id: str,
        context: Mapping[str, Any] | None,
        suppression_reason: str,
    ) -> dict[str, Any]:
        reevaluated = self._upm_priority_profile_for_message(
            projection=projection,
            raw_content=raw_content,
            interpretation_type=interpretation_type,
            interpretation=interpretation,
            member_id=member_id,
            context=context,
        )
        priority_class = str(reevaluated.get("priority_class") or "")
        if priority_class not in HOUSEHOLD_UPM_PRIORITY_CLASSES:
            return {
                "safe_to_suppress": False,
                "root_cause": "UPM_missing",
                "upm": reevaluated,
            }

        if priority_class == HOUSEHOLD_UPM_PRIORITY_CRITICAL:
            return {
                "safe_to_suppress": False,
                "root_cause": "suppression_overreach",
                "upm": reevaluated,
            }

        if suppression_reason == "merged_into_existing":
            return {
                "safe_to_suppress": True,
                "root_cause": "",
                "upm": reevaluated,
            }

        if suppression_reason == "collapsed_into_low_priority_decision":
            if (
                bool(reevaluated.get("conflict_forced_eligibility"))
                or bool(reevaluated.get("dependency_forced_eligibility"))
                or bool(reevaluated.get("conflict_risk"))
            ):
                return {
                    "safe_to_suppress": False,
                    "root_cause": "suppression_overreach",
                    "upm": reevaluated,
                }
            return {
                "safe_to_suppress": True,
                "root_cause": "",
                "upm": reevaluated,
            }

        if (
            bool(reevaluated.get("conflict_forced_eligibility"))
            or bool(reevaluated.get("dependency_forced_eligibility"))
            or bool(reevaluated.get("ambiguity_forced_eligibility"))
        ):
            return {
                "safe_to_suppress": False,
                "root_cause": "suppression_overreach",
                "upm": reevaluated,
            }

        if bool(reevaluated.get("requires_decision")) and bool(
            reevaluated.get("conflict_risk") or reevaluated.get("state_dependency")
        ):
            return {
                "safe_to_suppress": False,
                "root_cause": "suppression_overreach",
                "upm": reevaluated,
            }

        if suppression_reason in {
            "decision_cap_reached",
            "decision_density_cap_reached",
            "upm_actionability_below_threshold",
            "upm_confidence_below_threshold",
            "low_impact_actionable_noise",
        } and priority_class in {
            HOUSEHOLD_UPM_PRIORITY_HIGH,
            HOUSEHOLD_UPM_PRIORITY_MEDIUM,
        }:
            return {
                "safe_to_suppress": False,
                "root_cause": "suppression_overreach",
                "upm": reevaluated,
            }

        if bool(reevaluated.get("conflict_risk")) and not bool(reevaluated.get("requires_decision")):
            return {
                "safe_to_suppress": False,
                "root_cause": "conflict_under_eval",
                "upm": reevaluated,
            }

        return {
            "safe_to_suppress": True,
            "root_cause": "",
            "upm": reevaluated,
        }

    def _classify_missed_decision_root_cause(
        self,
        *,
        suppression_reason: str,
        upm_profile: Mapping[str, Any],
    ) -> str:
        priority_class = str(upm_profile.get("priority_class") or "")
        if not priority_class:
            return "UPM_missing"

        if bool(upm_profile.get("conflict_risk")) and not bool(upm_profile.get("requires_decision")):
            return "conflict_under_eval"

        if (
            bool(upm_profile.get("conflict_forced_eligibility"))
            or bool(upm_profile.get("dependency_forced_eligibility"))
            or bool(upm_profile.get("ambiguity_forced_eligibility"))
        ):
            return "suppression_overreach"

        if suppression_reason in {
            "resolution_unacknowledged",
            "merged_into_existing",
            "collapsed_into_low_priority_decision",
            "decision_cap_reached",
            "decision_density_cap_reached",
            "not_time_sensitive_and_resolvable",
            "ambiguity_low_impact",
            "upm_no_decision_required",
            "upm_actionability_below_threshold",
            "upm_confidence_below_threshold",
            "low_impact_actionable_noise",
        }:
            if (
                priority_class in {HOUSEHOLD_UPM_PRIORITY_CRITICAL, HOUSEHOLD_UPM_PRIORITY_HIGH}
                or bool(upm_profile.get("conflict_forced_eligibility"))
                or bool(upm_profile.get("dependency_forced_eligibility"))
                or bool(upm_profile.get("ambiguity_forced_eligibility"))
            ):
                return "suppression_overreach"
            return "routing_error"

        if priority_class == HOUSEHOLD_UPM_PRIORITY_NOISE and bool(upm_profile.get("requires_decision")):
            return "UPM_missing"
        return "routing_error"

    def _coerce_upm_score(self, value: Any, *, default: float = 0.0) -> float:
        try:
            return _clip01(float(value))
        except (TypeError, ValueError):
            return _clip01(default)

    def _upm_recent_household_promotions(
        self,
        *,
        projection: Mapping[str, Any],
        limit: int = HOUSEHOLD_UPM_HISTORY_WINDOW,
    ) -> list[Mapping[str, Any]]:
        promotions_raw = projection.get("household_promotions")
        if not isinstance(promotions_raw, Sequence):
            return []
        rows = [row for row in promotions_raw if isinstance(row, Mapping)]
        rows.sort(
            key=lambda row: (
                str(row.get("promoted_at") or ""),
                str(row.get("source_message_id") or ""),
            )
        )
        if limit <= 0:
            return rows
        return rows[-limit:]

    def _upm_historical_suppression_profile(
        self,
        *,
        projection: Mapping[str, Any],
    ) -> dict[str, Any]:
        rows = self._upm_recent_household_promotions(
            projection=projection,
            limit=HOUSEHOLD_UPM_HISTORY_WINDOW,
        )
        if not rows:
            return {
                "suppression_delta": 0.0,
                "conflict_frequency": 0.0,
                "decision_surface_gap": 0,
                "decision_surface_ratio": 1.0,
                "merged_overlap_count": 0,
            }

        actionable_total = 0
        decision_surface_total = 0
        conflict_total = 0
        suppressed_total = 0
        merged_total = 0

        for row in rows:
            promotion_status = str(row.get("promotion_status") or "").strip().lower()
            promotion_target = str(row.get("promotion_target") or "").strip().lower()
            interpretation_type = str(row.get("interpretation_type") or "").strip().lower()
            promoted_entity_type = str(row.get("promoted_entity_type") or "").strip().lower()
            secondary_entity_type = str(row.get("secondary_entity_type") or "").strip().lower()
            conflict_schedule_id = str(row.get("conflict_schedule_id") or "").strip()
            conflict_type = str(row.get("conflict_type") or "").strip().lower()

            if promotion_status.startswith("suppressed:"):
                suppressed_total += 1
            if "merged" in promotion_status:
                merged_total += 1

            if promotion_target in {"decision", "action", "calendar", "calendar_update"} and interpretation_type != "informational":
                actionable_total += 1

            if promoted_entity_type == "decision_card" or secondary_entity_type == "decision_card":
                decision_surface_total += 1

            if conflict_schedule_id or conflict_type:
                conflict_total += 1

        total_rows = len(rows)
        suppression_delta = _clip01(suppressed_total / float(total_rows))
        conflict_frequency = _clip01(conflict_total / float(total_rows))
        decision_surface_ratio = _clip01(
            decision_surface_total / float(max(1, actionable_total))
        )
        decision_surface_gap = max(0, actionable_total - decision_surface_total)

        return {
            "suppression_delta": round(suppression_delta, 4),
            "conflict_frequency": round(conflict_frequency, 4),
            "decision_surface_gap": int(decision_surface_gap),
            "decision_surface_ratio": round(decision_surface_ratio, 4),
            "merged_overlap_count": int(merged_total),
        }

    def _upm_suppressed_score_delta(
        self,
        *,
        suppression_reason: str,
        actionability_score: float,
        actionability_threshold: float,
        confidence_score: float,
        confidence_min: float,
    ) -> float:
        resolved_reason = suppression_reason.strip().lower()
        if not resolved_reason:
            return 0.0
        threshold_gap = max(
            0.0,
            actionability_threshold - actionability_score,
            confidence_min - confidence_score,
        )
        if threshold_gap <= 0.0 and resolved_reason in {
            "merged_into_existing",
            "collapsed_into_low_priority_decision",
            "decision_cap_reached",
            "decision_density_cap_reached",
        }:
            threshold_gap = HOUSEHOLD_UPM_BORDERLINE_MARGIN
        return round(_clip01(threshold_gap), 4)

    def _upm_alternative_path_for_suppression(
        self,
        *,
        suppression_reason: str,
        promotion_target: str,
        merged_decision_card_id: str,
    ) -> str:
        base_path = "decision"
        if promotion_target == "action":
            base_path = "action"
        elif promotion_target in {"calendar", "calendar_update"}:
            base_path = "calendar"

        resolved_reason = suppression_reason.strip().lower()
        if not resolved_reason:
            return base_path
        if resolved_reason == "merged_into_existing":
            return "decision.merge_existing"
        if resolved_reason == "collapsed_into_low_priority_decision":
            return "decision.merge_existing" if merged_decision_card_id else "decision.defer"
        if resolved_reason in {"decision_cap_reached", "decision_density_cap_reached"}:
            return f"{base_path}.queue"
        if resolved_reason in {
            "upm_actionability_below_threshold",
            "upm_confidence_below_threshold",
            "upm_no_decision_required",
            "low_impact_actionable_noise",
            "decision_not_required",
        }:
            return f"{base_path}.defer"
        return base_path

    def _max_active_promotion_decisions_for_context(
        self,
        *,
        upm_profile: Mapping[str, Any],
    ) -> int:
        event_density = self._coerce_upm_score(upm_profile.get("event_density_score"), default=0.0)
        state_load = self._coerce_upm_score(upm_profile.get("household_state_load"), default=0.0)
        suppression_delta = self._coerce_upm_score(upm_profile.get("suppression_delta"), default=0.0)
        decision_surface_gap = int(upm_profile.get("decision_surface_gap") or 0)
        cap = int(
            round(
                HOUSEHOLD_UPM_DECISION_CAP_MIN
                + (event_density * 2.0)
                + (state_load * 3.0)
                + (suppression_delta * 2.0)
            )
        )
        if decision_surface_gap >= HOUSEHOLD_UPM_COMPRESSION_GAP_GUARD:
            cap += 1
        cap = max(HOUSEHOLD_UPM_DECISION_CAP_MIN, min(HOUSEHOLD_UPM_DECISION_CAP_MAX, cap))
        return cap

    def _decision_creation_control_for_message(
        self,
        *,
        projection: Mapping[str, Any],
        raw_content: str,
        interpretation: Mapping[str, Any],
        interpretation_type: str,
        promotion_target: str,
        member_id: str,
        context: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        quality = self._decision_quality_filter(
            raw_content=raw_content,
            interpretation=interpretation,
            interpretation_type=interpretation_type,
        )
        upm_profile = self._upm_priority_profile_for_message(
            projection=projection,
            raw_content=raw_content,
            interpretation_type=interpretation_type,
            interpretation=interpretation,
            member_id=member_id,
            context=context,
        )
        priority_class = str(upm_profile.get("priority_class") or HOUSEHOLD_UPM_PRIORITY_NOISE)
        if priority_class not in HOUSEHOLD_UPM_PRIORITY_CLASSES:
            priority_class = HOUSEHOLD_UPM_PRIORITY_NOISE
        requires_decision = bool(upm_profile.get("requires_decision"))
        decision_score = self._coerce_upm_score(upm_profile.get("decision_score"), default=0.0)
        actionability_score = self._coerce_upm_score(upm_profile.get("actionability_score"), default=0.0)
        confidence_score = self._coerce_upm_score(upm_profile.get("confidence_score"), default=0.0)
        actionability_threshold = self._coerce_upm_score(
            upm_profile.get("actionability_threshold"),
            default=HOUSEHOLD_UPM_ACTIONABILITY_THRESHOLD,
        )
        confidence_min = self._coerce_upm_score(
            upm_profile.get("confidence_min"),
            default=HOUSEHOLD_UPM_CONFIDENCE_MIN,
        )
        conflict_forced_eligibility = bool(upm_profile.get("conflict_forced_eligibility"))
        dependency_forced_eligibility = bool(upm_profile.get("dependency_forced_eligibility"))
        ambiguity_forced_eligibility = bool(upm_profile.get("ambiguity_forced_eligibility"))
        borderline_signal = bool(upm_profile.get("borderline_event"))
        suppression_delta = self._coerce_upm_score(upm_profile.get("suppression_delta"), default=0.0)
        decision_surface_gap = int(upm_profile.get("decision_surface_gap") or 0)

        high_confidence_actionable = bool(
            confidence_score >= max(HOUSEHOLD_UPM_RECALL_SAFETY_CONFIDENCE, confidence_min - 0.02)
            and actionability_score >= max(HOUSEHOLD_UPM_RECALL_SAFETY_ACTIONABILITY, actionability_threshold - 0.02)
        )
        recall_safety_override = bool(
            priority_class
            in {
                HOUSEHOLD_UPM_PRIORITY_CRITICAL,
                HOUSEHOLD_UPM_PRIORITY_HIGH,
                HOUSEHOLD_UPM_PRIORITY_MEDIUM,
            }
            and high_confidence_actionable
            and (
                bool(upm_profile.get("conflict_risk"))
                or bool(upm_profile.get("state_dependency"))
                or bool(quality.get("is_time_sensitive"))
                or bool(quality.get("has_clear_options"))
            )
        )
        compression_guard_active = bool(
            priority_class in {HOUSEHOLD_UPM_PRIORITY_HIGH, HOUSEHOLD_UPM_PRIORITY_MEDIUM}
            and bool(quality.get("is_actionable"))
            and (
                bool(upm_profile.get("conflict_risk"))
                or bool(upm_profile.get("state_dependency"))
                or bool(quality.get("is_time_sensitive"))
            )
            and (
                decision_surface_gap >= HOUSEHOLD_UPM_COMPRESSION_GAP_GUARD
                and suppression_delta >= 0.16
            )
        )

        threshold_suppression_reason = ""
        if priority_class != HOUSEHOLD_UPM_PRIORITY_CRITICAL and not (
            conflict_forced_eligibility
            or dependency_forced_eligibility
            or ambiguity_forced_eligibility
        ):
            if actionability_score < actionability_threshold:
                requires_decision = False
                threshold_suppression_reason = "upm_actionability_below_threshold"
            elif confidence_score < confidence_min:
                requires_decision = False
                threshold_suppression_reason = "upm_confidence_below_threshold"

        if conflict_forced_eligibility or dependency_forced_eligibility or ambiguity_forced_eligibility:
            requires_decision = True
        if priority_class == HOUSEHOLD_UPM_PRIORITY_CRITICAL:
            requires_decision = True
        if recall_safety_override and threshold_suppression_reason:
            requires_decision = True
            threshold_suppression_reason = ""

        decision_priority = self._decision_priority_for_message(
            interpretation_type=interpretation_type,
            interpretation=interpretation,
            context=context,
        )
        if priority_class in {HOUSEHOLD_UPM_PRIORITY_CRITICAL, HOUSEHOLD_UPM_PRIORITY_HIGH}:
            decision_priority = "high"
        elif priority_class == HOUSEHOLD_UPM_PRIORITY_MEDIUM:
            decision_priority = "medium"
        elif priority_class in {HOUSEHOLD_UPM_PRIORITY_LOW, HOUSEHOLD_UPM_PRIORITY_NOISE}:
            decision_priority = "low"

        decision_merge_key = self._decision_merge_key_for_message(
            interpretation_type=interpretation_type,
            interpretation=interpretation,
            promotion_target=promotion_target,
            context=context,
        )
        if not decision_merge_key and borderline_signal and priority_class in {
            HOUSEHOLD_UPM_PRIORITY_HIGH,
            HOUSEHOLD_UPM_PRIORITY_MEDIUM,
            HOUSEHOLD_UPM_PRIORITY_LOW,
        }:
            decision_merge_key = f"upm-borderline:{interpretation_type}:household"

        open_decisions = self._open_decision_cards_from_projection(projection=projection)
        active_count = len(open_decisions)
        dynamic_decision_cap = self._max_active_promotion_decisions_for_context(upm_profile=upm_profile)
        merged_decision_card_id = self._find_open_decision_card_for_merge_key(
            projection=projection,
            merge_key=decision_merge_key,
        )

        def _suppressed_score_delta_for_reason(*, suppression_reason: str) -> float:
            return self._upm_suppressed_score_delta(
                suppression_reason=suppression_reason,
                actionability_score=actionability_score,
                actionability_threshold=actionability_threshold,
                confidence_score=confidence_score,
                confidence_min=confidence_min,
            )

        def _alternative_path_for_reason(*, suppression_reason: str, merged_id: str) -> str:
            return self._upm_alternative_path_for_suppression(
                suppression_reason=suppression_reason,
                promotion_target=promotion_target,
                merged_decision_card_id=merged_id,
            )

        def _control_payload(
            *,
            allow_create: bool,
            merged_decision_card_id_value: str,
            suppressed_reason: str,
            suppression_overridden: bool,
            suppression_root_cause: str,
            resolution_acknowledged: bool,
            requires_decision_value: bool | None = None,
        ) -> dict[str, Any]:
            return {
                "allow_create": allow_create,
                "merged_decision_card_id": merged_decision_card_id_value,
                "suppressed_reason": suppressed_reason,
                "suppressed_score_delta": _suppressed_score_delta_for_reason(
                    suppression_reason=suppressed_reason,
                ),
                "alternative_path": _alternative_path_for_reason(
                    suppression_reason=suppressed_reason,
                    merged_id=merged_decision_card_id_value,
                )
                if suppressed_reason
                else "",
                "suppression_overridden": suppression_overridden,
                "suppression_root_cause": suppression_root_cause,
                "resolution_acknowledged": bool(resolution_acknowledged),
                "decision_priority": decision_priority,
                "decision_merge_key": decision_merge_key,
                "decision_quality": quality,
                "dynamic_decision_cap": dynamic_decision_cap,
                "upm_priority_score": int(upm_profile.get("priority_score") or 0),
                "upm_decision_score": decision_score,
                "upm_actionability_score": actionability_score,
                "upm_confidence_score": confidence_score,
                "upm_actionability_threshold": actionability_threshold,
                "upm_confidence_min": confidence_min,
                "upm_priority_class": priority_class,
                "upm_requires_decision": bool(
                    requires_decision
                    if requires_decision_value is None
                    else requires_decision_value
                ),
                "upm_conflict_risk": bool(upm_profile.get("conflict_risk")),
                "upm_state_dependency": bool(upm_profile.get("state_dependency")),
                "upm_conflict_forced_eligibility": conflict_forced_eligibility,
                "upm_dependency_forced_eligibility": dependency_forced_eligibility,
                "upm_ambiguity_forced_eligibility": ambiguity_forced_eligibility,
                "upm_borderline_event": borderline_signal,
                "upm_priority_signals": list(upm_profile.get("priority_signals") or []),
                "upm_suppression_delta": suppression_delta,
                "upm_decision_surface_gap": decision_surface_gap,
                "upm_high_confidence_actionable": high_confidence_actionable,
                "upm_recall_safety_override": recall_safety_override,
                "upm_compression_guard": compression_guard_active,
            }

        def _suppression_decision(
            *,
            suppression_reason: str,
            merged_id: str = "",
        ) -> dict[str, Any]:
            requires_decision_value = requires_decision
            if suppression_reason in {"merged_into_existing", "collapsed_into_low_priority_decision"}:
                requires_decision_value = False
            guard = self._upm_pre_suppression_guard(
                projection=projection,
                raw_content=raw_content,
                interpretation_type=interpretation_type,
                interpretation=interpretation,
                member_id=member_id,
                context=context,
                suppression_reason=suppression_reason,
            )
            if not bool(guard.get("safe_to_suppress")):
                return _control_payload(
                    allow_create=True,
                    merged_decision_card_id_value="",
                    suppressed_reason="",
                    suppression_overridden=True,
                    suppression_root_cause=str(guard.get("root_cause") or "suppression_overreach"),
                    resolution_acknowledged=False,
                    requires_decision_value=requires_decision,
                )

            resolution_acknowledged, resolution_root_cause = self._upm_resolution_acknowledgment_for_suppression(
                projection=projection,
                suppression_reason=suppression_reason,
                merged_decision_card_id=merged_id,
                decision_merge_key=decision_merge_key,
                interpretation=interpretation,
                interpretation_type=interpretation_type,
                upm_profile=upm_profile,
            )
            if not resolution_acknowledged:
                return _control_payload(
                    allow_create=True,
                    merged_decision_card_id_value="",
                    suppressed_reason="",
                    suppression_overridden=True,
                    suppression_root_cause=resolution_root_cause or "resolution_unacknowledged",
                    resolution_acknowledged=False,
                    requires_decision_value=requires_decision,
                )

            return _control_payload(
                allow_create=False,
                merged_decision_card_id_value=merged_id,
                suppressed_reason=suppression_reason,
                suppression_overridden=False,
                suppression_root_cause="",
                resolution_acknowledged=True,
                requires_decision_value=requires_decision_value,
            )

        if priority_class == HOUSEHOLD_UPM_PRIORITY_CRITICAL:
            return _control_payload(
                allow_create=True,
                merged_decision_card_id_value="",
                suppressed_reason="",
                suppression_overridden=False,
                suppression_root_cause="",
                resolution_acknowledged=True,
            )

        if not requires_decision:
            return _suppression_decision(
                suppression_reason=threshold_suppression_reason or "upm_no_decision_required"
            )

        merge_eligible_high = bool(
            priority_class == HOUSEHOLD_UPM_PRIORITY_HIGH
            and decision_merge_key.startswith(("conflict:", "schedule-change:", "ambiguity:"))
        )
        if merged_decision_card_id and (
            priority_class in {
                HOUSEHOLD_UPM_PRIORITY_MEDIUM,
                HOUSEHOLD_UPM_PRIORITY_LOW,
                HOUSEHOLD_UPM_PRIORITY_NOISE,
            }
            or borderline_signal
            or merge_eligible_high
        ) and not compression_guard_active:
            return _suppression_decision(
                suppression_reason="merged_into_existing",
                merged_id=merged_decision_card_id,
            )

        if (
            priority_class in {HOUSEHOLD_UPM_PRIORITY_LOW, HOUSEHOLD_UPM_PRIORITY_NOISE}
            and not bool(upm_profile.get("conflict_risk"))
            and not bool(upm_profile.get("state_dependency"))
            and actionability_score < max(actionability_threshold, HOUSEHOLD_UPM_ACTIONABILITY_THRESHOLD + 0.04)
        ):
            return _suppression_decision(suppression_reason="low_impact_actionable_noise")

        if active_count >= dynamic_decision_cap and priority_class in {
            HOUSEHOLD_UPM_PRIORITY_MEDIUM,
            HOUSEHOLD_UPM_PRIORITY_LOW,
        } and not compression_guard_active and not bool(upm_profile.get("conflict_risk")):
            for row in open_decisions:
                metadata = row.get("metadata")
                if not isinstance(metadata, Mapping):
                    continue
                decision_reason = str(metadata.get("decision_reason") or "").strip().lower()
                if decision_reason not in HOUSEHOLD_LOW_PRIORITY_DECISION_REASONS:
                    continue
                collapsed_decision_id = str(row.get("decision_card_id") or "").strip()
                if not collapsed_decision_id:
                    continue

                collapsed_acknowledged, _ = self._upm_resolution_acknowledgment_for_suppression(
                    projection=projection,
                    suppression_reason="collapsed_into_low_priority_decision",
                    merged_decision_card_id=collapsed_decision_id,
                    decision_merge_key=decision_merge_key,
                    interpretation=interpretation,
                    interpretation_type=interpretation_type,
                    upm_profile=upm_profile,
                )
                if not collapsed_acknowledged:
                    continue

                return _suppression_decision(
                    suppression_reason="collapsed_into_low_priority_decision",
                    merged_id=collapsed_decision_id,
                )

            return _suppression_decision(suppression_reason="decision_density_cap_reached")

        return _control_payload(
            allow_create=True,
            merged_decision_card_id_value="",
            suppressed_reason="",
            suppression_overridden=False,
            suppression_root_cause="",
            resolution_acknowledged=True,
        )

    def _maybe_create_household_message_decision_card(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        request_id: str,
        message_id: str,
        raw_content: str,
        source: str,
        member_id: str,
        classification: str,
        interpretation_type: str,
        promotion_reason: str,
        promotion_target: str,
        interpretation: Mapping[str, Any],
        precomputed_projection: Mapping[str, Any] | None,
        context: Mapping[str, Any] | None = None,
        decision_card_id: str | None = None,
    ) -> dict[str, Any]:
        projection = (
            precomputed_projection
            if isinstance(precomputed_projection, Mapping)
            else self.get_projection(household_id)
        )
        control = self._decision_creation_control_for_message(
            projection=projection,
            raw_content=raw_content,
            interpretation=interpretation,
            interpretation_type=interpretation_type,
            promotion_target=promotion_target,
            member_id=member_id,
            context=context,
        )
        merged_decision_card_id = str(control.get("merged_decision_card_id") or "").strip()
        if merged_decision_card_id:
            return {
                "created": False,
                "merged": True,
                "decision_card_id": merged_decision_card_id,
                "status": "merged_existing",
                "suppressed_reason": str(control.get("suppressed_reason") or ""),
                "suppressed_score_delta": self._coerce_upm_score(control.get("suppressed_score_delta"), default=0.0),
                "alternative_path": str(control.get("alternative_path") or "decision.merge_existing"),
                "resolution_acknowledged": bool(control.get("resolution_acknowledged", True)),
                "upm_priority_class": str(control.get("upm_priority_class") or HOUSEHOLD_UPM_PRIORITY_NOISE),
                "upm_requires_decision": bool(control.get("upm_requires_decision")),
                "upm_conflict_risk": bool(control.get("upm_conflict_risk")),
                "upm_state_dependency": bool(control.get("upm_state_dependency")),
                "upm_decision_score": self._coerce_upm_score(control.get("upm_decision_score"), default=0.0),
                "upm_actionability_score": self._coerce_upm_score(control.get("upm_actionability_score"), default=0.0),
                "upm_confidence_score": self._coerce_upm_score(control.get("upm_confidence_score"), default=0.0),
                "upm_actionability_threshold": self._coerce_upm_score(
                    control.get("upm_actionability_threshold"),
                    default=HOUSEHOLD_UPM_ACTIONABILITY_THRESHOLD,
                ),
                "upm_confidence_min": self._coerce_upm_score(
                    control.get("upm_confidence_min"),
                    default=HOUSEHOLD_UPM_CONFIDENCE_MIN,
                ),
                "critical_event_detected": str(control.get("upm_priority_class") or "") == HOUSEHOLD_UPM_PRIORITY_CRITICAL,
            }

        if not bool(control.get("allow_create")):
            return {
                "created": False,
                "merged": False,
                "decision_card_id": "",
                "status": "suppressed",
                "suppressed_reason": str(control.get("suppressed_reason") or "suppressed"),
                "suppressed_score_delta": self._coerce_upm_score(control.get("suppressed_score_delta"), default=0.0),
                "alternative_path": str(control.get("alternative_path") or "decision.defer"),
                "resolution_acknowledged": bool(control.get("resolution_acknowledged", True)),
                "upm_priority_class": str(control.get("upm_priority_class") or HOUSEHOLD_UPM_PRIORITY_NOISE),
                "upm_requires_decision": bool(control.get("upm_requires_decision")),
                "upm_conflict_risk": bool(control.get("upm_conflict_risk")),
                "upm_state_dependency": bool(control.get("upm_state_dependency")),
                "upm_decision_score": self._coerce_upm_score(control.get("upm_decision_score"), default=0.0),
                "upm_actionability_score": self._coerce_upm_score(control.get("upm_actionability_score"), default=0.0),
                "upm_confidence_score": self._coerce_upm_score(control.get("upm_confidence_score"), default=0.0),
                "upm_actionability_threshold": self._coerce_upm_score(
                    control.get("upm_actionability_threshold"),
                    default=HOUSEHOLD_UPM_ACTIONABILITY_THRESHOLD,
                ),
                "upm_confidence_min": self._coerce_upm_score(
                    control.get("upm_confidence_min"),
                    default=HOUSEHOLD_UPM_CONFIDENCE_MIN,
                ),
                "critical_event_detected": str(control.get("upm_priority_class") or "") == HOUSEHOLD_UPM_PRIORITY_CRITICAL,
            }

        merged_context = dict(context or {})
        merged_context["decision_priority"] = str(control.get("decision_priority") or "low")
        merged_context["decision_merge_key"] = str(control.get("decision_merge_key") or "")
        merged_context["upm_priority_score"] = int(control.get("upm_priority_score") or 0)
        merged_context["upm_priority_class"] = str(control.get("upm_priority_class") or HOUSEHOLD_UPM_PRIORITY_NOISE)
        merged_context["upm_requires_decision"] = bool(control.get("upm_requires_decision"))
        merged_context["upm_conflict_risk"] = bool(control.get("upm_conflict_risk"))
        merged_context["upm_state_dependency"] = bool(control.get("upm_state_dependency"))
        merged_context["upm_decision_score"] = self._coerce_upm_score(control.get("upm_decision_score"), default=0.0)
        merged_context["upm_actionability_score"] = self._coerce_upm_score(control.get("upm_actionability_score"), default=0.0)
        merged_context["upm_confidence_score"] = self._coerce_upm_score(control.get("upm_confidence_score"), default=0.0)
        merged_context["upm_actionability_threshold"] = self._coerce_upm_score(
            control.get("upm_actionability_threshold"),
            default=HOUSEHOLD_UPM_ACTIONABILITY_THRESHOLD,
        )
        merged_context["upm_confidence_min"] = self._coerce_upm_score(
            control.get("upm_confidence_min"),
            default=HOUSEHOLD_UPM_CONFIDENCE_MIN,
        )
        merged_context["upm_borderline_event"] = bool(control.get("upm_borderline_event"))
        merged_context["upm_dynamic_decision_cap"] = int(
            control.get("dynamic_decision_cap") or HOUSEHOLD_UPM_DECISION_CAP_MIN
        )
        merged_context["decision_routing_model"] = "upm_unified"
        upm_priority_signals = [
            str(item).strip()
            for item in list(control.get("upm_priority_signals") or [])
            if str(item).strip()
        ]
        if upm_priority_signals:
            merged_context["upm_priority_signals"] = upm_priority_signals
        decision_quality = control.get("decision_quality")
        if isinstance(decision_quality, Mapping):
            for key, value in decision_quality.items():
                merged_context[f"decision_quality_{key}"] = bool(value)

        decision_result = self._create_household_message_promotion_decision_card(
            household_id=household_id,
            actor=actor,
            request_id=request_id,
            message_id=message_id,
            raw_content=raw_content,
            source=source,
            member_id=member_id,
            classification=classification,
            interpretation_type=interpretation_type,
            promotion_reason=promotion_reason,
            precomputed_projection=cast(dict[str, Any] | None, projection if isinstance(projection, dict) else None),
            context=merged_context,
            decision_card_id=decision_card_id,
        )
        decision_response = decision_result.get("response")
        resolved_decision_card_id = (
            str(decision_response.get("decision_card_id") or "")
            if isinstance(decision_response, Mapping)
            else str(decision_card_id or "")
        )
        return {
            "created": True,
            "merged": False,
            "decision_card_id": resolved_decision_card_id,
            "status": str(decision_result.get("status") or "accepted"),
            "suppressed_reason": "",
            "suppressed_score_delta": 0.0,
            "alternative_path": "decision",
            "resolution_acknowledged": True,
            "upm_priority_class": str(control.get("upm_priority_class") or HOUSEHOLD_UPM_PRIORITY_NOISE),
            "upm_requires_decision": bool(control.get("upm_requires_decision")),
            "upm_conflict_risk": bool(control.get("upm_conflict_risk")),
            "upm_state_dependency": bool(control.get("upm_state_dependency")),
            "upm_decision_score": self._coerce_upm_score(control.get("upm_decision_score"), default=0.0),
            "upm_actionability_score": self._coerce_upm_score(control.get("upm_actionability_score"), default=0.0),
            "upm_confidence_score": self._coerce_upm_score(control.get("upm_confidence_score"), default=0.0),
            "upm_actionability_threshold": self._coerce_upm_score(
                control.get("upm_actionability_threshold"),
                default=HOUSEHOLD_UPM_ACTIONABILITY_THRESHOLD,
            ),
            "upm_confidence_min": self._coerce_upm_score(
                control.get("upm_confidence_min"),
                default=HOUSEHOLD_UPM_CONFIDENCE_MIN,
            ),
            "critical_event_detected": str(control.get("upm_priority_class") or "") == HOUSEHOLD_UPM_PRIORITY_CRITICAL,
        }

    def _decision_audit_payload_for_message(
        self,
        *,
        input_id: str,
        upm_profile: Mapping[str, Any],
        decision_generated: bool,
        decision_resolutions: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        upm_priority_class = str(upm_profile.get("priority_class") or HOUSEHOLD_UPM_PRIORITY_NOISE)
        if upm_priority_class not in HOUSEHOLD_UPM_PRIORITY_CLASSES:
            upm_priority_class = HOUSEHOLD_UPM_PRIORITY_NOISE
        decision_blocked = False
        suppression_reason = ""
        suppressed_score_delta = 0.0
        alternative_path = ""
        resolution_acknowledged = True

        for resolution in decision_resolutions:
            created = bool(resolution.get("created"))
            merged = bool(resolution.get("merged"))
            resolved_reason = str(resolution.get("suppressed_reason") or "").strip()
            resolved_score_delta = self._coerce_upm_score(
                resolution.get("suppressed_score_delta"),
                default=0.0,
            )
            resolved_alternative_path = str(resolution.get("alternative_path") or "").strip()
            resolved_acknowledged = bool(resolution.get("resolution_acknowledged", True))
            if not resolved_acknowledged:
                resolution_acknowledged = False
            if merged:
                if not suppression_reason:
                    suppression_reason = resolved_reason or "merged_existing"
                if resolved_score_delta > suppressed_score_delta:
                    suppressed_score_delta = resolved_score_delta
                if resolved_alternative_path and not alternative_path:
                    alternative_path = resolved_alternative_path
                if not resolved_acknowledged:
                    decision_blocked = True
                    suppression_reason = "resolution_unacknowledged"
                continue
            if not created:
                decision_blocked = True
                if resolved_reason:
                    suppression_reason = resolved_reason
                elif not suppression_reason:
                    suppression_reason = "blocked"
                if resolved_score_delta > suppressed_score_delta:
                    suppressed_score_delta = resolved_score_delta
                if resolved_alternative_path:
                    alternative_path = resolved_alternative_path

        if decision_generated:
            decision_blocked = False
            suppression_reason = ""
            suppressed_score_delta = 0.0
            alternative_path = "decision"
            resolution_acknowledged = True
        elif not suppression_reason:
            suppression_reason = "decision_not_required"
            if not alternative_path:
                alternative_path = "decision.defer"

        if suppression_reason and not alternative_path:
            alternative_path = self._upm_alternative_path_for_suppression(
                suppression_reason=suppression_reason,
                promotion_target="decision",
                merged_decision_card_id="",
            )

        root_cause = ""
        if decision_blocked:
            root_cause = self._classify_missed_decision_root_cause(
                suppression_reason=suppression_reason,
                upm_profile=upm_profile,
            )

        return {
            "input_id": input_id,
            "suppression_reason": suppression_reason,
            "suppressed_score_delta": round(suppressed_score_delta, 4),
            "alternative_path": alternative_path,
            "resolution_acknowledged": resolution_acknowledged,
            "decision_routing_model": "upm_unified",
            "upm_priority_class": upm_priority_class,
            "upm_decision_score": self._coerce_upm_score(upm_profile.get("decision_score"), default=0.0),
            "upm_actionability_score": self._coerce_upm_score(upm_profile.get("actionability_score"), default=0.0),
            "upm_confidence_score": self._coerce_upm_score(upm_profile.get("confidence_score"), default=0.0),
            "upm_actionability_threshold": self._coerce_upm_score(
                upm_profile.get("actionability_threshold"),
                default=HOUSEHOLD_UPM_ACTIONABILITY_THRESHOLD,
            ),
            "upm_confidence_min": self._coerce_upm_score(
                upm_profile.get("confidence_min"),
                default=HOUSEHOLD_UPM_CONFIDENCE_MIN,
            ),
            "critical_event_detected": upm_priority_class == HOUSEHOLD_UPM_PRIORITY_CRITICAL,
            "decision_blocked": decision_blocked,
            "root_cause": root_cause,
        }

    def _emit_household_message_suppression_audit_trace(self, *, payload: Mapping[str, Any]) -> None:
        print(f"[SUPPRESSION-AUDIT] {_stable_json(dict(payload))}", flush=True)

    def _classify_household_message(
        self,
        *,
        raw_content: str,
        signals: Mapping[str, Any] | None = None,
    ) -> str:
        lowered = raw_content.lower()
        resolved_signals = signals or self._household_message_signals(raw_content=raw_content)

        if bool(resolved_signals.get("conflict_reported")):
            return "schedule"
        if bool(resolved_signals.get("cancellation")) or bool(resolved_signals.get("time_change")):
            return "schedule"
        if bool(resolved_signals.get("schedule_topic")) and bool(resolved_signals.get("schedule_intent")) and (
            bool(resolved_signals.get("explicit_time"))
            or resolved_signals.get("weekday_hint") is not None
            or any(token in lowered for token in ("tomorrow", "tonight", "morning", "afternoon", "evening", "next week"))
        ):
            return "schedule"
        if bool(resolved_signals.get("deadline")) or bool(resolved_signals.get("obligation")):
            return "action"

        if any(token in lowered for token in ("todo", "to do", "remember to", "need to", "must")):
            return "todo"
        if any(token in lowered for token in ("action", "follow up", "follow-up", "reply", "confirm", "call", "send", "review")):
            return "action"
        return "fyi"

    def _resolve_household_message_promotion_target(
        self,
        *,
        classification: str,
        raw_content: str,
        interpretation: Mapping[str, Any] | None = None,
    ) -> str:
        resolved_interpretation = interpretation or {}
        interpretation_type = str(resolved_interpretation.get("interpretation_type") or "")
        confidence = float(resolved_interpretation.get("confidence") or 0.0)
        upm_profile = resolved_interpretation.get("upm")
        upm_priority_class = ""
        upm_requires_decision = False
        if isinstance(upm_profile, Mapping):
            upm_priority_class = str(upm_profile.get("priority_class") or "").strip().lower()
            upm_requires_decision = bool(upm_profile.get("requires_decision"))

        if upm_priority_class == HOUSEHOLD_UPM_PRIORITY_NOISE and interpretation_type == "informational":
            return "ignore"
        if upm_requires_decision and interpretation_type in {"informational", "ambiguity", "conflict_reported"}:
            return "decision"
        if upm_priority_class in {HOUSEHOLD_UPM_PRIORITY_CRITICAL, HOUSEHOLD_UPM_PRIORITY_HIGH} and interpretation_type in {
            "cancellation",
            "time_change",
        }:
            return "calendar_update"
        if upm_priority_class in {HOUSEHOLD_UPM_PRIORITY_CRITICAL, HOUSEHOLD_UPM_PRIORITY_HIGH} and interpretation_type in {
            "deadline",
            "obligation",
        }:
            return "action"

        if interpretation_type == "conflict_reported":
            return "decision"
        if interpretation_type == "ambiguity":
            return "decision"
        if interpretation_type and interpretation_type != "informational" and confidence < HOUSEHOLD_MESSAGE_INTERPRETATION_CONFIDENCE_THRESHOLD:
            return "decision"
        if interpretation_type in {"cancellation", "time_change"}:
            return "calendar_update"
        if interpretation_type in {"deadline", "obligation"}:
            return "action"
        if interpretation_type == "schedule_create":
            return "calendar"
        if interpretation_type == "informational":
            return "ignore"

        lowered = raw_content.lower()
        if any(token in lowered for token in ("should we", "can we", "which", "decide", "?")):
            return "decision"
        if classification == "schedule":
            return "calendar"
        if classification in {"todo", "action"}:
            return "action"
        return "ignore"

    def _household_message_signals(self, *, raw_content: str) -> dict[str, Any]:
        lowered = raw_content.lower()
        time_mentions = self._extract_time_mentions(lowered=lowered)
        weekday_hint = self._extract_weekday_hint(lowered=lowered)
        schedule_topic = bool(
            re.search(
                r"\b(meeting|appointment|conference|calendar|pickup|dropoff|drop off|schedule|book|practice|game|class|dinner|reservation|bus|trip|field trip)\b",
                lowered,
            )
        )
        schedule_intent = bool(
            re.search(
                r"\b(schedule(?:d)?|reschedul(?:e|ed)|appointment|conference|meeting|reservation|pickup|pick up|dropoff|drop off|moved|move|at\s+\d)\b",
                lowered,
            )
        )
        conflict_reported = bool(
            re.search(
                r"\b(overlap|overlaps|conflict|double[-\s]?book|same time|clash|collide)\b",
                lowered,
            )
        )
        cancellation = bool(re.search(r"\b(cancelled|canceled|call(?:ed)? off|no longer|dropped)\b", lowered))
        time_change = bool(
            re.search(
                r"\b(moved|rescheduled|reschedule|changed to|starts?\s+(earlier|later)|shifted|delay(?:ed)?|running\s+\d+\s*minutes?\s+late|late\s+today)\b",
                lowered,
            )
        ) or bool(
            schedule_topic
            and re.search(r"\b(earlier|later)\b", lowered)
        )
        deadline = bool(re.search(r"\b(due|deadline)\b", lowered)) or bool(
            re.search(r"\b(by|before)\b", lowered)
            and (
                weekday_hint is not None
                or bool(time_mentions)
                or any(token in lowered for token in ("tomorrow", "today", "tonight", "eod", "end of day"))
            )
        )
        obligation_verbs = bool(
            re.search(
                r"\b(bring|pay|sign|submit|send|buy|confirm|reply|call|remember to|need to|must|missing\s+signature)\b",
                lowered,
            )
        )
        pickup_dropoff_signal = bool(re.search(r"\b(pick up|pickup|drop off|dropoff)\b", lowered))
        pickup_dropoff_schedule_like = bool(
            pickup_dropoff_signal
            and (
                bool(time_mentions)
                or weekday_hint is not None
                or any(token in lowered for token in ("today", "tomorrow", "tonight", "morning", "afternoon", "evening"))
            )
        )
        obligation = bool(obligation_verbs or (pickup_dropoff_signal and not pickup_dropoff_schedule_like))
        ambiguity = bool(
            "?" in lowered
            or re.search(r"\b(not sure|maybe|something changed|after game|changed for|might shift|may move|weather)\b", lowered)
        )
        promotional_noise = bool(re.search(r"\b(newsletter|promo|promotion|features|unsubscribe|sale)\b", lowered))

        return {
            "cancellation": cancellation,
            "time_change": time_change,
            "conflict_reported": conflict_reported,
            "deadline": deadline,
            "obligation": obligation,
            "ambiguity": ambiguity,
            "promotional_noise": promotional_noise,
            "schedule_topic": schedule_topic,
            "schedule_intent": schedule_intent,
            "explicit_time": bool(time_mentions),
            "weekday_hint": weekday_hint,
            "time_mentions": time_mentions,
        }

    def _parse_iso_datetime(self, raw_value: str | None) -> datetime | None:
        if not raw_value:
            return None
        normalized = str(raw_value).replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def _extract_weekday_hint(self, *, lowered: str) -> int | None:
        weekdays = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
        for index, weekday in enumerate(weekdays):
            if re.search(rf"\b{weekday}\b", lowered):
                return index
        return None

    def _next_weekday_anchor(
        self,
        *,
        reference_dt: datetime,
        weekday: int,
        hour: int,
        minute: int,
    ) -> datetime:
        days_delta = (weekday - reference_dt.weekday()) % 7
        candidate = (reference_dt + timedelta(days=days_delta)).replace(
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0,
        )
        if candidate <= reference_dt:
            candidate = candidate + timedelta(days=7)
        return candidate

    def _extract_time_mentions(self, *, lowered: str) -> list[tuple[int, int]]:
        mentions: list[tuple[int, int]] = []
        for match in re.finditer(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", lowered):
            hour = int(match.group(1)) % 12
            if str(match.group(3)) == "pm":
                hour += 12
            minute = int(match.group(2) or "0")
            mentions.append((hour, minute))

        for match in re.finditer(r"\b(?:at\s*)?(\d{1,2}):(\d{2})\b", lowered):
            hour = int(match.group(1))
            minute = int(match.group(2))
            if hour >= 24 or minute >= 60:
                continue

            inferred_hour = hour
            if 1 <= hour <= 11:
                if bool(re.search(r"\b(evening|tonight|dinner|reservation|after work|practice)\b", lowered)):
                    inferred_hour = hour + 12
                elif bool(re.search(r"\b(am|morning)\b", lowered)):
                    inferred_hour = hour
            elif hour == 12 and bool(re.search(r"\b(am|morning)\b", lowered)):
                inferred_hour = 0

            candidate = (inferred_hour, minute)
            if candidate not in mentions:
                mentions.append(candidate)

        if re.search(r"\bnoon\b", lowered):
            if (12, 0) not in mentions:
                mentions.append((12, 0))
        if re.search(r"\bmidnight\b", lowered):
            if (0, 0) not in mentions:
                mentions.append((0, 0))
        return mentions

    def _schedule_rows_from_projection(self, *, projection: Mapping[str, Any]) -> list[dict[str, Any]]:
        schedules = projection.get("schedules")
        if not isinstance(schedules, Mapping):
            return []
        rows: list[dict[str, Any]] = []
        for schedule_id, value in schedules.items():
            if not isinstance(value, Mapping):
                continue
            status = str(value.get("status") or "scheduled")
            if status == "cancelled":
                continue
            row = dict(value)
            row["schedule_id"] = str(value.get("schedule_id") or schedule_id)
            rows.append(row)
        rows.sort(key=lambda row: str(row.get("schedule_id") or ""))
        return rows

    def _schedule_row_from_projection(
        self,
        *,
        projection: Mapping[str, Any],
        schedule_id: str,
    ) -> dict[str, Any] | None:
        schedules = projection.get("schedules")
        if not isinstance(schedules, Mapping):
            return None
        row = schedules.get(schedule_id)
        if not isinstance(row, Mapping):
            return None
        resolved = dict(row)
        resolved["schedule_id"] = str(row.get("schedule_id") or schedule_id)
        return resolved

    def _resolve_schedule_dependency_for_message(
        self,
        *,
        raw_content: str,
        projection: Mapping[str, Any],
    ) -> str:
        lowered = raw_content.lower()
        stop_words = {
            "the",
            "and",
            "for",
            "with",
            "from",
            "that",
            "this",
            "your",
            "our",
            "now",
            "please",
            "before",
            "after",
            "about",
            "start",
            "starts",
            "moved",
            "cancelled",
            "canceled",
            "earlier",
            "later",
        }
        content_tokens = {
            token
            for token in re.findall(r"[a-z0-9]+", lowered)
            if len(token) > 2 and token not in stop_words
        }
        if not content_tokens:
            return ""

        weekday_hint = self._extract_weekday_hint(lowered=lowered)
        time_mentions = self._extract_time_mentions(lowered=lowered)
        rows = self._schedule_rows_from_projection(projection=projection)

        best_schedule_id = ""
        best_score = 0
        for row in rows:
            schedule_id = str(row.get("schedule_id") or "")
            title = str(row.get("title") or "").lower()
            title_tokens = {
                token
                for token in re.findall(r"[a-z0-9]+", title)
                if len(token) > 2 and token not in stop_words
            }
            overlap = content_tokens.intersection(title_tokens)
            score = len(overlap)

            start_dt = self._parse_iso_datetime(str(row.get("start_at") or ""))
            if weekday_hint is not None and start_dt is not None and start_dt.weekday() == weekday_hint:
                score += 2
            if time_mentions and start_dt is not None:
                for hour, minute in time_mentions:
                    if abs(((start_dt.hour * 60 + start_dt.minute) - (hour * 60 + minute))) <= 30:
                        score += 1
                        break

            if len(rows) == 1 and bool(re.search(r"\b(practice|game|meeting|appointment)\b", lowered)):
                score += 1

            if score > best_score:
                best_score = score
                best_schedule_id = schedule_id

        if best_score < 2:
            return ""
        return best_schedule_id

    def _find_schedule_conflict(
        self,
        *,
        projection: Mapping[str, Any],
        start_at: str,
        end_at: str,
        exclude_schedule_id: str | None = None,
        candidate_member_id: str | None = None,
        conflict_type_hint: str | None = None,
    ) -> str:
        details = self._detect_schedule_conflict_details(
            projection=projection,
            start_at=start_at,
            end_at=end_at,
            exclude_schedule_id=exclude_schedule_id,
            candidate_member_id=candidate_member_id,
            conflict_type_hint=conflict_type_hint,
        )
        return str(details.get("conflict_schedule_id") or "")

    def _conflict_severity_from_overlap(self, *, overlap_seconds: int, overlap_count: int) -> str:
        if overlap_count >= 3:
            return "critical"
        if overlap_count >= 2:
            return "high"
        if overlap_seconds >= 45 * 60:
            return "high"
        if overlap_seconds >= 15 * 60:
            return "medium"
        return "low"

    def _detect_schedule_conflict_details(
        self,
        *,
        projection: Mapping[str, Any],
        start_at: str,
        end_at: str,
        exclude_schedule_id: str | None = None,
        candidate_member_id: str | None = None,
        conflict_type_hint: str | None = None,
    ) -> dict[str, Any]:
        candidate_start = self._parse_iso_datetime(start_at)
        candidate_end = self._parse_iso_datetime(end_at)
        if candidate_start is None or candidate_end is None:
            return {
                "conflict_schedule_id": "",
                "conflict_type": "",
                "events_involved": [],
                "severity": "low",
                "requires_decision": False,
            }

        normalized_member_id = str(candidate_member_id or "").strip()
        rows = self._schedule_rows_from_projection(projection=projection)
        rows_by_id = {
            str(row.get("schedule_id") or ""): row
            for row in rows
            if str(row.get("schedule_id") or "")
        }
        overlaps: list[dict[str, Any]] = []

        for row in rows:
            schedule_id = str(row.get("schedule_id") or "")
            if exclude_schedule_id and schedule_id == exclude_schedule_id:
                continue
            row_start = self._parse_iso_datetime(str(row.get("start_at") or ""))
            row_end = self._parse_iso_datetime(str(row.get("end_at") or ""))
            if row_start is None or row_end is None:
                continue
            is_overlap = candidate_start < row_end and candidate_end > row_start
            is_adjacent = False
            if not is_overlap:
                gap_seconds = min(
                    abs(int((candidate_start - row_end).total_seconds())),
                    abs(int((row_start - candidate_end).total_seconds())),
                )
                is_adjacent = gap_seconds <= 15 * 60

            if is_overlap or is_adjacent:
                overlap_start = max(candidate_start, row_start)
                overlap_end = min(candidate_end, row_end)
                overlap_seconds = max(0, int((overlap_end - overlap_start).total_seconds()))
                row_member_id = str(row.get("member_id") or row.get("owner_user_id") or "").strip()
                overlaps.append(
                    {
                        "schedule_id": schedule_id,
                        "row_member_id": row_member_id,
                        "cross_member": bool(
                            normalized_member_id
                            and row_member_id
                            and row_member_id != normalized_member_id
                        ),
                        "near_window": bool(is_adjacent and not is_overlap),
                        "overlap_seconds": overlap_seconds,
                    }
                )

        if not overlaps:
            return {
                "conflict_schedule_id": "",
                "conflict_type": "",
                "events_involved": [],
                "severity": "low",
                "requires_decision": False,
            }

        overlaps.sort(
            key=lambda row: (
                -int(row.get("overlap_seconds") or 0),
                str(row.get("schedule_id") or ""),
            )
        )

        involved_ids: list[str] = []
        involved_id_set: set[str] = set()
        cross_member_detected = False
        for row in overlaps:
            schedule_id = str(row.get("schedule_id") or "")
            if not schedule_id or schedule_id in involved_id_set:
                continue
            involved_id_set.add(schedule_id)
            involved_ids.append(schedule_id)
            if bool(row.get("cross_member")):
                cross_member_detected = True

        # Re-run detection across dependency graph until stable so derived changes
        # surface cascading conflicts under realistic schedule density.
        frontier = list(involved_ids)
        iteration_budget = max(1, len(rows) * 2)
        iteration_count = 0
        while frontier and iteration_count < iteration_budget:
            iteration_count += 1
            anchor_id = frontier.pop(0)
            anchor_row = rows_by_id.get(anchor_id)
            if not isinstance(anchor_row, Mapping):
                continue
            anchor_start = self._parse_iso_datetime(str(anchor_row.get("start_at") or ""))
            anchor_end = self._parse_iso_datetime(str(anchor_row.get("end_at") or ""))
            anchor_member_id = str(anchor_row.get("member_id") or anchor_row.get("owner_user_id") or "").strip()
            if anchor_start is None or anchor_end is None:
                continue

            for row in rows:
                schedule_id = str(row.get("schedule_id") or "")
                if not schedule_id or schedule_id == anchor_id or schedule_id in involved_id_set:
                    continue
                if exclude_schedule_id and schedule_id == exclude_schedule_id:
                    continue

                row_start = self._parse_iso_datetime(str(row.get("start_at") or ""))
                row_end = self._parse_iso_datetime(str(row.get("end_at") or ""))
                if row_start is None or row_end is None:
                    continue
                anchor_overlap = anchor_start < row_end and anchor_end > row_start
                if not anchor_overlap:
                    anchor_gap_seconds = min(
                        abs(int((anchor_start - row_end).total_seconds())),
                        abs(int((row_start - anchor_end).total_seconds())),
                    )
                    if anchor_gap_seconds > 15 * 60:
                        continue
                else:
                    anchor_gap_seconds = 0

                involved_id_set.add(schedule_id)
                involved_ids.append(schedule_id)
                frontier.append(schedule_id)
                row_member_id = str(row.get("member_id") or row.get("owner_user_id") or "").strip()
                if (
                    (anchor_member_id and row_member_id and anchor_member_id != row_member_id)
                    or (normalized_member_id and row_member_id and normalized_member_id != row_member_id)
                ):
                    cross_member_detected = True

        primary = overlaps[0]
        normalized_hint = str(conflict_type_hint or "").strip().lower()
        conflict_type = "cross_member" if cross_member_detected else "direct"
        if len(involved_ids) > len(overlaps) or normalized_hint == "cascade":
            conflict_type = "cascade"
        elif normalized_hint == "derived" and conflict_type == "direct":
            conflict_type = "derived"
        elif normalized_hint == "derived" and conflict_type == "cross_member":
            conflict_type = "cascade"

        overlap_seconds = int(primary.get("overlap_seconds") or 0)
        near_window = bool(primary.get("near_window"))
        severity = self._conflict_severity_from_overlap(
            overlap_seconds=overlap_seconds,
            overlap_count=len(involved_ids),
        )
        if near_window and severity == "low":
            severity = "medium"
        if cross_member_detected and severity == "low":
            severity = "medium"
        if conflict_type == "cascade" and severity in {"low", "medium"}:
            severity = "high"
        requires_decision = bool(
            conflict_type in {"cross_member", "derived", "cascade"}
            or severity in {"medium", "high", "critical"}
        )
        return {
            "conflict_schedule_id": str(primary.get("schedule_id") or ""),
            "conflict_type": conflict_type,
            "events_involved": [schedule_id for schedule_id in involved_ids if schedule_id],
            "severity": severity,
            "requires_decision": requires_decision,
        }

    def _detect_reported_schedule_conflict(
        self,
        *,
        raw_content: str,
        created_at: str,
        projection: Mapping[str, Any],
        member_id: str,
    ) -> dict[str, Any]:
        lowered = raw_content.lower()
        rows = self._schedule_rows_from_projection(projection=projection)
        if not rows:
            return {
                "conflict_schedule_id": "",
                "conflict_type": "",
                "events_involved": [],
                "severity": "medium",
                "requires_decision": True,
            }

        derived_start_at, derived_end_at = self._derive_message_schedule_window(
            raw_content=raw_content,
            created_at=created_at,
        )
        inferred_window_conflict = self._detect_schedule_conflict_details(
            projection=projection,
            start_at=derived_start_at,
            end_at=derived_end_at,
            candidate_member_id=member_id,
            conflict_type_hint="derived",
        )
        if str(inferred_window_conflict.get("conflict_schedule_id") or ""):
            return inferred_window_conflict

        stop_words = {
            "the",
            "and",
            "for",
            "with",
            "from",
            "that",
            "this",
            "our",
            "your",
            "time",
            "at",
        }
        content_tokens = {
            token
            for token in re.findall(r"[a-z0-9]+", lowered)
            if len(token) > 2 and token not in stop_words
        }

        scored_rows: list[tuple[int, dict[str, Any]]] = []
        for row in rows:
            title_tokens = {
                token
                for token in re.findall(r"[a-z0-9]+", str(row.get("title") or "").lower())
                if len(token) > 2 and token not in stop_words
            }
            score = len(content_tokens.intersection(title_tokens))
            if score > 0:
                scored_rows.append((score, row))

        scored_rows.sort(key=lambda item: (-item[0], str(item[1].get("schedule_id") or "")))
        if len(scored_rows) >= 2:
            left = scored_rows[0][1]
            right = scored_rows[1][1]
            left_start = self._parse_iso_datetime(str(left.get("start_at") or ""))
            left_end = self._parse_iso_datetime(str(left.get("end_at") or ""))
            right_start = self._parse_iso_datetime(str(right.get("start_at") or ""))
            right_end = self._parse_iso_datetime(str(right.get("end_at") or ""))
            if left_start is not None and left_end is not None and right_start is not None and right_end is not None:
                overlaps = left_start < right_end and right_start < left_end
                close_window = abs((right_start - left_start).total_seconds()) <= 90 * 60
                if overlaps or close_window:
                    left_member = str(left.get("member_id") or "").strip()
                    right_member = str(right.get("member_id") or "").strip()
                    return {
                        "conflict_schedule_id": str(right.get("schedule_id") or ""),
                        "conflict_type": "cross_member" if left_member and right_member and left_member != right_member else "direct",
                        "events_involved": [
                            str(left.get("schedule_id") or ""),
                            str(right.get("schedule_id") or ""),
                        ],
                        "severity": "high" if overlaps else "medium",
                        "requires_decision": True,
                    }

        if scored_rows:
            anchor = scored_rows[0][1]
            anchor_start = self._parse_iso_datetime(str(anchor.get("start_at") or ""))
            anchor_end = self._parse_iso_datetime(str(anchor.get("end_at") or ""))
            if anchor_start is not None and anchor_end is not None:
                details = self._detect_schedule_conflict_details(
                    projection=projection,
                    start_at=_utc_iso(anchor_start),
                    end_at=_utc_iso(anchor_end),
                    exclude_schedule_id=str(anchor.get("schedule_id") or ""),
                    candidate_member_id=member_id,
                    conflict_type_hint="derived",
                )
                if str(details.get("conflict_schedule_id") or ""):
                    events = [str(anchor.get("schedule_id") or "")]
                    for schedule_id in list(details.get("events_involved") or []):
                        resolved_id = str(schedule_id or "").strip()
                        if resolved_id and resolved_id not in events:
                            events.append(resolved_id)
                    details["events_involved"] = events
                    return details

            return {
                "conflict_schedule_id": str(anchor.get("schedule_id") or ""),
                "conflict_type": "derived",
                "events_involved": [str(anchor.get("schedule_id") or "")],
                "severity": "medium",
                "requires_decision": True,
            }

        return {
            "conflict_schedule_id": "",
            "conflict_type": "",
            "events_involved": [],
            "severity": "medium",
            "requires_decision": True,
        }

    def _deadline_conflict_details(
        self,
        *,
        projection: Mapping[str, Any],
        due_at: str,
    ) -> dict[str, Any]:
        due_dt = self._parse_iso_datetime(due_at)
        if due_dt is None:
            return {
                "conflict_schedule_id": "",
                "conflict_type": "",
                "events_involved": [],
                "severity": "low",
                "requires_decision": False,
            }

        derived_details = self._detect_schedule_conflict_details(
            projection=projection,
            start_at=_utc_iso(due_dt - timedelta(minutes=30)),
            end_at=_utc_iso(due_dt + timedelta(minutes=30)),
            conflict_type_hint="derived",
        )
        if str(derived_details.get("conflict_schedule_id") or ""):
            return derived_details

        nearest_schedule_id = ""
        nearest_distance_seconds: int | None = None
        for row in self._schedule_rows_from_projection(projection=projection):
            schedule_id = str(row.get("schedule_id") or "")
            row_start = self._parse_iso_datetime(str(row.get("start_at") or ""))
            row_end = self._parse_iso_datetime(str(row.get("end_at") or ""))
            if row_start is None or row_end is None:
                continue
            if due_dt.date() != row_start.date() and due_dt.date() != row_end.date():
                continue
            if row_start <= due_dt <= row_end:
                return {
                    "conflict_schedule_id": schedule_id,
                    "conflict_type": "derived",
                    "events_involved": [schedule_id],
                    "severity": "high",
                    "requires_decision": True,
                }
            distance_seconds = min(
                abs(int((row_start - due_dt).total_seconds())),
                abs(int((row_end - due_dt).total_seconds())),
            )
            if nearest_distance_seconds is None or distance_seconds < nearest_distance_seconds:
                nearest_distance_seconds = distance_seconds
                nearest_schedule_id = schedule_id

        if nearest_schedule_id and nearest_distance_seconds is not None and nearest_distance_seconds <= 3600:
            return {
                "conflict_schedule_id": nearest_schedule_id,
                "conflict_type": "derived",
                "events_involved": [nearest_schedule_id],
                "severity": "medium",
                "requires_decision": True,
            }

        return {
            "conflict_schedule_id": "",
            "conflict_type": "",
            "events_involved": [],
            "severity": "low",
            "requires_decision": False,
        }

    def _deadline_conflict_schedule_id(
        self,
        *,
        projection: Mapping[str, Any],
        due_at: str,
    ) -> str:
        details = self._deadline_conflict_details(
            projection=projection,
            due_at=due_at,
        )
        return str(details.get("conflict_schedule_id") or "")

    def _interpret_household_message(
        self,
        *,
        raw_content: str,
        created_at: str,
        projection: Mapping[str, Any],
        member_id: str,
    ) -> dict[str, Any]:
        signals = self._household_message_signals(raw_content=raw_content)
        classification = self._classify_household_message(raw_content=raw_content, signals=signals)

        interpretation_type = "informational"
        confidence = 0.95
        promotion_reason = "informational.no_promotion"
        dependency_schedule_id = ""
        dependency_schedule_title = ""
        conflict_schedule_id = ""
        conflict_type = ""
        conflict_events_involved: list[str] = []
        conflict_severity = ""
        due_at: str | None = None
        derived_start_at = ""
        derived_end_at = ""
        requires_decision = False

        if bool(signals.get("conflict_reported")):
            interpretation_type = "conflict_reported"
            promotion_reason = "decision.conflict_signal_detected"
            conflict_details = self._detect_reported_schedule_conflict(
                raw_content=raw_content,
                created_at=created_at,
                projection=projection,
                member_id=member_id,
            )
            conflict_schedule_id = str(conflict_details.get("conflict_schedule_id") or "")
            conflict_type = str(conflict_details.get("conflict_type") or "")
            conflict_events_involved = [
                str(item).strip()
                for item in list(conflict_details.get("events_involved") or [])
                if str(item).strip()
            ]
            conflict_severity = str(conflict_details.get("severity") or "")
            requires_decision = bool(conflict_details.get("requires_decision", True))
            confidence = 0.9 if conflict_schedule_id else 0.74

        elif bool(signals.get("cancellation")):
            interpretation_type = "cancellation"
            promotion_reason = "calendar.cancellation_signal_detected"
            dependency_schedule_id = self._resolve_schedule_dependency_for_message(
                raw_content=raw_content,
                projection=projection,
            )
            dependency_row = None
            if dependency_schedule_id:
                dependency_row = self._schedule_row_from_projection(
                    projection=projection,
                    schedule_id=dependency_schedule_id,
                )
                dependency_schedule_title = str(dependency_row.get("title") or "") if isinstance(dependency_row, Mapping) else ""
                dependency_start_at = self._parse_iso_datetime(
                    str(dependency_row.get("start_at") or "")
                ) if isinstance(dependency_row, Mapping) else None
                dependency_end_at = self._parse_iso_datetime(
                    str(dependency_row.get("end_at") or "")
                ) if isinstance(dependency_row, Mapping) else None
                if dependency_start_at is not None and dependency_end_at is not None:
                    conflict_details = self._detect_schedule_conflict_details(
                        projection=projection,
                        start_at=_utc_iso(dependency_start_at),
                        end_at=_utc_iso(dependency_end_at),
                        exclude_schedule_id=dependency_schedule_id,
                        candidate_member_id=member_id,
                        conflict_type_hint="derived",
                    )
                    conflict_schedule_id = str(conflict_details.get("conflict_schedule_id") or "")
                    conflict_type = str(conflict_details.get("conflict_type") or "")
                    conflict_events_involved = [
                        str(item).strip()
                        for item in list(conflict_details.get("events_involved") or [])
                        if str(item).strip()
                    ]
                    conflict_severity = str(conflict_details.get("severity") or "")
            if dependency_schedule_id:
                confidence = 0.88
                requires_decision = bool(
                    conflict_schedule_id
                    and conflict_type in {"cross_member", "derived", "cascade"}
                )
            else:
                confidence = 0.62
                requires_decision = bool(signals.get("ambiguity"))

        elif bool(signals.get("time_change")):
            interpretation_type = "time_change"
            promotion_reason = "calendar.time_change_signal_detected"
            dependency_schedule_id = self._resolve_schedule_dependency_for_message(
                raw_content=raw_content,
                projection=projection,
            )
            dependency_row = (
                self._schedule_row_from_projection(
                    projection=projection,
                    schedule_id=dependency_schedule_id,
                )
                if dependency_schedule_id
                else None
            )
            dependency_schedule_title = (
                str(dependency_row.get("title") or "")
                if isinstance(dependency_row, Mapping)
                else ""
            )
            resolved_window = self._derive_time_change_schedule_window(
                raw_content=raw_content,
                created_at=created_at,
                dependency_schedule=dependency_row,
            )
            if resolved_window is not None:
                derived_start_at, derived_end_at = resolved_window
                conflict_details = self._detect_schedule_conflict_details(
                    projection=projection,
                    start_at=derived_start_at,
                    end_at=derived_end_at,
                    exclude_schedule_id=dependency_schedule_id or None,
                    candidate_member_id=member_id,
                    conflict_type_hint="derived",
                )
                conflict_schedule_id = str(conflict_details.get("conflict_schedule_id") or "")
                conflict_type = str(conflict_details.get("conflict_type") or "")
                conflict_events_involved = [
                    str(item).strip()
                    for item in list(conflict_details.get("events_involved") or [])
                    if str(item).strip()
                ]
                conflict_severity = str(conflict_details.get("severity") or "")
                requires_decision = bool(conflict_details.get("requires_decision", bool(conflict_schedule_id)))
                confidence = 0.84 if dependency_schedule_id else 0.72
            else:
                confidence = 0.58 if dependency_schedule_id else 0.5
                requires_decision = bool(dependency_schedule_id)
                if not dependency_schedule_id and bool(re.search(r"\b\d{1,3}\s*minutes?\s*late\b", raw_content.lower())):
                    requires_decision = True

        elif bool(signals.get("deadline")):
            interpretation_type = "deadline"
            promotion_reason = "task.deadline_signal_detected"
            due_at = self._derive_message_due_at(raw_content=raw_content, created_at=created_at)
            confidence = 0.86 if due_at else 0.62
            if due_at:
                conflict_details = self._deadline_conflict_details(
                    projection=projection,
                    due_at=due_at,
                )
                conflict_schedule_id = str(conflict_details.get("conflict_schedule_id") or "")
                conflict_type = str(conflict_details.get("conflict_type") or "")
                conflict_events_involved = [
                    str(item).strip()
                    for item in list(conflict_details.get("events_involved") or [])
                    if str(item).strip()
                ]
                conflict_severity = str(conflict_details.get("severity") or "")
                requires_decision = bool(conflict_details.get("requires_decision", bool(conflict_schedule_id)))

        elif bool(signals.get("obligation")):
            interpretation_type = "obligation"
            promotion_reason = "task.obligation_signal_detected"
            due_at = self._derive_message_due_at(raw_content=raw_content, created_at=created_at)
            confidence = 0.8 if due_at else 0.74
            if due_at:
                conflict_details = self._deadline_conflict_details(
                    projection=projection,
                    due_at=due_at,
                )
                conflict_schedule_id = str(conflict_details.get("conflict_schedule_id") or "")
                conflict_type = str(conflict_details.get("conflict_type") or "")
                conflict_events_involved = [
                    str(item).strip()
                    for item in list(conflict_details.get("events_involved") or [])
                    if str(item).strip()
                ]
                conflict_severity = str(conflict_details.get("severity") or "")
                requires_decision = bool(conflict_details.get("requires_decision", bool(conflict_schedule_id)))

        elif bool(signals.get("ambiguity")):
            interpretation_type = "ambiguity"
            promotion_reason = "decision.ambiguity_detected"
            requires_decision = bool(
                re.search(r"\b(tomorrow|tonight|next week|after|before|schedule|practice|meeting|deadline)\b", raw_content.lower())
            )
            confidence = 0.64 if requires_decision else 0.52

        elif classification == "schedule":
            interpretation_type = "schedule_create"
            promotion_reason = "calendar.schedule_signal_detected"
            derived_start_at, derived_end_at = self._derive_message_schedule_window(
                raw_content=raw_content,
                created_at=created_at,
            )
            conflict_details = self._detect_schedule_conflict_details(
                projection=projection,
                start_at=derived_start_at,
                end_at=derived_end_at,
                candidate_member_id=member_id,
            )
            conflict_schedule_id = str(conflict_details.get("conflict_schedule_id") or "")
            conflict_type = str(conflict_details.get("conflict_type") or "")
            conflict_events_involved = [
                str(item).strip()
                for item in list(conflict_details.get("events_involved") or [])
                if str(item).strip()
            ]
            conflict_severity = str(conflict_details.get("severity") or "")
            requires_decision = bool(conflict_details.get("requires_decision", bool(conflict_schedule_id)))
            confidence = 0.84 if conflict_schedule_id else 0.81

        if interpretation_type != "ambiguity" and interpretation_type != "informational":
            if confidence < HOUSEHOLD_MESSAGE_INTERPRETATION_CONFIDENCE_THRESHOLD:
                if interpretation_type in {"conflict_reported", "schedule_create"}:
                    requires_decision = True
                elif interpretation_type in {"time_change", "cancellation"} and not dependency_schedule_id:
                    requires_decision = True

        promotional_noise = bool(signals.get("promotional_noise"))
        if promotional_noise and interpretation_type == "informational":
            confidence = 0.99

        upm_profile = self._upm_priority_profile_for_message(
            projection=projection,
            raw_content=raw_content,
            interpretation_type=interpretation_type,
            interpretation={
                "confidence": confidence,
                "requires_decision": requires_decision,
                "dependency_schedule_id": dependency_schedule_id,
                "conflict_schedule_id": conflict_schedule_id,
                "conflict_type": conflict_type,
                "conflict_events_involved": conflict_events_involved,
                "due_at": due_at,
                "derived_start_at": derived_start_at,
                "derived_end_at": derived_end_at,
                "promotional_noise": promotional_noise,
            },
            member_id=member_id,
            context={
                "dependency_schedule_id": dependency_schedule_id,
                "conflict_schedule_id": conflict_schedule_id,
                "conflict_type": conflict_type,
                "conflict_events_involved": conflict_events_involved,
                "due_at": due_at or "",
                "derived_start_at": derived_start_at,
                "derived_end_at": derived_end_at,
            },
        )

        upm_conflict_schedule_id = str(upm_profile.get("conflict_schedule_id") or "").strip()
        if upm_conflict_schedule_id and not conflict_schedule_id:
            conflict_schedule_id = upm_conflict_schedule_id
            conflict_type = str(upm_profile.get("conflict_type") or conflict_type or "derived")
            conflict_events_involved = [
                str(item).strip()
                for item in list(upm_profile.get("conflict_events_involved") or [])
                if str(item).strip()
            ]
            if not conflict_severity:
                conflict_severity = "medium"

        # Re-run UPM with resolved conflict context before routing to guarantee consistency.
        upm_profile = self._upm_priority_profile_for_message(
            projection=projection,
            raw_content=raw_content,
            interpretation_type=interpretation_type,
            interpretation={
                "confidence": confidence,
                "requires_decision": requires_decision,
                "dependency_schedule_id": dependency_schedule_id,
                "conflict_schedule_id": conflict_schedule_id,
                "conflict_type": conflict_type,
                "conflict_events_involved": conflict_events_involved,
                "due_at": due_at,
                "derived_start_at": derived_start_at,
                "derived_end_at": derived_end_at,
                "promotional_noise": promotional_noise,
            },
            member_id=member_id,
            context={
                "dependency_schedule_id": dependency_schedule_id,
                "conflict_schedule_id": conflict_schedule_id,
                "conflict_type": conflict_type,
                "conflict_events_involved": conflict_events_involved,
                "due_at": due_at or "",
                "derived_start_at": derived_start_at,
                "derived_end_at": derived_end_at,
            },
        )
        requires_decision = bool(upm_profile.get("requires_decision"))

        return {
            "classification": classification,
            "interpretation_type": interpretation_type,
            "confidence": round(confidence, 4),
            "promotion_reason": promotion_reason,
            "dependency_schedule_id": dependency_schedule_id,
            "dependency_schedule_title": dependency_schedule_title,
            "conflict_schedule_id": conflict_schedule_id,
            "conflict_type": conflict_type,
            "conflict_events_involved": conflict_events_involved,
            "conflict_severity": conflict_severity,
            "conflict_details": {
                "type": conflict_type,
                "events": list(conflict_events_involved),
                "severity": conflict_severity or "low",
                "requires_decision": bool(requires_decision),
            },
            "due_at": due_at,
            "derived_start_at": derived_start_at,
            "derived_end_at": derived_end_at,
            "requires_decision": requires_decision,
            "upm": {
                "priority_score": int(upm_profile.get("priority_score") or 0),
                "priority_class": str(upm_profile.get("priority_class") or HOUSEHOLD_UPM_PRIORITY_NOISE),
                "requires_decision": bool(upm_profile.get("requires_decision")),
                "conflict_risk": bool(upm_profile.get("conflict_risk")),
                "state_dependency": bool(upm_profile.get("state_dependency")),
                "decision_score": round(float(upm_profile.get("decision_score") or 0.0), 4),
                "actionability_score": round(float(upm_profile.get("actionability_score") or 0.0), 4),
                "confidence_score": round(float(upm_profile.get("confidence_score") or 0.0), 4),
                "actionability_threshold": round(float(upm_profile.get("actionability_threshold") or 0.0), 4),
                "confidence_min": round(float(upm_profile.get("confidence_min") or 0.0), 4),
                "borderline_event": bool(upm_profile.get("borderline_event")),
                "event_density_score": round(float(upm_profile.get("event_density_score") or 0.0), 4),
                "decision_queue_score": round(float(upm_profile.get("decision_queue_score") or 0.0), 4),
                "conflict_backlog_score": round(float(upm_profile.get("conflict_backlog_score") or 0.0), 4),
                "household_state_load": round(float(upm_profile.get("household_state_load") or 0.0), 4),
                "conflict_forced_eligibility": bool(upm_profile.get("conflict_forced_eligibility")),
                "dependency_forced_eligibility": bool(upm_profile.get("dependency_forced_eligibility")),
                "ambiguity_forced_eligibility": bool(upm_profile.get("ambiguity_forced_eligibility")),
                "priority_signals": [
                    str(item).strip()
                    for item in list(upm_profile.get("priority_signals") or [])
                    if str(item).strip()
                ],
            },
        }

    def _compact_message_title(self, raw_content: str, *, fallback: str) -> str:
        normalized = " ".join(str(raw_content or "").split())
        if not normalized:
            return fallback
        words = normalized.split(" ")
        compact = " ".join(words[:8]).strip()
        if not compact:
            return fallback
        return compact[:120]

    def _derive_message_due_at(self, *, raw_content: str, created_at: str) -> str | None:
        lowered = raw_content.lower()
        reference_dt = self._parse_iso_datetime(created_at) or _utc_now()
        weekday_hint = self._extract_weekday_hint(lowered=lowered)
        time_mentions = self._extract_time_mentions(lowered=lowered)

        if weekday_hint is not None and re.search(r"\b(by|before|on)\b", lowered):
            hour, minute = time_mentions[0] if time_mentions else (17, 0)
            return _utc_iso(
                self._next_weekday_anchor(
                    reference_dt=reference_dt,
                    weekday=weekday_hint,
                    hour=hour,
                    minute=minute,
                )
            )

        if "tomorrow" in lowered:
            due_dt = (reference_dt + timedelta(days=1)).replace(hour=17, minute=0, second=0, microsecond=0)
            return _utc_iso(due_dt)

        if time_mentions and re.search(r"\b(by|before|due)\b", lowered):
            hour, minute = time_mentions[0]
            due_dt = reference_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if due_dt <= reference_dt:
                due_dt = due_dt + timedelta(days=1)
            return _utc_iso(due_dt)

        if any(token in lowered for token in ("today", "tonight", "asap", "urgent", "end of day", "eod")):
            due_dt = reference_dt.replace(hour=20, minute=0, second=0, microsecond=0)
            if due_dt <= reference_dt:
                due_dt = reference_dt + timedelta(hours=2)
            return _utc_iso(due_dt)

        return None

    def _derive_message_schedule_window(self, *, raw_content: str, created_at: str) -> tuple[str, str]:
        lowered = raw_content.lower()
        reference_dt = self._parse_iso_datetime(created_at) or _utc_now()
        weekday_hint = self._extract_weekday_hint(lowered=lowered)
        time_mentions = self._extract_time_mentions(lowered=lowered)

        start_dt = reference_dt + timedelta(hours=1)
        if weekday_hint is not None:
            hour, minute = time_mentions[0] if time_mentions else (9, 0)
            start_dt = self._next_weekday_anchor(
                reference_dt=reference_dt,
                weekday=weekday_hint,
                hour=hour,
                minute=minute,
            )
        elif "tomorrow" in lowered:
            start_dt = (reference_dt + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        elif "morning" in lowered:
            start_dt = reference_dt.replace(hour=9, minute=0, second=0, microsecond=0)
        elif "afternoon" in lowered:
            start_dt = reference_dt.replace(hour=14, minute=0, second=0, microsecond=0)
        elif "evening" in lowered or "tonight" in lowered:
            start_dt = reference_dt.replace(hour=18, minute=0, second=0, microsecond=0)
        elif time_mentions:
            hour, minute = time_mentions[0]
            start_dt = reference_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)

        if start_dt <= reference_dt:
            start_dt = start_dt + timedelta(days=1)

        end_dt = start_dt + timedelta(minutes=30)
        return _utc_iso(start_dt), _utc_iso(end_dt)

    def _derive_time_change_schedule_window(
        self,
        *,
        raw_content: str,
        created_at: str,
        dependency_schedule: Mapping[str, Any] | None,
    ) -> tuple[str, str] | None:
        lowered = raw_content.lower()
        reference_dt = self._parse_iso_datetime(created_at) or _utc_now()
        dependency_start = self._parse_iso_datetime(
            str(dependency_schedule.get("start_at") or "")
        ) if isinstance(dependency_schedule, Mapping) else None
        dependency_end = self._parse_iso_datetime(
            str(dependency_schedule.get("end_at") or "")
        ) if isinstance(dependency_schedule, Mapping) else None
        duration = (
            dependency_end - dependency_start
            if dependency_start is not None and dependency_end is not None
            else timedelta(minutes=30)
        )
        weekday_hint = self._extract_weekday_hint(lowered=lowered)

        time_mentions = self._extract_time_mentions(lowered=lowered)
        if len(time_mentions) >= 2:
            hour, minute = time_mentions[-1]
            base_date = dependency_start.date() if dependency_start is not None else reference_dt.date()
            start_dt = datetime(
                year=base_date.year,
                month=base_date.month,
                day=base_date.day,
                hour=hour,
                minute=minute,
                tzinfo=UTC,
            )
            if start_dt <= reference_dt and dependency_start is None:
                start_dt = start_dt + timedelta(days=1)
            return _utc_iso(start_dt), _utc_iso(start_dt + duration)

        if len(time_mentions) == 1 and bool(re.search(r"\b(moved|rescheduled|changed to|at)\b", lowered)):
            hour, minute = time_mentions[0]
            base_date = dependency_start.date() if dependency_start is not None else reference_dt.date()
            start_dt = datetime(
                year=base_date.year,
                month=base_date.month,
                day=base_date.day,
                hour=hour,
                minute=minute,
                tzinfo=UTC,
            )
            if start_dt <= reference_dt and dependency_start is None:
                start_dt = start_dt + timedelta(days=1)
            return _utc_iso(start_dt), _utc_iso(start_dt + duration)

        if weekday_hint is not None:
            if time_mentions:
                hour, minute = time_mentions[0]
            elif "morning" in lowered:
                hour, minute = (9, 0)
            elif "afternoon" in lowered:
                hour, minute = (14, 0)
            elif "evening" in lowered or "tonight" in lowered:
                hour, minute = (18, 0)
            elif dependency_start is not None:
                hour, minute = (dependency_start.hour, dependency_start.minute)
            else:
                hour, minute = (17, 0)
            start_dt = self._next_weekday_anchor(
                reference_dt=reference_dt,
                weekday=weekday_hint,
                hour=hour,
                minute=minute,
            )
            return _utc_iso(start_dt), _utc_iso(start_dt + duration)

        if dependency_start is not None and any(token in lowered for token in ("morning", "afternoon", "evening", "tonight")):
            if "morning" in lowered:
                hour, minute = (9, 0)
            elif "afternoon" in lowered:
                hour, minute = (14, 0)
            else:
                hour, minute = (18, 0)
            start_dt = dependency_start.replace(hour=hour, minute=minute, second=0, microsecond=0)
            return _utc_iso(start_dt), _utc_iso(start_dt + duration)

        if dependency_start is not None and "tomorrow" in lowered:
            start_dt = (dependency_start + timedelta(days=1)).replace(second=0, microsecond=0)
            return _utc_iso(start_dt), _utc_iso(start_dt + duration)

        if dependency_start is None:
            if weekday_hint is not None:
                if time_mentions:
                    hour, minute = time_mentions[0]
                elif "morning" in lowered:
                    hour, minute = (9, 0)
                elif "afternoon" in lowered:
                    hour, minute = (14, 0)
                elif "evening" in lowered or "tonight" in lowered:
                    hour, minute = (18, 0)
                else:
                    hour, minute = (17, 0)
                start_dt = self._next_weekday_anchor(
                    reference_dt=reference_dt,
                    weekday=weekday_hint,
                    hour=hour,
                    minute=minute,
                )
                return _utc_iso(start_dt), _utc_iso(start_dt + duration)

            if "tomorrow" in lowered:
                hour, minute = time_mentions[0] if time_mentions else (9, 0)
                start_dt = (reference_dt + timedelta(days=1)).replace(
                    hour=hour,
                    minute=minute,
                    second=0,
                    microsecond=0,
                )
                return _utc_iso(start_dt), _utc_iso(start_dt + duration)

        if dependency_start is not None and dependency_end is not None:
            minute_shift_match = re.search(r"\b(\d{1,3})\s*minutes?\s*(earlier|later)\b", lowered)
            if minute_shift_match is not None:
                minutes = int(minute_shift_match.group(1))
                direction = str(minute_shift_match.group(2))
                delta = timedelta(minutes=minutes)
                if direction == "earlier":
                    delta = -delta
                return _utc_iso(dependency_start + delta), _utc_iso(dependency_end + delta)

            if re.search(r"\bearlier\b", lowered):
                delta = timedelta(minutes=-30)
                return _utc_iso(dependency_start + delta), _utc_iso(dependency_end + delta)
            if re.search(r"\blater\b", lowered):
                delta = timedelta(minutes=30)
                return _utc_iso(dependency_start + delta), _utc_iso(dependency_end + delta)

        late_match = re.search(r"\b(\d{1,3})\s*minutes?\s*late\b", lowered)
        if late_match is not None:
            delay_minutes = int(late_match.group(1))
            if dependency_start is not None and dependency_end is not None:
                delta = timedelta(minutes=delay_minutes)
                return _utc_iso(dependency_start + delta), _utc_iso(dependency_end + delta)
            start_dt = reference_dt + timedelta(minutes=delay_minutes)
            end_dt = start_dt + duration
            return _utc_iso(start_dt), _utc_iso(end_dt)

        return None

    def _task_payload_from_household_message(
        self,
        normalized: Mapping[str, Any],
        *,
        interpretation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        raw_content = str(normalized.get("raw_content") or "")
        created_at = str(normalized.get("created_at") or _utc_iso())
        lowered = raw_content.lower()
        interpretation_type = str((interpretation or {}).get("interpretation_type") or "")

        priority = "high" if any(token in lowered for token in ("urgent", "asap", "today", "tonight")) else "medium"
        if interpretation_type in {"deadline", "obligation"} and priority == "medium":
            priority = "high"

        due_at = str((interpretation or {}).get("due_at") or "").strip() or self._derive_message_due_at(
            raw_content=raw_content,
            created_at=created_at,
        )
        return {
            "title": self._compact_message_title(raw_content, fallback="Household follow-up"),
            "description": raw_content,
            "priority": priority,
            "due_at": due_at,
            "owner_user_id": str(normalized.get("member_id") or "system"),
        }

    def _schedule_payload_from_household_message(
        self,
        normalized: Mapping[str, Any],
        *,
        interpretation: Mapping[str, Any] | None = None,
        projection: Mapping[str, Any] | None = None,
    ) -> dict[str, str]:
        raw_content = str(normalized.get("raw_content") or "")
        created_at = str(normalized.get("created_at") or _utc_iso())
        interpretation_payload = interpretation or {}

        derived_start_at = str(interpretation_payload.get("derived_start_at") or "").strip()
        derived_end_at = str(interpretation_payload.get("derived_end_at") or "").strip()
        if derived_start_at and derived_end_at:
            start_at, end_at = derived_start_at, derived_end_at
        else:
            start_at, end_at = self._derive_message_schedule_window(
                raw_content=raw_content,
                created_at=created_at,
            )

        dependency_schedule_title = str(interpretation_payload.get("dependency_schedule_title") or "").strip()
        dependency_schedule_id = str(interpretation_payload.get("dependency_schedule_id") or "").strip()
        if not dependency_schedule_title and dependency_schedule_id and isinstance(projection, Mapping):
            dependency_row = self._schedule_row_from_projection(
                projection=projection,
                schedule_id=dependency_schedule_id,
            )
            if isinstance(dependency_row, Mapping):
                dependency_schedule_title = str(dependency_row.get("title") or "").strip()

        interpretation_type = str(interpretation_payload.get("interpretation_type") or "")
        title = self._compact_message_title(raw_content, fallback="Household schedule item")
        if interpretation_type == "time_change" and dependency_schedule_title:
            title = dependency_schedule_title[:120]

        return {
            "title": title,
            "start_at": start_at,
            "end_at": end_at,
        }

    def _decision_title_from_household_message(self, raw_content: str) -> str:
        title = self._compact_message_title(raw_content, fallback="Resolve household message")
        return f"Resolve: {title}"[:140]

    def _normalize_schedule_create_payload(self, *, payload: dict[str, Any], request_id: str) -> dict[str, str]:
        title = str(payload.get("title") or "").strip()
        if not title:
            raise ValueError("schedule.create title is required")

        start_at_raw = str(payload.get("start_at") or "").strip()
        end_at_raw = str(payload.get("end_at") or "").strip()
        if not start_at_raw or not end_at_raw:
            raise ValueError("schedule.create requires start_at and end_at")

        start_at = self._normalize_schedule_timestamp(start_at_raw)
        end_at = self._normalize_schedule_timestamp(end_at_raw)
        if end_at <= start_at:
            raise ValueError("schedule.create requires end_at > start_at")

        schedule_id = str(payload.get("schedule_id") or "").strip() or f"schedule-{request_id}"
        return {
            "schedule_id": schedule_id,
            "title": title,
            "start_at": start_at,
            "end_at": end_at,
        }

    def _normalize_reminder_create_payload(self, *, payload: dict[str, Any], request_id: str) -> dict[str, str]:
        title = str(payload.get("title") or "").strip()
        if not title:
            raise ValueError("reminder.create title is required")

        trigger_at_raw = str(payload.get("trigger_at") or "").strip()
        if not trigger_at_raw:
            raise ValueError("reminder.create trigger_at is required")

        reminder_id = str(payload.get("reminder_id") or "").strip() or f"reminder-{request_id}"
        message = str(payload.get("message") or "").strip()
        return {
            "reminder_id": reminder_id,
            "title": title,
            "message": message,
            "trigger_at": self._normalize_reminder_timestamp(trigger_at_raw),
        }

    def _normalize_reminder_timestamp(self, raw_timestamp: str) -> str:
        normalized = raw_timestamp.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError("reminder timestamps must be ISO-8601") from exc

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return _utc_iso(parsed.astimezone(UTC))

    def _normalize_schedule_timestamp(self, raw_timestamp: str) -> str:
        normalized = raw_timestamp.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError("schedule timestamps must be ISO-8601") from exc

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return _utc_iso(parsed.astimezone(UTC))

    def _normalize_task_create_payload(
        self,
        *,
        payload: dict[str, Any],
        request_id: str,
        default_owner_user_id: str,
        rules_snapshot: Mapping[str, Any] | Any = None,
    ) -> dict[str, Any]:
        resolved_rules = dict(rules_snapshot) if isinstance(rules_snapshot, Mapping) else {}
        title = str(payload.get("title") or "").strip()
        if not title:
            raise ValueError("task.create title is required")
        max_title_length = int(resolved_rules.get("task_title_max_length") or 160)
        if len(title) > max_title_length:
            raise ValueError(f"task.create title exceeds {max_title_length} characters")

        description = str(payload.get("description") or "").strip()
        priority = str(payload.get("priority") or "medium").strip().lower() or "medium"
        allowed_priorities = resolved_rules.get("task_priority_values")
        if isinstance(allowed_priorities, list) and allowed_priorities:
            priority_values = frozenset(str(item).strip().lower() for item in allowed_priorities if str(item).strip())
        else:
            priority_values = TASK_PRIORITY_VALUES

        if priority not in priority_values:
            raise ValueError("task.create priority must be low, medium, or high")

        due_at_raw = str(payload.get("due_at") or "").strip()
        due_at = self._normalize_due_at(due_at_raw)

        owner_user_id = str(payload.get("owner_user_id") or payload.get("assignee_user_id") or "").strip()
        if not owner_user_id:
            owner_user_id = default_owner_user_id

        return {
            "request_id": request_id,
            "title": title,
            "description": description,
            "priority": priority,
            "due_at": due_at,
            "owner_user_id": owner_user_id,
            "requires_financial_approval": bool(payload.get("requires_financial_approval", False)),
        }

    def _normalize_due_at(self, raw_due_at: str) -> str | None:
        if not raw_due_at:
            return None

        normalized = raw_due_at.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError("task.create due_at must be ISO-8601 when provided") from exc

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)

        return _utc_iso(parsed.astimezone(UTC))

    def _classify_task_risk(
        self,
        task_payload: dict[str, Any],
        *,
        risk_thresholds: Mapping[str, Any] | Any = None,
    ) -> dict[str, Any]:
        resolved_thresholds = dict(risk_thresholds) if isinstance(risk_thresholds, Mapping) else {}
        high_risk_keywords_raw = resolved_thresholds.get("high_risk_keywords")
        if isinstance(high_risk_keywords_raw, list) and high_risk_keywords_raw:
            high_risk_keywords = tuple(
                str(keyword).strip().lower()
                for keyword in high_risk_keywords_raw
                if str(keyword).strip()
            )
        else:
            high_risk_keywords = TASK_HIGH_RISK_KEYWORDS

        financial_approval_is_high = bool(resolved_thresholds.get("financial_approval_is_high", True))
        high_priority_promotes_to_medium = bool(
            resolved_thresholds.get("high_priority_promotes_to_medium", True)
        )
        due_date_promotes_to_medium = bool(
            resolved_thresholds.get("due_date_promotes_to_medium", True)
        )

        title_and_description = (
            f"{task_payload.get('title', '')} {task_payload.get('description', '')}".strip().lower()
        )

        level = "low"
        signals: list[str] = []

        if financial_approval_is_high and bool(task_payload.get("requires_financial_approval")):
            level = "high"
            signals.append("financial_approval_requested")

        if any(keyword in title_and_description for keyword in high_risk_keywords):
            level = "high"
            signals.append("high_risk_keywords")

        priority = str(task_payload.get("priority") or "medium")
        if level != "high" and high_priority_promotes_to_medium and priority == "high":
            level = "medium"
            signals.append("high_priority")

        if due_date_promotes_to_medium and task_payload.get("due_at") and level == "low":
            level = "medium"
            signals.append("scheduled_deadline")

        if not signals:
            signals.append("internal_task_only")

        rationale_map = {
            "low": "Low-risk internal household task.",
            "medium": "Medium-risk task due to urgency or scheduling constraints.",
            "high": "High-risk task requiring explicit human confirmation.",
        }

        return {
            "level": level,
            "signals": sorted(set(signals)),
            "rationale": rationale_map[level],
        }

    def _task_fsm_transitions(self, *, risk_level: str) -> list[dict[str, Any]]:
        transitions = [
            ("received", "rules_passed"),
            ("rules_passed", "risk_assessed"),
        ]
        final_state = "pending_approval" if risk_level == "high" else "created"
        transitions.append(("risk_assessed", final_state))

        transition_rows: list[dict[str, Any]] = []
        for index, (from_state, to_state) in enumerate(transitions, start=1):
            transition_rows.append(
                {
                    "index": index,
                    "from_state": from_state,
                    "to_state": to_state,
                    "transitioned_at": _utc_iso(),
                }
            )

        return transition_rows

    def _build_household_response(self, *, household_id: str, request_id: str, query: str) -> HouseholdOSRunResponse:
        projection = self.get_projection(household_id)
        tasks = projection.get("tasks")
        open_tasks = len(tasks) if isinstance(tasks, dict) else 0
        normalized_query = query.strip()
        lowered = normalized_query.lower()

        if any(token in lowered for token in ("appointment", "doctor", "dentist", "calendar")):
            urgency = "medium"
            title = "Schedule appointment for tomorrow morning"
            description = "Reserve a low-conflict appointment window pending user approval."
            scheduled_for = "2026-04-20 10:30-11:15"
            summary = "appointment coordination request"
            extracted = ["appointment"]
        elif any(token in lowered for token in ("workout", "exercise", "fitness")):
            urgency = "medium"
            title = "Schedule workout block after evening handoff"
            description = "Add a 45-minute workout block that avoids known family commitments."
            scheduled_for = "2026-04-19 19:00-19:45"
            summary = "fitness planning request"
            extracted = ["fitness"]
        elif any(token in lowered for token in ("dinner", "meal", "cook", "grocery")):
            urgency = "medium"
            title = "Create dinner plan for tonight"
            description = "Select a dinner option and prepare required ingredients after approval."
            scheduled_for = "2026-04-19 18:30-19:15"
            summary = "meal planning request"
            extracted = ["meal"]
        else:
            urgency = "low"
            title = "Adjust household coordination queue"
            description = "Create a single prioritized next action from the household queue."
            scheduled_for = None
            summary = "general household coordination request"
            extracted = []

        action_id = f"{request_id}-primary"
        state_version = int(projection.get("state_version", 0)) + 1
        pending_count = len(projection.get("pending_actions", [])) + 1

        return HouseholdOSRunResponse(
            request_id=request_id,
            intent_interpretation=IntentInterpretation(
                summary=summary,
                urgency=urgency,
                extracted_signals=extracted,
            ),
            current_state_summary=CurrentStateSummary(
                household_id=household_id,
                reference_time=_utc_iso(),
                calendar_events=len(projection.get("events", [])),
                open_tasks=open_tasks,
                meals_recorded=0,
                low_grocery_items=[],
                fitness_routines=0,
                constraints_count=0,
                pending_approvals=pending_count,
                state_version=state_version,
            ),
            recommended_action=RecommendedNextAction(
                action_id=action_id,
                title=title,
                description=description,
                urgency=urgency,
                scheduled_for=scheduled_for,
                approval_required=True,
                approval_status="pending",
            ),
            grouped_approval_payload=GroupedApprovalPayload(
                group_id=f"{request_id}-group",
                label="Batch Household Action Execution",
                action_ids=[action_id],
                execution_mode="inert_until_approved",
                approval_status="pending",
            ),
            follow_ups=[],
            reasoning_trace=[
                "Command validated through canonical command path.",
                "Action remains inert until explicit approval is recorded.",
            ],
        )

    def _emit_projection_snapshot(self, *, household_id: str, actor: CommandActor, projection: dict[str, Any]) -> None:
        checksum = self._projection_checksum(projection)
        self._emit_event(
            household_id=household_id,
            actor=actor,
            event_type="projection.snapshot",
            source="runtime.projection",
            payload={
                "checksum": checksum,
                "state_version": int(projection.get("state_version", 0)),
                "recorded_at": _utc_iso(),
            },
            idempotency_key=f"projection.snapshot:{household_id}:{checksum}",
        )

    def _replay_projection(self, *, household_id: str, rows: list[Any]) -> dict[str, Any]:
        # Keep reducer ownership in core replay while documenting canonical helper set:
        # apply_task_created_projection
        # apply_task_completed_projection
        # apply_schedule_created_projection
        # apply_schedule_cancelled_projection
        replay_events = [
            {
                "event_id": str(row.event_id),
                "event_type": str(row.type),
                "timestamp": row.timestamp,
                "household_id": str(getattr(row, "household_id", "") or household_id),
                "payload": dict(row.payload or {}) if isinstance(row.payload, dict) else {},
                "source": str(getattr(row, "source", "runtime.action_pipeline") or "runtime.action_pipeline"),
            }
            for row in rows
        ]
        return project_state(replay_events)

    def _empty_projection(self, household_id: str) -> dict[str, Any]:
        return {
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
            "state_version": 0,
            "last_event_id": "",
            "checksum": "",
            "drift": empty_drift_classification(),
            "drift_reasons": empty_drift_reasons(),
        }

    def _emit_event(
        self,
        *,
        household_id: str,
        actor: CommandActor,
        event_type: str,
        source: str,
        payload: dict[str, Any],
        idempotency_key: str | None = None,
        policy_resolution: PolicyResolution | None = None,
    ) -> object | None:
        timestamp = _utc_now()
        resolved_policy = policy_resolution or self._policy_registry.resolve_policy(timestamp)
        enriched_payload = dict(payload)
        enriched_payload.setdefault("policy_version_id", resolved_policy.version_id)
        enriched_payload.setdefault("evaluation_context_hash", resolved_policy.evaluation_context_hash)

        envelope = CanonicalEventEnvelope(
            event_id=str(uuid4()),
            event_type=event_type,
            user_id=actor.user_id,
            household_id=household_id,
            source=source,
            payload=enriched_payload,
            version=1,
            severity="info",
            idempotency_key=idempotency_key,
            actor_type=actor.actor_type,
            timestamp=timestamp,
        )
        return self._router.route(envelope)

    def _semantic_fingerprint(
        self,
        *,
        command_type: str,
        household_id: str,
        actor: CommandActor,
        payload: dict[str, Any],
    ) -> str:
        return _stable_hash(
            {
                "command_type": command_type,
                "household_id": household_id,
                "actor_type": actor.actor_type,
                "user_id": actor.user_id,
                "payload": payload,
            }
        )

    def _find_duplicate_command(
        self,
        *,
        household_id: str,
        command_type: str,
        semantic_fingerprint: str,
    ) -> dict[str, Any] | None:
        command_events = self._event_log.get_event_logs(
            household_id=household_id,
            event_type="command.received",
            limit=2000,
        )
        for row in command_events:
            payload = dict(row.payload or {}) if isinstance(row.payload, dict) else {}
            if str(payload.get("command_type") or "") != command_type:
                continue
            if str(payload.get("semantic_fingerprint") or "") == semantic_fingerprint:
                return payload
        return None

    def _request_id(self, command_type: str, semantic_fingerprint: str) -> str:
        return f"{command_type.replace('.', '-')}-{semantic_fingerprint[:12]}"

    def _response_for_request(self, projection: dict[str, Any], request_id: str) -> dict[str, Any] | None:
        responses = projection.get("responses")
        if not isinstance(responses, dict):
            return None
        response = responses.get(request_id)
        if isinstance(response, dict):
            return dict(response)
        return None

    def _request_id_from_action(self, projection: dict[str, Any], action_id: str) -> str | None:
        actions = projection.get("actions")
        if not isinstance(actions, dict):
            return None
        payload = actions.get(action_id)
        if not isinstance(payload, dict):
            return None
        request_id = str(payload.get("request_id") or "")
        return request_id or None

    def _action_id_from_request(self, projection: dict[str, Any], request_id: str) -> str | None:
        actions = projection.get("actions")
        if not isinstance(actions, dict):
            return None
        for action_id, payload in actions.items():
            if isinstance(payload, dict) and str(payload.get("request_id") or "") == request_id:
                return str(action_id)
        return None

    def _projection_checksum(self, projection: dict[str, Any]) -> str:
        checksum_payload = {
            "responses": projection.get("responses", {}),
            "actions": projection.get("actions", {}),
            "tasks": projection.get("tasks", {}),
            "tasks_list": projection.get("tasks_list", []),
            "schedules": projection.get("schedules", {}),
            "schedule_list": projection.get("schedule_list", []),
            "reminders": projection.get("reminders", {}),
            "reminder_list": projection.get("reminder_list", []),
            "notifications": projection.get("notifications", {}),
            "notification_list": projection.get("notification_list", []),
            "ingested_emails": projection.get("ingested_emails", []),
            "email_actions": projection.get("email_actions", []),
            "calendar_events": projection.get("calendar_events", []),
            "calendar_conflicts": projection.get("calendar_conflicts", []),
            "household_messages": projection.get("household_messages", []),
            "household_promotions": projection.get("household_promotions", []),
            "task_transition_log": projection.get("task_transition_log", {}),
            "sagas": projection.get("sagas", {}),
            "control_plane": projection.get("control_plane", {}),
            "policy_bindings": projection.get("policy_bindings", {}),
            "events": projection.get("events", []),
            "state_version": projection.get("state_version", 0),
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
            checksum_payload[projection_key] = projection.get(projection_key)
        return _stable_hash(checksum_payload)


def get_command_runtime_service() -> CommandRuntimeService:
    global _COMMAND_RUNTIME_SERVICE
    if _COMMAND_RUNTIME_SERVICE is None:
        with _COMMAND_RUNTIME_SERVICE_LOCK:
            if _COMMAND_RUNTIME_SERVICE is None:
                _COMMAND_RUNTIME_SERVICE = CommandRuntimeService()
    return _COMMAND_RUNTIME_SERVICE


def reset_command_runtime_service() -> None:
    global _COMMAND_RUNTIME_SERVICE
    with _COMMAND_RUNTIME_SERVICE_LOCK:
        _COMMAND_RUNTIME_SERVICE = None


_COMMAND_RUNTIME_SERVICE: CommandRuntimeService | None = None
_COMMAND_RUNTIME_SERVICE_LOCK = Lock()
