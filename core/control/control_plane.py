from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping, Protocol

from core.health import empty_drift_classification, empty_drift_reasons, has_critical_drift
from core.replay import ReplayValidationError, replay, validate_replay
from core.sagas import SagaDefinition


class ControlPlaneError(ValueError):
    """Raised when control-plane input contracts are invalid."""


class ControlEventEmitter(Protocol):
    def __call__(self, *, event_type: str, payload: dict[str, Any], idempotency_key: str | None = None) -> object | None:
        ...


class ControlEventReader(Protocol):
    def __call__(self) -> Iterable[Mapping[str, Any] | Any]:
        ...


@dataclass(frozen=True)
class CircuitBreakerRule:
    breaker_id: str
    threshold: int
    lookback_events: int
    affected_saga_types: tuple[str, ...] = ("*",)
    failure_event_types: tuple[str, ...] = ("saga.failed", "saga.step_failed", "saga.compensated")
    recovery_event_types: tuple[str, ...] = ("saga.completed",)
    recovery_threshold: int = 2


@dataclass(frozen=True)
class ControlDecision:
    allowed: bool
    status: str
    reason: str | None
    policy_version_id: str | None
    emitted_events: list[str]
    circuit_state: dict[str, Any]
    failure_snapshot: dict[str, Any]
    conflict_snapshot: dict[str, Any]
    replay_validation: dict[str, Any]


@dataclass(frozen=True)
class _ObservedEvent:
    event_id: str
    event_type: str
    timestamp: datetime
    household_id: str
    payload: dict[str, Any]
    source: str


class ControlPlane:
    """Deterministic governance layer for saga admission and containment."""

    def __init__(
        self,
        *,
        breaker_rules: tuple[CircuitBreakerRule, ...] | None = None,
        max_concurrent_high_risk: int = 1,
    ) -> None:
        self._breaker_rules = breaker_rules or (
            CircuitBreakerRule(
                breaker_id="saga_failure_burst",
                threshold=3,
                lookback_events=20,
                affected_saga_types=("*",),
                recovery_threshold=2,
            ),
        )
        self._max_concurrent_high_risk = max(1, int(max_concurrent_high_risk))

    def evaluate_execution(
        self,
        *,
        definition: SagaDefinition,
        household_id: str,
        request_id: str,
        emit_event: ControlEventEmitter,
        read_events: ControlEventReader,
        policy_snapshot: Mapping[str, Any] | None = None,
    ) -> ControlDecision:
        if not definition.id.strip():
            raise ControlPlaneError("saga definition id is required")
        if not household_id.strip():
            raise ControlPlaneError("household_id is required")
        if not request_id.strip():
            raise ControlPlaneError("request_id is required")

        observed_events = self._normalize_events(read_events())
        saga_monitor = self._build_saga_monitor(observed_events)
        saga_type = self._infer_saga_type(
            saga_id=definition.id,
            metadata=definition.metadata,
            saga_types_by_id=saga_monitor["saga_types_by_id"],
        )
        breaker_rules, max_concurrent_high_risk, policy_version_id = self._resolve_policy_control_config(
            policy_snapshot
        )

        failure_snapshot = self._detect_failure_patterns(
            events=observed_events,
            saga_type=saga_type,
            saga_types_by_id=saga_monitor["saga_types_by_id"],
            breaker_rules=breaker_rules,
        )
        conflict_snapshot = self._detect_cross_saga_conflicts(
            definition=definition,
            monitor=saga_monitor,
            events=observed_events,
        )
        circuit_state, transition = self._evaluate_circuit_state(
            events=observed_events,
            saga_type=saga_type,
            saga_types_by_id=saga_monitor["saga_types_by_id"],
            breaker_rules=breaker_rules,
        )

        emitted_events: list[str] = []
        if transition == "open":
            emitted_events.append("system.circuit_opened")
            emit_event(
                event_type="system.circuit_opened",
                payload={
                    "breaker_id": str(circuit_state.get("breaker_id") or ""),
                    "saga_type": saga_type,
                    "request_id": request_id,
                    "reason": str(circuit_state.get("reason") or ""),
                    "failure_count_recent": int(circuit_state.get("failure_count_recent") or 0),
                    "policy_version_id": policy_version_id,
                },
                idempotency_key=(
                    f"control:{request_id}:{circuit_state.get('breaker_id')}:{saga_type}:circuit_opened"
                ),
            )
        elif transition == "close":
            emitted_events.append("system.circuit_closed")
            emit_event(
                event_type="system.circuit_closed",
                payload={
                    "breaker_id": str(circuit_state.get("breaker_id") or ""),
                    "saga_type": saga_type,
                    "request_id": request_id,
                    "reason": str(circuit_state.get("reason") or ""),
                    "recovery_success_count_recent": int(
                        circuit_state.get("recovery_success_count_recent") or 0
                    ),
                    "policy_version_id": policy_version_id,
                },
                idempotency_key=(
                    f"control:{request_id}:{circuit_state.get('breaker_id')}:{saga_type}:circuit_closed"
                ),
            )

        status = "allowed"
        reason: str | None = None

        if bool(conflict_snapshot.get("has_divergence", False)):
            status = "halted"
            reason = "event_log_divergence"
        elif bool(conflict_snapshot.get("has_resource_conflict", False)):
            status = "halted"
            reason = "cross_saga_conflict"
        elif bool(circuit_state.get("is_open", False)):
            status = "halted"
            reason = "circuit_open"
        elif self._should_throttle(
            definition=definition,
            monitor=saga_monitor,
            max_concurrent_high_risk=max_concurrent_high_risk,
        ):
            status = "throttled"
            reason = "risk_throttle"

        if status == "halted":
            emitted_events.append("saga.halted")
            emit_event(
                event_type="saga.halted",
                payload={
                    "saga_id": definition.id,
                    "request_id": request_id,
                    "reason": reason,
                    "conflicts": list(conflict_snapshot.get("conflicting_sagas") or []),
                    "policy_version_id": policy_version_id,
                },
                idempotency_key=f"control:{request_id}:{definition.id}:saga.halted:{reason}",
            )
        elif status == "throttled":
            emitted_events.append("saga.throttled")
            emit_event(
                event_type="saga.throttled",
                payload={
                    "saga_id": definition.id,
                    "request_id": request_id,
                    "reason": reason,
                    "risk_level": str(definition.metadata.get("risk_level") or "low").lower(),
                    "policy_version_id": policy_version_id,
                },
                idempotency_key=f"control:{request_id}:{definition.id}:saga.throttled:{reason}",
            )

        replay_validation = self._validate_replay_consistency(observed_events)

        return ControlDecision(
            allowed=status == "allowed",
            status=status,
            reason=reason,
            policy_version_id=policy_version_id,
            emitted_events=emitted_events,
            circuit_state=circuit_state,
            failure_snapshot=failure_snapshot,
            conflict_snapshot=conflict_snapshot,
            replay_validation=replay_validation,
        )

    def _normalize_events(self, events: Iterable[Mapping[str, Any] | Any]) -> list[_ObservedEvent]:
        observed: list[_ObservedEvent] = []
        for row in events:
            if isinstance(row, Mapping):
                raw = dict(row)
                payload = raw.get("payload")
                event = _ObservedEvent(
                    event_id=str(raw.get("event_id") or raw.get("id") or "").strip(),
                    event_type=str(raw.get("event_type") or raw.get("type") or "").strip(),
                    timestamp=self._as_datetime(raw.get("timestamp")),
                    household_id=str(raw.get("household_id") or "").strip(),
                    payload=dict(payload) if isinstance(payload, Mapping) else {},
                    source=str(raw.get("source") or "unknown"),
                )
            else:
                payload_raw = getattr(row, "payload", None)
                event = _ObservedEvent(
                    event_id=str(getattr(row, "event_id", getattr(row, "id", "")) or "").strip(),
                    event_type=str(getattr(row, "event_type", getattr(row, "type", "")) or "").strip(),
                    timestamp=self._as_datetime(getattr(row, "timestamp", None)),
                    household_id=str(getattr(row, "household_id", "") or "").strip(),
                    payload=dict(payload_raw) if isinstance(payload_raw, Mapping) else {},
                    source=str(getattr(row, "source", "unknown") or "unknown"),
                )

            if event.event_id and event.event_type and event.household_id:
                observed.append(event)

        observed.sort(key=lambda item: (item.timestamp, item.event_id))
        return observed

    def _as_datetime(self, raw: Any) -> datetime:
        if isinstance(raw, datetime):
            if raw.tzinfo is None:
                return raw.replace(tzinfo=UTC)
            return raw.astimezone(UTC)
        if isinstance(raw, str) and raw.strip():
            normalized = raw.strip().replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        return datetime(1970, 1, 1, tzinfo=UTC)

    def _build_saga_monitor(self, events: list[_ObservedEvent]) -> dict[str, Any]:
        sagas: dict[str, dict[str, Any]] = {}
        saga_types_by_id: dict[str, str] = {}

        for event in events:
            payload = event.payload
            saga_id = str(payload.get("saga_id") or "").strip()
            if not saga_id:
                continue

            if event.event_type == "saga.started":
                metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
                saga_type = self._infer_saga_type(saga_id=saga_id, metadata=metadata, saga_types_by_id=saga_types_by_id)
                resources = self._resource_keys_from_metadata(metadata)
                risk_level = str(metadata.get("risk_level") or "low").strip().lower()

                saga_types_by_id[saga_id] = saga_type
                sagas[saga_id] = {
                    "saga_id": saga_id,
                    "status": "running",
                    "saga_type": saga_type,
                    "request_id": str(payload.get("request_id") or ""),
                    "resource_keys": resources,
                    "risk_level": risk_level,
                }
                continue

            saga_row = sagas.setdefault(
                saga_id,
                {
                    "saga_id": saga_id,
                    "status": "running",
                    "saga_type": self._infer_saga_type(
                        saga_id=saga_id,
                        metadata={},
                        saga_types_by_id=saga_types_by_id,
                    ),
                    "request_id": str(payload.get("request_id") or ""),
                    "resource_keys": set(),
                    "risk_level": "low",
                },
            )

            if event.event_type == "saga.step_failed":
                saga_row["status"] = "failed"
            elif event.event_type in {"saga.completed", "saga.compensated", "saga.halted", "saga.throttled"}:
                saga_row["status"] = event.event_type.replace("saga.", "")

        active = [
            row
            for row in sagas.values()
            if str(row.get("status") or "") in {"running", "failed"}
        ]

        return {
            "sagas": sagas,
            "active_sagas": active,
            "saga_types_by_id": saga_types_by_id,
        }

    def _detect_failure_patterns(
        self,
        *,
        events: list[_ObservedEvent],
        saga_type: str,
        saga_types_by_id: Mapping[str, str],
        breaker_rules: tuple[CircuitBreakerRule, ...],
    ) -> dict[str, Any]:
        rule = self._select_breaker_rule(breaker_rules=breaker_rules, saga_type=saga_type)
        if rule is None:
            return {
                "saga_type": saga_type,
                "total_failures": 0,
                "recent_failures": 0,
                "window_size": 0,
                "failure_event_ids": [],
                "threshold": 0,
            }
        failures: list[_ObservedEvent] = []

        for event in events:
            if event.event_type not in rule.failure_event_types:
                continue
            if not self._event_matches_saga_type(event=event, saga_type=saga_type, saga_types_by_id=saga_types_by_id):
                continue
            failures.append(event)

        recent = failures[-rule.lookback_events :]
        return {
            "saga_type": saga_type,
            "total_failures": len(failures),
            "recent_failures": len(recent),
            "window_size": rule.lookback_events,
            "failure_event_ids": [row.event_id for row in recent],
            "threshold": rule.threshold,
        }

    def _detect_cross_saga_conflicts(
        self,
        *,
        definition: SagaDefinition,
        monitor: Mapping[str, Any],
        events: list[_ObservedEvent],
    ) -> dict[str, Any]:
        requested_resources = self._requested_resources(definition)
        conflicting_sagas: list[str] = []
        conflicting_resources: set[str] = set()

        for saga_row in monitor.get("active_sagas") or []:
            active_saga_id = str(saga_row.get("saga_id") or "")
            if not active_saga_id or active_saga_id == definition.id:
                continue

            active_resources = {str(item) for item in saga_row.get("resource_keys") or set() if str(item).strip()}
            overlap = requested_resources.intersection(active_resources)
            if overlap:
                conflicting_sagas.append(active_saga_id)
                conflicting_resources.update(overlap)

        divergence_reasons: list[str] = []
        has_divergence = False

        try:
            replay_state = replay(
                [
                    {
                        "event_id": row.event_id,
                        "event_type": row.event_type,
                        "timestamp": row.timestamp,
                        "household_id": row.household_id,
                        "payload": row.payload,
                        "source": row.source,
                    }
                    for row in events
                ]
            )
            replayed_sagas = replay_state.get("derived_state", {}).get("sagas", {})
            observed_projection = self._build_control_saga_projection(events)
            replay_check = validate_replay(
                {"sagas": observed_projection},
                {"sagas": replayed_sagas},
            )
            replay_drift = replay_check.get("drift")
            replay_reasons = replay_check.get("drift_reasons") if isinstance(replay_check, Mapping) else {}
            if not bool(replay_check.get("matches", False)) or has_critical_drift(replay_drift):
                has_divergence = True
                divergence_reasons.extend(
                    list((replay_reasons or {}).get("integrity") or [])
                    + list((replay_reasons or {}).get("causal") or [])
                )
        except ReplayValidationError as exc:
            has_divergence = True
            divergence_reasons.append(f"replay_validation_error:{exc}")

        return {
            "has_resource_conflict": len(conflicting_sagas) > 0,
            "conflicting_sagas": sorted(set(conflicting_sagas)),
            "conflicting_resources": sorted(conflicting_resources),
            "has_divergence": has_divergence,
            "divergence_reasons": divergence_reasons,
        }

    def _evaluate_circuit_state(
        self,
        *,
        events: list[_ObservedEvent],
        saga_type: str,
        saga_types_by_id: Mapping[str, str],
        breaker_rules: tuple[CircuitBreakerRule, ...],
    ) -> tuple[dict[str, Any], str | None]:
        for rule in breaker_rules:
            if not self._rule_applies(rule=rule, saga_type=saga_type):
                continue

            indexed = list(enumerate(events))
            relevant_failures = [
                (index, event)
                for index, event in indexed
                if event.event_type in rule.failure_event_types
                and self._event_matches_saga_type(
                    event=event,
                    saga_type=saga_type,
                    saga_types_by_id=saga_types_by_id,
                )
            ]
            relevant_recoveries = [
                (index, event)
                for index, event in indexed
                if event.event_type in rule.recovery_event_types
                and self._event_matches_saga_type(
                    event=event,
                    saga_type=saga_type,
                    saga_types_by_id=saga_types_by_id,
                )
            ]

            open_indices = [
                index
                for index, event in indexed
                if event.event_type == "system.circuit_opened"
                and str(event.payload.get("breaker_id") or "") == rule.breaker_id
                and str(event.payload.get("saga_type") or "") == saga_type
            ]
            close_indices = [
                index
                for index, event in indexed
                if event.event_type == "system.circuit_closed"
                and str(event.payload.get("breaker_id") or "") == rule.breaker_id
                and str(event.payload.get("saga_type") or "") == saga_type
            ]

            last_open = open_indices[-1] if open_indices else -1
            last_close = close_indices[-1] if close_indices else -1
            is_open = last_open > last_close

            recent_failures = [row for _, row in relevant_failures[-rule.lookback_events :]]
            recovery_after_open = [row for idx, row in relevant_recoveries if idx > last_open]
            failures_after_open = [row for idx, row in relevant_failures if idx > last_open]

            transition: str | None = None
            reason = ""
            if is_open:
                if len(recovery_after_open) >= rule.recovery_threshold and len(failures_after_open) == 0:
                    transition = "close"
                    is_open = False
                    reason = "recovery_threshold_reached"
            elif len(recent_failures) >= rule.threshold:
                transition = "open"
                is_open = True
                reason = "failure_threshold_reached"

            state = {
                "breaker_id": rule.breaker_id,
                "saga_type": saga_type,
                "is_open": is_open,
                "reason": reason,
                "failure_count_recent": len(recent_failures),
                "recovery_success_count_recent": len(recovery_after_open),
                "threshold": rule.threshold,
                "lookback_events": rule.lookback_events,
                "recovery_threshold": rule.recovery_threshold,
            }
            return state, transition

        return {
            "breaker_id": "",
            "saga_type": saga_type,
            "is_open": False,
            "reason": "",
            "failure_count_recent": 0,
            "recovery_success_count_recent": 0,
            "threshold": 0,
            "lookback_events": 0,
            "recovery_threshold": 0,
        }, None

    def _validate_replay_consistency(self, events: list[_ObservedEvent]) -> dict[str, Any]:
        serializable = [
            {
                "event_id": row.event_id,
                "event_type": row.event_type,
                "timestamp": row.timestamp,
                "household_id": row.household_id,
                "payload": row.payload,
                "source": row.source,
            }
            for row in events
        ]

        try:
            replayed = replay(serializable)
        except ReplayValidationError as exc:
            drift = empty_drift_classification()
            drift["integrity"] = True
            drift_reasons = empty_drift_reasons()
            drift_reasons["integrity"].append(f"replay_validation_error:{exc}")
            return {
                "matches": False,
                "drift": drift,
                "drift_reasons": drift_reasons,
                "live_checksum": "",
                "replayed_checksum": "",
            }

        observed_projection = self._build_control_saga_projection(events)
        replayed_projection = replayed.get("derived_state", {}).get("sagas", {})
        return validate_replay(
            {"sagas": observed_projection},
            {"sagas": replayed_projection},
        )

    def _build_control_saga_projection(self, events: list[_ObservedEvent]) -> dict[str, Any]:
        sagas: dict[str, dict[str, Any]] = {}
        for event in events:
            payload = event.payload
            saga_id = str(payload.get("saga_id") or "").strip()
            if not saga_id:
                continue

            if event.event_type == "saga.started":
                sagas[saga_id] = {
                    "status": "running",
                    "request_id": str(payload.get("request_id") or ""),
                    "executed_steps": [],
                    "failed_step": None,
                    "compensated_steps": [],
                }
                continue

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

            if event.event_type == "saga.step_succeeded":
                step_id = str(payload.get("step_id") or "").strip()
                if step_id and step_id not in saga_row["executed_steps"]:
                    saga_row["executed_steps"].append(step_id)
            elif event.event_type == "saga.step_failed":
                saga_row["status"] = "failed"
                saga_row["failed_step"] = str(payload.get("step_id") or "").strip() or None
            elif event.event_type == "saga.compensation_applied":
                step_id = str(payload.get("step_id") or "").strip()
                if step_id and step_id not in saga_row["compensated_steps"]:
                    saga_row["compensated_steps"].append(step_id)
            elif event.event_type == "saga.completed":
                saga_row["status"] = "completed"
            elif event.event_type == "saga.compensated":
                saga_row["status"] = "compensated"
            elif event.event_type == "saga.halted":
                saga_row["status"] = "halted"
            elif event.event_type == "saga.throttled":
                saga_row["status"] = "throttled"

        return sagas

    def _rule_applies(self, *, rule: CircuitBreakerRule, saga_type: str) -> bool:
        return "*" in rule.affected_saga_types or saga_type in rule.affected_saga_types

    def _event_matches_saga_type(
        self,
        *,
        event: _ObservedEvent,
        saga_type: str,
        saga_types_by_id: Mapping[str, str],
    ) -> bool:
        if saga_type == "*":
            return True

        payload = event.payload
        payload_saga_type = str(payload.get("saga_type") or "").strip()
        if payload_saga_type:
            return payload_saga_type == saga_type

        saga_id = str(payload.get("saga_id") or "").strip()
        if saga_id:
            resolved = saga_types_by_id.get(saga_id) or self._infer_saga_type(
                saga_id=saga_id,
                metadata=payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {},
                saga_types_by_id=saga_types_by_id,
            )
            return resolved == saga_type

        return saga_type == "unknown"

    def _infer_saga_type(
        self,
        *,
        saga_id: str,
        metadata: Mapping[str, Any],
        saga_types_by_id: Mapping[str, str],
    ) -> str:
        from_registry = saga_types_by_id.get(saga_id)
        if from_registry:
            return str(from_registry)

        explicit = str(metadata.get("saga_type") or "").strip()
        if explicit:
            return explicit

        candidate = saga_id.strip()
        for separator in (":", "/", "-"):
            if separator in candidate:
                prefix = candidate.split(separator, 1)[0].strip()
                if prefix:
                    return prefix
        return candidate or "unknown"

    def _resource_keys_from_metadata(self, metadata: Mapping[str, Any]) -> set[str]:
        resources: set[str] = set()
        resource_keys = metadata.get("resource_keys")
        if isinstance(resource_keys, list):
            for item in resource_keys:
                value = str(item).strip()
                if value:
                    resources.add(value)
        return resources

    def _requested_resources(self, definition: SagaDefinition) -> set[str]:
        resources = self._resource_keys_from_metadata(definition.metadata)
        for step in definition.steps:
            metadata = step.metadata if isinstance(step.metadata, Mapping) else {}
            resources.update(self._resource_keys_from_metadata(metadata))
            resource_key = str(metadata.get("resource_key") or "").strip()
            if resource_key:
                resources.add(resource_key)
        return resources

    def _should_throttle(
        self,
        *,
        definition: SagaDefinition,
        monitor: Mapping[str, Any],
        max_concurrent_high_risk: int,
    ) -> bool:
        risk_level = str(definition.metadata.get("risk_level") or "low").strip().lower()
        if risk_level != "high":
            return False

        active_high_risk = 0
        for saga_row in monitor.get("active_sagas") or []:
            row_risk = str(saga_row.get("risk_level") or "low").strip().lower()
            if row_risk == "high":
                active_high_risk += 1

        return active_high_risk >= max(1, int(max_concurrent_high_risk))

    def _resolve_policy_control_config(
        self,
        policy_snapshot: Mapping[str, Any] | None,
    ) -> tuple[tuple[CircuitBreakerRule, ...], int, str | None]:
        if not isinstance(policy_snapshot, Mapping):
            return self._breaker_rules, self._max_concurrent_high_risk, None

        max_concurrent_high_risk = int(
            policy_snapshot.get("max_concurrent_high_risk") or self._max_concurrent_high_risk
        )
        raw_breaker_rules = policy_snapshot.get("breaker_rules")

        resolved_rules: list[CircuitBreakerRule] = []
        if isinstance(raw_breaker_rules, (list, tuple)):
            for raw_rule in raw_breaker_rules:
                if not isinstance(raw_rule, Mapping):
                    continue
                resolved_rules.append(
                    CircuitBreakerRule(
                        breaker_id=str(raw_rule.get("breaker_id") or "saga_failure_burst"),
                        threshold=max(1, int(raw_rule.get("threshold") or 1)),
                        lookback_events=max(1, int(raw_rule.get("lookback_events") or 1)),
                        affected_saga_types=tuple(
                            str(item)
                            for item in (raw_rule.get("affected_saga_types") or ["*"])
                            if str(item).strip()
                        )
                        or ("*",),
                        failure_event_types=tuple(
                            str(item)
                            for item in (
                                raw_rule.get("failure_event_types")
                                or ("saga.failed", "saga.step_failed", "saga.compensated")
                            )
                            if str(item).strip()
                        )
                        or ("saga.failed", "saga.step_failed", "saga.compensated"),
                        recovery_event_types=tuple(
                            str(item)
                            for item in (raw_rule.get("recovery_event_types") or ("saga.completed",))
                            if str(item).strip()
                        )
                        or ("saga.completed",),
                        recovery_threshold=max(1, int(raw_rule.get("recovery_threshold") or 1)),
                    )
                )

        policy_version_id = str(policy_snapshot.get("policy_version_id") or "").strip() or None
        return (
            tuple(resolved_rules) if resolved_rules else self._breaker_rules,
            max_concurrent_high_risk,
            policy_version_id,
        )

    def _select_breaker_rule(
        self,
        *,
        breaker_rules: tuple[CircuitBreakerRule, ...],
        saga_type: str,
    ) -> CircuitBreakerRule | None:
        for rule in breaker_rules:
            if self._rule_applies(rule=rule, saga_type=saga_type):
                return rule
        return None
