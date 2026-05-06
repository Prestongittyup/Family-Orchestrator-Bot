from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from core.health import empty_drift_classification, empty_drift_reasons


class PolicyResolutionError(ValueError):
    """Raised when policy versions cannot be resolved deterministically."""


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        frozen = {str(key): _freeze_value(item) for key, item in value.items()}
        return MappingProxyType(dict(sorted(frozen.items())))
    if isinstance(value, list):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze_value(item) for item in value)
    return value


def _to_plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _to_plain(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_to_plain(item) for item in value]
    return value


def _stable_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PolicyVersion:
    version_id: str
    rules_snapshot: Mapping[str, Any]
    risk_thresholds_snapshot: Mapping[str, Any]
    control_plane_thresholds_snapshot: Mapping[str, Any]
    activation_timestamp: datetime
    deprecation_timestamp: datetime | None = None

    @property
    def evaluation_context_hash(self) -> str:
        return _stable_hash(
            {
                "version_id": self.version_id,
                "rules_snapshot": _to_plain(self.rules_snapshot),
                "risk_thresholds_snapshot": _to_plain(self.risk_thresholds_snapshot),
                "control_plane_thresholds_snapshot": _to_plain(self.control_plane_thresholds_snapshot),
                "activation_timestamp": self.activation_timestamp.isoformat(),
                "deprecation_timestamp": (
                    self.deprecation_timestamp.isoformat() if self.deprecation_timestamp else None
                ),
            }
        )


@dataclass(frozen=True)
class PolicyResolution:
    version_id: str
    evaluation_context_hash: str
    rules_snapshot: Mapping[str, Any]
    risk_thresholds_snapshot: Mapping[str, Any]
    control_plane_thresholds_snapshot: Mapping[str, Any]
    activation_timestamp: datetime
    deprecation_timestamp: datetime | None


class PolicyVersionRegistry:
    def __init__(self, versions: Iterable[PolicyVersion] | None = None) -> None:
        self._versions: list[PolicyVersion] = []
        if versions:
            for version in versions:
                self.register_policy_version(
                    version_id=version.version_id,
                    rules_snapshot=_to_plain(version.rules_snapshot),
                    risk_thresholds_snapshot=_to_plain(version.risk_thresholds_snapshot),
                    control_plane_thresholds_snapshot=_to_plain(version.control_plane_thresholds_snapshot),
                    activation_timestamp=version.activation_timestamp,
                    deprecation_timestamp=version.deprecation_timestamp,
                )

    @property
    def versions(self) -> tuple[PolicyVersion, ...]:
        return tuple(sorted(self._versions, key=lambda row: row.activation_timestamp))

    def register_policy_version(
        self,
        *,
        version_id: str,
        rules_snapshot: Mapping[str, Any],
        risk_thresholds_snapshot: Mapping[str, Any],
        control_plane_thresholds_snapshot: Mapping[str, Any],
        activation_timestamp: datetime,
        deprecation_timestamp: datetime | None = None,
    ) -> PolicyVersion:
        normalized_version_id = str(version_id).strip()
        if not normalized_version_id:
            raise PolicyResolutionError("Policy version_id is required")
        if any(existing.version_id == normalized_version_id for existing in self._versions):
            raise PolicyResolutionError(f"Duplicate policy version_id: {normalized_version_id}")

        activation = self._coerce_datetime(activation_timestamp)
        deprecation = self._coerce_datetime(deprecation_timestamp) if deprecation_timestamp else None
        if deprecation is not None and deprecation <= activation:
            raise PolicyResolutionError("deprecation_timestamp must be greater than activation_timestamp")

        version = PolicyVersion(
            version_id=normalized_version_id,
            rules_snapshot=_freeze_value(dict(rules_snapshot)),
            risk_thresholds_snapshot=_freeze_value(dict(risk_thresholds_snapshot)),
            control_plane_thresholds_snapshot=_freeze_value(dict(control_plane_thresholds_snapshot)),
            activation_timestamp=activation,
            deprecation_timestamp=deprecation,
        )

        self._validate_interval_non_overlap(version)
        self._versions.append(version)
        return version

    def resolve_policy(self, timestamp: datetime) -> PolicyResolution:
        if not self._versions:
            raise PolicyResolutionError("No policy versions registered")

        resolved_timestamp = self._coerce_datetime(timestamp)
        candidates = [
            version
            for version in self._versions
            if version.activation_timestamp <= resolved_timestamp
            and (
                version.deprecation_timestamp is None
                or resolved_timestamp < version.deprecation_timestamp
            )
        ]

        if not candidates:
            raise PolicyResolutionError(
                f"No policy version active for timestamp {resolved_timestamp.isoformat()}"
            )
        if len(candidates) > 1:
            version_ids = sorted(version.version_id for version in candidates)
            raise PolicyResolutionError(
                f"Ambiguous policy resolution for timestamp {resolved_timestamp.isoformat()}: {version_ids}"
            )

        version = candidates[0]
        return PolicyResolution(
            version_id=version.version_id,
            evaluation_context_hash=version.evaluation_context_hash,
            rules_snapshot=version.rules_snapshot,
            risk_thresholds_snapshot=version.risk_thresholds_snapshot,
            control_plane_thresholds_snapshot=version.control_plane_thresholds_snapshot,
            activation_timestamp=version.activation_timestamp,
            deprecation_timestamp=version.deprecation_timestamp,
        )

    def resolve_policy_by_version_id(self, version_id: str) -> PolicyResolution:
        normalized = str(version_id).strip()
        for version in self._versions:
            if version.version_id == normalized:
                return PolicyResolution(
                    version_id=version.version_id,
                    evaluation_context_hash=version.evaluation_context_hash,
                    rules_snapshot=version.rules_snapshot,
                    risk_thresholds_snapshot=version.risk_thresholds_snapshot,
                    control_plane_thresholds_snapshot=version.control_plane_thresholds_snapshot,
                    activation_timestamp=version.activation_timestamp,
                    deprecation_timestamp=version.deprecation_timestamp,
                )
        raise PolicyResolutionError(f"Unknown policy version_id: {normalized}")

    def reconstruct_event_policy_bindings(
        self,
        events: Iterable[Mapping[str, Any] | Any],
    ) -> dict[str, Any]:
        bindings: list[dict[str, Any]] = []
        missing_policy_reference: list[str] = []
        mismatched_resolution: list[str] = []

        for event in events:
            event_payload = self._extract_payload(event)
            event_id = self._extract_field(event, "event_id", "id")
            event_timestamp_raw = self._extract_field(event, "timestamp")
            event_timestamp = self._coerce_datetime(event_timestamp_raw)

            bound_version_id = str(event_payload.get("policy_version_id") or "").strip()
            bound_hash = str(event_payload.get("evaluation_context_hash") or "").strip()
            if not bound_version_id or not bound_hash:
                missing_policy_reference.append(event_id)
                continue

            resolved = self.resolve_policy(event_timestamp)
            if bound_version_id != resolved.version_id or bound_hash != resolved.evaluation_context_hash:
                mismatched_resolution.append(event_id)

            bindings.append(
                {
                    "event_id": event_id,
                    "timestamp": event_timestamp.isoformat(),
                    "policy_version_id": bound_version_id,
                    "evaluation_context_hash": bound_hash,
                }
            )

        return {
            "bindings": bindings,
            "missing_policy_reference": missing_policy_reference,
            "mismatched_resolution": mismatched_resolution,
            "matches": len(missing_policy_reference) == 0 and len(mismatched_resolution) == 0,
        }

    def detect_policy_drift(
        self,
        events: Iterable[Mapping[str, Any] | Any],
        *,
        current_timestamp: datetime | None = None,
    ) -> dict[str, Any]:
        binding_report = self.reconstruct_event_policy_bindings(events)
        compare_timestamp = self._coerce_datetime(current_timestamp or datetime.now(UTC))
        current_policy = self.resolve_policy(compare_timestamp)

        evolved_events: list[str] = []
        divergence_candidates: list[str] = []
        for binding in binding_report.get("bindings", []):
            if str(binding.get("evaluation_context_hash") or "") != current_policy.evaluation_context_hash:
                event_id = str(binding.get("event_id") or "")
                evolved_events.append(event_id)
                divergence_candidates.append(event_id)

        drift = empty_drift_classification()
        drift_reasons = empty_drift_reasons()
        if binding_report.get("missing_policy_reference"):
            drift["integrity"] = True
            drift_reasons["integrity"].append("missing_policy_reference")
        if binding_report.get("mismatched_resolution"):
            drift["integrity"] = True
            drift_reasons["integrity"].append("historical_resolution_mismatch")
        if evolved_events:
            drift["structural"] = True
            drift_reasons["structural"].append("policy_evolved")

        return {
            "matches_historical_bindings": bool(binding_report.get("matches", False)),
            "current_policy_version_id": current_policy.version_id,
            "current_evaluation_context_hash": current_policy.evaluation_context_hash,
            "evolved_event_ids": evolved_events,
            "divergence_candidates": sorted(set(divergence_candidates)),
            "drift": drift,
            "drift_reasons": drift_reasons,
        }

    def _validate_interval_non_overlap(self, incoming: PolicyVersion) -> None:
        for existing in self._versions:
            existing_end = existing.deprecation_timestamp or datetime.max.replace(tzinfo=UTC)
            incoming_end = incoming.deprecation_timestamp or datetime.max.replace(tzinfo=UTC)

            starts_before_existing_end = incoming.activation_timestamp < existing_end
            existing_starts_before_incoming_end = existing.activation_timestamp < incoming_end
            if starts_before_existing_end and existing_starts_before_incoming_end:
                raise PolicyResolutionError(
                    "Policy activation windows must not overlap: "
                    f"{incoming.version_id} overlaps {existing.version_id}"
                )

    def _extract_payload(self, event: Mapping[str, Any] | Any) -> dict[str, Any]:
        if isinstance(event, Mapping):
            payload = event.get("payload")
        else:
            payload = getattr(event, "payload", None)
        return dict(payload) if isinstance(payload, Mapping) else {}

    def _extract_field(self, event: Mapping[str, Any] | Any, *field_names: str) -> Any:
        if isinstance(event, Mapping):
            raw = dict(event)
            for field_name in field_names:
                if field_name in raw:
                    return raw.get(field_name)
            return None

        for field_name in field_names:
            if hasattr(event, field_name):
                return getattr(event, field_name)
        return None

    def _coerce_datetime(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)

        if isinstance(value, str) and value.strip():
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)

        raise PolicyResolutionError(f"Invalid datetime value: {value!r}")


def build_default_policy_registry() -> PolicyVersionRegistry:
    registry = PolicyVersionRegistry()
    registry.register_policy_version(
        version_id="policy.v1",
        rules_snapshot={
            "task_title_max_length": 160,
            "task_priority_values": ["low", "medium", "high"],
            "supported_task_command_types": ["task.create", "create_task"],
            "supported_saga_command_types": ["saga.execute", "workflow.saga.execute"],
        },
        risk_thresholds_snapshot={
            "high_risk_keywords": [
                "bank",
                "wire",
                "transfer",
                "payment",
                "pay",
                "password",
                "security",
            ],
            "financial_approval_is_high": True,
            "high_priority_promotes_to_medium": True,
            "due_date_promotes_to_medium": True,
        },
        control_plane_thresholds_snapshot={
            "max_concurrent_high_risk": 1,
            "breaker_rules": [
                {
                    "breaker_id": "saga_failure_burst",
                    "threshold": 3,
                    "lookback_events": 20,
                    "affected_saga_types": ["*"],
                    "failure_event_types": ["saga.failed", "saga.step_failed", "saga.compensated"],
                    "recovery_event_types": ["saga.completed"],
                    "recovery_threshold": 2,
                }
            ],
        },
        activation_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        deprecation_timestamp=None,
    )
    return registry
