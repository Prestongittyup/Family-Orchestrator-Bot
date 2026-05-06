from __future__ import annotations

from typing import Any, Literal, Mapping


_DRIFT_KEYS: tuple[str, str, str] = ("structural", "integrity", "causal")
SystemHealth = Literal["green", "yellow", "red"]


def empty_drift_classification() -> dict[str, bool]:
    return {
        "structural": False,
        "integrity": False,
        "causal": False,
    }


def empty_drift_reasons() -> dict[str, list[str]]:
    return {
        "structural": [],
        "integrity": [],
        "causal": [],
    }


def normalize_drift_classification(raw: Mapping[str, Any] | None) -> dict[str, bool]:
    if not isinstance(raw, Mapping):
        return empty_drift_classification()

    return {
        key: bool(raw.get(key, False))
        for key in _DRIFT_KEYS
    }


def merge_drift_classifications(*drift_rows: Mapping[str, Any] | None) -> dict[str, bool]:
    merged = empty_drift_classification()
    for row in drift_rows:
        normalized = normalize_drift_classification(row)
        for key in _DRIFT_KEYS:
            merged[key] = merged[key] or normalized[key]
    return merged


def has_critical_drift(drift: Mapping[str, Any] | None) -> bool:
    normalized = normalize_drift_classification(drift)
    return normalized["integrity"] or normalized["causal"]


def system_health_inputs_from_projection(projection: Mapping[str, Any] | None) -> dict[str, bool]:
    projection_payload = projection if isinstance(projection, Mapping) else {}
    drift = normalize_drift_classification(projection_payload.get("drift"))

    return {
        "event_log_integrity": bool(str(projection_payload.get("checksum") or "").strip()),
        "replay_consistency": not drift["integrity"],
        "policy_determinism": _policy_binding_consistency_from_projection(projection_payload),
        "control_plane_consistency": not drift["causal"],
        "saga_validity": _saga_validity_from_projection(projection_payload),
    }


def _policy_binding_consistency_from_projection(projection: Mapping[str, Any]) -> bool:
    bindings = projection.get("policy_bindings")
    if not isinstance(bindings, Mapping):
        return False

    missing = bindings.get("missing_policy_reference")
    if isinstance(missing, list) and len(missing) > 0:
        return False

    versions = bindings.get("policy_versions")
    if not isinstance(versions, Mapping):
        return True

    for version_row in versions.values():
        if not isinstance(version_row, Mapping):
            continue
        hashes = version_row.get("evaluation_context_hashes")
        if isinstance(hashes, list) and len(hashes) > 1:
            return False

    return True


def _saga_validity_from_projection(projection: Mapping[str, Any]) -> bool:
    sagas = projection.get("sagas")
    if not isinstance(sagas, Mapping):
        return True

    for saga_row in sagas.values():
        if not isinstance(saga_row, Mapping):
            continue
        status = str(saga_row.get("status") or "")
        if status == "failed":
            return False

    return True


def derive_system_health(
    *,
    event_log_integrity: bool,
    replay_consistency: bool,
    policy_determinism: bool,
    control_plane_consistency: bool,
    saga_validity: bool,
) -> SystemHealth:
    if not event_log_integrity or not replay_consistency or not control_plane_consistency:
        return "red"
    if not policy_determinism or not saga_validity:
        return "yellow"
    return "green"
