from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RuntimeFeatureFlags:
    ingestion_enabled: bool
    tracing_enabled: bool
    debug_mode: bool


_DEFAULT_FLAGS: dict[str, bool] = {
    "ingestion_enabled": True,
    "tracing_enabled": True,
    "debug_mode": False,
}

_household_overrides: dict[str, dict[str, bool]] = {}
_environment_overrides: dict[str, dict[str, bool]] = {}
_flags_lock = threading.RLock()


def _to_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def get_active_environment() -> str:
    return os.getenv("APP_ENV", "development").strip().lower() or "development"


def _get_process_env_defaults() -> dict[str, bool]:
    return {
        "ingestion_enabled": _to_bool(os.getenv("FOB_INGESTION_ENABLED"), _DEFAULT_FLAGS["ingestion_enabled"]),
        "tracing_enabled": _to_bool(os.getenv("FOB_TRACING_ENABLED"), _DEFAULT_FLAGS["tracing_enabled"]),
        "debug_mode": _to_bool(os.getenv("FOB_DEBUG_MODE"), _DEFAULT_FLAGS["debug_mode"]),
    }


def _normalize_updates(updates: dict[str, Any]) -> dict[str, bool]:
    normalized: dict[str, bool] = {}
    for key, value in updates.items():
        if key not in _DEFAULT_FLAGS:
            raise ValueError(f"Unknown feature flag '{key}'")
        if not isinstance(value, bool):
            raise ValueError(f"Feature flag '{key}' must be bool")
        normalized[key] = value
    return normalized


def resolve_feature_flags(
    *,
    household_id: str | None = None,
    environment: str | None = None,
) -> RuntimeFeatureFlags:
    effective_environment = (environment or get_active_environment()).strip().lower() or "development"
    env_defaults = _get_process_env_defaults()

    with _flags_lock:
        env_override = dict(_environment_overrides.get(effective_environment, {}))
        household_override = dict(_household_overrides.get(household_id, {})) if household_id else {}

    merged = dict(env_defaults)
    merged.update(env_override)
    merged.update(household_override)

    return RuntimeFeatureFlags(
        ingestion_enabled=bool(merged["ingestion_enabled"]),
        tracing_enabled=bool(merged["tracing_enabled"]),
        debug_mode=bool(merged["debug_mode"]),
    )


def get_feature_flags_view(
    *,
    household_id: str | None = None,
    environment: str | None = None,
) -> dict[str, Any]:
    effective_environment = (environment or get_active_environment()).strip().lower() or "development"
    defaults = _get_process_env_defaults()

    with _flags_lock:
        env_override = dict(_environment_overrides.get(effective_environment, {}))
        household_override = dict(_household_overrides.get(household_id, {})) if household_id else {}

    resolved = resolve_feature_flags(household_id=household_id, environment=effective_environment)

    return {
        "environment": effective_environment,
        "household_id": household_id,
        "defaults": defaults,
        "environment_override": env_override,
        "household_override": household_override,
        "effective": {
            "ingestion_enabled": resolved.ingestion_enabled,
            "tracing_enabled": resolved.tracing_enabled,
            "debug_mode": resolved.debug_mode,
        },
    }


def set_household_feature_flags(household_id: str, updates: dict[str, Any]) -> dict[str, bool]:
    if not household_id:
        raise ValueError("household_id is required")
    normalized = _normalize_updates(updates)
    with _flags_lock:
        current = dict(_household_overrides.get(household_id, {}))
        current.update(normalized)
        _household_overrides[household_id] = current
        return dict(current)


def clear_household_feature_flags(household_id: str) -> None:
    with _flags_lock:
        _household_overrides.pop(household_id, None)


def set_environment_feature_flags(environment: str, updates: dict[str, Any]) -> dict[str, bool]:
    if not environment:
        raise ValueError("environment is required")
    normalized_env = environment.strip().lower()
    if not normalized_env:
        raise ValueError("environment is required")

    normalized_updates = _normalize_updates(updates)
    with _flags_lock:
        current = dict(_environment_overrides.get(normalized_env, {}))
        current.update(normalized_updates)
        _environment_overrides[normalized_env] = current
        return dict(current)


def clear_environment_feature_flags(environment: str) -> None:
    normalized_env = environment.strip().lower()
    with _flags_lock:
        _environment_overrides.pop(normalized_env, None)


def _reset_feature_flags_for_tests() -> None:
    with _flags_lock:
        _household_overrides.clear()
        _environment_overrides.clear()
