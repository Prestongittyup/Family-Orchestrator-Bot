from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class IngestionExecutionConfig:
    profile: str
    retry_attempts: int
    backoff_seconds: float
    max_backoff_seconds: float
    rate_limit_window_seconds: float
    rate_limit_max_cycles: int


_PROFILE_DEFAULTS: dict[str, dict[str, float | int]] = {
    # Fast iteration profile: low backoff and permissive rate limiting.
    "dev": {
        "retry_attempts": 1,
        "backoff_seconds": 0.0,
        "max_backoff_seconds": 0.0,
        "rate_limit_window_seconds": 1.0,
        "rate_limit_max_cycles": 1000,
    },
    # Staging profile: moderate retries and moderate rate controls.
    "staging": {
        "retry_attempts": 2,
        "backoff_seconds": 0.05,
        "max_backoff_seconds": 0.2,
        "rate_limit_window_seconds": 10.0,
        "rate_limit_max_cycles": 50,
    },
    # Production profile: stricter retries and tighter rate controls.
    "production": {
        "retry_attempts": 3,
        "backoff_seconds": 0.1,
        "max_backoff_seconds": 1.0,
        "rate_limit_window_seconds": 60.0,
        "rate_limit_max_cycles": 60,
    },
}

_DEFAULT_PROFILE = "dev"


def list_ingestion_profiles() -> list[str]:
    return sorted(_PROFILE_DEFAULTS.keys())


def get_active_ingestion_profile() -> str:
    raw = os.getenv("INGESTION_EXECUTION_PROFILE", _DEFAULT_PROFILE).strip().lower()
    if raw not in _PROFILE_DEFAULTS:
        raise ValueError(
            f"Unsupported ingestion execution profile '{raw}'. "
            f"Supported profiles: {list_ingestion_profiles()}"
        )
    return raw


def _read_int_override(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw.strip())


def _read_float_override(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw.strip())


def get_ingestion_execution_config(*, profile: str | None = None) -> IngestionExecutionConfig:
    active_profile = (profile or get_active_ingestion_profile()).strip().lower()
    if active_profile not in _PROFILE_DEFAULTS:
        raise ValueError(
            f"Unsupported ingestion execution profile '{active_profile}'. "
            f"Supported profiles: {list_ingestion_profiles()}"
        )

    profile_defaults = _PROFILE_DEFAULTS[active_profile]

    retry_attempts = max(
        1,
        _read_int_override(
            "INGESTION_RETRY_ATTEMPTS",
            int(profile_defaults["retry_attempts"]),
        ),
    )
    backoff_seconds = max(
        0.0,
        _read_float_override(
            "INGESTION_BACKOFF_SECONDS",
            float(profile_defaults["backoff_seconds"]),
        ),
    )
    max_backoff_seconds = max(
        backoff_seconds,
        _read_float_override(
            "INGESTION_MAX_BACKOFF_SECONDS",
            float(profile_defaults["max_backoff_seconds"]),
        ),
    )
    rate_limit_window_seconds = max(
        0.001,
        _read_float_override(
            "INGESTION_RATE_LIMIT_WINDOW_SECONDS",
            float(profile_defaults["rate_limit_window_seconds"]),
        ),
    )
    rate_limit_max_cycles = max(
        1,
        _read_int_override(
            "INGESTION_RATE_LIMIT_MAX_CYCLES",
            int(profile_defaults["rate_limit_max_cycles"]),
        ),
    )

    return IngestionExecutionConfig(
        profile=active_profile,
        retry_attempts=retry_attempts,
        backoff_seconds=backoff_seconds,
        max_backoff_seconds=max_backoff_seconds,
        rate_limit_window_seconds=rate_limit_window_seconds,
        rate_limit_max_cycles=rate_limit_max_cycles,
    )
