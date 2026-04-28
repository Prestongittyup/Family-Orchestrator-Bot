from __future__ import annotations

import os


_RESTRICTED_ENVIRONMENTS = {"prod", "production"}


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


def is_feature_flag_write_restricted(*, environment: str | None = None) -> bool:
    active_env = (environment or get_active_environment()).strip().lower() or "development"
    default_restricted = active_env in _RESTRICTED_ENVIRONMENTS
    # Optional override for emergency hardening in non-prod environments.
    return _to_bool(os.getenv("FOB_ADMIN_GUARD_ENABLED"), default_restricted)


def is_valid_admin_token(token: str | None) -> bool:
    expected = os.getenv("FOB_ADMIN_TOKEN", "").strip()
    if expected == "":
        return False
    if token is None:
        return False
    return token.strip() == expected


def can_write_feature_flags(*, token: str | None, environment: str | None = None) -> bool:
    if not is_feature_flag_write_restricted(environment=environment):
        return True
    return is_valid_admin_token(token)
