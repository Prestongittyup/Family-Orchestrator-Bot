from __future__ import annotations

from typing import Any


_AUDIT_BYPASS_PATHS = frozenset(
    {
        "/v1/identity/household/create",
        "/v1/identity/bootstrap",
    }
)
_AUDIT_BYPASS_MODES = frozenset({"smoke", "standard"})


def is_audit_bypass_request(path: str, headers: dict[str, str] | None) -> bool:
    if path not in _AUDIT_BYPASS_PATHS or not headers:
        return False

    bypass_flag = headers.get("x-audit-bypass", "").strip().lower()
    audit_mode = headers.get("x-audit-mode", "").strip().lower()
    audit_source = headers.get("x-audit-source", "").strip().lower()
    return (
        bypass_flag in {"1", "true", "yes"}
        and audit_mode in _AUDIT_BYPASS_MODES
        and audit_source == "production_torture_audit"
    )


def scope_headers(scope: dict[str, Any]) -> dict[str, str]:
    raw_headers = scope.get("headers") or []
    headers: dict[str, str] = {}
    for key, value in raw_headers:
        headers[key.decode("latin-1").lower()] = value.decode("latin-1")
    return headers
