from __future__ import annotations

import importlib
import sys
from typing import Iterable


FORBIDDEN_IMPORT_PREFIXES: tuple[str, ...] = (
    # OS-1 ingress/ingestion paths
    "apps.api.ingestion",
    # OS-2 decision engine paths
    "apps.api.services.decision_engine",
    "apps.api.services.decision_engine",
    # Brief rendering layer
    "apps.api.endpoints.brief_renderer_v1",
)


class IntegrationCoreBoundaryViolation(ImportError):
    pass


def _is_forbidden(module_name: str, forbidden_prefixes: Iterable[str] = FORBIDDEN_IMPORT_PREFIXES) -> bool:
    name = str(module_name or "").strip()
    if not name:
        return False

    for prefix in forbidden_prefixes:
        if name == prefix or name.startswith(f"{prefix}."):
            return True
    return False


def assert_allowed_import(module_name: str) -> None:
    if _is_forbidden(module_name):
        raise IntegrationCoreBoundaryViolation(
            f"Integration Core import blocked by architecture boundary: {module_name}"
        )


def guarded_import(module_name: str):
    """Runtime import guard for integration-core extension points."""
    assert_allowed_import(module_name)
    return importlib.import_module(module_name)


def validate_loaded_module_boundaries() -> None:
    """Best-effort runtime safety check for already loaded modules."""
    violations = [name for name in sys.modules.keys() if _is_forbidden(name)]
    if violations:
        sample = ", ".join(sorted(violations)[:5])
        raise IntegrationCoreBoundaryViolation(
            f"Forbidden modules detected in runtime: {sample}"
        )

