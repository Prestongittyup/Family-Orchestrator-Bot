from __future__ import annotations

import importlib
import ast
import re
import sys
from typing import Iterable


FORBIDDEN_IMPORT_PREFIXES: tuple[str, ...] = (
    # OS-1 ingress/ingestion paths
    "apps.api.ingestion",
    # OS-2 decision engine paths
    "apps.api.services.decision_engine",
    # Legacy/deleted runtime paths
    "apps.api.services.synthesis_engine",
    "apps.api.services.worker",
    "modules.core.services.orchestrator_lite",
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


def assert_runtime_architecture_source(module_name: str, source_text: str) -> None:
    """
    Static source guard for architectural freeze checks.

    Enforces:
    - No orchestrator class declarations outside integration_core/orchestrator.py
    - No provider.fetch_events calls outside integration_core/state_builder.py
    - No DecisionEngine class declarations outside integration_core/decision_engine.py
    """
    name = str(module_name or "").strip()
    source = str(source_text or "")

    if not name:
        return

    if name != "apps.api.integration_core.orchestrator":
        if re.search(r"^\s*class\s+\w*Orchestrator\b", source, flags=re.MULTILINE):
            raise IntegrationCoreBoundaryViolation(
                f"Non-canonical orchestrator class detected in module: {name}"
            )

    if name != "apps.api.integration_core.state_builder":
        has_fetch_events_call = False
        try:
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "fetch_events"
                ):
                    has_fetch_events_call = True
                    break
        except SyntaxError:
            has_fetch_events_call = ".fetch_events(" in source

        if has_fetch_events_call:
            raise IntegrationCoreBoundaryViolation(
                f"Provider fetch boundary violation in module: {name}"
            )

    if name != "apps.api.integration_core.decision_engine":
        if re.search(r"^\s*class\s+DecisionEngine\b", source, flags=re.MULTILINE):
            raise IntegrationCoreBoundaryViolation(
                f"Duplicate DecisionEngine implementation detected in module: {name}"
            )

