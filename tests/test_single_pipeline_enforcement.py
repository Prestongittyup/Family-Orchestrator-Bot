"""
Single Pipeline Enforcement Tests
----------------------------------
Regression guards that prove only the integration_core pipeline exists in
the runtime codebase.  These tests fail immediately if any competing pipeline
is re-introduced.

Validated invariants:
1. synthesis_engine module does not exist
2. worker module does not exist
3. services/decision_engine module does not exist (canonical is integration_core)
4. No source file outside integration_core imports a competing pipeline symbol
5. The only fetch boundary is StateBuilder (no provider calls outside it)
6. The canonical decision engine is importable and functional
"""
from __future__ import annotations

import ast
import importlib
import importlib.util
import os
from pathlib import Path

import pytest

from apps.api.integration_core.architecture_guard import (
    IntegrationCoreBoundaryViolation,
    assert_runtime_architecture_source,
)

ROOT = Path(__file__).parent.parent
APPS_API = ROOT / "apps" / "api"


# ---------------------------------------------------------------------------
# 1. Deleted modules must not exist
# ---------------------------------------------------------------------------

def test_synthesis_engine_module_is_deleted() -> None:
    path = APPS_API / "services" / "synthesis_engine.py"
    assert not path.exists(), (
        "synthesis_engine.py must not exist — it is a competing pipeline. "
        "Delete it and remove all callers."
    )


def test_worker_module_is_deleted() -> None:
    path = APPS_API / "services" / "worker.py"
    assert not path.exists(), (
        "worker.py must not exist — it was an orphaned broken module. "
        "Delete it if re-introduced."
    )


def test_services_decision_engine_is_deleted() -> None:
    path = APPS_API / "services" / "decision_engine.py"
    assert not path.exists(), (
        "apps/api/services/decision_engine.py must not exist. "
        "The canonical decision engine is apps/api/integration_core/decision_engine.py."
    )


# ---------------------------------------------------------------------------
# 2. Canonical decision engine is importable and functional
# ---------------------------------------------------------------------------

def test_canonical_decision_engine_importable() -> None:
    from apps.api.integration_core.decision_engine import DecisionEngine, DecisionContext  # noqa: F401
    assert DecisionEngine is not None
    assert DecisionContext is not None


def test_canonical_decision_engine_process_produces_context() -> None:
    from apps.api.integration_core.decision_engine import DecisionEngine
    from apps.api.integration_core.models.household_state import HouseholdState

    state = HouseholdState(
        user_id="u1",
        calendar_events=[],
        tasks=[],
        alerts=[],
    )
    engine = DecisionEngine()
    ctx = engine.process(state)
    assert ctx.next_event is None
    assert ctx.top_events == []
    assert ctx.conflicts == []


# ---------------------------------------------------------------------------
# 3. No source file imports deleted modules
# ---------------------------------------------------------------------------

DELETED_SYMBOLS = [
    "services.synthesis_engine",
    "services.worker",
    "services.decision_engine",
    "services.planning_boundary_contract",
]


def _all_python_sources() -> list[Path]:
    sources = []
    for root, dirs, files in os.walk(APPS_API):
        # skip archive and __pycache__
        dirs[:] = [d for d in dirs if d not in ("__pycache__", "archive")]
        for f in files:
            if f.endswith(".py"):
                sources.append(Path(root) / f)
    return sources


def _module_name_for_path(path: Path) -> str:
    rel = path.relative_to(ROOT).as_posix()
    if rel.endswith(".py"):
        rel = rel[:-3]
    return rel.replace("/", ".")


@pytest.mark.parametrize("symbol", DELETED_SYMBOLS)
def test_no_source_imports_deleted_symbol(symbol: str) -> None:
    # architecture_guard.py is exempt: it references these names in its
    # forbidden-list constants, not as actual imports.
    EXEMPT = {APPS_API / "integration_core" / "architecture_guard.py"}
    violations = []
    for src in _all_python_sources():
        if src in EXEMPT:
            continue
        text = src.read_text(encoding="utf-8", errors="replace")
        if symbol in text:
            violations.append(str(src.relative_to(ROOT)))
    assert not violations, (
        f"Deleted symbol '{symbol}' is still referenced in:\n"
        + "\n".join(f"  {v}" for v in violations)
    )


# ---------------------------------------------------------------------------
# 4. Provider fetch boundary — only StateBuilder may call provider.fetch_events
# ---------------------------------------------------------------------------

def test_only_state_builder_calls_fetch_events() -> None:
    """
    The only file allowed to call provider.fetch_events() or .fetch_*() is
    integration_core/state_builder.py.  Any other file doing so is a fetch
    boundary violation.
    """
    state_builder = APPS_API / "integration_core" / "state_builder.py"
    violations = []

    for src in _all_python_sources():
        if src == state_builder:
            continue
        text = src.read_text(encoding="utf-8", errors="replace")
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "fetch_events"
            ):
                violations.append(str(src.relative_to(ROOT)))
                break

    assert not violations, (
        "Provider fetch calls detected outside state_builder.py:\n"
        + "\n".join(f"  {v}" for v in violations)
    )


# ---------------------------------------------------------------------------
# 5. No duplicate orchestrators (modules orchestrator_lite must not be imported
#    from apps/ source files)
# ---------------------------------------------------------------------------

def test_no_apps_source_imports_orchestrator_lite() -> None:
    exempt = {APPS_API / "integration_core" / "architecture_guard.py"}
    violations = []
    for src in _all_python_sources():
        if src in exempt:
            continue
        text = src.read_text(encoding="utf-8", errors="replace")
        if "orchestrator_lite" in text:
            violations.append(str(src.relative_to(ROOT)))
    assert not violations, (
        "apps/ source files must not import modules.core.services.orchestrator_lite "
        "(competing orchestrator). Found in:\n"
        + "\n".join(f"  {v}" for v in violations)
    )


def test_no_new_orchestrator_or_duplicate_decision_engine_classes() -> None:
    """
    Architecture freeze guard:
    - class *Orchestrator is only allowed in integration_core/orchestrator.py
    - class DecisionEngine is only allowed in integration_core/decision_engine.py
    """
    violations: list[str] = []
    for src in _all_python_sources():
        module_name = _module_name_for_path(src)
        source = src.read_text(encoding="utf-8", errors="replace")
        try:
            assert_runtime_architecture_source(module_name, source)
        except IntegrationCoreBoundaryViolation as exc:
            violations.append(f"{src.relative_to(ROOT)} :: {exc}")

    assert not violations, "\n".join(violations)


def test_only_integration_core_orchestrator_exists() -> None:
    """Required alias test name for architecture freeze reporting."""
    test_no_new_orchestrator_or_duplicate_decision_engine_classes()


def test_only_integration_core_decision_engine_exists() -> None:
    """Required alias test name for architecture freeze reporting."""
    test_no_new_orchestrator_or_duplicate_decision_engine_classes()


def test_only_state_builder_imports_providers_module() -> None:
    """
    Static import rule: providers module must only be imported by state_builder.
    This prevents provider coupling from spreading beyond the fetch boundary.
    """
    allowed = {APPS_API / "integration_core" / "state_builder.py"}
    violations: list[str] = []

    for src in _all_python_sources():
        tree = ast.parse(src.read_text(encoding="utf-8", errors="replace"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "apps.api.integration_core.providers":
                if src not in allowed:
                    violations.append(str(src.relative_to(ROOT)))
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "apps.api.integration_core.providers" and src not in allowed:
                        violations.append(str(src.relative_to(ROOT)))

    assert not violations, (
        "providers module import is only allowed in state_builder.py. Found in:\n"
        + "\n".join(f"  {v}" for v in sorted(set(violations)))
    )


def test_brief_endpoint_calls_orchestrator_only() -> None:
    """
    Brief endpoint must call create_orchestrator and must not directly import
    providers or call fetch_events.
    """
    brief_endpoint = APPS_API / "endpoints" / "brief_endpoint.py"
    text = brief_endpoint.read_text(encoding="utf-8", errors="replace")

    assert "create_orchestrator" in text
    assert "integration_core.providers" not in text
    assert ".fetch_events(" not in text


def test_no_pipeline_symbols_outside_integration_core() -> None:
    """Required alias test name for architecture freeze reporting."""
    for symbol in DELETED_SYMBOLS:
        test_no_source_imports_deleted_symbol(symbol)


def test_endpoint_uses_only_orchestrator() -> None:
    """Required alias test name for architecture freeze reporting."""
    test_brief_endpoint_calls_orchestrator_only()
