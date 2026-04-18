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

import importlib
import importlib.util
import os
from pathlib import Path

import pytest

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
        # Look for direct provider fetch calls (not in comments)
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if ".fetch_events(" in line or (
                "provider" in line.lower() and ".fetch(" in line
            ):
                violations.append(f"{src.relative_to(ROOT)}:  {stripped}")

    assert not violations, (
        "Provider fetch calls detected outside state_builder.py:\n"
        + "\n".join(f"  {v}" for v in violations)
    )


# ---------------------------------------------------------------------------
# 5. No duplicate orchestrators (modules orchestrator_lite must not be imported
#    from apps/ source files)
# ---------------------------------------------------------------------------

def test_no_apps_source_imports_orchestrator_lite() -> None:
    violations = []
    for src in _all_python_sources():
        text = src.read_text(encoding="utf-8", errors="replace")
        if "orchestrator_lite" in text:
            violations.append(str(src.relative_to(ROOT)))
    assert not violations, (
        "apps/ source files must not import modules.core.services.orchestrator_lite "
        "(competing orchestrator). Found in:\n"
        + "\n".join(f"  {v}" for v in violations)
    )
