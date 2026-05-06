from __future__ import annotations

import ast
from pathlib import Path

import pytest

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate]


ROOT = Path(__file__).resolve().parents[1]
CANONICAL_DECISION_CARD_REGISTRY = ROOT / "decision_card_system" / "registry.py"
LEGACY_DECISION_CARD_SHIM = ROOT / "core" / "replay" / "decision_card_registry.py"

SCAN_ROOTS = [
    ROOT / "app",
    ROOT / "apps",
    ROOT / "assistant",
    ROOT / "core",
    ROOT / "household_os",
]

EXCLUDED_DIR_NAMES = {
    ".git",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    "node_modules",
    "archive",
    "tests",
    "legacy",
}

ALLOWED_DECISION_CARD_EVENT_REFERENCES = {
    (ROOT / "decision_card_system" / "registry.py").resolve(),
    (ROOT / "core" / "replay" / "decision_card_registry.py").resolve(),
    (ROOT / "core" / "replay" / "event_replay_engine.py").resolve(),
    (ROOT / "app" / "services" / "commands" / "runtime.py").resolve(),
}

DECISION_CARD_EVENT_TOKENS = {
    "DecisionCardGenerated",
    "DecisionCardSurfaced",
    "DecisionCardAcknowledged",
    "DecisionCardResolved",
    "DecisionCardApplied",
}


def _iter_runtime_python_files() -> list[Path]:
    files: list[Path] = []
    for root in SCAN_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if any(part in EXCLUDED_DIR_NAMES for part in path.parts):
                continue
            files.append(path)
    return sorted(set(files))


@pytest.mark.integration
def test_canonical_decision_card_registry_exists() -> None:
    assert CANONICAL_DECISION_CARD_REGISTRY.exists(), (
        "Canonical decision-card registry is required at decision_card_system/registry.py"
    )

    source = CANONICAL_DECISION_CARD_REGISTRY.read_text(encoding="utf-8")
    assert "def createDecisionCard(" in source

    shim_source = LEGACY_DECISION_CARD_SHIM.read_text(encoding="utf-8")
    assert "from decision_card_system.registry import *" in shim_source
    assert "def createDecisionCard(" not in shim_source


@pytest.mark.integration
def test_decision_card_registry_module_is_unique() -> None:
    registry_files = sorted(
        [
        path.resolve()
        for path in ROOT.rglob("*.py")
        if path.name in {"registry.py", "decision_card_registry.py"}
        if "archive" not in path.parts and "tests" not in path.parts
        if "decision_card_system" in path.parts or path == LEGACY_DECISION_CARD_SHIM
        ]
    )

    expected = sorted([CANONICAL_DECISION_CARD_REGISTRY.resolve(), LEGACY_DECISION_CARD_SHIM.resolve()])
    assert registry_files == expected, (
        "Decision-card registry topology lock failed. Expected one canonical registry module plus legacy shim, "
        f"found: {[path.relative_to(ROOT) for path in registry_files]}"
    )


@pytest.mark.integration
def test_no_shadow_create_decision_card_apis() -> None:
    forbidden_defs = {"createDecisionCard", "create_decision_card"}
    violations: list[str] = []

    for path in _iter_runtime_python_files():
        if path.resolve() == CANONICAL_DECISION_CARD_REGISTRY.resolve():
            continue

        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in forbidden_defs:
                violations.append(f"{path.relative_to(ROOT)}:{node.lineno}:{node.name}")

    assert not violations, (
        "Only the canonical registry may expose decision-card creation APIs. Found shadow definitions:\n"
        + "\n".join(sorted(violations))
    )


@pytest.mark.integration
def test_decision_card_event_tokens_are_not_scattered() -> None:
    findings: list[str] = []

    for path in _iter_runtime_python_files():
        resolved = path.resolve()
        if resolved in ALLOWED_DECISION_CARD_EVENT_REFERENCES:
            continue

        text = path.read_text(encoding="utf-8")
        for token in DECISION_CARD_EVENT_TOKENS:
            if token in text:
                findings.append(f"{path.relative_to(ROOT)}:{token}")

    assert not findings, (
        "Decision-card event topology lock failed. Event tokens should only appear in canonical runtime files. "
        "Move or delete shadow logic references:\n"
        + "\n".join(sorted(findings))
    )
