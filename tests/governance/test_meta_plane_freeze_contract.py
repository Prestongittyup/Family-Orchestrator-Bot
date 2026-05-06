from __future__ import annotations

from pathlib import Path

import pytest


_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate, pytest.mark.integration]


ROOT = Path(__file__).resolve().parents[2]

_META_DOC_TOKENS = (
    "GOAL_PROGRESS_MAP.md",
    "ARCHITECTURE_DRIFT_LOG.md",
)

_ALLOWED_TEST_FILES_WITH_META_TOKENS = {
    "tests/governance/test_meta_plane_freeze_contract.py",
    "tests/test_governance_gates.py",
}


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_tests_do_not_depend_on_meta_plane_doc_state() -> None:
    violations: list[str] = []

    for path in (ROOT / "tests").rglob("*.py"):
        if not path.is_file():
            continue

        relative_path = path.relative_to(ROOT).as_posix()
        if relative_path in _ALLOWED_TEST_FILES_WITH_META_TOKENS:
            continue

        source = path.read_text(encoding="utf-8")
        if any(token in source for token in _META_DOC_TOKENS):
            violations.append(relative_path)

    assert not violations, (
        "Meta-plane documentation tokens leaked into test enforcement logic: "
        f"{sorted(violations)}"
    )


def test_architecture_guard_does_not_depend_on_meta_plane_docs() -> None:
    source = _read("scripts/architecture_layer_guard.py")

    forbidden_tokens = (
        "GOAL_PROGRESS_MAP.md",
        "ARCHITECTURE_DRIFT_LOG.md",
        "docs/README.md",
    )
    for token in forbidden_tokens:
        assert token not in source, (
            f"Meta-plane token leaked into architecture guard enforcement: {token}"
        )


def test_system_architecture_suite_does_not_require_meta_plane_docs() -> None:
    source = _read("tests/system/test_architecture_suite.py")

    forbidden_tokens = (
        "GOAL_PROGRESS_MAP.md",
        "ARCHITECTURE_DRIFT_LOG.md",
        "docs/README.md",
    )
    for token in forbidden_tokens:
        assert token not in source, (
            f"Meta-plane token leaked into architecture suite enforcement: {token}"
        )


def test_governance_gate_allowlists_exclude_meta_plane_docs() -> None:
    from tests import test_governance_gates as governance_gates

    assert "README.md" not in governance_gates.ROOT_MARKDOWN_ALLOWLIST

    forbidden_docs_tokens = (
        "docs/README.md",
        "docs/architecture/GOAL_PROGRESS_MAP.md",
        "docs/architecture/ARCHITECTURE_DRIFT_LOG.md",
    )
    for token in forbidden_docs_tokens:
        assert token not in governance_gates.DOCS_MARKDOWN_ALLOWLIST
