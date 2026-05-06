from __future__ import annotations
import pytest

from pathlib import Path

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate]



ROOT = Path(__file__).resolve().parents[1]
GOVERNANCE_WORKFLOW = ROOT / ".github" / "workflows" / "governance-gate.yml"
GITIGNORE = ROOT / ".gitignore"

ROOT_MARKDOWN_ALLOWLIST = {
    ".copilot-instructions.md",
    "ARCHITECTURAL_CONTRACT_SPEC.md",
}

META_PLANE_ROOT_DOCS = {
    "README.md",
}

DOCS_MARKDOWN_ALLOWLIST = {
    "docs/HOUSEHOLD_OS_API_QUICK_START.md",
    "docs/PROJECT_DIRECTIVE.md",
    "docs/architecture/Architecture Implementation Plan.md",
    "docs/architecture/Enforcement Checklist.md",
    "docs/architecture/LAYER_MAP.md",
    "docs/architecture/RFC-001.md",
    "docs/architecture/Runtime Flow Spec.md",
}

META_PLANE_DOCS = {
    "docs/README.md",
    "docs/architecture/GOAL_PROGRESS_MAP.md",
    "docs/architecture/ARCHITECTURE_DRIFT_LOG.md",
}


@pytest.mark.integration
def test_governance_workflow_targets_existing_gate_tests() -> None:
    workflow_text = GOVERNANCE_WORKFLOW.read_text(encoding="utf-8")

    expected_tests = (
        "tests/test_hard_freeze_regression.py",
        "tests/test_governance_gates.py",
        "tests/test_layer_redundancy_guard.py",
        "tests/test_ui_canonical_wiring_guard.py",
    )

    for relative_path in expected_tests:
        assert relative_path in workflow_text
        assert (ROOT / relative_path).exists()


@pytest.mark.integration
def test_gitignore_has_secret_and_artifact_guardrails() -> None:
    content = GITIGNORE.read_text(encoding="utf-8")

    required_markers = (
        ".env",
        ".env.*",
        "!.env.example",
        "*_checkpoint.json",
        "*_report.json",
        "*.report.json",
        "*.report.second.json",
        "verification_reports/root_artifacts/",
        "verification_reports/repo_audit_runs/",
        "verification_reports/repo_audit_report.from_engine*.json",
        "verification_reports/repo_audit_report.from_engine*.md",
    )

    for marker in required_markers:
        assert marker in content


@pytest.mark.integration
def test_root_markdown_is_pruned_to_canonical_set() -> None:
    root_markdown = {
        path.name
        for path in ROOT.iterdir()
        if path.is_file() and path.suffix.lower() == ".md"
    }

    enforced_root_markdown = root_markdown - META_PLANE_ROOT_DOCS

    assert enforced_root_markdown == ROOT_MARKDOWN_ALLOWLIST, (
        "Root markdown drift detected. Keep only canonical docs in repo root. "
        f"Found (enforced set): {sorted(enforced_root_markdown)}"
    )


@pytest.mark.integration
def test_docs_markdown_is_pruned_to_canonical_set() -> None:
    docs_markdown = {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "docs").rglob("*.md")
        if path.is_file()
    }

    enforced_docs_markdown = docs_markdown - META_PLANE_DOCS

    assert enforced_docs_markdown == DOCS_MARKDOWN_ALLOWLIST, (
        "Docs markdown drift detected. Keep only canonical Home OS docs set. "
        f"Found (enforced set): {sorted(enforced_docs_markdown)}"
    )