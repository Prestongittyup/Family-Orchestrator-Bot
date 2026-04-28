from __future__ import annotations

import fnmatch
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

BLOCKED_ROOT_ARTIFACT_PATTERNS: tuple[str, ...] = (
    "*_checkpoint.json",
    "*_report.json",
    "*.report.json",
    "*.report.second.json",
    "*_results.json",
    "production_torture_report*.json",
    "sse_*_report*.json",
    "phase4_gate_validation*.json",
    "cleanup_analysis_*.json",
    "truth_gate_output*.log",
    "torture_gate_output.log",
    "baseline_test_output.log",
    "*.jsonl",
)


def _is_blocked_root_artifact(file_name: str) -> bool:
    return any(
        fnmatch.fnmatch(file_name, pattern)
        for pattern in BLOCKED_ROOT_ARTIFACT_PATTERNS
    )


def test_no_generated_runtime_artifacts_in_repo_root() -> None:
    offenders = sorted(
        path.name
        for path in ROOT.iterdir()
        if path.is_file() and _is_blocked_root_artifact(path.name)
    )

    assert offenders == [], (
        "Generated runtime/audit artifacts must not live in repository root. "
        "Move them under verification_reports/root_artifacts/. "
        f"Found: {offenders}"
    )
