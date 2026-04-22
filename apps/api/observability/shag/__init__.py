"""
Self-Healing Architecture Guard (SHAG)
=======================================
Package entry-point.  Exposes a single ``run_shag()`` function that ties
together all four stages:

  1. FailureClassifier   — detect architectural failures
  2. RecoveryStrategyEngine — map failures to safe strategies
  3. PatchGenerator      — produce minimal corrective diff
  4. SHAGReporter        — write REMEDIATION_REPORT.md, .diff, .json

Typical usage from scripts/shag_guard.py:
    from apps.api.observability.shag import run_shag
    report = run_shag(
        baseline_map=baseline,
        current_map=current,
        mode=RemediationMode.SUGGEST,
    )

Or from CI:
    python scripts/shag_guard.py --mode suggest
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from apps.api.observability.shag.models import (
    FailureFinding,
    FailureType,
    RecoveryAction,
    RecoveryStrategy,
    RemediationMode,
    RemediationPatch,
    SHAGReport,
    SHAGVerdict,
    Severity,
    PatchHunk,
)
from apps.api.observability.shag.classifier import FailureClassifier
from apps.api.observability.shag.recovery import RecoveryStrategyEngine
from apps.api.observability.shag.patch_gen import PatchGenerator
from apps.api.observability.shag.reporter import SHAGReporter, render_remediation_report_markdown

__all__ = [
    # Models
    "FailureFinding",
    "FailureType",
    "RecoveryAction",
    "RecoveryStrategy",
    "RemediationMode",
    "RemediationPatch",
    "PatchHunk",
    "SHAGReport",
    "SHAGVerdict",
    "Severity",
    # Engines
    "FailureClassifier",
    "RecoveryStrategyEngine",
    "PatchGenerator",
    "SHAGReporter",
    "render_remediation_report_markdown",
    # Main entry-point
    "run_shag",
]


def run_shag(
    *,
    baseline_map: dict[str, Any],
    current_map: dict[str, Any],
    arch_risk_report: dict[str, Any] | None = None,
    drift_report: dict[str, Any] | None = None,
    git_diff: str = "",
    mode: RemediationMode = RemediationMode.SUGGEST,
    output_dir: Path | None = None,
    repo_root: Path | None = None,
) -> SHAGReport:
    """Run the full SHAG pipeline and return a SHAGReport.

    Args:
        baseline_map:      Previous execution_map.json (baseline).
        current_map:       Current execution_map.json.
        arch_risk_report:  ARCHITECTURE_RISK_REPORT.json content (optional).
        drift_report:      execution_drift_report.json content (optional).
        git_diff:          Unified diff text for the PR/commit (optional).
        mode:              DRY_RUN / SUGGEST / AUTO.
        output_dir:        Where to write artifacts (default: data/shag).
        repo_root:         Repository root for git operations (default: cwd).

    Returns:
        SHAGReport with findings, strategies, patch, and verdict.
    """
    resolved_root = repo_root or Path.cwd()
    resolved_output = output_dir or (resolved_root / "data" / "shag")

    # Stage 1: Classify
    classifier = FailureClassifier()
    findings = classifier.classify(
        baseline_map=baseline_map,
        current_map=current_map,
        arch_risk_report=arch_risk_report,
        drift_report=drift_report,
        git_diff=git_diff,
    )

    # Stage 2: Recovery strategies
    strategy_engine = RecoveryStrategyEngine()
    strategies = strategy_engine.plan(findings)
    verdict = strategy_engine.overall_verdict(strategies)

    # Stage 3: Patch (only in SUGGEST / AUTO, and only if safe strategies exist)
    patch: RemediationPatch | None = None
    if mode in {RemediationMode.SUGGEST, RemediationMode.AUTO}:
        patch_gen = PatchGenerator(repo_root=resolved_root)
        patch = patch_gen.generate(strategies)

    # Build final report
    report = SHAGReport(
        verdict=verdict,
        mode=mode,
        findings=findings,
        strategies=strategies,
        patch=patch,
        metadata={
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "mode": mode.value,
            "baseline_trace_count": baseline_map.get("trace_count", 0),
            "current_trace_count": current_map.get("trace_count", 0),
            "git_diff_present": bool(git_diff),
            "arch_risk_report_present": arch_risk_report is not None,
            "drift_report_present": drift_report is not None,
        },
    )

    # Stage 4: Write artifacts (unless DRY_RUN)
    if mode != RemediationMode.DRY_RUN:
        reporter = SHAGReporter(output_dir=resolved_output)
        patch_gen_ref = PatchGenerator(repo_root=resolved_root)
        reporter.write(report, patch_renderer=patch_gen_ref.render_diff)

    return report
