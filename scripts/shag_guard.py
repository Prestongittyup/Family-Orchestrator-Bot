#!/usr/bin/env python3
"""
scripts/shag_guard.py
----------------------
CI entry-point for the Self-Healing Architecture Guard (SHAG).

What it does
------------
1. Loads baseline + current execution maps from data/execution_traces/.
2. Optionally loads ARCHITECTURE_RISK_REPORT.json and execution_drift_report.json.
3. Optionally reads a git diff (from --diff-file or from git diff HEAD).
4. Runs the full SHAG pipeline (classify → strategise → patch → report).
5. Writes artifacts to data/shag/:
     REMEDIATION_REPORT.md
     remediation_patch.diff
     remediation_actions.json
6. Exits with appropriate code for CI gating.

Exit codes
----------
  0  PASS or WARN with no critical findings
  1  REQUIRE_APPROVAL or BLOCK (CI should fail unless overridden)
  2  Configuration / input error

CI behaviour by verdict
-----------------------
  PASS             → exit 0
  WARN             → exit 0  (log suggestions only)
  REQUIRE_APPROVAL → exit 1  (allow override via ALLOW_AUTO_REMEDIATION=true env var)
  BLOCK            → exit 1  (never overridable for STATE_MACHINE_VIOLATION)

Usage
-----
  python scripts/shag_guard.py
  python scripts/shag_guard.py --mode dry-run
  python scripts/shag_guard.py --mode auto
  python scripts/shag_guard.py --diff-file path/to/pr.diff
  python scripts/shag_guard.py --arch-risk ARCHITECTURE_RISK_REPORT.json
  python scripts/shag_guard.py --drift-report execution_drift_report.json
  ALLOW_AUTO_REMEDIATION=true python scripts/shag_guard.py
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.observability.shag import run_shag
from apps.api.observability.shag.models import (
    RemediationMode,
    SHAGVerdict,
    Severity,
)

TRACE_DIR = ROOT / "data" / "execution_traces"
SHAG_OUTPUT_DIR = ROOT / "data" / "shag"

# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _load_json(path: Path, label: str) -> dict | None:
    if not path.exists():
        print(f"[SHAG] {label} not found at {path.relative_to(ROOT)} — skipping.")
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"[SHAG] WARNING: Failed to parse {label}: {exc}")
        return None


def _load_git_diff(diff_file: Path | None) -> str:
    """Read diff from a file or from git diff HEAD."""
    if diff_file and diff_file.exists():
        print(f"[SHAG] Using diff file: {diff_file.relative_to(ROOT)}")
        return diff_file.read_text(encoding="utf-8", errors="replace")

    # Try git diff HEAD
    try:
        result = subprocess.run(
            ["git", "diff", "HEAD"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            print("[SHAG] Using git diff HEAD.")
            return result.stdout
    except (OSError, subprocess.TimeoutExpired):
        pass

    # Fallback: staged changes
    try:
        result = subprocess.run(
            ["git", "diff", "--cached"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            print("[SHAG] Using git diff --cached (staged changes).")
            return result.stdout
    except (OSError, subprocess.TimeoutExpired):
        pass

    print("[SHAG] No git diff available — proceeding without diff context.")
    return ""


# ---------------------------------------------------------------------------
# CI gating logic
# ---------------------------------------------------------------------------

def _should_fail(verdict: SHAGVerdict, findings: list, allow_override: bool) -> bool:
    """Return True if CI should fail."""
    if verdict == SHAGVerdict.PASS:
        return False
    if verdict == SHAGVerdict.WARN:
        return False

    # REQUIRE_APPROVAL or BLOCK
    has_state_machine_violation = any(
        f.failure_type.value == "STATE_MACHINE_VIOLATION" for f in findings
    )

    if verdict == SHAGVerdict.BLOCK and has_state_machine_violation:
        # STATE_MACHINE_VIOLATION is never overridable
        return True

    if allow_override:
        print(
            "[SHAG] ALLOW_AUTO_REMEDIATION=true — overriding CI gate "
            f"for verdict {verdict.value}."
        )
        return False

    return True


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SHAG — Self-Healing Architecture Guard CI runner"
    )
    parser.add_argument(
        "--mode",
        choices=["dry-run", "suggest", "auto"],
        default="suggest",
        help="Execution mode (default: suggest)",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=TRACE_DIR / "baseline_map.json",
        help="Path to baseline execution map JSON",
    )
    parser.add_argument(
        "--current",
        type=Path,
        default=TRACE_DIR / "execution_map.json",
        help="Path to current execution map JSON",
    )
    parser.add_argument(
        "--arch-risk",
        type=Path,
        default=None,
        help="Path to ARCHITECTURE_RISK_REPORT.json (optional)",
    )
    parser.add_argument(
        "--drift-report",
        type=Path,
        default=None,
        help="Path to execution_drift_report.json (optional)",
    )
    parser.add_argument(
        "--diff-file",
        type=Path,
        default=None,
        help="Path to a git diff file (optional; falls back to git diff HEAD)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=SHAG_OUTPUT_DIR,
        help=f"Directory for output artifacts (default: {SHAG_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--no-fail",
        action="store_true",
        help="Never exit non-zero (informational mode)",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    allow_override = os.environ.get("ALLOW_AUTO_REMEDIATION", "").lower() in {
        "1", "true", "yes"
    }

    # Map CLI mode to enum
    mode_map = {
        "dry-run": RemediationMode.DRY_RUN,
        "suggest": RemediationMode.SUGGEST,
        "auto": RemediationMode.AUTO,
    }
    mode = mode_map[args.mode]

    print(f"[SHAG] Mode: {mode.value}")

    # Load inputs
    baseline_map = _load_json(args.baseline, "Baseline execution map")
    current_map = _load_json(args.current, "Current execution map")

    if not baseline_map:
        print("[SHAG] ERROR: baseline_map is required. "
              "Run 'python scripts/eil_ci.py --save-baseline' first.")
        return 2

    if not current_map:
        print("[SHAG] ERROR: current execution map is required. "
              "Run 'python scripts/build_execution_map.py' first.")
        return 2

    arch_risk_report: dict | None = None
    if args.arch_risk:
        arch_risk_report = _load_json(args.arch_risk, "Architecture risk report")

    drift_report: dict | None = None
    if args.drift_report:
        drift_report = _load_json(args.drift_report, "Drift report")

    git_diff = _load_git_diff(args.diff_file)

    # Run SHAG
    print("[SHAG] Running failure classification...")
    report = run_shag(
        baseline_map=baseline_map,
        current_map=current_map,
        arch_risk_report=arch_risk_report,
        drift_report=drift_report,
        git_diff=git_diff,
        mode=mode,
        output_dir=args.output_dir,
        repo_root=ROOT,
    )

    # Print summary
    print(f"\n[SHAG] Verdict: {report.verdict.value}")
    print(f"[SHAG] Findings: {len(report.findings)} "
          f"(critical={report.critical_count}, high={report.high_count})")

    for finding in report.findings:
        print(f"  [{finding.severity.value}] {finding.failure_type.value}: "
              f"{finding.function_key}")

    if report.strategies:
        needs_manual = [s for s in report.strategies if s.manual_required]
        patch_eligible = [s for s in report.strategies if s.patch_eligible]
        if needs_manual:
            print(f"[SHAG] {len(needs_manual)} finding(s) require manual resolution.")
        if patch_eligible:
            print(f"[SHAG] {len(patch_eligible)} finding(s) are patch-eligible.")

    if mode != RemediationMode.DRY_RUN:
        out_rel = args.output_dir.relative_to(ROOT) if args.output_dir.is_relative_to(ROOT) else args.output_dir
        print(f"[SHAG] Artifacts written to: {out_rel}/")

    # CI gate
    if args.no_fail:
        return 0

    if _should_fail(report.verdict, report.findings, allow_override):
        print(f"\n[SHAG] BUILD FAILED — verdict: {report.verdict.value}")
        print("[SHAG] Review REMEDIATION_REPORT.md for recommended actions.")
        return 1

    print(f"\n[SHAG] BUILD PASSED — verdict: {report.verdict.value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
