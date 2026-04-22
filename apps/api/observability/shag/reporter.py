"""
SHAG Reporter
-------------
Generates the three output artifacts from a SHAGReport:

  A. REMEDIATION_REPORT.md   — human-readable markdown
  B. remediation_patch.diff  — corrective diff (if applicable)
  C. remediation_actions.json — machine-readable JSON

All rendering is stateless — call render_*() functions at any time.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from apps.api.observability.shag.models import (
    FailureType,
    RecoveryAction,
    RemediationMode,
    SHAGReport,
    SHAGVerdict,
    Severity,
)

# ---------------------------------------------------------------------------
# Verdict banners
# ---------------------------------------------------------------------------
_VERDICT_EMOJI: dict[SHAGVerdict, str] = {
    SHAGVerdict.PASS: "PASS",
    SHAGVerdict.WARN: "WARN",
    SHAGVerdict.REQUIRE_APPROVAL: "APPROVAL REQUIRED",
    SHAGVerdict.BLOCK: "BLOCKED",
}

_SEVERITY_ICON: dict[Severity, str] = {
    Severity.CRITICAL: "[CRITICAL]",
    Severity.HIGH: "[HIGH]",
    Severity.MEDIUM: "[MEDIUM]",
    Severity.LOW: "[LOW]",
    Severity.INFO: "[INFO]",
}

_FAILURE_TYPE_EXPLANATION: dict[FailureType, str] = {
    FailureType.HOT_PATH_BREAK: (
        "A previously active (WARM/HOT) function has lost execution coverage. "
        "This indicates a change broke a critical execution path."
    ),
    FailureType.ENTRYPOINT_DRIFT: (
        "The set of functions reachable from a known API entrypoint has changed. "
        "This may indicate route handler modification or missing wiring."
    ),
    FailureType.GRAPH_EXPANSION: (
        "New callers or new traced functions appeared that were not in the execution "
        "baseline. Unexpected execution paths may introduce instability."
    ),
    FailureType.STATE_MACHINE_VIOLATION: (
        "The architecture risk report flagged a structural violation: circular "
        "dependency, disabled enforcement gate, or import contract breach. "
        "Auto-remediation is BLOCKED — human review required."
    ),
    FailureType.DEAD_CODE_REVIVAL: (
        "A function that was DEAD in the baseline is now active. Unexpected activation "
        "of dormant code may indicate an unintended import, route leak, or fixture error."
    ),
}

_ACTION_EXPLANATION: dict[RecoveryAction, str] = {
    RecoveryAction.REVERT_FUNCTION: "Revert the modified function to its pre-change state.",
    RecoveryAction.ISOLATE_CHANGE: "Isolate the offending commit or change to a separate branch for review.",
    RecoveryAction.PARTIAL_ROLLBACK: "Roll back only the affected route handler, leaving unrelated code intact.",
    RecoveryAction.SUGGEST_COMPAT_SHIM: "Add a compatibility shim to preserve existing API contracts.",
    RecoveryAction.QUARANTINE_MODULE: "Add a quarantine annotation and prevent further usage until audited.",
    RecoveryAction.FLAG_DEPENDENCY_CHAIN: "Flag the entire dependency chain for manual audit.",
    RecoveryAction.BLOCK_REQUIRE_MANUAL_FIX: "BLOCKED — a human engineer must resolve this before deployment.",
    RecoveryAction.LOG_ONLY: "Log the finding for awareness; no automated action taken.",
}


# ---------------------------------------------------------------------------
# Markdown renderer
# ---------------------------------------------------------------------------

def render_remediation_report_markdown(report: SHAGReport) -> str:
    lines: list[str] = []
    verdict_label = _VERDICT_EMOJI.get(report.verdict, report.verdict.value)
    ts = report.metadata.get("generated_at", datetime.now(UTC).isoformat())

    lines += [
        "# REMEDIATION_REPORT",
        "",
        f"Generated: {ts}",
        f"Mode: {report.mode.value}",
        f"Verdict: **{verdict_label}**",
        "",
    ]

    # Summary table
    lines += [
        "## Summary",
        "",
        f"| Metric | Value |",
        f"| --- | --- |",
        f"| Total findings | {len(report.findings)} |",
        f"| Critical | {report.critical_count} |",
        f"| High | {report.high_count} |",
        f"| Manual review required | {'Yes' if report.has_manual_required else 'No'} |",
        f"| Patch generated | {'Yes' if report.patch and not report.patch.is_empty() else 'No'} |",
        "",
    ]

    # Trace/baseline metadata
    meta = report.metadata
    if meta.get("baseline_trace_count") or meta.get("current_trace_count"):
        lines += [
            "## Execution Snapshot",
            "",
            f"| | Baseline | Current |",
            f"| --- | --- | --- |",
            f"| Trace count | {meta.get('baseline_trace_count', '?')} "
            f"| {meta.get('current_trace_count', '?')} |",
            "",
        ]

    if not report.findings:
        lines += ["## Findings", "", "No architectural failures detected.", ""]
        return "\n".join(lines).strip() + "\n"

    # Findings
    lines += ["## Findings", ""]
    for i, finding in enumerate(report.findings, 1):
        sev_label = _SEVERITY_ICON.get(finding.severity, f"[{finding.severity.value}]")
        lines += [
            f"### {i}. {sev_label} {finding.failure_type.value}",
            "",
            f"**Function:** `{finding.function_key}`",
            "",
            f"**Description:** {finding.description}",
            "",
            f"**Risk explanation:** {_FAILURE_TYPE_EXPLANATION.get(finding.failure_type, '')}",
            "",
        ]

        # Evidence table
        evidence = finding.evidence
        if evidence:
            lines.append("**Evidence:**")
            lines.append("")
            lines.append("| Key | Value |")
            lines.append("| --- | --- |")
            for k, v in sorted(evidence.items()):
                lines.append(f"| {k} | `{v}` |")
            lines.append("")

    # Strategies
    lines += ["## Recommended Strategies", ""]
    for strategy in report.strategies:
        fn = strategy.finding.function_key
        primary = strategy.primary_action.value
        patch_note = "Patch eligible" if strategy.patch_eligible else "No auto-patch"
        manual_note = " **(Manual required)**" if strategy.manual_required else ""

        lines += [
            f"### `{fn}` — {primary}{manual_note}",
            "",
            f"**Primary action:** {_ACTION_EXPLANATION.get(strategy.primary_action, primary)}",
            "",
        ]
        if strategy.secondary_actions:
            lines.append("**Additional actions:**")
            lines.append("")
            for action in strategy.secondary_actions:
                lines.append(f"- {_ACTION_EXPLANATION.get(action, action.value)}")
            lines.append("")

        lines += [
            f"**Assessment:** {strategy.explanation}",
            "",
            f"*{patch_note}*",
            "",
        ]

    # Rollback recommendation
    revertable = [s for s in report.strategies if s.patch_eligible]
    blocked = [s for s in report.strategies if s.manual_required]

    lines += ["## Rollback Recommendation", ""]
    if blocked:
        lines += [
            "**BLOCKED** — the following findings require manual resolution:",
            "",
        ]
        for s in blocked:
            lines.append(f"- `{s.finding.function_key}` ({s.finding.failure_type.value})")
        lines.append("")

    if revertable and report.mode != RemediationMode.DRY_RUN:
        lines += [
            "The following functions can be auto-reverted (patch eligible):",
            "",
        ]
        for s in revertable:
            lines.append(f"- `{s.finding.function_key}`")
        lines += [
            "",
            "Review `remediation_patch.diff` before applying.",
            "",
        ]

    if report.patch and not report.patch.is_empty():
        lines += [
            "**Affected files in patch:**",
            "",
        ]
        for f in report.patch.affected_files:
            lines.append(f"- `{f}`")
        lines.append("")

    if report.mode == RemediationMode.DRY_RUN:
        lines += [
            "> **DRY_RUN mode** — no patches written to disk.", ""
        ]
    elif report.mode == RemediationMode.AUTO:
        lines += [
            "> **AUTO mode** — patch and report written to disk.", ""
        ]

    return "\n".join(lines).strip() + "\n"


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

def render_remediation_actions_json(report: SHAGReport) -> str:
    return json.dumps(report.to_actions_dict(), indent=2, sort_keys=True)


# ---------------------------------------------------------------------------
# Writer — saves all three artifacts
# ---------------------------------------------------------------------------

class SHAGReporter:
    """Writes the three output artifacts based on mode."""

    def __init__(self, output_dir: Path) -> None:
        self._output_dir = output_dir

    def write(
        self,
        report: SHAGReport,
        patch_renderer: Any | None = None,  # PatchGenerator.render_diff
    ) -> dict[str, Path]:
        """Write report artifacts to output_dir.

        Returns a dict of {artifact_name: Path} for written files.
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)
        written: dict[str, Path] = {}

        # A. REMEDIATION_REPORT.md — always written unless DRY_RUN
        if report.mode != RemediationMode.DRY_RUN:
            md_path = self._output_dir / "REMEDIATION_REPORT.md"
            md_path.write_text(
                render_remediation_report_markdown(report), encoding="utf-8"
            )
            written["REMEDIATION_REPORT.md"] = md_path

        # B. remediation_patch.diff — only in SUGGEST and AUTO
        if (
            report.mode in {RemediationMode.SUGGEST, RemediationMode.AUTO}
            and report.patch is not None
            and patch_renderer is not None
        ):
            diff_text = patch_renderer(report.patch)
            diff_path = self._output_dir / "remediation_patch.diff"
            diff_path.write_text(diff_text, encoding="utf-8")
            written["remediation_patch.diff"] = diff_path

        # C. remediation_actions.json — always written
        if report.mode != RemediationMode.DRY_RUN:
            json_path = self._output_dir / "remediation_actions.json"
            json_path.write_text(
                render_remediation_actions_json(report), encoding="utf-8"
            )
            written["remediation_actions.json"] = json_path

        return written
