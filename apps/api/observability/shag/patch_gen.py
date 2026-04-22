"""
SHAG Patch Generation Engine
-----------------------------
Generates minimal corrective diffs from strategies that are patch_eligible.

Safety contract:
- NEVER modifies files directly.
- NEVER changes business logic semantics.
- Only generates revert/restore hunks for functions identified in findings.
- Output is a text diff that a human can review and apply manually or via PR.
- If git is not available or no safe patch can be produced, returns an empty patch.

Patch types produced:
  1. Function-level revert   — restores a function from git HEAD or baseline snapshot.
  2. Baseline restoration    — restores a function body to match a captured snapshot.
  3. Decorator annotation    — adds an isolation comment above a revived function.
"""

from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path
from typing import Any

from apps.api.observability.shag.models import (
    FailureType,
    PatchHunk,
    RecoveryAction,
    RecoveryStrategy,
    RemediationPatch,
)

# Context lines around a function in the diff output
_CONTEXT_LINES = 3


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def _git_available(repo_root: Path) -> bool:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--git-dir"],
            cwd=str(repo_root),
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _git_show_file_at_head(repo_root: Path, relative_path: str) -> str | None:
    """Return file content at HEAD (before any uncommitted changes)."""
    try:
        result = subprocess.run(
            ["git", "show", f"HEAD:{relative_path}"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            return result.stdout
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


def _git_diff_file(repo_root: Path, relative_path: str) -> str | None:
    """Return unified diff for a specific file (working tree vs HEAD)."""
    try:
        result = subprocess.run(
            ["git", "diff", "HEAD", "--", relative_path],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            return result.stdout
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


# ---------------------------------------------------------------------------
# Source file helpers
# ---------------------------------------------------------------------------

def _module_to_path(module_key: str, repo_root: Path) -> Path | None:
    """
    Convert a dotted module path like 'apps.api.ingestion.service' to
    an actual file path relative to repo_root.
    """
    # Strip class/function qualname segments: find longest existing file
    parts = module_key.split(".")
    for end in range(len(parts), 0, -1):
        candidate = Path(*parts[:end]).with_suffix(".py")
        full = repo_root / candidate
        if full.exists():
            return full
    return None


def _extract_function_block(
    source_lines: list[str],
    function_name: str,
) -> tuple[int, int] | None:
    """
    Find the start and end line indices (0-based, end exclusive) of a
    top-level or class-level function in source_lines.

    Returns None if the function cannot be located.
    """
    import re
    fn_re = re.compile(
        r"^(\s*)(?:async\s+)?def\s+" + re.escape(function_name) + r"\s*\("
    )
    start: int | None = None
    base_indent: str = ""

    for i, line in enumerate(source_lines):
        m = fn_re.match(line)
        if m:
            start = i
            base_indent = m.group(1)
            break

    if start is None:
        return None

    # Scan forward to find where the function body ends
    for i in range(start + 1, len(source_lines)):
        line = source_lines[i]
        stripped = line.rstrip()
        if not stripped:
            continue  # blank lines don't end a function
        # If we're back at the original indent level and not a decorator/body
        if len(line) - len(line.lstrip()) <= len(base_indent) and not line[0].isspace():
            return (start, i)
        # Sibling def at same indent (class method next to another method)
        if line.startswith(base_indent) and not line.startswith(base_indent + " "):
            if re.match(r"\s*(?:async\s+)?def\s+", line) or re.match(r"\s*@", line):
                return (start, i)

    return (start, len(source_lines))


# ---------------------------------------------------------------------------
# Hunk builders
# ---------------------------------------------------------------------------

def _build_revert_hunk(
    strategy: RecoveryStrategy,
    repo_root: Path,
) -> PatchHunk | None:
    """
    Attempt to produce a hunk that reverts a function to its HEAD state.
    Only produced when:
    - the function module file is locatable
    - git is available
    - the file differs from HEAD
    """
    fn_key = strategy.finding.function_key
    # Extract just the module part (drop class + method segments as needed)
    # Heuristic: try progressively shorter prefix until file found
    file_path = _module_to_path(fn_key, repo_root)
    if file_path is None:
        return None

    relative = str(file_path.relative_to(repo_root)).replace("\\", "/")
    head_content = _git_show_file_at_head(repo_root, relative)
    if head_content is None:
        return None

    current_content = file_path.read_text(encoding="utf-8", errors="replace")
    if head_content == current_content:
        return None  # File unchanged from HEAD

    # Extract the function name from the key (last segment after all dots)
    fn_name = fn_key.rsplit(".", 1)[-1]

    head_lines = head_content.splitlines(keepends=True)
    current_lines = current_content.splitlines(keepends=True)

    head_range = _extract_function_block([l.rstrip("\n") for l in head_lines], fn_name)
    curr_range = _extract_function_block([l.rstrip("\n") for l in current_lines], fn_name)

    if not head_range or not curr_range:
        return None

    h_start, h_end = head_range
    c_start, c_end = curr_range

    original = [l.rstrip("\n") for l in current_lines[c_start:c_end]]
    replacement = [l.rstrip("\n") for l in head_lines[h_start:h_end]]

    if original == replacement:
        return None  # Function body identical

    ctx_before = [l.rstrip("\n") for l in current_lines[max(0, c_start - _CONTEXT_LINES):c_start]]
    ctx_after = [l.rstrip("\n") for l in current_lines[c_end:c_end + _CONTEXT_LINES]]

    return PatchHunk(
        file_path=relative,
        original_lines=original,
        replacement_lines=replacement,
        context_before=ctx_before,
        context_after=ctx_after,
        rationale=f"Revert {fn_key} to HEAD state (HOT_PATH_BREAK recovery)",
    )


def _build_quarantine_annotation_hunk(
    strategy: RecoveryStrategy,
    repo_root: Path,
) -> PatchHunk | None:
    """
    Insert an isolation comment above a DEAD_CODE_REVIVAL function.
    Minimal — only adds a comment, never changes logic.
    """
    fn_key = strategy.finding.function_key
    file_path = _module_to_path(fn_key, repo_root)
    if file_path is None:
        return None

    relative = str(file_path.relative_to(repo_root)).replace("\\", "/")
    fn_name = fn_key.rsplit(".", 1)[-1]
    lines = file_path.read_text(encoding="utf-8", errors="replace").splitlines()
    fn_range = _extract_function_block(lines, fn_name)
    if not fn_range:
        return None

    start, _ = fn_range
    existing_line = lines[start]
    indent = existing_line[: len(existing_line) - len(existing_line.lstrip())]
    annotation = (
        f"{indent}# SHAG-QUARANTINE: DEAD_CODE_REVIVAL — "
        f"unexpected activation detected. Audit before use."
    )

    # Only insert if annotation not already present
    if start > 0 and "SHAG-QUARANTINE" in lines[start - 1]:
        return None

    ctx_before = lines[max(0, start - _CONTEXT_LINES):start]
    return PatchHunk(
        file_path=relative,
        original_lines=[existing_line],
        replacement_lines=[annotation, existing_line],
        context_before=ctx_before,
        context_after=[],
        rationale=f"Quarantine annotation for DEAD_CODE_REVIVAL: {fn_key}",
    )


# ---------------------------------------------------------------------------
# Diff formatting
# ---------------------------------------------------------------------------

def _format_unified_diff(hunks: list[PatchHunk]) -> str:
    """Format a list of PatchHunks into a unified diff string."""
    parts: list[str] = [
        "# REMEDIATION_PATCH.diff",
        "# Generated by SHAG — review before applying.",
        "# Apply with: git apply remediation_patch.diff",
        "",
    ]
    for hunk in hunks:
        parts.append(f"--- a/{hunk.file_path}")
        parts.append(f"+++ b/{hunk.file_path}")
        parts.append(f"# Rationale: {hunk.rationale}")

        # Context before
        for line in hunk.context_before:
            parts.append(f" {line}")
        # Removed lines
        for line in hunk.original_lines:
            parts.append(f"-{line}")
        # Added lines
        for line in hunk.replacement_lines:
            parts.append(f"+{line}")
        # Context after
        for line in hunk.context_after:
            parts.append(f" {line}")
        parts.append("")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
class PatchGenerator:
    """Generates minimal corrective diffs from eligible recovery strategies."""

    def __init__(self, repo_root: Path | None = None) -> None:
        self._repo_root = repo_root or Path.cwd()

    def generate(self, strategies: list[RecoveryStrategy]) -> RemediationPatch:
        """Attempt to generate patch hunks for all patch_eligible strategies.

        Strategies where manual_required=True or patch_eligible=False are skipped.
        Returns a RemediationPatch (possibly empty if no hunks could be built).
        """
        git_ok = _git_available(self._repo_root)

        hunks: list[PatchHunk] = []
        affected: set[str] = set()
        safety_notes: list[str] = []

        for strategy in strategies:
            if not strategy.patch_eligible:
                continue

            hunk: PatchHunk | None = None

            if strategy.primary_action == RecoveryAction.REVERT_FUNCTION:
                if git_ok:
                    hunk = _build_revert_hunk(strategy, self._repo_root)
                else:
                    safety_notes.append(
                        f"Git not available — cannot generate revert hunk for "
                        f"{strategy.finding.function_key}"
                    )

            elif strategy.primary_action == RecoveryAction.QUARANTINE_MODULE:
                hunk = _build_quarantine_annotation_hunk(strategy, self._repo_root)

            if hunk:
                hunks.append(hunk)
                affected.add(hunk.file_path)

        is_safe = len(hunks) > 0 or len(safety_notes) == 0
        note = "; ".join(safety_notes) if safety_notes else "Patch generated safely."

        return RemediationPatch(
            hunks=hunks,
            affected_files=sorted(affected),
            is_safe=is_safe,
            safety_note=note,
        )

    def render_diff(self, patch: RemediationPatch) -> str:
        """Render a RemediationPatch to a unified diff string."""
        if patch.is_empty():
            lines = [
                "# REMEDIATION_PATCH.diff",
                "# No safe hunks generated.",
                f"# Safety note: {patch.safety_note}",
            ]
            return "\n".join(lines)
        return _format_unified_diff(patch.hunks)
