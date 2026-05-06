"""
SHAG Models
-----------
All dataclasses and enums used across the Self-Healing Architecture Guard.

These are pure data containers — no logic lives here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Failure Classifications
# ---------------------------------------------------------------------------

class FailureType(str, Enum):
    """Five canonical architectural failure categories."""

    HOT_PATH_BREAK = "HOT_PATH_BREAK"
    """A WARM or HOT function lost execution coverage and dropped to COLD/DEAD."""

    ENTRYPOINT_DRIFT = "ENTRYPOINT_DRIFT"
    """The set of functions reachable from a known entrypoint has changed."""

    GRAPH_EXPANSION = "GRAPH_EXPANSION"
    """New callers appeared on existing functions, or entirely new traced functions
    appeared that were not in the baseline."""

    STATE_MACHINE_VIOLATION = "STATE_MACHINE_VIOLATION"
    """An architecture risk report flagged a circular dependency, forbidden
    import, or state-machine invariant breach."""

    DEAD_CODE_REVIVAL = "DEAD_CODE_REVIVAL"
    """A function that was DEAD in the baseline is now active (COLD/WARM/HOT)."""


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class RemediationMode(str, Enum):
    DRY_RUN = "DRY_RUN"    # analysis only — no files written
    SUGGEST = "SUGGEST"    # produce report + recommendations, no diff
    AUTO = "AUTO"          # produce report + generate patch diff


class SHAGVerdict(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    REQUIRE_APPROVAL = "REQUIRE_APPROVAL"
    BLOCK = "BLOCK"


# ---------------------------------------------------------------------------
# Failure findings
# ---------------------------------------------------------------------------

@dataclass
class FailureFinding:
    """A single classified architectural failure event."""

    failure_type: FailureType
    severity: Severity
    function_key: str                   # fully-qualified function name
    description: str
    evidence: dict[str, Any] = field(default_factory=dict)
    """Raw data that caused this classification (before/after counts, caller diffs…)"""

    def to_dict(self) -> dict[str, Any]:
        return {
            "failure_type": self.failure_type.value,
            "severity": self.severity.value,
            "function_key": self.function_key,
            "description": self.description,
            "evidence": self.evidence,
        }


# ---------------------------------------------------------------------------
# Recovery strategies
# ---------------------------------------------------------------------------

class RecoveryAction(str, Enum):
    REVERT_FUNCTION = "REVERT_FUNCTION"
    ISOLATE_CHANGE = "ISOLATE_CHANGE"
    PARTIAL_ROLLBACK = "PARTIAL_ROLLBACK"
    SUGGEST_COMPAT_SHIM = "SUGGEST_COMPAT_SHIM"
    QUARANTINE_MODULE = "QUARANTINE_MODULE"
    FLAG_DEPENDENCY_CHAIN = "FLAG_DEPENDENCY_CHAIN"
    BLOCK_REQUIRE_MANUAL_FIX = "BLOCK_REQUIRE_MANUAL_FIX"
    LOG_ONLY = "LOG_ONLY"


@dataclass
class RecoveryStrategy:
    """Recommended recovery actions for a single failure finding."""

    finding: FailureFinding
    primary_action: RecoveryAction
    secondary_actions: list[RecoveryAction] = field(default_factory=list)
    patch_eligible: bool = False
    """Whether a minimal corrective diff can be auto-generated."""
    manual_required: bool = False
    """If True, no auto-remediation is safe — human must intervene."""
    explanation: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "failure_type": self.finding.failure_type.value,
            "function_key": self.finding.function_key,
            "severity": self.finding.severity.value,
            "primary_action": self.primary_action.value,
            "secondary_actions": [a.value for a in self.secondary_actions],
            "patch_eligible": self.patch_eligible,
            "manual_required": self.manual_required,
            "explanation": self.explanation,
        }


# ---------------------------------------------------------------------------
# Patch
# ---------------------------------------------------------------------------

@dataclass
class PatchHunk:
    """A single hunk in a generated corrective diff."""

    file_path: str
    original_lines: list[str]
    replacement_lines: list[str]
    context_before: list[str] = field(default_factory=list)
    context_after: list[str] = field(default_factory=list)
    rationale: str = ""


@dataclass
class RemediationPatch:
    """Collection of hunks that together form REMEDIATION_PATCH.diff."""

    hunks: list[PatchHunk] = field(default_factory=list)
    affected_files: list[str] = field(default_factory=list)
    is_safe: bool = True
    safety_note: str = ""

    def is_empty(self) -> bool:
        return len(self.hunks) == 0


# ---------------------------------------------------------------------------
# Final report
# ---------------------------------------------------------------------------

@dataclass
class SHAGReport:
    """Top-level output of the SHAG system."""

    verdict: SHAGVerdict
    mode: RemediationMode
    findings: list[FailureFinding] = field(default_factory=list)
    strategies: list[RecoveryStrategy] = field(default_factory=list)
    patch: RemediationPatch | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    """Generated timestamps, input hash, trace counts, etc."""

    @property
    def critical_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.CRITICAL)

    @property
    def high_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.HIGH)

    @property
    def has_manual_required(self) -> bool:
        return any(s.manual_required for s in self.strategies)

    def to_actions_dict(self) -> dict[str, Any]:
        """Serialize to remediation_actions.json format."""
        return {
            "verdict": self.verdict.value,
            "mode": self.mode.value,
            "summary": {
                "total_findings": len(self.findings),
                "critical": self.critical_count,
                "high": self.high_count,
                "manual_required": self.has_manual_required,
            },
            "findings": [f.to_dict() for f in self.findings],
            "strategies": [s.to_dict() for s in self.strategies],
            "patch_generated": self.patch is not None and not self.patch.is_empty(),
            "metadata": self.metadata,
        }
