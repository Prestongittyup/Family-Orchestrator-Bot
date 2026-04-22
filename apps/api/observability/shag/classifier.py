"""
SHAG Failure Classifier
-----------------------
Stateless engine that consumes execution maps, drift reports, and architecture
risk reports, then emits a list of FailureFinding objects.

Each classification rule is an isolated method.  No state is mutated.
Same inputs always produce the same findings (deterministic).

Input schemas
-------------
baseline_map / current_map:  execution_map.json format
  {
    "trace_count": int,
    "functions": {
      "<module.qualname>": {
        "execution_count": int,
        "temperature": "HOT"|"WARM"|"COLD"|"DEAD",
        "unique_callers": [str, ...]
      }
    },
    "entrypoint_reachability": {
      "<entrypoint>": [<function_key>, ...]
    }
  }

arch_risk_report:  ARCHITECTURE_RISK_REPORT.json
  {
    "verdict": "PASS"|"WARN"|"REQUIRE_APPROVAL"|"BLOCK",
    "circular_dependencies": [[str, ...]],
    "violations": [{"type": str, "detail": str, ...}],
    ...
  }

drift_report:  execution_drift_report.json
  {
    "new_functions": [str],
    "removed_functions": [str],
    "temperature_changes": {
      "<function_key>": {"before": str, "after": str}
    },
    "new_callers": {
      "<function_key>": [str]
    }
  }

git_diff:  plain text unified diff string (may be empty / None)
"""

from __future__ import annotations

import re
from typing import Any

from apps.api.observability.shag.models import (
    FailureFinding,
    FailureType,
    Severity,
)

# Temperature rank: higher = hotter
_TEMP_RANK: dict[str, int] = {"DEAD": 0, "COLD": 1, "WARM": 2, "HOT": 3}


def _rank(temperature: str) -> int:
    return _TEMP_RANK.get(temperature.upper(), 0)


def _functions(map_: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return map_.get("functions", {})


def _reachability(map_: dict[str, Any]) -> dict[str, list[str]]:
    return map_.get("entrypoint_reachability", {})


# ---------------------------------------------------------------------------
# Rule 1 — HOT_PATH_BREAK
# ---------------------------------------------------------------------------
def _classify_hot_path_breaks(
    baseline: dict[str, Any],
    current: dict[str, Any],
) -> list[FailureFinding]:
    """WARM/HOT function in baseline dropped to COLD or DEAD in current."""
    findings: list[FailureFinding] = []
    baseline_fns = _functions(baseline)
    current_fns = _functions(current)

    for fn, b_meta in baseline_fns.items():
        b_temp = str(b_meta.get("temperature", "DEAD")).upper()
        if _rank(b_temp) < _rank("WARM"):
            continue  # only care about WARM/HOT baseline

        c_meta = current_fns.get(fn)
        c_temp = str(c_meta.get("temperature", "DEAD")).upper() if c_meta else "DEAD"

        if _rank(c_temp) < _rank(b_temp):
            severity = Severity.CRITICAL if b_temp == "HOT" else Severity.HIGH
            findings.append(
                FailureFinding(
                    failure_type=FailureType.HOT_PATH_BREAK,
                    severity=severity,
                    function_key=fn,
                    description=(
                        f"Function was {b_temp} in baseline but is now {c_temp}. "
                        f"Execution coverage dropped from "
                        f"{b_meta.get('execution_count', 0)} to "
                        f"{c_meta.get('execution_count', 0) if c_meta else 0} calls."
                    ),
                    evidence={
                        "baseline_temperature": b_temp,
                        "current_temperature": c_temp,
                        "baseline_count": b_meta.get("execution_count", 0),
                        "current_count": c_meta.get("execution_count", 0) if c_meta else 0,
                    },
                )
            )
    return findings


# ---------------------------------------------------------------------------
# Rule 2 — ENTRYPOINT_DRIFT
# ---------------------------------------------------------------------------
def _classify_entrypoint_drift(
    baseline: dict[str, Any],
    current: dict[str, Any],
) -> list[FailureFinding]:
    """The set of functions reachable from a known entrypoint changed."""
    findings: list[FailureFinding] = []
    b_reach = _reachability(baseline)
    c_reach = _reachability(current)

    all_entrypoints = set(b_reach.keys()) | set(c_reach.keys())

    for ep in sorted(all_entrypoints):
        b_set = set(b_reach.get(ep, []))
        c_set = set(c_reach.get(ep, []))

        lost = sorted(b_set - c_set)
        gained = sorted(c_set - b_set)

        if not lost and not gained:
            continue

        # Lost reachability from a known entrypoint is more serious
        severity = Severity.HIGH if lost else Severity.MEDIUM
        findings.append(
            FailureFinding(
                failure_type=FailureType.ENTRYPOINT_DRIFT,
                severity=severity,
                function_key=ep,
                description=(
                    f"Entrypoint '{ep}' reachability changed. "
                    f"Lost: {lost or 'none'}. Gained: {gained or 'none'}."
                ),
                evidence={
                    "entrypoint": ep,
                    "lost_functions": lost,
                    "gained_functions": gained,
                    "baseline_reach": sorted(b_set),
                    "current_reach": sorted(c_set),
                },
            )
        )
    return findings


# ---------------------------------------------------------------------------
# Rule 3 — GRAPH_EXPANSION
# ---------------------------------------------------------------------------
def _classify_graph_expansion(
    baseline: dict[str, Any],
    current: dict[str, Any],
    drift_report: dict[str, Any] | None,
) -> list[FailureFinding]:
    """New callers appeared on existing functions, or entirely new functions appeared."""
    findings: list[FailureFinding] = []
    baseline_fns = _functions(baseline)
    current_fns = _functions(current)

    # New callers on existing functions
    for fn, c_meta in current_fns.items():
        c_callers = set(c_meta.get("unique_callers", []))
        b_meta = baseline_fns.get(fn)
        b_callers = set(b_meta.get("unique_callers", [])) if b_meta else set()
        new_callers = sorted(c_callers - b_callers)
        if new_callers:
            findings.append(
                FailureFinding(
                    failure_type=FailureType.GRAPH_EXPANSION,
                    severity=Severity.MEDIUM,
                    function_key=fn,
                    description=(
                        f"Function '{fn}' acquired new callers not present in baseline: "
                        f"{new_callers}."
                    ),
                    evidence={
                        "new_callers": new_callers,
                        "baseline_callers": sorted(b_callers),
                        "current_callers": sorted(c_callers),
                    },
                )
            )

    # Entirely new active functions (not in baseline at all, and now not DEAD)
    new_active_fns = sorted(
        fn for fn, meta in current_fns.items()
        if fn not in baseline_fns and _rank(str(meta.get("temperature", "DEAD"))) > 0
    )

    # Supplement with drift_report if available
    if drift_report:
        extra_new = drift_report.get("new_functions", [])
        new_active_fns = sorted(set(new_active_fns) | set(extra_new))

    for fn in new_active_fns:
        if any(f.function_key == fn and f.failure_type == FailureType.GRAPH_EXPANSION
               for f in findings):
            continue  # already captured via caller expansion above
        c_meta = current_fns.get(fn, {})
        findings.append(
            FailureFinding(
                failure_type=FailureType.GRAPH_EXPANSION,
                severity=Severity.MEDIUM,
                function_key=fn,
                description=(
                    f"New function '{fn}' is now active (not in baseline). "
                    f"Temperature: {c_meta.get('temperature', 'UNKNOWN')}."
                ),
                evidence={
                    "in_baseline": False,
                    "current_temperature": c_meta.get("temperature", "UNKNOWN"),
                    "current_count": c_meta.get("execution_count", 0),
                },
            )
        )
    return findings


# ---------------------------------------------------------------------------
# Rule 4 — STATE_MACHINE_VIOLATION
# ---------------------------------------------------------------------------
def _classify_state_machine_violations(
    arch_risk_report: dict[str, Any] | None,
) -> list[FailureFinding]:
    """Surface violations from the architecture risk report."""
    findings: list[FailureFinding] = []
    if not arch_risk_report:
        return findings

    # Circular dependencies
    for cycle in arch_risk_report.get("circular_dependencies", []):
        if not cycle:
            continue
        cycle_str = " -> ".join(cycle)
        findings.append(
            FailureFinding(
                failure_type=FailureType.STATE_MACHINE_VIOLATION,
                severity=Severity.CRITICAL,
                function_key=cycle[0],
                description=f"Circular dependency detected: {cycle_str}",
                evidence={"cycle": cycle},
            )
        )

    # Explicit violations list
    for violation in arch_risk_report.get("violations", []):
        v_type = str(violation.get("type", "unknown"))
        v_detail = str(violation.get("detail", ""))
        v_fn = str(violation.get("function_key", violation.get("module", "unknown")))
        findings.append(
            FailureFinding(
                failure_type=FailureType.STATE_MACHINE_VIOLATION,
                severity=Severity.HIGH,
                function_key=v_fn,
                description=f"Architecture violation [{v_type}]: {v_detail}",
                evidence=dict(violation),
            )
        )

    # Enforcement layer disabled
    enforcement = arch_risk_report.get("enforcement_layer_status", {})
    for gate_name, active in enforcement.items():
        if active is False:
            findings.append(
                FailureFinding(
                    failure_type=FailureType.STATE_MACHINE_VIOLATION,
                    severity=Severity.HIGH,
                    function_key=gate_name,
                    description=f"Architecture enforcement gate '{gate_name}' is disabled.",
                    evidence={"gate": gate_name, "active": active},
                )
            )

    return findings


# ---------------------------------------------------------------------------
# Rule 5 — DEAD_CODE_REVIVAL
# ---------------------------------------------------------------------------
def _classify_dead_code_revival(
    baseline: dict[str, Any],
    current: dict[str, Any],
) -> list[FailureFinding]:
    """A function that was DEAD in baseline is now active."""
    findings: list[FailureFinding] = []
    baseline_fns = _functions(baseline)
    current_fns = _functions(current)

    for fn, b_meta in baseline_fns.items():
        b_temp = str(b_meta.get("temperature", "DEAD")).upper()
        if b_temp != "DEAD":
            continue

        c_meta = current_fns.get(fn)
        if not c_meta:
            continue
        c_temp = str(c_meta.get("temperature", "DEAD")).upper()
        if c_temp == "DEAD":
            continue

        findings.append(
            FailureFinding(
                failure_type=FailureType.DEAD_CODE_REVIVAL,
                severity=Severity.HIGH,
                function_key=fn,
                description=(
                    f"Previously DEAD function '{fn}' is now {c_temp} "
                    f"({c_meta.get('execution_count', 0)} calls). Unexpected activation."
                ),
                evidence={
                    "baseline_temperature": b_temp,
                    "current_temperature": c_temp,
                    "current_count": c_meta.get("execution_count", 0),
                },
            )
        )
    return findings


# ---------------------------------------------------------------------------
# Git diff: extract modified function keys
# ---------------------------------------------------------------------------
def _extract_modified_functions_from_diff(git_diff: str) -> list[str]:
    """
    Heuristically extract fully-qualified Python function paths from a git diff.

    Looks for lines like:  +    def my_method(  or  -    def my_method(
    inside file hunks and attempts to reconstruct module.qualname.
    """
    modified: list[str] = []
    current_file: str = ""
    current_class: str = ""
    # def_re matches sync + async defs
    def_re = re.compile(r"^[+-]\s+(?:async\s+)?def\s+(\w+)\s*\(")
    class_re = re.compile(r"^[+-]?\s*class\s+(\w+)\s*[:(]")
    file_re = re.compile(r"^\+\+\+ b/(.+)$")

    for line in git_diff.splitlines():
        # Track file context
        m = file_re.match(line)
        if m:
            # Convert path like apps/api/ingestion/service.py → apps.api.ingestion.service
            raw = m.group(1).removesuffix(".py").replace("/", ".")
            current_file = raw
            current_class = ""
            continue

        # Track class context
        m = class_re.match(line)
        if m:
            current_class = m.group(1)
            continue

        # Detect modified functions
        m = def_re.match(line)
        if m and current_file:
            fn_name = m.group(1)
            if current_class:
                qualified = f"{current_file}.{current_class}.{fn_name}"
            else:
                qualified = f"{current_file}.{fn_name}"
            if qualified not in modified:
                modified.append(qualified)

    return modified


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
class FailureClassifier:
    """Runs all five classification rules and returns combined findings."""

    def classify(
        self,
        *,
        baseline_map: dict[str, Any],
        current_map: dict[str, Any],
        arch_risk_report: dict[str, Any] | None = None,
        drift_report: dict[str, Any] | None = None,
        git_diff: str = "",
    ) -> list[FailureFinding]:
        """Classify architectural failures from execution deltas and reports.

        Args:
            baseline_map:      Previous execution map (baseline).
            current_map:       Current execution map.
            arch_risk_report:  ARCHITECTURE_RISK_REPORT.json content (optional).
            drift_report:      execution_drift_report.json content (optional).
            git_diff:          Unified diff text for the PR/commit (optional).

        Returns:
            Sorted list of FailureFinding by (severity rank desc, function_key asc).
        """
        findings: list[FailureFinding] = []

        findings.extend(_classify_hot_path_breaks(baseline_map, current_map))
        findings.extend(_classify_entrypoint_drift(baseline_map, current_map))
        findings.extend(_classify_graph_expansion(baseline_map, current_map, drift_report))
        findings.extend(_classify_state_machine_violations(arch_risk_report))
        findings.extend(_classify_dead_code_revival(baseline_map, current_map))

        # Annotate findings that overlap with modified functions in the git diff
        if git_diff:
            modified_fns = set(_extract_modified_functions_from_diff(git_diff))
            for finding in findings:
                if finding.function_key in modified_fns:
                    finding.evidence["in_git_diff"] = True

        sev_rank = {
            Severity.CRITICAL: 0,
            Severity.HIGH: 1,
            Severity.MEDIUM: 2,
            Severity.LOW: 3,
            Severity.INFO: 4,
        }
        return sorted(
            findings,
            key=lambda f: (sev_rank.get(f.severity, 9), f.function_key),
        )
