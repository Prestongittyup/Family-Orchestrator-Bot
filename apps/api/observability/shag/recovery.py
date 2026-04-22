"""
SHAG Recovery Strategy Engine
------------------------------
Maps each FailureFinding to a RecoveryStrategy.

Design rules:
- STATE_MACHINE_VIOLATION always blocks auto-remediation (manual required).
- HOT_PATH_BREAK on CRITICAL severity blocks auto-remediation.
- All strategies are deterministic — same finding → same strategy.
- Strategies are stateless (no I/O, no side effects).
"""

from __future__ import annotations

from apps.api.observability.shag.models import (
    FailureFinding,
    FailureType,
    RecoveryAction,
    RecoveryStrategy,
    Severity,
)


# ---------------------------------------------------------------------------
# Per-type strategy factories
# ---------------------------------------------------------------------------

def _strategy_hot_path_break(finding: FailureFinding) -> RecoveryStrategy:
    """
    HOT_PATH_BREAK → revert modified functions, isolate offending change.

    CRITICAL severity blocks auto-remediation (risk of silent data issues).
    """
    is_critical = finding.severity == Severity.CRITICAL
    return RecoveryStrategy(
        finding=finding,
        primary_action=RecoveryAction.REVERT_FUNCTION,
        secondary_actions=[RecoveryAction.ISOLATE_CHANGE],
        patch_eligible=not is_critical,
        manual_required=is_critical,
        explanation=(
            f"Function '{finding.function_key}' was on the hot execution path "
            f"but is now {finding.evidence.get('current_temperature', 'DEAD')}. "
            + (
                "CRITICAL severity: manual review required before any revert. "
                if is_critical
                else "Propose revert of the function to its pre-change state and "
                     "isolate the offending commit."
            )
        ),
    )


def _strategy_entrypoint_drift(finding: FailureFinding) -> RecoveryStrategy:
    """
    ENTRYPOINT_DRIFT → partial rollback of route handler + compat shim.
    """
    lost = finding.evidence.get("lost_functions", [])
    gained = finding.evidence.get("gained_functions", [])
    manual = len(lost) > 0  # Lost reachability is riskier than gain
    return RecoveryStrategy(
        finding=finding,
        primary_action=RecoveryAction.PARTIAL_ROLLBACK,
        secondary_actions=[RecoveryAction.SUGGEST_COMPAT_SHIM],
        patch_eligible=not manual,
        manual_required=manual,
        explanation=(
            f"Entrypoint '{finding.function_key}' reachability changed. "
            + (f"Lost functions: {lost}. Manual rollback recommended. " if lost else "")
            + (f"Gained functions: {gained}. Consider compat shim to maintain "
               "backward compatibility." if gained else "")
        ),
    )


def _strategy_graph_expansion(finding: FailureFinding) -> RecoveryStrategy:
    """
    GRAPH_EXPANSION → isolate new execution paths, flag dependency chain.
    """
    new_callers = finding.evidence.get("new_callers", [])
    return RecoveryStrategy(
        finding=finding,
        primary_action=RecoveryAction.ISOLATE_CHANGE,
        secondary_actions=[RecoveryAction.FLAG_DEPENDENCY_CHAIN],
        patch_eligible=False,
        manual_required=False,
        explanation=(
            f"Function '{finding.function_key}' has new callers or is newly active. "
            + (f"New callers: {new_callers}. " if new_callers else "")
            + "Isolate the new execution path and audit the dependency chain before "
              "promoting to production."
        ),
    )


def _strategy_state_machine_violation(finding: FailureFinding) -> RecoveryStrategy:
    """
    STATE_MACHINE_VIOLATION → always blocks auto-remediation.
    """
    return RecoveryStrategy(
        finding=finding,
        primary_action=RecoveryAction.BLOCK_REQUIRE_MANUAL_FIX,
        secondary_actions=[],
        patch_eligible=False,
        manual_required=True,
        explanation=(
            f"Architecture violation in '{finding.function_key}': "
            f"{finding.description}. "
            "Auto-remediation is BLOCKED for state machine violations. "
            "A human engineer must resolve this before any deployment."
        ),
    )


def _strategy_dead_code_revival(finding: FailureFinding) -> RecoveryStrategy:
    """
    DEAD_CODE_REVIVAL → quarantine module, flag unexpected activation.
    """
    return RecoveryStrategy(
        finding=finding,
        primary_action=RecoveryAction.QUARANTINE_MODULE,
        secondary_actions=[RecoveryAction.FLAG_DEPENDENCY_CHAIN],
        patch_eligible=False,
        manual_required=True,
        explanation=(
            f"Previously DEAD function '{finding.function_key}' is now "
            f"{finding.evidence.get('current_temperature', 'active')} with "
            f"{finding.evidence.get('current_count', 0)} calls. "
            "Quarantine this module for audit — unexpected revival may indicate "
            "an unintended import, route registration, or test fixture leak."
        ),
    )


# ---------------------------------------------------------------------------
# Dispatch map
# ---------------------------------------------------------------------------
_STRATEGY_MAP = {
    FailureType.HOT_PATH_BREAK: _strategy_hot_path_break,
    FailureType.ENTRYPOINT_DRIFT: _strategy_entrypoint_drift,
    FailureType.GRAPH_EXPANSION: _strategy_graph_expansion,
    FailureType.STATE_MACHINE_VIOLATION: _strategy_state_machine_violation,
    FailureType.DEAD_CODE_REVIVAL: _strategy_dead_code_revival,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
class RecoveryStrategyEngine:
    """Maps each finding to a deterministic RecoveryStrategy."""

    def plan(self, findings: list[FailureFinding]) -> list[RecoveryStrategy]:
        """Return one RecoveryStrategy per finding, in same order.

        Args:
            findings: Classified failures from FailureClassifier.

        Returns:
            List of RecoveryStrategy objects, one-to-one with findings.
        """
        strategies: list[RecoveryStrategy] = []
        for finding in findings:
            factory = _STRATEGY_MAP.get(finding.failure_type)
            if factory:
                strategies.append(factory(finding))
            else:
                # Fallback: log-only for unknown types
                strategies.append(
                    RecoveryStrategy(
                        finding=finding,
                        primary_action=RecoveryAction.LOG_ONLY,
                        explanation=f"No strategy defined for {finding.failure_type}.",
                    )
                )
        return strategies

    def overall_verdict(
        self, strategies: list[RecoveryStrategy]
    ) -> "SHAGVerdict":
        """Derive top-level SHAG verdict from strategy set.

        Imported lazily to avoid circular import.
        """
        from apps.api.observability.shag.models import SHAGVerdict

        if any(s.primary_action == RecoveryAction.BLOCK_REQUIRE_MANUAL_FIX
               for s in strategies):
            return SHAGVerdict.BLOCK

        if any(s.manual_required for s in strategies):
            return SHAGVerdict.REQUIRE_APPROVAL

        # Map the highest finding severity to a verdict
        severities = {f.severity for s in strategies for f in [s.finding]}
        from apps.api.observability.shag.models import Severity
        if Severity.CRITICAL in severities:
            return SHAGVerdict.REQUIRE_APPROVAL
        if Severity.HIGH in severities:
            return SHAGVerdict.WARN
        if strategies:
            return SHAGVerdict.WARN

        return SHAGVerdict.PASS
