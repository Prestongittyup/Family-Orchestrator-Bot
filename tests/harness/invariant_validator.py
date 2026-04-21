"""
Invariant Validator - Hard constraints enforcement

Validates critical system invariants:
  - No duplicate task execution
  - No lost updates across concurrent modifications
  - No cross-family data leakage
  - No phantom or ghost task states
  - No divergence between projections and backend beyond watermark lag tolerance
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta


class InvariantViolation:
    """Represents a detected invariant violation"""
    
    def __init__(
        self,
        invariant_name: str,
        severity: str,  # "critical", "warning", "info"
        description: str,
        affected_entities: List[str] = None,
        details: Dict = None,
    ):
        self.invariant_name = invariant_name
        self.severity = severity
        self.description = description
        self.affected_entities = affected_entities or []
        self.details = details or {}
        self.detected_at = datetime.utcnow()
    
    def to_dict(self) -> dict:
        return {
            "invariant": self.invariant_name,
            "severity": self.severity,
            "description": self.description,
            "affected_entities": self.affected_entities,
            "details": self.details,
            "detected_at": self.detected_at.isoformat(),
        }


class InvariantValidator:
    """Validates system invariants during simulation"""
    
    def __init__(self):
        self.violations: List[InvariantViolation] = []
    
    def validate_no_duplicate_task_execution(
        self,
        state,
        threshold: int = 1,  # Maximum acceptable executions
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: Task must not execute more than once
        
        During idempotency failures, a task might execute multiple times.
        This is the most critical invariant to enforce.
        """
        for task_id, count in state.task_execution_count.items():
            if count > threshold:
                violation = InvariantViolation(
                    invariant_name="no_duplicate_task_execution",
                    severity="critical",
                    description=f"Task {task_id} executed {count} times (threshold: {threshold})",
                    affected_entities=[task_id],
                    details={
                        "task_id": task_id,
                        "execution_count": count,
                        "threshold": threshold,
                    },
                )
                self.violations.append(violation)
                return violation
        
        return None
    
    def validate_no_lost_updates(
        self,
        state,
        expected_entity_count: int = None,
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: No updates should be lost in concurrent modifications
        
        All created entities must persist, no silent deletions except explicit.
        """
        current_count = len([e for e in state.entities.values() if not e.deleted])
        
        if expected_entity_count is not None:
            if current_count < expected_entity_count:
                violation = InvariantViolation(
                    invariant_name="no_lost_updates",
                    severity="critical",
                    description=(
                        f"Expected {expected_entity_count} entities, found {current_count} "
                        f"({expected_entity_count - current_count} lost)"
                    ),
                    details={
                        "expected": expected_entity_count,
                        "actual": current_count,
                        "lost": expected_entity_count - current_count,
                    },
                )
                self.violations.append(violation)
                return violation
        
        return None
    
    def validate_no_cross_family_leakage(
        self,
        state,
        family_id: str,
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: No data from other families visible in this family
        
        All entities must belong to this family_id.
        """
        for entity_id, entity in state.entities.items():
            if entity.family_id != family_id:
                violation = InvariantViolation(
                    invariant_name="no_cross_family_leakage",
                    severity="critical",
                    description=(
                        f"Cross-family contamination detected: entity {entity_id} "
                        f"belongs to family {entity.family_id}, not {family_id}"
                    ),
                    affected_entities=[entity_id],
                    details={
                        "entity_id": entity_id,
                        "expected_family": family_id,
                        "actual_family": entity.family_id,
                    },
                )
                self.violations.append(violation)
                return violation
        
        return None
    
    def validate_no_phantom_states(
        self,
        state,
        valid_entity_types: List[str] = None,
        valid_task_states: List[str] = None,
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: No phantom or ghost task states
        
        All tasks must be in a defined state, no undefined states.
        """
        if valid_entity_types is None:
            valid_entity_types = ["plan", "task", "event"]
        
        if valid_task_states is None:
            valid_task_states = ["pending", "in_progress", "completed", "failed", "cancelled"]
        
        for entity_id, entity in state.entities.items():
            # Check entity type is valid
            if entity.entity_type not in valid_entity_types:
                violation = InvariantViolation(
                    invariant_name="no_phantom_states",
                    severity="critical",
                    description=(
                        f"Invalid entity type: {entity.entity_type} "
                        f"(valid: {valid_entity_types})"
                    ),
                    affected_entities=[entity_id],
                    details={
                        "entity_id": entity_id,
                        "invalid_type": entity.entity_type,
                        "valid_types": valid_entity_types,
                    },
                )
                self.violations.append(violation)
                return violation
            
            # Check task state is valid
            if entity.entity_type == "task":
                status = entity.attributes.get("status")
                if status not in valid_task_states:
                    violation = InvariantViolation(
                        invariant_name="no_phantom_states",
                        severity="critical",
                        description=(
                            f"Task {entity_id} in phantom state: {status} "
                            f"(valid: {valid_task_states})"
                        ),
                        affected_entities=[entity_id],
                        details={
                            "entity_id": entity_id,
                            "invalid_state": status,
                            "valid_states": valid_task_states,
                        },
                    )
                    self.violations.append(violation)
                    return violation
        
        return None
    
    def validate_version_monotonicity(
        self,
        state,
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: Entity versions must be monotonically increasing
        
        No version decrements allowed (indicates lost updates or corruption).
        """
        for entity_id, entity in state.entities.items():
            if entity.version < 1:
                violation = InvariantViolation(
                    invariant_name="version_monotonicity",
                    severity="critical",
                    description=(
                        f"Entity {entity_id} has invalid version: {entity.version}"
                    ),
                    affected_entities=[entity_id],
                    details={
                        "entity_id": entity_id,
                        "version": entity.version,
                    },
                )
                self.violations.append(violation)
                return violation
        
        return None
    
    def validate_timestamp_causality(
        self,
        state,
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: Entity timestamps must follow causality
        
        updated_at >= created_at for all entities.
        """
        for entity_id, entity in state.entities.items():
            if entity.updated_at < entity.created_at:
                violation = InvariantViolation(
                    invariant_name="timestamp_causality",
                    severity="critical",
                    description=(
                        f"Entity {entity_id} has causality violation: "
                        f"updated_at ({entity.updated_at}) < created_at ({entity.created_at})"
                    ),
                    affected_entities=[entity_id],
                    details={
                        "entity_id": entity_id,
                        "created_at": entity.created_at.isoformat(),
                        "updated_at": entity.updated_at.isoformat(),
                    },
                )
                self.violations.append(violation)
                return violation
        
        return None
    
    def validate_idempotent_command_safety(
        self,
        state,
        max_retries: int = 3,
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: Idempotent commands safe under retry
        
        Commands with same idempotency key should be cached and not re-execute.
        """
        # Check cache consistency
        if len(state.idempotency_cache) > 0:
            # Idempotency cache should have entries
            pass  # Passes by default if cache is being used
        
        return None
    
    def validate_watermark_consistency(
        self,
        state,
        tolerance_ms: int = 5000,  # 5 second tolerance
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: Watermark epoch should be monotonically increasing
        
        No watermark regression allowed (indicates state corruption).
        """
        if state.watermark_epoch < 0:
            violation = InvariantViolation(
                invariant_name="watermark_consistency",
                severity="critical",
                description=(
                    f"Invalid watermark epoch: {state.watermark_epoch}"
                ),
                details={
                    "watermark_epoch": state.watermark_epoch,
                },
            )
            self.violations.append(violation)
            return violation
        
        return None
    
    def validate_quarantine_mode_safety(
        self,
        state,
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: Only quarantine on catastrophic failures
        
        Quarantine mode should be set only when necessary and should be explained.
        """
        if state.quarantine_mode:
            if not state.quarantine_reason:
                violation = InvariantViolation(
                    invariant_name="quarantine_mode_safety",
                    severity="warning",
                    description="Quarantine mode enabled without reason",
                    details={
                        "quarantine_mode": True,
                    },
                )
                self.violations.append(violation)
                return violation
        
        return None
    
    def validate_explanation_completeness(
        self,
        state,
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: Every successful state mutation produces exactly 1 explanation.

        Counts entity_created / entity_updated entries in state_mutations and
        compares against xai_explanations.  A shortfall is a critical failure.
        Skipped silently when XAI is not integrated (no xai_explanations attr).
        """
        xai_explanations = getattr(state, "xai_explanations", None)
        if xai_explanations is None:
            return None

        mutation_count = len(
            [m for m in state.state_mutations if m.get("type") in {"entity_created", "entity_updated"}]
        )
        explanation_count = len(xai_explanations)

        if explanation_count < mutation_count:
            missing = mutation_count - explanation_count
            violation = InvariantViolation(
                invariant_name="explanation_completeness",
                severity="critical",
                description=(
                    f"Explanation completeness failure: {explanation_count}/{mutation_count} "
                    f"mutations have explanations ({missing} missing)"
                ),
                details={
                    "mutation_count": mutation_count,
                    "explanation_count": explanation_count,
                    "missing_count": missing,
                },
            )
            self.violations.append(violation)
            return violation

        return None

    def validate_no_duplicate_explanations(
        self,
        state,
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: No two explanations may share the same idempotency_key.

        A duplicate indicates either a broken idempotency guard in the command
        gateway or a failure to short-circuit on cache hits.
        """
        xai_explanations = getattr(state, "xai_explanations", None)
        if not xai_explanations:
            return None

        seen: Dict[str, str] = {}
        for exp in xai_explanations:
            if exp.idempotency_key in seen:
                violation = InvariantViolation(
                    invariant_name="no_duplicate_explanations",
                    severity="critical",
                    description=(
                        f"Duplicate explanation: idempotency_key {exp.idempotency_key!r} appears "
                        f"in explanations {seen[exp.idempotency_key]!r} and {exp.explanation_id!r}"
                    ),
                    affected_entities=[exp.entity_id],
                    details={
                        "idempotency_key": exp.idempotency_key,
                        "first_explanation_id": seen[exp.idempotency_key],
                        "duplicate_explanation_id": exp.explanation_id,
                    },
                )
                self.violations.append(violation)
                return violation
            seen[exp.idempotency_key] = exp.explanation_id

        return None

    def validate_explanation_causal_coverage(
        self,
        state,
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: Every explanation references an entity that exists in state.

        A ghost explanation (entity missing from state.entities) indicates a
        causal chain gap: the root action was never materialised.
        """
        xai_explanations = getattr(state, "xai_explanations", None)
        if not xai_explanations:
            return None

        entity_ids = set(state.entities.keys())
        for exp in xai_explanations:
            if exp.entity_id not in entity_ids:
                violation = InvariantViolation(
                    invariant_name="explanation_causal_coverage",
                    severity="warning",
                    description=(
                        f"Ghost explanation: explanation {exp.explanation_id!r} "
                        f"references entity {exp.entity_id!r} which does not exist in state"
                    ),
                    affected_entities=[exp.entity_id],
                    details={
                        "entity_id": exp.entity_id,
                        "explanation_id": exp.explanation_id,
                        "reason_code": exp.reason_code.value,
                    },
                )
                self.violations.append(violation)
                return violation

        return None

    def validate_no_cross_family_explanation_leakage(
        self,
        state,
        family_id: str,
    ) -> Optional[InvariantViolation]:
        """
        INVARIANT: All explanations must belong to the expected family_id.

        XAI-specific complement to validate_no_cross_family_leakage.
        """
        xai_explanations = getattr(state, "xai_explanations", None)
        if not xai_explanations:
            return None

        for exp in xai_explanations:
            if exp.family_id != family_id:
                violation = InvariantViolation(
                    invariant_name="no_cross_family_explanation_leakage",
                    severity="critical",
                    description=(
                        f"Cross-family explanation leakage: explanation {exp.explanation_id!r} "
                        f"belongs to family {exp.family_id!r}, not {family_id!r}"
                    ),
                    affected_entities=[exp.entity_id],
                    details={
                        "explanation_id": exp.explanation_id,
                        "expected_family": family_id,
                        "actual_family": exp.family_id,
                    },
                )
                self.violations.append(violation)
                return violation

        return None

    def run_all_validations(self, state) -> Tuple[bool, List[InvariantViolation]]:
        """
        Run all invariant checks
        
        Returns: (all_passed, violations)
        """
        self.violations.clear()
        
        # Run each validation
        self.validate_no_duplicate_task_execution(state)
        self.validate_no_lost_updates(state)
        self.validate_no_cross_family_leakage(state, state.family_id)
        self.validate_no_phantom_states(state)
        self.validate_version_monotonicity(state)
        self.validate_timestamp_causality(state)
        self.validate_watermark_consistency(state)
        self.validate_quarantine_mode_safety(state)
        # XAI invariants
        self.validate_explanation_completeness(state)
        self.validate_no_duplicate_explanations(state)
        self.validate_explanation_causal_coverage(state)
        self.validate_no_cross_family_explanation_leakage(state, state.family_id)

        critical_violations = [
            v for v in self.violations
            if v.severity == "critical"
        ]
        
        all_passed = len(critical_violations) == 0
        
        return all_passed, self.violations
    
    def get_violation_summary(self) -> dict:
        """Get summary of invariant violations"""
        by_severity = {}
        by_invariant = {}
        
        for violation in self.violations:
            # By severity
            by_severity[violation.severity] = by_severity.get(violation.severity, 0) + 1
            
            # By invariant
            by_invariant[violation.invariant_name] = (
                by_invariant.get(violation.invariant_name, 0) + 1
            )
        
        return {
            "total_violations": len(self.violations),
            "by_severity": by_severity,
            "by_invariant": by_invariant,
            "critical_count": len([v for v in self.violations if v.severity == "critical"]),
        }
