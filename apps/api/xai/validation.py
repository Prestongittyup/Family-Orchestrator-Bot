"""
XAI Layer — Validation Strategy
==================================

Deterministic replay and completeness validator for the explanation pipeline.

Validates that:
1. All state transitions produce an explanation (completeness).
2. Replay determinism — same CausalContext always produces identical output.
3. No duplicate explanations for the same originating event.
4. Explanation chain completeness — downstream effects are all covered.
5. Family isolation — no explanation references a foreign family_id.

Usage::

    validator = XAIValidator()
    report = validator.validate_replay(contexts, family_id="family-1")
    assert report.all_passed, report.failures
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from apps.api.xai.causal_mapper import CausalContext, CausalMapper
from apps.api.xai.schema import ExplanationSchema
from apps.api.xai.store import ExplanationStore
from apps.api.xai.templates import TemplateEngine


@dataclass
class ValidationFailure:
    rule: str
    severity: str  # "critical" | "warning"
    description: str
    entity_id: str | None = None
    idempotency_key: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationReport:
    family_id: str
    checked_at: datetime
    total_checked: int
    failures: list[ValidationFailure] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(f.severity != "critical" for f in self.failures)

    @property
    def critical_count(self) -> int:
        return sum(1 for f in self.failures if f.severity == "critical")

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.failures if f.severity == "warning")


class XAIValidator:
    """
    Validation strategy for the XAI explanation pipeline.

    All checks are deterministic and stateless — they operate on inputs
    and return structured ValidationReport objects.  No side effects.
    """

    def __init__(
        self,
        store: ExplanationStore | None = None,
        mapper: CausalMapper | None = None,
    ) -> None:
        self._store = store or ExplanationStore()
        self._mapper = mapper or CausalMapper()
        self._template_engine = TemplateEngine()

    # ------------------------------------------------------------------
    # 1. Replay determinism check
    # ------------------------------------------------------------------

    def validate_replay(
        self,
        contexts: list[CausalContext],
        family_id: str,
        num_replays: int = 2,
    ) -> ValidationReport:
        """
        Verify that mapping the same CausalContext N times produces stable output.

        Checks:
        - explanation_id remains identical across all replays.
        - explanation_text remains identical across all replays.
        - reason_code remains identical.
        """
        failures: list[ValidationFailure] = []

        for ctx in contexts:
            first_result = self._mapper.map(ctx)
            for replay_n in range(1, num_replays):
                replayed = self._mapper.map(ctx)

                if replayed.explanation_id != first_result.explanation_id:
                    failures.append(
                        ValidationFailure(
                            rule="REPLAY_DETERMINISM_ID",
                            severity="critical",
                            description=(
                                f"explanation_id diverged on replay {replay_n}: "
                                f"{first_result.explanation_id!r} → {replayed.explanation_id!r}"
                            ),
                            entity_id=ctx.entity_id,
                            idempotency_key=ctx.idempotency_key,
                        )
                    )

                if replayed.explanation_text != first_result.explanation_text:
                    failures.append(
                        ValidationFailure(
                            rule="REPLAY_DETERMINISM_TEXT",
                            severity="critical",
                            description=(
                                f"explanation_text diverged on replay {replay_n}."
                            ),
                            entity_id=ctx.entity_id,
                            idempotency_key=ctx.idempotency_key,
                            details={
                                "first": first_result.explanation_text,
                                "replay": replayed.explanation_text,
                            },
                        )
                    )

                if replayed.reason_code != first_result.reason_code:
                    failures.append(
                        ValidationFailure(
                            rule="REPLAY_DETERMINISM_REASON_CODE",
                            severity="critical",
                            description=(
                                f"reason_code diverged on replay {replay_n}: "
                                f"{first_result.reason_code.value!r} → "
                                f"{replayed.reason_code.value!r}"
                            ),
                            entity_id=ctx.entity_id,
                            idempotency_key=ctx.idempotency_key,
                        )
                    )

        return ValidationReport(
            family_id=family_id,
            checked_at=datetime.utcnow(),
            total_checked=len(contexts) * num_replays,
            failures=failures,
        )

    # ------------------------------------------------------------------
    # 2. Idempotency (no duplicate explanations)
    # ------------------------------------------------------------------

    def validate_no_duplicates(
        self,
        explanations: list[ExplanationSchema],
        family_id: str,
    ) -> ValidationReport:
        """
        Check that no two explanations share the same idempotency_key or
        explanation_id within a family.
        """
        failures: list[ValidationFailure] = []

        seen_idem: dict[str, str] = {}
        seen_id: dict[str, str] = {}

        for exp in explanations:
            if exp.idempotency_key in seen_idem:
                failures.append(
                    ValidationFailure(
                        rule="NO_DUPLICATE_IDEMPOTENCY_KEY",
                        severity="critical",
                        description=(
                            f"Duplicate idempotency_key {exp.idempotency_key!r} found "
                            f"in explanations {seen_idem[exp.idempotency_key]!r} and "
                            f"{exp.explanation_id!r}."
                        ),
                        entity_id=exp.entity_id,
                        idempotency_key=exp.idempotency_key,
                    )
                )
            else:
                seen_idem[exp.idempotency_key] = exp.explanation_id

            if exp.explanation_id in seen_id:
                failures.append(
                    ValidationFailure(
                        rule="NO_DUPLICATE_EXPLANATION_ID",
                        severity="critical",
                        description=(
                            f"Duplicate explanation_id {exp.explanation_id!r}."
                        ),
                        entity_id=exp.entity_id,
                        idempotency_key=exp.idempotency_key,
                    )
                )
            else:
                seen_id[exp.explanation_id] = exp.idempotency_key

        return ValidationReport(
            family_id=family_id,
            checked_at=datetime.utcnow(),
            total_checked=len(explanations),
            failures=failures,
        )

    # ------------------------------------------------------------------
    # 3. Family isolation (no cross-tenant data)
    # ------------------------------------------------------------------

    def validate_family_isolation(
        self,
        explanations: list[ExplanationSchema],
        expected_family_id: str,
    ) -> ValidationReport:
        """
        Verify every explanation belongs to the expected family.
        """
        failures: list[ValidationFailure] = []

        for exp in explanations:
            if exp.family_id != expected_family_id:
                failures.append(
                    ValidationFailure(
                        rule="FAMILY_ISOLATION",
                        severity="critical",
                        description=(
                            f"Explanation {exp.explanation_id!r} has family_id "
                            f"{exp.family_id!r} but expected {expected_family_id!r}."
                        ),
                        entity_id=exp.entity_id,
                        idempotency_key=exp.idempotency_key,
                    )
                )

        return ValidationReport(
            family_id=expected_family_id,
            checked_at=datetime.utcnow(),
            total_checked=len(explanations),
            failures=failures,
        )

    # ------------------------------------------------------------------
    # 4. Completeness check (all contexts produced an explanation)
    # ------------------------------------------------------------------

    def validate_completeness(
        self,
        contexts: list[CausalContext],
        stored_explanations: list[ExplanationSchema],
        family_id: str,
    ) -> ValidationReport:
        """
        Verify that every CausalContext has a corresponding persisted explanation.

        Missing explanations are reported as CRITICAL failures.
        """
        failures: list[ValidationFailure] = []

        stored_keys = {exp.idempotency_key for exp in stored_explanations}

        for ctx in contexts:
            expected_key = ctx.idempotency_key
            if expected_key not in stored_keys:
                failures.append(
                    ValidationFailure(
                        rule="EXPLANATION_COMPLETENESS",
                        severity="critical",
                        description=(
                            f"No explanation found for command "
                            f"{ctx.command_type!r} / entity {ctx.entity_id!r}."
                        ),
                        entity_id=ctx.entity_id,
                        idempotency_key=expected_key,
                        details={"command_type": ctx.command_type},
                    )
                )

        return ValidationReport(
            family_id=family_id,
            checked_at=datetime.utcnow(),
            total_checked=len(contexts),
            failures=failures,
        )

    # ------------------------------------------------------------------
    # 5. Downstream effect chain completeness
    # ------------------------------------------------------------------

    def validate_downstream_coverage(
        self,
        explanations: list[ExplanationSchema],
        family_id: str,
    ) -> ValidationReport:
        """
        Verify that every entity_id referenced in any explanation's
        downstream_effects also has its own explanation in the set.

        This catches broken causal chains where an effect has no root explanation.
        """
        failures: list[ValidationFailure] = []

        covered_entity_ids = {exp.entity_id for exp in explanations}

        for exp in explanations:
            for downstream_id in exp.downstream_effects:
                if downstream_id not in covered_entity_ids:
                    failures.append(
                        ValidationFailure(
                            rule="DOWNSTREAM_COVERAGE",
                            severity="warning",
                            description=(
                                f"Entity {downstream_id!r} appears as a downstream "
                                f"effect of explanation {exp.explanation_id!r} but "
                                f"has no explanation of its own."
                            ),
                            entity_id=downstream_id,
                            details={"root_explanation_id": exp.explanation_id},
                        )
                    )

        return ValidationReport(
            family_id=family_id,
            checked_at=datetime.utcnow(),
            total_checked=len(explanations),
            failures=failures,
        )

    # ------------------------------------------------------------------
    # Composite: run all checks
    # ------------------------------------------------------------------

    def run_all(
        self,
        contexts: list[CausalContext],
        explanations: list[ExplanationSchema],
        family_id: str,
    ) -> ValidationReport:
        """
        Run all five validation rules and merge their failures into one report.
        """
        reports = [
            self.validate_replay(contexts, family_id),
            self.validate_no_duplicates(explanations, family_id),
            self.validate_family_isolation(explanations, family_id),
            self.validate_completeness(contexts, explanations, family_id),
            self.validate_downstream_coverage(explanations, family_id),
        ]

        all_failures: list[ValidationFailure] = []
        total_checked = 0
        for r in reports:
            all_failures.extend(r.failures)
            total_checked += r.total_checked

        return ValidationReport(
            family_id=family_id,
            checked_at=datetime.utcnow(),
            total_checked=total_checked,
            failures=all_failures,
        )
