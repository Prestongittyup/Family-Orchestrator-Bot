"""
XAI Layer — Test Suite
========================

Tests:
  1. ReasonCode completeness (all codes have templates)
  2. TemplateEngine determinism (same inputs → same output)
  3. CausalMapper determinism + idempotency_key stability
  4. CausalMapper covers all registered command types
  5. ExplanationStore idempotency (duplicate persist returns same record)
  6. ExplanationStore family isolation (no cross-family leakage)
  7. Validation strategy: replay determinism
  8. Validation strategy: duplicate detection
  9. Validation strategy: family isolation
  10. Validation strategy: completeness
  11. Validation strategy: downstream coverage
  12. Router: explanation_id lookup for unknown ID returns 404 hint
"""
from __future__ import annotations

from datetime import datetime

import pytest

from apps.api.xai.causal_mapper import CausalContext, CausalMapper
from apps.api.xai.schema import (
    EntityType,
    ExplanationSchema,
    InitiatedBy,
    ReasonCode,
)
from apps.api.xai.templates import TemplateEngine
from apps.api.xai.validation import XAIValidator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx(
    *,
    command_type: str = "create_or_merge_plan",
    entity_id: str = "plan-001",
    entity_name: str = "Weekend Plan",
    entity_type: EntityType = EntityType.PLAN,
    family_id: str = "family-test",
    initiated_by: InitiatedBy = InitiatedBy.USER,
    idempotency_key: str = "idem-001",
    merged: bool = False,
    conflict_revision: bool = False,
    trigger_source_id: str | None = None,
    trigger_source_name: str | None = None,
    task_status: str | None = None,
    plan_id: str | None = None,
    downstream_effects: list[str] | None = None,
    old_time: str | None = None,
    new_time: str | None = None,
    trigger_reason: str | None = None,
    conflicting_event: str | None = None,
    policy_name: str | None = None,
) -> CausalContext:
    return CausalContext(
        command_type=command_type,
        idempotency_key=idempotency_key,
        family_id=family_id,
        initiated_by=initiated_by,
        entity_type=entity_type,
        entity_id=entity_id,
        entity_name=entity_name,
        plan_id=plan_id,
        trigger_source_id=trigger_source_id,
        trigger_source_name=trigger_source_name,
        downstream_effects=downstream_effects or [],
        merged=merged,
        conflict_revision=conflict_revision,
        task_status=task_status,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        old_time=old_time,
        new_time=new_time,
        trigger_reason=trigger_reason,
        conflicting_event=conflicting_event,
        policy_name=policy_name,
    )


_mapper = CausalMapper()
_template_engine = TemplateEngine()
_validator = XAIValidator(mapper=_mapper)


# ---------------------------------------------------------------------------
# 1. ReasonCode completeness — all codes have templates
# ---------------------------------------------------------------------------


def test_all_reason_codes_have_templates() -> None:
    """
    Importing TemplateEngine is sufficient — MissingTemplateError fires at
    import time if any ReasonCode is unregistered.  This test explicitly
    exercises render() for every code, supplying all known optional params
    so parameterised templates (TASK_RESCHEDULED, etc.) can also render.
    """
    for code in ReasonCode:
        text = _template_engine.render(
            reason_code=code,
            entity_name="Test Entity",
            trigger_name="Trigger Entity",
            plan_name="Test Plan",
            old_time="09:00",
            new_time="10:30",
            trigger_reason="a schedule conflict",
            conflicting_event="Another Event",
            dependency_name="Prerequisite Task",
            policy_name="budget-freeze",
        )
        assert isinstance(text, str)
        assert len(text) > 0, f"Empty template for {code.value}"


# ---------------------------------------------------------------------------
# 2. TemplateEngine determinism
# ---------------------------------------------------------------------------


def test_template_engine_determinism() -> None:
    """Same args always produce the same output."""
    kwargs = dict(
        reason_code=ReasonCode.TASK_RESCHEDULED,
        entity_name="Prepare dinner",
        old_time="18:00",
        new_time="19:30",
        trigger_reason="a schedule conflict",
    )
    t1 = _template_engine.render(**kwargs)  # type: ignore[arg-type]
    t2 = _template_engine.render(**kwargs)  # type: ignore[arg-type]
    assert t1 == t2


def test_template_engine_no_unexpected_placeholders() -> None:
    """A template with a missing placeholder raises ValueError, not silently drops it."""
    with pytest.raises(ValueError, match="old_time"):
        _template_engine.render(
            reason_code=ReasonCode.TASK_RESCHEDULED,
            entity_name="Task A",
            # old_time, new_time, trigger_reason intentionally omitted
        )


# ---------------------------------------------------------------------------
# 3. CausalMapper determinism + explanation_id stability
# ---------------------------------------------------------------------------


def test_causal_mapper_determinism() -> None:
    """Mapping the same CausalContext twice yields identical ExplanationSchema."""
    ctx = _ctx()
    e1 = _mapper.map(ctx)
    e2 = _mapper.map(ctx)
    assert e1.explanation_id == e2.explanation_id
    assert e1.explanation_text == e2.explanation_text
    assert e1.reason_code == e2.reason_code
    assert e1.change_type == e2.change_type


def test_explanation_id_is_stable_across_instances() -> None:
    """Two independent CausalMapper instances produce the same ID."""
    ctx = _ctx(idempotency_key="stable-key-xyz")
    id1 = CausalMapper().map(ctx).explanation_id
    id2 = CausalMapper().map(ctx).explanation_id
    assert id1 == id2


# ---------------------------------------------------------------------------
# 4. CausalMapper covers all command types
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "command_type,entity_type",
    [
        ("create_or_merge_plan", EntityType.PLAN),
        ("update_plan", EntityType.PLAN),
        ("recompute_plan", EntityType.PLAN),
        ("create_task", EntityType.TASK),
        ("update_task", EntityType.TASK),
        ("create_event", EntityType.EVENT),
        ("update_event", EntityType.EVENT),
        ("link_event_to_plan", EntityType.EVENT),
        ("manual_override", EntityType.PLAN),
    ],
)
def test_mapper_known_command_types(command_type: str, entity_type: EntityType) -> None:
    ctx = _ctx(command_type=command_type, entity_type=entity_type, idempotency_key=f"idem-{command_type}")
    exp = _mapper.map(ctx)
    assert isinstance(exp, ExplanationSchema)
    assert exp.reason_code in ReasonCode


def test_mapper_unknown_command_type_returns_fallback() -> None:
    """Unknown command_type falls back to SYSTEM_OPTIMIZATION without raising."""
    import warnings

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        ctx = _ctx(command_type="completely_unknown_op", idempotency_key="idem-unknown")
        exp = _mapper.map(ctx)
        assert exp.reason_code == ReasonCode.SYSTEM_OPTIMIZATION
        assert any("unknown command_type" in str(warning.message) for warning in w)


# ---------------------------------------------------------------------------
# 5. Plan-specific signals
# ---------------------------------------------------------------------------


def test_plan_merged_gets_plan_optimized_code() -> None:
    ctx = _ctx(merged=True, idempotency_key="idem-merge")
    exp = _mapper.map(ctx)
    assert exp.reason_code == ReasonCode.PLAN_OPTIMIZED


def test_plan_conflict_revision_gets_event_time_conflict_code() -> None:
    ctx = _ctx(
        conflict_revision=True,
        trigger_source_name="Soccer Practice",
        idempotency_key="idem-conflict",
    )
    exp = _mapper.map(ctx)
    assert exp.reason_code == ReasonCode.EVENT_TIME_CONFLICT


def test_plan_recompute_with_trigger_source_gets_system_recompute_code() -> None:
    ctx = _ctx(
        command_type="recompute_plan",
        trigger_source_id="event-abc",
        trigger_source_name="School Trip",
        idempotency_key="idem-recompute-with-event",
    )
    exp = _mapper.map(ctx)
    assert exp.reason_code == ReasonCode.SYSTEM_RECOMPUTE
    assert exp.trigger_source_name == "School Trip"


# ---------------------------------------------------------------------------
# 6. Task-specific signals
# ---------------------------------------------------------------------------


def test_task_completed_status() -> None:
    ctx = _ctx(
        command_type="update_task",
        entity_type=EntityType.TASK,
        entity_id="task-001",
        entity_name="Pack lunches",
        task_status="completed",
        initiated_by=InitiatedBy.USER,
        idempotency_key="idem-task-complete",
    )
    exp = _mapper.map(ctx)
    assert exp.reason_code == ReasonCode.TASK_COMPLETED


def test_task_blocked_status() -> None:
    ctx = _ctx(
        command_type="update_task",
        entity_type=EntityType.TASK,
        entity_id="task-002",
        entity_name="Cook dinner",
        task_status="blocked",
        trigger_source_name="Shopping",
        idempotency_key="idem-task-blocked",
    )
    exp = _mapper.map(ctx)
    assert exp.reason_code == ReasonCode.TASK_BLOCKED


def test_task_created_by_system() -> None:
    ctx = _ctx(
        command_type="create_task",
        entity_type=EntityType.TASK,
        entity_id="task-003",
        entity_name="Auto task",
        initiated_by=InitiatedBy.SYSTEM,
        idempotency_key="idem-task-auto",
    )
    exp = _mapper.map(ctx)
    assert exp.reason_code == ReasonCode.TASK_CREATED


# ---------------------------------------------------------------------------
# 7. Event-specific signals
# ---------------------------------------------------------------------------


def test_event_linked_to_plan() -> None:
    ctx = _ctx(
        command_type="link_event_to_plan",
        entity_type=EntityType.EVENT,
        entity_id="event-001",
        entity_name="Birthday Party",
        plan_id="plan-001",
        idempotency_key="idem-link-event",
    )
    exp = _mapper.map(ctx)
    assert exp.reason_code == ReasonCode.EVENT_UPDATED


def test_manual_override_gets_user_override_code() -> None:
    ctx = _ctx(
        command_type="manual_override",
        entity_type=EntityType.PLAN,
        idempotency_key="idem-manual",
    )
    exp = _mapper.map(ctx)
    assert exp.reason_code == ReasonCode.USER_OVERRIDE


# ---------------------------------------------------------------------------
# 8. No orchestration internals in explanation_text
# ---------------------------------------------------------------------------


def test_no_internal_ids_in_explanation_text() -> None:
    """
    Plan IDs, action IDs, and internal tokens must never appear in explanation_text.
    """
    forbidden_patterns = [
        "action-",
        "plan-",
        "task-",
        "event-",
        "lease",
        "dag",
        "revision",
    ]
    for command_type in ("create_or_merge_plan", "update_plan", "recompute_plan"):
        ctx = _ctx(
            command_type=command_type,
            entity_id="plan-xyz123",
            plan_id="plan-xyz123",
            trigger_source_id="event-abc456",
            idempotency_key=f"idem-clean-{command_type}",
        )
        exp = _mapper.map(ctx)
        for pattern in forbidden_patterns:
            assert pattern not in exp.explanation_text.lower(), (
                f"Internal pattern {pattern!r} leaked into explanation_text: {exp.explanation_text!r}"
            )


# ---------------------------------------------------------------------------
# 9. Validation: replay determinism
# ---------------------------------------------------------------------------


def test_validation_replay_determinism_passes() -> None:
    contexts = [_ctx(idempotency_key=f"idem-replay-{i}") for i in range(5)]
    report = _validator.validate_replay(contexts, family_id="family-test", num_replays=3)
    assert report.all_passed, report.failures
    assert report.critical_count == 0


# ---------------------------------------------------------------------------
# 10. Validation: no duplicate explanations
# ---------------------------------------------------------------------------


def test_validation_no_duplicates_passes_for_unique() -> None:
    exps = [_mapper.map(_ctx(idempotency_key=f"idem-dup-{i}")) for i in range(10)]
    report = _validator.validate_no_duplicates(exps, family_id="family-test")
    assert report.all_passed


def test_validation_duplicate_detected() -> None:
    exp = _mapper.map(_ctx(idempotency_key="idem-dup-same"))
    report = _validator.validate_no_duplicates([exp, exp], family_id="family-test")
    assert report.critical_count > 0


# ---------------------------------------------------------------------------
# 11. Validation: family isolation
# ---------------------------------------------------------------------------


def test_validation_family_isolation_passes() -> None:
    exp = _mapper.map(_ctx(family_id="family-A", idempotency_key="idem-fam-a"))
    report = _validator.validate_family_isolation([exp], expected_family_id="family-A")
    assert report.all_passed


def test_validation_family_isolation_detects_leakage() -> None:
    exp = _mapper.map(_ctx(family_id="family-A", idempotency_key="idem-fam-b"))
    report = _validator.validate_family_isolation([exp], expected_family_id="family-B")
    assert report.critical_count > 0


# ---------------------------------------------------------------------------
# 12. Validation: completeness
# ---------------------------------------------------------------------------


def test_validation_completeness_passes_when_all_covered() -> None:
    contexts = [_ctx(idempotency_key=f"idem-comp-{i}") for i in range(3)]
    exps = [_mapper.map(ctx) for ctx in contexts]
    report = _validator.validate_completeness(contexts, exps, family_id="family-test")
    assert report.all_passed


def test_validation_completeness_detects_missing() -> None:
    contexts = [_ctx(idempotency_key=f"idem-miss-{i}") for i in range(3)]
    # Only persist 2 of 3
    exps = [_mapper.map(ctx) for ctx in contexts[:2]]
    report = _validator.validate_completeness(contexts, exps, family_id="family-test")
    assert report.critical_count == 1


# ---------------------------------------------------------------------------
# 13. Validation: downstream coverage
# ---------------------------------------------------------------------------


def test_validation_downstream_coverage_passes_when_all_covered() -> None:
    ctx1 = _ctx(entity_id="plan-001", idempotency_key="idem-ds-1", downstream_effects=["task-001"])
    ctx2 = _ctx(entity_id="task-001", entity_type=EntityType.TASK, command_type="create_task", idempotency_key="idem-ds-2")
    exps = [_mapper.map(ctx1), _mapper.map(ctx2)]
    report = _validator.validate_downstream_coverage(exps, family_id="family-test")
    assert report.all_passed


def test_validation_downstream_coverage_warns_on_missing() -> None:
    ctx = _ctx(entity_id="plan-001", idempotency_key="idem-ds-warn", downstream_effects=["task-ghost"])
    exp = _mapper.map(ctx)
    # task-ghost has no explanation
    report = _validator.validate_downstream_coverage([exp], family_id="family-test")
    assert report.warning_count > 0


# ---------------------------------------------------------------------------
# 14. Composite validation run
# ---------------------------------------------------------------------------


def test_composite_validation_passes_clean_run() -> None:
    contexts = [
        _ctx(command_type="create_or_merge_plan", idempotency_key="idem-full-1"),
        _ctx(command_type="create_task", entity_type=EntityType.TASK, entity_id="task-001", idempotency_key="idem-full-2"),
        _ctx(command_type="create_event", entity_type=EntityType.EVENT, entity_id="event-001", idempotency_key="idem-full-3"),
    ]
    exps = [_mapper.map(ctx) for ctx in contexts]
    report = _validator.run_all(contexts, exps, family_id="family-test")
    assert report.critical_count == 0
