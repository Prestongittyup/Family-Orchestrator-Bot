"""
XAI Layer — Causal Mapping Engine
====================================

Maps orchestration-level transitions to ``ExplanationSchema`` objects.

Design constraints
------------------
- Deterministic and idempotent: same CausalContext → same Explanation ID and text.
- Replay-safe: the explanation_id is derived via SHA-256 from idempotency_key
  so replaying the same command never creates a second record.
- No orchestration internals are leaked:
    * ``action_id``, ``plan_id``, DAG references, lease tokens — never exposed.
    * Only product-domain language surfaces in explanation_text and reason_code.
- ``plan_id`` is intentionally allowed inside ExplanationSchema as a query
  dimension; it is NOT forwarded to explanation_text.

Usage pattern
-------------
    ctx = CausalContext(
        command_type="create_or_merge_plan",
        idempotency_key="idem-abc123",
        family_id="family-1",
        initiated_by=InitiatedBy.USER,
        entity_type=EntityType.PLAN,
        entity_id="plan-xyz",
        entity_name="Weekend Family Dinner",
        plan_id="plan-xyz",
        merged=False,
        conflict_revision=False,
        revision=1,
    )
    explanation = CausalMapper().map(ctx)
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from apps.api.xai.schema import (
    ChangeType,
    EntityType,
    ExplanationSchema,
    InitiatedBy,
    ReasonCode,
    TriggerType,
)
from apps.api.xai.templates import TemplateEngine

_ENGINE = TemplateEngine()


# ---------------------------------------------------------------------------
# CausalContext — structured orchestration signal consumed by the mapper
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CausalContext:
    """
    Structured orchestration signal, stripped of internal plumbing.

    Callers (command_gateway, event handlers) populate this and hand it
    to CausalMapper.map(). Never expose action_id, DAG, or lease fields.
    """

    # Required: routing key
    command_type: str

    # Required: idempotency anchor (drives explanation_id derivation)
    idempotency_key: str

    # Required: ownership
    family_id: str
    initiated_by: InitiatedBy

    # Required: what changed
    entity_type: EntityType
    entity_id: str
    entity_name: str

    # Optional: change metadata
    plan_id: str | None = None
    trigger_source_id: str | None = None
    trigger_source_name: str | None = None
    downstream_effects: list[str] = field(default_factory=list)
    projection_version: str | None = None
    timestamp: datetime | None = None

    # Optional: plan-specific signals
    merged: bool = False
    conflict_revision: bool = False
    revision: int = 1
    recompute_reason: str | None = None  # raw internal reason; never forwarded to text

    # Optional: task-specific signals
    task_status: str | None = None

    # Optional: event-specific signals
    event_linked_to_plan: bool = False
    event_triggered_recompute: bool = False

    # Optional: richer explanation params forwarded to template render()
    old_time: str | None = None
    new_time: str | None = None
    trigger_reason: str | None = None
    conflicting_event: str | None = None
    policy_name: str | None = None


# ---------------------------------------------------------------------------
# Routing table: command_type → (change_type, trigger_type, reason_code) fn
# ---------------------------------------------------------------------------

# Each entry is a callable that accepts CausalContext and returns
# (ChangeType, TriggerType, ReasonCode).
# Using callables (not a flat dict) so conditional signals (merged, conflict_revision)
# are handled inline without branching in the mapper core.

_MappingFn = "Callable[[CausalContext], tuple[ChangeType, TriggerType, ReasonCode]]"


def _map_create_or_merge_plan(
    ctx: CausalContext,
) -> tuple[ChangeType, TriggerType, ReasonCode]:
    if ctx.conflict_revision:
        return (
            ChangeType.REVISED,
            TriggerType.PLAN_NORMALIZATION,
            ReasonCode.EVENT_TIME_CONFLICT,
        )
    if ctx.merged:
        return (
            ChangeType.MERGED,
            TriggerType.PLAN_NORMALIZATION,
            ReasonCode.PLAN_OPTIMIZED,
        )
    return (
        ChangeType.CREATED,
        TriggerType.USER_ACTION
        if ctx.initiated_by == InitiatedBy.USER
        else TriggerType.SYSTEM_RECOMPUTE,
        ReasonCode.PLAN_CREATED,
    )


def _map_update_plan(
    ctx: CausalContext,
) -> tuple[ChangeType, TriggerType, ReasonCode]:
    if ctx.initiated_by == InitiatedBy.USER:
        return (
            ChangeType.UPDATED,
            TriggerType.USER_ACTION,
            ReasonCode.USER_UPDATED,
        )
    return (
        ChangeType.UPDATED,
        TriggerType.SYSTEM_RECOMPUTE,
        ReasonCode.PLAN_RECOMPUTED,
    )


def _map_recompute_plan(
    ctx: CausalContext,
) -> tuple[ChangeType, TriggerType, ReasonCode]:
    # Only use the event-specific code when we have a human-readable trigger name;
    # without it the template cannot render without leaking internal IDs.
    if ctx.trigger_source_id is not None and ctx.trigger_source_name is not None:
        return (
            ChangeType.RECOMPUTED,
            TriggerType.EVENT_CONFLICT,
            ReasonCode.SYSTEM_RECOMPUTE,
        )
    return (
        ChangeType.RECOMPUTED,
        TriggerType.SYSTEM_RECOMPUTE,
        ReasonCode.PLAN_RECOMPUTED,
    )


def _map_create_task(
    ctx: CausalContext,
) -> tuple[ChangeType, TriggerType, ReasonCode]:
    if ctx.initiated_by == InitiatedBy.SYSTEM:
        return (
            ChangeType.CREATED,
            TriggerType.SYSTEM_RECOMPUTE,
            ReasonCode.TASK_CREATED,
        )
    return (
        ChangeType.CREATED,
        TriggerType.USER_ACTION,
        ReasonCode.TASK_CREATED,
    )


def _map_update_task(
    ctx: CausalContext,
) -> tuple[ChangeType, TriggerType, ReasonCode]:
    if ctx.task_status == "completed":
        return (
            ChangeType.COMPLETED,
            TriggerType.USER_ACTION
            if ctx.initiated_by == InitiatedBy.USER
            else TriggerType.SYSTEM_RECOMPUTE,
            ReasonCode.TASK_COMPLETED,
        )
    if ctx.task_status == "blocked":
        return (
            ChangeType.BLOCKED,
            TriggerType.DEPENDENCY_CHANGE,
            ReasonCode.TASK_BLOCKED,
        )
    if ctx.task_status == "cancelled":
        return (
            ChangeType.CANCELLED,
            TriggerType.USER_ACTION,
            ReasonCode.TASK_DELETED,
        )
    if ctx.trigger_source_id is not None:
        return (
            ChangeType.RESCHEDULED,
            TriggerType.EVENT_CONFLICT,
            ReasonCode.TASK_RESCHEDULED,
        )
    return (
        ChangeType.UPDATED,
        TriggerType.USER_ACTION
        if ctx.initiated_by == InitiatedBy.USER
        else TriggerType.SYSTEM_RECOMPUTE,
        ReasonCode.TASK_COMPLETED,  # fallback for generic task update
    )


def _map_create_event(
    ctx: CausalContext,
) -> tuple[ChangeType, TriggerType, ReasonCode]:
    return (
        ChangeType.CREATED,
        TriggerType.USER_ACTION
        if ctx.initiated_by == InitiatedBy.USER
        else TriggerType.SYSTEM_RECOMPUTE,
        ReasonCode.EVENT_CREATED,
    )


def _map_update_event(
    ctx: CausalContext,
) -> tuple[ChangeType, TriggerType, ReasonCode]:
    if ctx.event_triggered_recompute:
        return (
            ChangeType.UPDATED,
            TriggerType.SCHEDULE_ADJUSTMENT,
            ReasonCode.SYSTEM_RECOMPUTE,
        )
    return (
        ChangeType.UPDATED,
        TriggerType.USER_ACTION
        if ctx.initiated_by == InitiatedBy.USER
        else TriggerType.SYSTEM_RECOMPUTE,
        ReasonCode.EVENT_UPDATED,
    )


def _map_link_event_to_plan(
    ctx: CausalContext,
) -> tuple[ChangeType, TriggerType, ReasonCode]:
    return (
        ChangeType.LINKED,
        TriggerType.USER_ACTION
        if ctx.initiated_by == InitiatedBy.USER
        else TriggerType.PLAN_NORMALIZATION,
        ReasonCode.EVENT_UPDATED,
    )


def _map_manual_override(
    ctx: CausalContext,
) -> tuple[ChangeType, TriggerType, ReasonCode]:
    return (
        ChangeType.UPDATED,
        TriggerType.USER_ACTION,
        ReasonCode.USER_OVERRIDE,
    )


# Registry keyed on command_type strings (matches command_gateway command_type values)
_COMMAND_MAPPING: dict[
    str,
    Any,  # Callable[[CausalContext], tuple[ChangeType, TriggerType, ReasonCode]]
] = {
    "create_or_merge_plan": _map_create_or_merge_plan,
    "update_plan": _map_update_plan,
    "recompute_plan": _map_recompute_plan,
    "create_task": _map_create_task,
    "update_task": _map_update_task,
    "create_event": _map_create_event,
    "update_event": _map_update_event,
    "link_event_to_plan": _map_link_event_to_plan,
    "manual_override": _map_manual_override,
}


# ---------------------------------------------------------------------------
# CausalMapper — public interface
# ---------------------------------------------------------------------------


class CausalMapper:
    """
    Deterministic mapper: CausalContext → ExplanationSchema.

    Rules
    -----
    - explanation_id is a stable UUID5 derived from idempotency_key; replay
      of the same command produces an identical explanation_id so the store's
      idempotency guard silently deduplicates.
    - explanation_text is always generated by TemplateEngine.render().
    - Unknown command_type values map to a SYSTEM_OPTIMIZATION fallback rather
      than raising, so new command types never crash the explanation pipeline.
      A warning is emitted so the gap is visible.
    """

    def map(self, ctx: CausalContext) -> ExplanationSchema:
        """
        Produce a fully-populated ExplanationSchema from a CausalContext.

        Parameters
        ----------
        ctx : CausalContext
            Structured orchestration signal.

        Returns
        -------
        ExplanationSchema
            Serialisable explanation, ready for persistence and API delivery.
        """
        change_type, trigger_type, reason_code = self._resolve(ctx)

        explanation_id = self._derive_id(ctx.idempotency_key)
        timestamp = ctx.timestamp or datetime.utcnow()

        explanation_text = _ENGINE.render(
            reason_code=reason_code,
            entity_name=ctx.entity_name,
            trigger_name=ctx.trigger_source_name,
            plan_name=self._plan_display_name(ctx, reason_code),
            old_time=ctx.old_time,
            new_time=ctx.new_time,
            trigger_reason=ctx.trigger_reason,
            conflicting_event=ctx.conflicting_event or ctx.trigger_source_name,
            dependency_name=ctx.trigger_source_name,
            policy_name=ctx.policy_name,
        )

        return ExplanationSchema(
            explanation_id=explanation_id,
            family_id=ctx.family_id,
            entity_type=ctx.entity_type,
            entity_id=ctx.entity_id,
            entity_name=ctx.entity_name,
            change_type=change_type,
            trigger_type=trigger_type,
            trigger_source_id=ctx.trigger_source_id,
            trigger_source_name=ctx.trigger_source_name,
            initiated_by=ctx.initiated_by,
            reason_code=reason_code,
            explanation_text=explanation_text,
            timestamp=timestamp,
            downstream_effects=list(ctx.downstream_effects),
            plan_id=ctx.plan_id,
            projection_version=ctx.projection_version,
            idempotency_key=ctx.idempotency_key,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve(
        self, ctx: CausalContext
    ) -> tuple[ChangeType, TriggerType, ReasonCode]:
        fn = _COMMAND_MAPPING.get(ctx.command_type)
        if fn is None:
            # Unknown command — safe fallback; does not raise
            import warnings

            warnings.warn(
                f"CausalMapper: unknown command_type {ctx.command_type!r}. "
                "Falling back to SYSTEM_OPTIMIZATION.",
                UserWarning,
                stacklevel=3,
            )
            return (
                ChangeType.UPDATED,
                TriggerType.SYSTEM_RECOMPUTE,
                ReasonCode.SYSTEM_OPTIMIZATION,
            )
        return fn(ctx)  # type: ignore[operator]

    @staticmethod
    def _derive_id(idempotency_key: str) -> str:
        """
        Derive a stable UUID5 from the idempotency_key.

        The namespace is fixed so the output is fully deterministic
        across processes and restarts.
        """
        namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # RFC 4122 URL namespace
        return str(uuid.uuid5(namespace, idempotency_key))

    @staticmethod
    def _plan_display_name(ctx: CausalContext, reason_code: ReasonCode) -> str | None:
        """
        Return a safe display name for the {plan_name} template placeholder.

        Rules (in priority order):
        1. PLAN entity — use entity_name directly (it IS the plan).
        2. TASK / EVENT with trigger_source_name — use trigger_source_name when
           it refers to the parent plan (e.g. EVENT_LINKED_TO_PLAN, TASK_CREATED).
        3. Codes that reference a plan in their template but ctx has no name —
           return a generic fallback string so the template can always render
           without leaking internal IDs.
        4. Codes whose templates don't use {plan_name} — return None (safe).
        """
        _NEEDS_PLAN_NAME: frozenset[ReasonCode] = frozenset(
            {
                ReasonCode.TASK_CREATED,
            }
        )

        if ctx.entity_type == EntityType.PLAN:
            return ctx.entity_name

        if reason_code in _NEEDS_PLAN_NAME:
            # Use trigger_source_name if it refers to the plan; fall back to
            # a generic label so the template always renders safely.
            return ctx.trigger_source_name or "this plan"

        return None
