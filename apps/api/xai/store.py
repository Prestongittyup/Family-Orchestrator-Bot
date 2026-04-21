"""
XAI Layer — Explanation Store
================================

Append-only persistence service for ExplanationSchema records.

Design constraints
------------------
- persist() is idempotent: duplicate idempotency_key silently returns
  the already-stored explanation without raising.
- No UPDATE / DELETE operations.  This is an audit-grade append-only log.
- Query methods are pure reads with no side effects.
- All query entry points are scoped per family_id for cross-tenant safety.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy.exc import IntegrityError

from apps.api.core.database import SessionLocal
from apps.api.xai.db_model import ExplanationRecord
from apps.api.xai.schema import ExplanationSchema


class ExplanationStore:
    """
    Append-only explanation persistence.

    All public methods either write once (idempotent) or read.
    No updates or deletes exist.

    Usage::

        store = ExplanationStore()
        stored = store.persist(explanation)
        # replaying same command: store.persist(same_explanation) → same record, no duplicate
    """

    # ------------------------------------------------------------------
    # Write (append-only, idempotent)
    # ------------------------------------------------------------------

    def persist(self, explanation: ExplanationSchema) -> ExplanationSchema:
        """
        Persist an explanation.  Idempotent — safe to call on replay.

        If an explanation with the same idempotency_key already exists,
        returns the already-persisted record unchanged (no INSERT occurs).

        Parameters
        ----------
        explanation : ExplanationSchema
            Fully-populated explanation (produced by CausalMapper).

        Returns
        -------
        ExplanationSchema
            The persisted (or already-existing) explanation.
        """
        session = SessionLocal()
        try:
            # Fast-path: check existence before INSERT attempt
            existing = (
                session.query(ExplanationRecord)
                .filter(ExplanationRecord.idempotency_key == explanation.idempotency_key)
                .first()
            )
            if existing is not None:
                return self._to_schema(existing)

            record = ExplanationRecord(
                id=explanation.explanation_id,
                family_id=explanation.family_id,
                entity_type=explanation.entity_type.value,
                entity_id=explanation.entity_id,
                entity_name=explanation.entity_name,
                change_type=explanation.change_type.value,
                trigger_type=explanation.trigger_type.value,
                trigger_source_id=explanation.trigger_source_id,
                trigger_source_name=explanation.trigger_source_name,
                initiated_by=explanation.initiated_by.value,
                reason_code=explanation.reason_code.value,
                explanation_text=explanation.explanation_text,
                downstream_effects_json=json.dumps(explanation.downstream_effects),
                plan_id=explanation.plan_id,
                projection_version=explanation.projection_version,
                idempotency_key=explanation.idempotency_key,
                created_at=explanation.timestamp,
            )

            session.add(record)
            try:
                session.commit()
            except IntegrityError:
                # Race condition: another thread persisted the same key
                session.rollback()
                existing = (
                    session.query(ExplanationRecord)
                    .filter(ExplanationRecord.idempotency_key == explanation.idempotency_key)
                    .first()
                )
                if existing is not None:
                    return self._to_schema(existing)
                raise  # Unexpected — re-raise for caller to handle

            session.refresh(record)
            return self._to_schema(record)

        finally:
            session.close()

    # ------------------------------------------------------------------
    # Query (read-only, family-scoped)
    # ------------------------------------------------------------------

    def get_by_entity(
        self,
        *,
        entity_id: str,
        family_id: str,
        limit: int = 50,
    ) -> list[ExplanationSchema]:
        """
        Return explanations for a specific entity, newest first.

        Scoped to family_id for cross-tenant safety.
        """
        session = SessionLocal()
        try:
            rows = (
                session.query(ExplanationRecord)
                .filter(
                    ExplanationRecord.entity_id == entity_id,
                    ExplanationRecord.family_id == family_id,
                )
                .order_by(ExplanationRecord.created_at.desc())
                .limit(limit)
                .all()
            )
            return [self._to_schema(r) for r in rows]
        finally:
            session.close()

    def get_by_plan(
        self,
        *,
        plan_id: str,
        family_id: str,
        limit: int = 100,
    ) -> list[ExplanationSchema]:
        """
        Return all explanations for entities belonging to a plan, newest first.
        """
        session = SessionLocal()
        try:
            rows = (
                session.query(ExplanationRecord)
                .filter(
                    ExplanationRecord.plan_id == plan_id,
                    ExplanationRecord.family_id == family_id,
                )
                .order_by(ExplanationRecord.created_at.desc())
                .limit(limit)
                .all()
            )
            return [self._to_schema(r) for r in rows]
        finally:
            session.close()

    def get_by_family(
        self,
        *,
        family_id: str,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 100,
    ) -> list[ExplanationSchema]:
        """
        Return explanations for all entities in a family within an optional
        time window, newest first.
        """
        session = SessionLocal()
        try:
            q = session.query(ExplanationRecord).filter(
                ExplanationRecord.family_id == family_id
            )
            if since is not None:
                q = q.filter(ExplanationRecord.created_at >= since)
            if until is not None:
                q = q.filter(ExplanationRecord.created_at <= until)
            rows = q.order_by(ExplanationRecord.created_at.desc()).limit(limit).all()
            return [self._to_schema(r) for r in rows]
        finally:
            session.close()

    def get_recent(
        self,
        *,
        family_id: str,
        limit: int = 20,
    ) -> list[ExplanationSchema]:
        """
        Return the most recent explanations for a family.  Convenience wrapper.
        """
        return self.get_by_family(family_id=family_id, limit=limit)

    # ------------------------------------------------------------------
    # Validation helpers (used by the validation strategy)
    # ------------------------------------------------------------------

    def explanation_exists(self, idempotency_key: str) -> bool:
        """Return True if an explanation with this key has already been stored."""
        session = SessionLocal()
        try:
            return (
                session.query(ExplanationRecord.id)
                .filter(ExplanationRecord.idempotency_key == idempotency_key)
                .first()
                is not None
            )
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_schema(record: ExplanationRecord) -> ExplanationSchema:
        from apps.api.xai.schema import (
            ChangeType,
            EntityType,
            InitiatedBy,
            ReasonCode,
            TriggerType,
        )

        downstream: list[str] = []
        try:
            raw = json.loads(record.downstream_effects_json or "[]")
            if isinstance(raw, list):
                downstream = [str(x) for x in raw]
        except (json.JSONDecodeError, TypeError):
            pass

        return ExplanationSchema(
            explanation_id=record.id,
            family_id=record.family_id,
            entity_type=EntityType(record.entity_type),
            entity_id=record.entity_id,
            entity_name=record.entity_name,
            change_type=ChangeType(record.change_type),
            trigger_type=TriggerType(record.trigger_type),
            trigger_source_id=record.trigger_source_id,
            trigger_source_name=record.trigger_source_name,
            initiated_by=InitiatedBy(record.initiated_by),
            reason_code=ReasonCode(record.reason_code),
            explanation_text=record.explanation_text,
            timestamp=record.created_at,
            downstream_effects=downstream,
            plan_id=record.plan_id,
            projection_version=record.projection_version,
            idempotency_key=record.idempotency_key,
        )
