"""
XAI Layer — Explanation Store Model (SQLAlchemy)
==================================================

Append-only persistence model for ExplanationSchema records.

Design constraints
------------------
- Append-only: no UPDATE or DELETE paths exist.
- idempotency_key has a UNIQUE constraint so replaying the same command
  silently skips the write rather than inserting a duplicate.
- downstream_effects persisted as JSON text; no relational join needed
  since the field is read-only metadata.
- Indexed on: entity_id, plan_id, family_id, created_at — matches
  the four Query API access patterns.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from apps.api.core.database import Base


class ExplanationRecord(Base):
    """
    Persisted explanation — one row per unique causal event.

    Columns mirror ExplanationSchema exactly; no transformation is
    needed on read.  JSON columns use Text to remain compatible with
    SQLite (used in development) as well as Postgres (production).
    """

    __tablename__ = "xai_explanations"

    # Primary key: deterministic UUID derived from idempotency_key.
    # Never auto-generated at write time.
    id: Mapped[str] = mapped_column(String, primary_key=True)

    # Ownership — never cross-tenant
    family_id: Mapped[str] = mapped_column(String, nullable=False)

    # Entity reference
    entity_type: Mapped[str] = mapped_column(String, nullable=False)
    entity_id: Mapped[str] = mapped_column(String, nullable=False)
    entity_name: Mapped[str] = mapped_column(String, nullable=False)
    change_type: Mapped[str] = mapped_column(String, nullable=False)

    # Causality
    trigger_type: Mapped[str] = mapped_column(String, nullable=False)
    trigger_source_id: Mapped[str | None] = mapped_column(String, nullable=True)
    trigger_source_name: Mapped[str | None] = mapped_column(String, nullable=True)
    initiated_by: Mapped[str] = mapped_column(String, nullable=False)
    reason_code: Mapped[str] = mapped_column(String, nullable=False)

    # Human-readable output
    explanation_text: Mapped[str] = mapped_column(Text, nullable=False)

    # Downstream effects (JSON array of entity_id strings)
    downstream_effects_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")

    # Linkage
    plan_id: Mapped[str | None] = mapped_column(String, nullable=True)
    projection_version: Mapped[str | None] = mapped_column(String, nullable=True)

    # Idempotency — unique constraint enforces append-only deduplication
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    inserted_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_xai_idempotency_key"),
        # Composite index for the primary query patterns
        Index("ix_xai_entity_id", "entity_id"),
        Index("ix_xai_plan_id", "plan_id"),
        Index("ix_xai_family_id_created_at", "family_id", "created_at"),
    )
