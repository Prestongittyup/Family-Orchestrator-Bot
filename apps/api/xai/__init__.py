"""
XAI Layer — Explainability Engine
==================================

Translates orchestration-level causality into deterministic, user-facing
explanations for any Plan / Task / Event change.

NOT logging. NOT UI formatting.
A structured, queryable explanation system.

Public surface:
  - ExplanationSchema   — canonical explanation shape
  - ReasonCode          — strict controlled vocabulary
  - CausalMapper        — deterministic action/event → explanation mapping
  - ExplanationStore    — append-only persistence + idempotency guard
  - router              — Query API (FastAPI router, prefix=/v1/explanations)
"""
from apps.api.xai.schema import (
    ChangeType,
    EntityType,
    ExplanationSchema,
    InitiatedBy,
    ReasonCode,
    TriggerType,
)

__all__ = [
    "ChangeType",
    "EntityType",
    "ExplanationSchema",
    "InitiatedBy",
    "ReasonCode",
    "TriggerType",
]
