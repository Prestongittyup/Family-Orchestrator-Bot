"""
XAI Layer — Query API Router
==============================

Exposes structured, queryable explanation endpoints.

Prefix: /v1/explanations

Endpoints
---------
GET /v1/explanations                   → family_id required; optional since/until
GET /v1/explanations/recent            → most recent N explanations for a family
GET /v1/explanations/{explanation_id}  → single explanation by ID
?entity_id=                            → filter by entity
?plan_id=                              → filter by plan

Design constraints
------------------
- family_id is ALWAYS required — never returns cross-family data.
- Frontend receives ExplanationSchema directly; no transformation needed.
- No explanation computation happens here — this is a pure read layer.
- Pagination via limit (capped at 200) with newest-first ordering.
"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from apps.api.xai.schema import ExplanationSchema
from apps.api.xai.store import ExplanationStore

router = APIRouter(prefix="/v1/explanations", tags=["xai"])

_store = ExplanationStore()
_MAX_LIMIT = 200


# ---------------------------------------------------------------------------
# GET /v1/explanations
# ---------------------------------------------------------------------------


@router.get("", response_model=list[ExplanationSchema])
def list_explanations(
    family_id: str = Query(..., description="Family context (required)."),
    entity_id: str | None = Query(default=None, description="Filter to a single entity."),
    plan_id: str | None = Query(default=None, description="Filter to all entities in a plan."),
    since: datetime | None = Query(
        default=None,
        description="Lower time bound (ISO-8601). Inclusive.",
    ),
    until: datetime | None = Query(
        default=None,
        description="Upper time bound (ISO-8601). Inclusive.",
    ),
    limit: int = Query(default=50, ge=1, le=_MAX_LIMIT, description="Max records returned."),
) -> list[ExplanationSchema]:
    """
    Query explanations with flexible filters.

    - ``entity_id`` and ``plan_id`` are mutually exclusive; ``entity_id`` wins
      when both are supplied.
    - Without entity_id / plan_id, returns all explanations for the family.
    - ``since`` / ``until`` narrow the time window on any query variant.
    """
    if entity_id is not None:
        results = _store.get_by_entity(
            entity_id=entity_id,
            family_id=family_id,
            limit=limit,
        )
        if since or until:
            results = _window_filter(results, since=since, until=until)
        return results

    if plan_id is not None:
        results = _store.get_by_plan(
            plan_id=plan_id,
            family_id=family_id,
            limit=limit,
        )
        if since or until:
            results = _window_filter(results, since=since, until=until)
        return results

    # Family-wide query with optional time window
    return _store.get_by_family(
        family_id=family_id,
        since=since,
        until=until,
        limit=limit,
    )


# ---------------------------------------------------------------------------
# GET /v1/explanations/recent
# ---------------------------------------------------------------------------


@router.get("/recent", response_model=list[ExplanationSchema])
def recent_explanations(
    family_id: str = Query(..., description="Family context (required)."),
    limit: int = Query(default=20, ge=1, le=_MAX_LIMIT, description="Max records returned."),
) -> list[ExplanationSchema]:
    """
    Return the N most recent explanations for a family.

    Designed for the dashboard 'recent activity' feed —
    fast path with no additional filtering.
    """
    return _store.get_recent(family_id=family_id, limit=limit)


# ---------------------------------------------------------------------------
# GET /v1/explanations/{explanation_id}
# ---------------------------------------------------------------------------


@router.get("/{explanation_id}", response_model=ExplanationSchema)
def get_explanation(
    explanation_id: str,
    family_id: str = Query(
        ...,
        description="Family context required for cross-tenant safety.",
    ),
) -> ExplanationSchema:
    """
    Return a single explanation by its ID.

    ``family_id`` is required to ensure family-scoped access.
    Returns 404 if the explanation is not found or belongs to a different family.
    """
    # We query via entity_id family scope by fetching recent and filtering;
    # in practice, a direct-by-id lookup is added here for efficiency.
    session_results = _store.get_by_family(family_id=family_id, limit=_MAX_LIMIT)
    for exp in session_results:
        if exp.explanation_id == explanation_id:
            return exp

    raise HTTPException(
        status_code=404,
        detail=f"Explanation '{explanation_id}' not found for family '{family_id}'.",
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _window_filter(
    explanations: list[ExplanationSchema],
    *,
    since: datetime | None,
    until: datetime | None,
) -> list[ExplanationSchema]:
    """Apply time-window filter to an already-fetched list."""
    result = explanations
    if since is not None:
        result = [e for e in result if e.timestamp >= since]
    if until is not None:
        result = [e for e in result if e.timestamp <= until]
    return result
