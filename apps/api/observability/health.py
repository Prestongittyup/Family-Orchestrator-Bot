"""
Health and readiness endpoints.

GET /health   → liveness probe (always fast, no I/O)
GET /ready    → readiness probe (checks DB + core subsystems)
GET /metrics  → full metrics snapshot
"""
from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from apps.api.observability.metrics import metrics

router = APIRouter(tags=["observability"])


@router.get("/health", include_in_schema=True)
async def health() -> JSONResponse:
    """Liveness probe — returns 200 if the process is alive."""
    return JSONResponse({"status": "ok"})


@router.get("/ready", include_in_schema=True)
async def ready() -> JSONResponse:
    """Readiness probe — returns 200 only when all subsystems are initialised."""
    checks: dict[str, bool] = {}

    # --- DB check ---
    try:
        from apps.api.core.database import SessionLocal
        session = SessionLocal()
        session.execute(__import__("sqlalchemy").text("SELECT 1"))
        session.close()
        checks["db"] = True
    except Exception:
        checks["db"] = False

    # --- Broadcaster check ---
    try:
        from apps.api.realtime.broadcaster import broadcaster
        checks["broadcaster"] = broadcaster is not None
    except Exception:
        checks["broadcaster"] = False

    # --- Idempotency service check ---
    try:
        from apps.api.services import idempotency_key_service  # noqa: F401
        checks["idempotency"] = True
    except Exception:
        checks["idempotency"] = False

    all_ok = all(checks.values())
    status_code = 200 if all_ok else 503
    return JSONResponse(
        {
            "status": "ready" if all_ok else "not_ready",
            "checks": checks,
        },
        status_code=status_code,
    )


@router.get("/metrics", include_in_schema=True)
async def get_metrics() -> JSONResponse:
    """Return full metrics snapshot."""
    return JSONResponse(metrics.snapshot())
