from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import traceback

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from apps.api.core.database import Base, DATABASE_URL, engine
from apps.api.core.asgi_admission import AdmissionGateASGI
from apps.api.core.auth_middleware import install_auth_middleware
from apps.api.core.backpressure_middleware import install_request_backpressure_middleware
from apps.api.core.idempotency_middleware import install_idempotency_middleware
from apps.api.endpoints.brief_endpoint import router as brief_router
from apps.api.endpoints.evaluation_router import router as evaluation_router
from apps.api.endpoints.integrations_router import router as integrations_router
from apps.api.endpoints.integrations_router import ui_router
from apps.api.endpoints.ui_bootstrap_router import router as ui_bootstrap_router
from apps.api.endpoints.operational_router import router as operational_router
from apps.api.endpoints.identity_router import router as identity_router
from apps.api.endpoints.calendar_router import router as calendar_router
from apps.api.endpoints.auth_router import router as auth_router
from apps.api.endpoints.realtime_router import router as realtime_router
from apps.api.hpal import router as hpal_router
from apps.api.endpoints.calendar_router import router as calendar_router
from apps.api.xai.router import router as xai_router
from insights.insight_router import router as insights_router
from policy_engine.policy_router import router as policy_router
from apps.api.schemas.event import SystemEvent
from apps.api.services.canonical_event_adapter import CanonicalEventAdapter
from apps.api.services.canonical_event_router import canonical_event_router
from apps.api.observability.health import router as health_router
from apps.api.observability.logging import log_error
from apps.api.observability.metrics import metrics
from apps.api.observability.safety import router as safety_router
from apps.api.endpoints.system_router import router as system_router
from apps.api.core.boot_diagnostics import assert_boot_invariants
from apps.api.runtime.loop_tracing import trace_loop_context
from apps.api.observability.execution_trace import trace_function


def _sanitize_db_url(url: str) -> str:
    if "://" not in url:
        return url
    scheme, rest = url.split("://", 1)
    if "@" in rest:
        creds, tail = rest.split("@", 1)
        if ":" in creds:
            user, _secret = creds.split(":", 1)
            return f"{scheme}://{user}:***@{tail}"
    return url


def _boot_config_hash() -> str:
    tracked = {
        "AUTH_TOKEN_ISSUER": os.getenv("AUTH_TOKEN_ISSUER", ""),
        "AUTH_ACCESS_MINUTES": os.getenv("AUTH_ACCESS_MINUTES", ""),
        "AUTH_REFRESH_DAYS": os.getenv("AUTH_REFRESH_DAYS", ""),
        "REDIS_URL": os.getenv("REDIS_URL", ""),
        "DATABASE_URL": DATABASE_URL,
    }
    payload = json.dumps(tracked, sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _active_router_list(app: FastAPI) -> list[dict[str, object]]:
    routers: list[dict[str, object]] = []
    for route in app.routes:
        path = getattr(route, "path", None)
        methods = sorted(list(getattr(route, "methods", []) or []))
        if path and methods:
            routers.append({"path": path, "methods": methods})
    return routers


def create_app() -> FastAPI:
    """
    Single authoritative app factory.

    All routers must be registered here.  Nothing outside this function
    may call app.include_router().  main.py exposes app = create_app()
    for uvicorn and tests alike so there is exactly one assembly path.
    """
    _app = FastAPI(title="Family Orchestration Bot API", debug=True)

    _app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://ui:5173",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Global hardening middleware stack.
    # Middleware runs in reverse registration order for decorator-based handlers.
    # Required runtime order:
    # [1] backpressure (outermost), [2] auth, [3] idempotency, [4] route handlers.
    install_idempotency_middleware(_app)
    install_auth_middleware(_app)
    install_request_backpressure_middleware(_app)

    @_app.exception_handler(SQLAlchemyTimeoutError)
    async def handle_db_pool_timeout(_request, exc: SQLAlchemyTimeoutError) -> JSONResponse:
        metrics.increment("db_acquire_timeout_count")
        metrics.note_db_pool_rejection()
        log_error("db_pool_exhausted", exc)
        return JSONResponse({"detail": "database_temporarily_unavailable"}, status_code=503)

    # ---------------------------------------------------------------
    # Router registration — all routes wired in one place
    # ---------------------------------------------------------------
    _app.include_router(system_router)
    _app.include_router(identity_router)
    _app.include_router(brief_router)
    _app.include_router(calendar_router)
    _app.include_router(auth_router)
    _app.include_router(realtime_router)
    _app.include_router(integrations_router)
    _app.include_router(ui_router)
    _app.include_router(ui_bootstrap_router)
    _app.include_router(evaluation_router)
    _app.include_router(operational_router)
    _app.include_router(insights_router)
    _app.include_router(policy_router)
    _app.include_router(hpal_router)
    _app.include_router(xai_router)
    _app.include_router(calendar_router)
    _app.include_router(health_router)
    _app.include_router(safety_router)

    # ---------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------
    @_app.on_event("startup")
    async def on_startup() -> None:
        trace_loop_context("main.on_startup")
        # Import ALL models so their tables are registered with Base.metadata
        # CRITICAL: Database tables are only created if the model class is imported
        try:
            import apps.api.xai.db_model  # noqa: F401
            import apps.api.models.identity  # noqa: F401
            import apps.api.models.idempotency_key  # noqa: F401
            import apps.api.models.task  # noqa: F401
            import apps.api.models.event_log  # noqa: F401

            boot_port = os.getenv("PORT") or os.getenv("UVICORN_PORT") or "unknown"
            print(f"[BOOT] pid={os.getpid()}")
            print(f"[BOOT] cwd={Path.cwd()}")
            print(f"[BOOT] requested_port={boot_port}")
            print(f"[BOOT] env_config_hash={_boot_config_hash()}")
            print(f"[BOOT] db_url={_sanitize_db_url(DATABASE_URL)}")
            print(f"[BOOT] active_routers={json.dumps(_active_router_list(_app), sort_keys=True)}")
            
            print("[STARTUP] Importing all models...")
            
            # Create tables
            print("[STARTUP] Creating database tables...")
            Base.metadata.create_all(bind=engine)
            print("[STARTUP] [OK] Database tables created")

            # Start event-loop lag guard background task.
            from apps.api.runtime.event_loop_guard import event_loop_guard
            await event_loop_guard.start()
            print("[STARTUP] [OK] Event loop guard started")

            # Run hard boot assertions. Any violation aborts startup.
            print("[STARTUP] Running boot invariants...")
            diags = assert_boot_invariants()
            
            print(f"[STARTUP] [OK] Boot invariants satisfied: {json.dumps(diags.to_dict(), sort_keys=True)}")
        except Exception as exc:
            error_msg = f"[STARTUP] CRITICAL FAILURE: {str(exc)}"
            print(error_msg)
            traceback.print_exc()
            raise

    @_app.on_event("shutdown")
    def on_shutdown() -> None:
        loop = None
        try:
            trace_loop_context("main.on_shutdown")
        except RuntimeError:
            loop = None
        from apps.api.runtime.event_loop_guard import event_loop_guard
        event_loop_guard.stop()

    # ---------------------------------------------------------------
    # Core event ingest (non-integration pipeline)
    # ---------------------------------------------------------------
    @_app.post("/event")
    @trace_function(entrypoint="api.event_ingest", actor_type="api_user", source="api")
    def ingest_event(event: SystemEvent) -> dict:
        try:
            result = canonical_event_router.route(
                CanonicalEventAdapter.to_envelope(event),
                persist=True,
                dispatch=True,
            )
            return {"status": "processed", "result": result}
        except Exception as exc:
            print("/event exception:", repr(exc))
            traceback.print_exc()
            raise

    return _app


# ---------------------------------------------------------------------------
# Module-level singleton — consumed by uvicorn and test clients
# ---------------------------------------------------------------------------
_fastapi_app = create_app()
app = AdmissionGateASGI(_fastapi_app)

if __name__ == "__main__":
    import uvicorn
    # limit_concurrency caps OS-level accepted connections per worker before
    # they reach the ASGI app, providing a front-door rejection layer that
    # supplements the in-app backpressure middleware.
    # timeout_keep_alive=2 releases idle connections quickly under load.
    uvicorn.run(
        "apps.api.main:app",
        host="0.0.0.0",
        port=8000,
        workers=2,
        limit_concurrency=50,
        timeout_keep_alive=0,
        log_level="debug",
        access_log=True,
    )
