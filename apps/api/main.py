from __future__ import annotations

import traceback

from fastapi import FastAPI

from apps.api.core.database import Base, engine
from apps.api.endpoints.brief_endpoint import router as brief_router
from apps.api.endpoints.integrations_router import router as integrations_router
from apps.api.endpoints.integrations_router import ui_router
from apps.api.schemas.event import SystemEvent
from apps.api.services.router_service import route_event


def create_app() -> FastAPI:
    """
    Single authoritative app factory.

    All routers must be registered here.  Nothing outside this function
    may call app.include_router().  main.py exposes app = create_app()
    for uvicorn and tests alike so there is exactly one assembly path.
    """
    _app = FastAPI(title="Family Orchestration Bot API", debug=True)

    # ---------------------------------------------------------------
    # Router registration — all routes wired in one place
    # ---------------------------------------------------------------
    _app.include_router(brief_router)
    _app.include_router(integrations_router)
    _app.include_router(ui_router)

    # ---------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------
    @_app.on_event("startup")
    def on_startup() -> None:
        Base.metadata.create_all(bind=engine)

    @_app.on_event("shutdown")
    def on_shutdown() -> None:
        pass

    # ---------------------------------------------------------------
    # Core event ingest (non-integration pipeline)
    # ---------------------------------------------------------------
    @_app.post("/event")
    def ingest_event(event: SystemEvent) -> dict:
        try:
            task = route_event(event)
            result = None if task is None else {
                "id": task.id,
                "household_id": task.household_id,
                "title": task.title,
                "description": task.description,
                "status": task.status,
                "priority": task.priority,
                "created_at": task.created_at,
                "updated_at": task.updated_at,
            }
            return {"status": "processed", "result": result}
        except Exception as exc:
            print("/event exception:", repr(exc))
            traceback.print_exc()
            raise

    return _app


# ---------------------------------------------------------------------------
# Module-level singleton — consumed by uvicorn and test clients
# ---------------------------------------------------------------------------
app = create_app()
