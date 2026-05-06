from __future__ import annotations

from contextlib import asynccontextmanager
from collections import defaultdict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.adapters.db.session_factory import Base, engine
from app.api.command import ingest_router, router as command_router
from app.api.notifications import router as notifications_router
from app.api.reminders import router as reminders_router
from app.api.schedule import router as schedule_router
from app.api.tasks import router as tasks_router
from core.architecture.architecture_guard import enforce_architecture_on_startup


def _route_ownership_snapshot(app: FastAPI) -> list[str]:
    owners: dict[str, list[str]] = defaultdict(list)
    for route in app.routes:
        path = str(getattr(route, "path", ""))
        methods = sorted(str(method) for method in (getattr(route, "methods", set()) or set()))
        if not path or not methods:
            continue
        key = f"{','.join(methods)} {path}"
        owners[key].append(str(getattr(route, "name", "")))

    lines: list[str] = []
    for key in sorted(owners):
        route_owners = sorted(name for name in owners[key] if name)
        owner_summary = ",".join(route_owners) if route_owners else "<unknown>"
        lines.append(f"{key} -> {owner_summary}")
    return lines


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        _app.state.architecture_diagnostic = enforce_architecture_on_startup(context="startup")
        Base.metadata.create_all(bind=engine)
        _app.state.route_ownership = _route_ownership_snapshot(_app)
        print("[ROUTE-OWNERSHIP] begin", flush=True)
        for line in _app.state.route_ownership:
            print(f"[ROUTE-OWNERSHIP] {line}", flush=True)
        print("[ROUTE-OWNERSHIP] end", flush=True)
        yield

    app = FastAPI(
        title="Household Intelligence Backend",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Ensure event log ORM model is registered before metadata initialization.
    from app.adapters.db.models import event_log as _event_log_model

    _ = _event_log_model

    app.include_router(command_router)
    app.include_router(ingest_router)
    app.include_router(tasks_router)
    app.include_router(schedule_router)
    app.include_router(reminders_router)
    app.include_router(notifications_router)

    @app.get("/healthz", tags=["system"])
    async def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health", tags=["system"])
    async def healthcheck_alias() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
