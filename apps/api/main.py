from __future__ import annotations

import traceback

from fastapi import FastAPI

from apps.api.core.database import Base, engine
from apps.api.schemas.event import SystemEvent
from apps.api.services.router_service import route_event


app = FastAPI(title="Family Orchestration Bot API", debug=True)


@app.on_event("startup")
def on_startup() -> None:
    Base.metadata.create_all(bind=engine)


@app.get("/")
def root() -> dict[str, str]:
    return {"status": "running"}


@app.post("/event")
def ingest_event(event: SystemEvent) -> dict[str, object]:
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
        return {
            "status": "processed",
            "result": result,
        }
    except Exception as exc:
        print("/event exception:", repr(exc))
        traceback.print_exc()
        raise
