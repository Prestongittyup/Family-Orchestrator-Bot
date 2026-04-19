from __future__ import annotations

from apps.api.models.task import Task
from apps.api.schemas.event import SystemEvent
from apps.api.services.task_service import create_task


LEGACY_ISOLATED = True


def route_event(event: SystemEvent) -> Task | None:
    if event.type == "task_created":
        title = event.payload["title"]
        return create_task(event.household_id, title)

    return None
