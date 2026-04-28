from __future__ import annotations

from archive.apps.api.core.event_bus_base import EventBusBase
from archive.apps.api.core.event_registry import (
    _calendar_event_scheduled_adapter,
    _email_received_adapter,
    _task_created_adapter,
)


def register_event_handlers(event_bus: EventBusBase) -> None:
    event_bus.register("task_created", _task_created_adapter)
    event_bus.register("email_received", _email_received_adapter)
    event_bus.register("calendar_event_scheduled", _calendar_event_scheduled_adapter)
