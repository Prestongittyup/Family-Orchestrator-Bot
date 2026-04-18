from __future__ import annotations

from apps.api.core.event_bus_base import EventBusBase
from apps.api.core.event_registry import _email_received_adapter, _task_created_adapter


def register_event_handlers(event_bus: EventBusBase) -> None:
    event_bus.register("task_created", _task_created_adapter)
    event_bus.register("email_received", _email_received_adapter)
