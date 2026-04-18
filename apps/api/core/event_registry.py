from __future__ import annotations

from apps.api.core.event_bus import get_event_bus
from apps.api.modules.email.email_service import handle_email_received
from apps.api.schemas.event import SystemEvent
from apps.api.schemas.events.email_events import EmailReceivedEvent
from apps.api.schemas.events.task_events import TaskCreatedEvent
from apps.api.services.task_service import create_task


def _handle_task_created(household_id: str, data: TaskCreatedEvent):
    return create_task(household_id, data.title)


def _task_created_adapter(event: SystemEvent):
    data = TaskCreatedEvent(**event.payload)
    return _handle_task_created(event.household_id, data)


def _email_received_adapter(event: SystemEvent):
    data = EmailReceivedEvent(**event.payload)
    return handle_email_received(event.household_id, data)


event_bus = get_event_bus()
