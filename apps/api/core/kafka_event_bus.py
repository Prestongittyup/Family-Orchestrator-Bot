from __future__ import annotations

from collections.abc import Callable

from apps.api.core.event_bus_base import EventBusBase
from apps.api.schemas.event import SystemEvent


class KafkaEventBus(EventBusBase):
    def __init__(self) -> None:
        # Metadata-only registration storage for future implementation.
        self._registered_handlers: dict[str, list[Callable[[SystemEvent], object]]] = {}

    def register(self, event_type: str, handler: Callable[[SystemEvent], object]) -> None:
        handlers = self._registered_handlers.setdefault(event_type, [])
        handlers.append(handler)

    def publish(self, event: SystemEvent) -> list[object] | None:
        # Stub only: no transport, no network, no side effects.
        return {"status": "kafka_not_implemented"}  # type: ignore[return-value]
