from __future__ import annotations

import os
from collections.abc import Callable

from apps.api.core.event_bus_async import AsyncEventBus
from apps.api.core.event_bus_base import EventBusBase
from apps.api.core.kafka_event_bus import KafkaEventBus
from apps.api.schemas.event import SystemEvent


class InMemoryEventBus(EventBusBase):
    def __init__(self) -> None:
        self._registry: dict[str, list[Callable[[SystemEvent], object]]] = {}

    def register(self, event_type: str, handler: Callable[[SystemEvent], object]) -> None:
        if event_type not in self._registry:
            self._registry[event_type] = []
        self._registry[event_type].append(handler)

    def publish(self, event: SystemEvent) -> list[object] | None:
        handlers = self._registry.get(event.type)
        if not handlers:
            return None

        results: list[object] = []
        for handler in handlers:
            results.append(handler(event))
        return results


class EventBus(InMemoryEventBus):
    """Compatibility alias preserving existing imports and behavior."""


_event_bus_instance: EventBusBase | None = None


def get_event_bus() -> EventBusBase:
    """Return the global EventBus implementation based on configuration."""
    global _event_bus_instance

    if _event_bus_instance is not None:
        return _event_bus_instance

    bus_type = os.getenv("EVENT_BUS_TYPE", "sync").strip().lower()
    if bus_type == "kafka":
        _event_bus_instance = KafkaEventBus()
    elif bus_type == "async":
        _event_bus_instance = AsyncEventBus()
    else:
        # "sync" is canonical; keep "inmemory" compatibility for existing configs.
        _event_bus_instance = InMemoryEventBus()

    return _event_bus_instance

