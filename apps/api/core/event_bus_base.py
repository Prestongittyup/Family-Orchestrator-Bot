from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable

from apps.api.schemas.event import SystemEvent


class EventBusBase(ABC):
    @abstractmethod
    def publish(self, event: SystemEvent) -> list[object] | dict | None:
        pass

    @abstractmethod
    def register(self, event_type: str, handler: Callable[[SystemEvent], object]) -> None:
        pass
