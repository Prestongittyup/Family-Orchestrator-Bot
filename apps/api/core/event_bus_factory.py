from __future__ import annotations

from apps.api.core.event_bus import get_event_bus as _get_event_bus
from apps.api.core.event_bus_base import EventBusBase


def get_event_bus() -> EventBusBase:
    """Compatibility wrapper for legacy imports."""
    return _get_event_bus()