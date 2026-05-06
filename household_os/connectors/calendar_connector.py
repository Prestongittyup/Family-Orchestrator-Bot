from __future__ import annotations

from copy import deepcopy
from typing import Any


class CalendarConnector:
    """Pure I/O adapter for calendar event retrieval."""

    def read_events(self, state: Any) -> list[dict[str, Any]]:
        return [deepcopy(event.as_dict()) for event in state.calendar_events]
