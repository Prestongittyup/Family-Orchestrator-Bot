from __future__ import annotations

from copy import deepcopy
from typing import Any

from apps.api.integration_core.models.household_state import HouseholdState


class CalendarConnector:
    """Pure I/O adapter for calendar event retrieval."""

    def read_events(self, state: HouseholdState) -> list[dict[str, Any]]:
        return [deepcopy(event.as_dict()) for event in state.calendar_events]
