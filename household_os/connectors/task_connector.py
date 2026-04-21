from __future__ import annotations

from copy import deepcopy
from typing import Any

from apps.api.integration_core.models.household_state import HouseholdState


class TaskConnector:
    """Pure I/O adapter for task retrieval."""

    def read_tasks(self, state: HouseholdState) -> list[dict[str, Any]]:
        return [deepcopy(task) for task in state.tasks]
