from __future__ import annotations

from copy import deepcopy
from typing import Any


class TaskConnector:
    """Pure I/O adapter for task retrieval."""

    def read_tasks(self, state: Any) -> list[dict[str, Any]]:
        return [deepcopy(task) for task in state.tasks]
