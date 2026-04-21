from __future__ import annotations

from copy import deepcopy
from typing import Any


class GroceryConnector:
    """Pure I/O adapter for grocery inventory retrieval."""

    def read_inventory(self, graph: dict[str, Any]) -> dict[str, int]:
        raw = dict(graph.get("grocery_inventory", {}))
        return {str(key): int(value) for key, value in deepcopy(raw).items()}
