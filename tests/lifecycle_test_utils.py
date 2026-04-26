from __future__ import annotations

from typing import Any


LIFECYCLE_KEYS = {"current_state", "from_state", "to_state", "status"}
IGNORED_ROOT_KEYS = {"_lifecycle_hydration", "_lifecycle_hydration_views"}


def _collect_lifecycle_fields(node: Any, *, path: str = "$") -> dict[str, Any]:
    collected: dict[str, Any] = {}
    if isinstance(node, dict):
        for key, value in node.items():
            if key in IGNORED_ROOT_KEYS:
                continue
            next_path = f"{path}.{key}"
            if key in LIFECYCLE_KEYS:
                collected[next_path] = value
            collected.update(_collect_lifecycle_fields(value, path=next_path))
        return collected

    if isinstance(node, list):
        for index, value in enumerate(node):
            collected.update(_collect_lifecycle_fields(value, path=f"{path}[{index}]"))
    return collected


def assert_no_lifecycle_mutation(dict_before: dict[str, Any], dict_after: dict[str, Any]) -> None:
    before_fields = _collect_lifecycle_fields(dict_before)
    after_fields = _collect_lifecycle_fields(dict_after)
    comparable_after = {path: after_fields.get(path) for path in before_fields}
    assert comparable_after == before_fields