from __future__ import annotations

import asyncio
import traceback
from typing import Any


_RESOURCE_CREATION: dict[int, dict[str, Any]] = {}
_VIOLATIONS: list[dict[str, Any]] = []
_CONTEXT_EVENTS: list[dict[str, Any]] = []


def trace_loop_context(label: str) -> None:
    loop = asyncio.get_running_loop()
    _CONTEXT_EVENTS.append({"label": label, "loop_id": id(loop)})
    print(f"[LOOP] context={label} loop_id={id(loop)}")


def register_loop_resource(resource: object, label: str) -> None:
    loop = asyncio.get_running_loop()
    resource_loop = getattr(resource, "_loop", None)
    _RESOURCE_CREATION[id(resource)] = {
        "label": label,
        "loop_id": id(loop),
        "resource_loop_id": None if resource_loop is None else id(resource_loop),
        "resource_type": type(resource).__name__,
        "stack": "".join(traceback.format_stack(limit=12)),
    }
    print(
        f"[TRACE] {label} "
        f"resource_id={id(resource)} "
        f"resource_loop={id(resource_loop) if resource_loop is not None else 'None'} "
        f"current_loop={id(loop)}"
    )


def trace_loop_binding(resource: object, label: str) -> None:
    loop = asyncio.get_running_loop()
    resource_loop = getattr(resource, "_loop", None)
    creation = _RESOURCE_CREATION.get(id(resource), {})

    print(
        f"[TRACE] {label} "
        f"resource_id={id(resource)} "
        f"resource_loop={id(resource_loop) if resource_loop is not None else 'None'} "
        f"current_loop={id(loop)}"
    )

    if resource_loop is not None and resource_loop is not loop:
        violation = {
            "label": label,
            "resource_id": id(resource),
            "resource_type": type(resource).__name__,
            "resource_loop": id(resource_loop),
            "current_loop": id(loop),
            "creation_label": creation.get("label", "unknown"),
            "creation_loop": creation.get("loop_id", "unknown"),
            "creation_stack": creation.get("stack", ""),
            "current_stack": "".join(traceback.format_stack(limit=12)),
        }
        _VIOLATIONS.append(violation)
        raise RuntimeError(
            f"[LOOP VIOLATION] {label} "
            f"resource_id={id(resource)} "
            f"resource_type={type(resource).__name__} "
            f"resource_loop={id(resource_loop)} "
            f"current_loop={id(loop)} "
            f"creation_label={creation.get('label', 'unknown')} "
            f"creation_loop={creation.get('loop_id', 'unknown')}"
        )


def trace_task_binding(task: asyncio.Task[Any], label: str) -> None:
    loop = asyncio.get_running_loop()
    task_loop = task.get_loop()
    print(
        f"[TRACE] {label} "
        f"task_id={id(task)} "
        f"task_loop={id(task_loop)} "
        f"current_loop={id(loop)}"
    )
    if task_loop is not loop:
        _VIOLATIONS.append(
            {
                "label": label,
                "task_id": id(task),
                "resource_type": "Task",
                "task_loop": id(task_loop),
                "current_loop": id(loop),
                "current_stack": "".join(traceback.format_stack(limit=12)),
            }
        )
        raise RuntimeError(
            f"[LOOP VIOLATION] {label} "
            f"task_id={id(task)} "
            f"task_loop={id(task_loop)} "
            f"current_loop={id(loop)}"
        )


def trace_gather_binding(tasks: list[asyncio.Task[Any]], label: str) -> None:
    for task in tasks:
        trace_task_binding(task, label)


def record_violation(event: dict[str, Any]) -> None:
    _VIOLATIONS.append(dict(event))


def get_violation_events() -> list[dict[str, Any]]:
    return list(_VIOLATIONS)


def clear_violation_events() -> None:
    _VIOLATIONS.clear()


def get_context_events() -> list[dict[str, Any]]:
    return list(_CONTEXT_EVENTS)


def clear_context_events() -> None:
    _CONTEXT_EVENTS.clear()
