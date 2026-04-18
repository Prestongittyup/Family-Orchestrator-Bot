from __future__ import annotations

from collections.abc import Mapping
from typing import Any


ALLOWED_ADAPTER_BEHAVIORS: tuple[str, ...] = (
    "normalization",
    "deterministic scoring",
    "deterministic sorting",
    "visibility filtering",
    "format enrichment (time, labels)",
)


FORBIDDEN_ADAPTER_BEHAVIORS: tuple[str, ...] = (
    "scheduling decisions (final placement authority)",
    "optimization across tasks",
    "conflict resolution beyond visibility filtering",
    "cross-task dependency reasoning",
)


_ADAPTER_INJECTION_FIELDS: frozenset[str] = frozenset(
    {
        "raw_time_input",
        "priority_score",
    }
)


_FORBIDDEN_BEHAVIOR_KEYS: frozenset[str] = frozenset(
    {
        "final_slot",
        "final_schedule",
        "placement_authority",
        "schedule_decision",
        "optimized_plan",
        "optimization_score",
        "global_optimization",
        "conflict_resolution",
        "resolved_conflicts",
        "dependency_graph",
        "dependency_chain",
        "cross_task_reasoning",
    }
)


def _validate_action_keys(actions: list[dict[str, Any]], section_name: str) -> list[str]:
    errors: list[str] = []
    for index, action in enumerate(actions):
        if not isinstance(action, dict):
            errors.append(f"{section_name}[{index}] must be a dict")
            continue

        for key in action.keys():
            if key in _FORBIDDEN_BEHAVIOR_KEYS:
                errors.append(f"{section_name}[{index}] contains forbidden key '{key}'")

        if "raw_time_input" in action and not isinstance(action.get("raw_time_input"), str):
            errors.append(f"{section_name}[{index}].raw_time_input must be a string")

        if "priority_score" in action:
            try:
                float(action.get("priority_score"))
            except (TypeError, ValueError):
                errors.append(f"{section_name}[{index}].priority_score must be numeric")

        # Adapter governance: only known injection fields may be adapter-specific additions.
        unknown_injected = {
            key
            for key in action.keys()
            if key.startswith("adapter_") and key not in _ADAPTER_INJECTION_FIELDS
        }
        if unknown_injected:
            errors.append(
                f"{section_name}[{index}] contains unknown adapter fields: "
                f"{', '.join(sorted(unknown_injected))}"
            )

    return errors


def validate_adapter_output_contract(output: Mapping[str, Any]) -> dict[str, Any]:
    """
    Soft governance validation for adapter-layer output.

    This function intentionally does NOT raise. Strict enforcement belongs to the
    planning boundary contract (system-level hard gate).
    """
    errors: list[str] = []

    if not isinstance(output, Mapping):
        return {"valid": False, "errors": ["output must be a mapping"]}

    if "scheduled_actions" in output and not isinstance(output.get("scheduled_actions"), list):
        errors.append("scheduled_actions must be a list")
    if "unscheduled_actions" in output and not isinstance(output.get("unscheduled_actions"), list):
        errors.append("unscheduled_actions must be a list")
    if "priorities" in output and not isinstance(output.get("priorities"), list):
        errors.append("priorities must be a list")
    if "warnings" in output and not isinstance(output.get("warnings"), list):
        errors.append("warnings must be a list")
    if "risks" in output and not isinstance(output.get("risks"), list):
        errors.append("risks must be a list")
    if "summary" in output and not isinstance(output.get("summary"), str):
        errors.append("summary must be a string")

    scheduled = output.get("scheduled_actions", [])
    unscheduled = output.get("unscheduled_actions", [])
    priorities = output.get("priorities", [])

    if not isinstance(scheduled, list) or not isinstance(unscheduled, list):
        errors.append("scheduled_actions and unscheduled_actions must be lists")
    else:
        scheduled_dicts = [row for row in scheduled if isinstance(row, dict)]
        unscheduled_dicts = [row for row in unscheduled if isinstance(row, dict)]
        if len(scheduled_dicts) != len(scheduled):
            errors.append("scheduled_actions must contain dict items")
        if len(unscheduled_dicts) != len(unscheduled):
            errors.append("unscheduled_actions must contain dict items")
        errors.extend(_validate_action_keys(scheduled_dicts, "scheduled_actions"))
        errors.extend(_validate_action_keys(unscheduled_dicts, "unscheduled_actions"))

    if not isinstance(priorities, list):
        errors.append("priorities must be a list")

    for index, row in enumerate(priorities if isinstance(priorities, list) else []):
        if isinstance(row, dict):
            forbidden = sorted(set(row.keys()) & _FORBIDDEN_BEHAVIOR_KEYS)
            if forbidden:
                errors.append(f"priorities[{index}] contains forbidden keys: {', '.join(forbidden)}")

    return {
        "valid": not errors,
        "errors": errors,
    }