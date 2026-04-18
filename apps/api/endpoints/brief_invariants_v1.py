from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from apps.api.endpoints.brief_contract_v1 import (
    BRIEF_V1_ALLOWED_FIELDS,
    BRIEF_V1_REQUIRED_FIELDS,
    BriefV1,
)


class BriefV1ValidationError(ValueError):
    pass


def _coerce_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _coerce_int(value: Any, default: int = 10**9) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _scheduled_key(item: Mapping[str, Any]) -> tuple[int, str, str, str]:
    return (
        _coerce_int(item.get("ordering_position")),
        _coerce_str(item.get("start_time")),
        _coerce_str(item.get("title")),
        _coerce_str(item.get("source_module")),
    )


def _unscheduled_key(item: Mapping[str, Any]) -> tuple[int, float, str, str]:
    return (
        _coerce_int(item.get("ordering_position")),
        -_coerce_float(item.get("normalized_priority")),
        _coerce_str(item.get("title")),
        _coerce_str(item.get("source_module")),
    )


def _priority_key(item: Mapping[str, Any]) -> tuple[float, float, str, str, int]:
    return (
        -_coerce_float(item.get("normalized_priority")),
        -_coerce_float(item.get("score")),
        _coerce_str(item.get("title")),
        _coerce_str(item.get("source_module")),
        _coerce_int(item.get("rank"), default=10**9),
    )


def _has_list_of_dicts(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    return all(isinstance(row, dict) for row in value)


def project_brief_to_v1(output: Mapping[str, Any]) -> BriefV1:
    if set(BRIEF_V1_REQUIRED_FIELDS).issubset(output.keys()):
        return {
            "scheduled_actions": [dict(item) for item in output.get("scheduled_actions", [])],
            "unscheduled_actions": [dict(item) for item in output.get("unscheduled_actions", [])],
            "priorities": [dict(item) for item in output.get("priorities", [])],
            "warnings": [dict(item) if isinstance(item, dict) else {"value": item} for item in output.get("warnings", [])],
            "risks": [dict(item) if isinstance(item, dict) else {"value": item} for item in output.get("risks", [])],
            "summary": _coerce_str(output.get("summary", "")),
        }

    suggestions = [dict(item) for item in output.get("suggestions", []) if isinstance(item, dict)]

    unscheduled = [
        item
        for item in suggestions
        if _coerce_str(item.get("decision_type")).lower() in {"deferred", "unscheduled", ""}
    ]

    return {
        "scheduled_actions": [dict(item) for item in output.get("suggested_actions", []) if isinstance(item, dict)],
        "unscheduled_actions": unscheduled,
        "priorities": [dict(item) for item in output.get("priorities", []) if isinstance(item, dict)],
        "warnings": [dict(item) if isinstance(item, dict) else {"value": item} for item in output.get("warnings", [])],
        "risks": [dict(item) if isinstance(item, dict) else {"value": item} for item in output.get("risks", [])],
        "summary": _coerce_str(output.get("summary_text", output.get("summary", ""))),
    }


def _validate_structure(brief_v1: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []

    missing = [field for field in BRIEF_V1_REQUIRED_FIELDS if field not in brief_v1]
    if missing:
        errors.append(f"missing required fields: {', '.join(missing)}")

    unexpected = sorted(set(brief_v1.keys()) - BRIEF_V1_ALLOWED_FIELDS)
    if unexpected:
        errors.append(f"unexpected fields: {', '.join(unexpected)}")

    if "scheduled_actions" in brief_v1 and not _has_list_of_dicts(brief_v1["scheduled_actions"]):
        errors.append("scheduled_actions must be a list[dict]")

    if "unscheduled_actions" in brief_v1 and not _has_list_of_dicts(brief_v1["unscheduled_actions"]):
        errors.append("unscheduled_actions must be a list[dict]")

    if "priorities" in brief_v1 and not _has_list_of_dicts(brief_v1["priorities"]):
        errors.append("priorities must be a list[dict]")

    if "warnings" in brief_v1 and not isinstance(brief_v1["warnings"], list):
        errors.append("warnings must be a list")

    if "risks" in brief_v1 and not isinstance(brief_v1["risks"], list):
        errors.append("risks must be a list")

    if "summary" in brief_v1 and not isinstance(brief_v1["summary"], str):
        errors.append("summary must be a string")

    return errors


def _validate_ordering(brief_v1: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []

    scheduled = brief_v1.get("scheduled_actions", [])
    if isinstance(scheduled, list):
        if scheduled != sorted(scheduled, key=_scheduled_key):
            errors.append("scheduled_actions violates deterministic ordering")

    unscheduled = brief_v1.get("unscheduled_actions", [])
    if isinstance(unscheduled, list):
        if unscheduled != sorted(unscheduled, key=_unscheduled_key):
            errors.append("unscheduled_actions violates deterministic ordering")

    priorities = brief_v1.get("priorities", [])
    if isinstance(priorities, list):
        if priorities != sorted(priorities, key=_priority_key):
            errors.append("priorities violates deterministic ordering")

    return errors


def validate_brief_v1(
    output: Mapping[str, Any],
    *,
    enabled: bool = False,
    raise_on_error: bool = True,
) -> dict[str, Any]:
    if not enabled:
        return {"valid": True, "errors": [], "brief_v1": None}

    brief_v1 = project_brief_to_v1(output)

    errors = _validate_structure(brief_v1)
    errors.extend(_validate_ordering(brief_v1))

    if errors and raise_on_error:
        raise BriefV1ValidationError("; ".join(errors))

    return {
        "valid": not errors,
        "errors": errors,
        "brief_v1": brief_v1,
    }
