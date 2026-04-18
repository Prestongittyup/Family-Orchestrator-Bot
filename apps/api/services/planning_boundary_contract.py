from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from apps.api.endpoints.brief_contract_v1 import BRIEF_V1_ALLOWED_FIELDS, BRIEF_V1_REQUIRED_FIELDS
from apps.api.endpoints.brief_invariants_v1 import project_brief_to_v1


@dataclass(frozen=True)
class PlanningBoundaryContract:
    allowed_fields_entering_os2: frozenset[str]
    forbidden_fields_crossing_boundary: frozenset[str]
    required_normalized_structure: Mapping[str, str]


DEFAULT_PLANNING_BOUNDARY_CONTRACT = PlanningBoundaryContract(
    allowed_fields_entering_os2=frozenset(BRIEF_V1_ALLOWED_FIELDS),
    forbidden_fields_crossing_boundary=frozenset(
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
    ),
    required_normalized_structure={
        "scheduled_actions": "list[dict]",
        "unscheduled_actions": "list[dict]",
        "priorities": "list[dict]",
        "warnings": "list",
        "risks": "list",
        "summary": "str",
    },
)


class PlanningBoundaryViolation(AssertionError):
    pass


def _validate_normalized_shape(output: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
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
    return errors


def _validate_forbidden_fields(
    brief_v1: Mapping[str, Any],
    *,
    forbidden_fields: frozenset[str],
) -> list[str]:
    errors: list[str] = []
    for section in ("scheduled_actions", "unscheduled_actions", "priorities"):
        rows = brief_v1.get(section, [])
        if not isinstance(rows, list):
            continue
        for idx, row in enumerate(rows):
            if not isinstance(row, dict):
                errors.append(f"{section}[{idx}] must be a dict")
                continue
            found = sorted(set(row.keys()) & forbidden_fields)
            if found:
                errors.append(f"{section}[{idx}] contains forbidden fields: {', '.join(found)}")
    return errors


def validate_planning_boundary(
    planning_input: Mapping[str, Any],
    *,
    contract: PlanningBoundaryContract = DEFAULT_PLANNING_BOUNDARY_CONTRACT,
) -> bool:
    if not isinstance(planning_input, Mapping):
        raise PlanningBoundaryViolation("planning input must be a mapping")

    errors: list[str] = []
    errors.extend(_validate_normalized_shape(planning_input))

    missing_required = [field for field in BRIEF_V1_REQUIRED_FIELDS if field not in planning_input]
    if missing_required:
        errors.append(f"missing required fields: {', '.join(missing_required)}")

    if errors:
        raise PlanningBoundaryViolation("; ".join(errors))

    try:
        projected = project_brief_to_v1(planning_input)
    except Exception as exc:
        raise PlanningBoundaryViolation(f"unable to project planning input to brief_v1: {exc}") from exc

    top_keys = set(projected.keys())
    if top_keys != set(contract.allowed_fields_entering_os2):
        raise PlanningBoundaryViolation("projected brief_v1 contains unexpected structural keys")

    forbidden_errors = _validate_forbidden_fields(
        projected,
        forbidden_fields=contract.forbidden_fields_crossing_boundary,
    )
    if forbidden_errors:
        raise PlanningBoundaryViolation("; ".join(forbidden_errors))

    return True


def pre_os2_validation(adapter_output: Mapping[str, Any]) -> Mapping[str, Any]:
    """Single strict enforcement hook before any OS-2 planning consumption."""
    validate_planning_boundary(adapter_output)
    return adapter_output