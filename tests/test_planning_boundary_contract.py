from __future__ import annotations

from copy import deepcopy

import pytest

from brief_endpoint import map_manual_to_brief
from apps.api.ingestion.adapters.adapter_governance import validate_adapter_output_contract
from apps.api.services.planning_boundary_contract import (
    PlanningBoundaryViolation,
    pre_os2_validation,
    validate_planning_boundary,
)


def _base_brief() -> dict:
    return {
        "scheduled_actions": [],
        "unscheduled_actions": [],
        "priorities": [],
        "warnings": [],
        "risks": [],
        "summary": "",
    }


def test_adapter_output_without_violations_passes_boundary() -> None:
    output = map_manual_to_brief(
        deepcopy(_base_brief()),
        [
            {"title": "Fix bug", "time": "morning"},
            {"title": "Pay bill"},
        ],
    )

    assert validate_planning_boundary(output) is True
    assert pre_os2_validation(output) == output


def test_forbidden_planning_fields_blocked_only_at_boundary() -> None:
    output = map_manual_to_brief(deepcopy(_base_brief()), [{"title": "Call provider", "time": "14:00"}])
    output["scheduled_actions"][0]["dependency_graph"] = {"a": "b"}

    # Adapter governance is advisory (soft) only.
    soft = validate_adapter_output_contract(output)
    assert soft["valid"] is False
    assert any("dependency_graph" in error for error in soft["errors"])

    # Hard block happens only at planning boundary.
    with pytest.raises(PlanningBoundaryViolation) as exc_info:
        pre_os2_validation(output)

    assert "dependency_graph" in str(exc_info.value)


def test_governance_duplication_does_not_exist_across_layers() -> None:
    output = map_manual_to_brief(deepcopy(_base_brief()), [{"title": "Read notes"}])
    output["priorities"] = {"invalid": True}

    # Soft governance reports but does not raise.
    soft = validate_adapter_output_contract(output)
    assert soft["valid"] is False
    assert "priorities must be a list" in soft["errors"]

    # Boundary is the only hard enforcement gate.
    with pytest.raises(PlanningBoundaryViolation):
        validate_planning_boundary(output)


def test_boundary_validation_is_deterministic_across_repeated_runs() -> None:
    items = [
        {"title": "Fix urgent bug", "time": "morning"},
        {"title": "Team meeting", "time": "afternoon"},
        {"title": "Cook dinner", "time": "evening"},
    ]

    first = map_manual_to_brief(deepcopy(_base_brief()), items)
    second = map_manual_to_brief(deepcopy(_base_brief()), items)

    assert validate_planning_boundary(first) is True
    assert validate_planning_boundary(second) is True
    assert pre_os2_validation(first) == first
    assert pre_os2_validation(second) == second