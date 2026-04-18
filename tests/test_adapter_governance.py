from __future__ import annotations

from copy import deepcopy

from brief_endpoint import map_manual_to_brief
from apps.api.ingestion.adapters.adapter_governance import (
    ALLOWED_ADAPTER_BEHAVIORS,
    FORBIDDEN_ADAPTER_BEHAVIORS,
    validate_adapter_output_contract,
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


def test_governance_behavior_lists_are_frozen_and_explicit() -> None:
    assert ALLOWED_ADAPTER_BEHAVIORS == (
        "normalization",
        "deterministic scoring",
        "deterministic sorting",
        "visibility filtering",
        "format enrichment (time, labels)",
    )
    assert FORBIDDEN_ADAPTER_BEHAVIORS == (
        "scheduling decisions (final placement authority)",
        "optimization across tasks",
        "conflict resolution beyond visibility filtering",
        "cross-task dependency reasoning",
    )


def test_validate_adapter_output_contract_accepts_valid_adapter_output() -> None:
    output = map_manual_to_brief(
        deepcopy(_base_brief()),
        [
            {"title": "Fix bug", "time": "morning"},
            {"title": "Read notes"},
        ],
    )
    result = validate_adapter_output_contract(output)
    assert result["valid"] is True
    assert result["errors"] == []


def test_validate_adapter_output_contract_rejects_forbidden_behavior_keys_softly() -> None:
    output = map_manual_to_brief(deepcopy(_base_brief()), [{"title": "Call provider", "time": "14:00"}])
    output["scheduled_actions"][0]["dependency_graph"] = {"a": "b"}

    result = validate_adapter_output_contract(output)
    assert result["valid"] is False
    assert any("forbidden key 'dependency_graph'" in row for row in result["errors"])


def test_validate_adapter_output_contract_reports_structural_drift_softly() -> None:
    output = map_manual_to_brief(deepcopy(_base_brief()), [{"title": "Pay bill", "time": "evening"}])
    # Break BriefV1 boundary via invalid section type.
    output["priorities"] = {"unexpected": True}

    result = validate_adapter_output_contract(output)
    assert result["valid"] is False
    assert "priorities must be a list" in result["errors"]


def test_governance_does_not_require_renderer_or_schema_changes() -> None:
    output = map_manual_to_brief(
        deepcopy(_base_brief()),
        [
            {"title": "Morning review", "time": "morning"},
            {"title": "Evening prep", "time": "tonight"},
        ],
    )

    assert set(output.keys()) == {"scheduled_actions", "unscheduled_actions", "priorities", "warnings", "risks", "summary"}
    result = validate_adapter_output_contract(output)
    assert result["valid"] is True