from __future__ import annotations

from apps.api.ingestion.adapters.adapter_governance import (
    ALLOWED_ADAPTER_BEHAVIORS,
    FORBIDDEN_ADAPTER_BEHAVIORS,
    validate_adapter_output_contract,
)


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


def test_validate_adapter_output_contract_accepts_minimal_valid_brief() -> None:
    output = {
        "scheduled_actions": [{"title": "Fix bug", "start_time": "2026-04-16T09:00:00", "priority_score": 2.0}],
        "unscheduled_actions": [],
        "priorities": [],
        "warnings": [],
        "risks": [],
        "summary": "",
    }
    result = validate_adapter_output_contract(output)
    assert result["valid"] is True
    assert result["errors"] == []


def test_validate_adapter_output_contract_rejects_forbidden_behavior_keys_softly() -> None:
    output = {
        "scheduled_actions": [{"title": "Call provider", "start_time": "2026-04-16T14:00:00", "priority_score": 1.0, "dependency_graph": {"a": "b"}}],
        "unscheduled_actions": [],
        "priorities": [],
        "warnings": [],
        "risks": [],
        "summary": "",
    }
    result = validate_adapter_output_contract(output)
    assert result["valid"] is False
    assert any("forbidden key 'dependency_graph'" in row for row in result["errors"])


def test_validate_adapter_output_contract_reports_structural_drift_softly() -> None:
    output = {
        "scheduled_actions": [],
        "unscheduled_actions": [],
        "priorities": {"unexpected": True},
        "warnings": [],
        "risks": [],
        "summary": "",
    }
    result = validate_adapter_output_contract(output)
    assert result["valid"] is False
    assert "priorities must be a list" in result["errors"]
