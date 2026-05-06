from __future__ import annotations

import copy
from typing import Any

import pytest

from core.contracts.system_contract_validator import validate_system_contract


pytestmark = [pytest.mark.ci_gate, pytest.mark.migration, pytest.mark.reliability]


def _event_stream() -> list[dict[str, Any]]:
    return [
        {
            "event_id": "e1",
            "event_type": "task.rules_evaluated",
            "household_id": "household-1",
            "timestamp": "2026-04-30T10:00:00Z",
            "payload": {"request_id": "req-1", "rules_passed": True},
        },
        {
            "event_id": "e2",
            "event_type": "task.risk_assessed",
            "household_id": "household-1",
            "timestamp": "2026-04-30T10:00:01Z",
            "payload": {"request_id": "req-1", "risk": {"level": "low"}},
        },
    ]


def _base_contract_payloads() -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    projection = {
        "tasks": {
            "task-1": {
                "request_id": "req-1",
                "title": "Prepare dinner",
                "lifecycle_state": "created",
            }
        },
        "summary": {"task_count": 1},
    }

    decisions = [
        {"rule": "risk_gate", "decision": "allow"},
        {"rule": "idempotency", "decision": "allow"},
    ]

    runtime_output = {
        "fsm_state": {"tasks": {"task-1": {"current_state": "created"}}},
        "projection": projection,
        "control_plane_decisions": decisions,
    }

    replay_output = copy.deepcopy(runtime_output)
    api_response = {
        "request_id": "req-1",
        "projection": copy.deepcopy(projection),
    }
    return runtime_output, replay_output, decisions, api_response


def test_runtime_equals_replay_output() -> None:
    runtime_output, replay_output, decisions, api_response = _base_contract_payloads()

    result = validate_system_contract(
        event_stream=_event_stream(),
        runtime_output=runtime_output,
        replay_output=replay_output,
        control_plane_decisions=decisions,
        api_response=api_response,
    )

    assert result == {"matches": True, "differences": []}


def test_control_plane_equals_replay_decisions() -> None:
    runtime_output, replay_output, decisions, api_response = _base_contract_payloads()

    result = validate_system_contract(
        event_stream=_event_stream(),
        runtime_output=runtime_output,
        replay_output=replay_output,
        control_plane_decisions=decisions,
        api_response=api_response,
    )

    assert result["matches"] is True
    assert "control_decision_parity_mismatch" not in result["differences"]


def test_api_matches_projection_truth() -> None:
    runtime_output, replay_output, decisions, api_response = _base_contract_payloads()

    result = validate_system_contract(
        event_stream=_event_stream(),
        runtime_output=runtime_output,
        replay_output=replay_output,
        control_plane_decisions=decisions,
        api_response=api_response,
    )

    assert result["matches"] is True
    assert "api_projection_truth_mismatch" not in result["differences"]


def test_same_input_produces_same_output() -> None:
    runtime_output, replay_output, decisions, api_response = _base_contract_payloads()
    inputs = {
        "event_stream": _event_stream(),
        "runtime_output": runtime_output,
        "replay_output": replay_output,
        "control_plane_decisions": decisions,
        "api_response": api_response,
    }

    first = validate_system_contract(**inputs)
    second = validate_system_contract(**copy.deepcopy(inputs))

    assert first == second
    assert first == {"matches": True, "differences": []}


def test_replay_runtime_drift_detected() -> None:
    runtime_output, replay_output, decisions, api_response = _base_contract_payloads()

    replay_output["projection"]["tasks"]["task-1"]["lifecycle_state"] = "committed"
    decisions_drift = [
        {"rule": "risk_gate", "decision": "allow"},
        {"rule": "idempotency", "decision": "block"},
    ]

    result = validate_system_contract(
        event_stream=_event_stream(),
        runtime_output=runtime_output,
        replay_output=replay_output,
        control_plane_decisions=decisions_drift,
        api_response=api_response,
    )

    assert result["matches"] is False
    assert "structural_equality_mismatch:runtime_vs_replay" in result["differences"]
    assert "projection_equality_mismatch:runtime_vs_replay" in result["differences"]
    assert "control_decision_parity_mismatch" in result["differences"]
