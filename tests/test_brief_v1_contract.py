from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from apps.api.endpoints.brief_contract_v1 import BRIEF_V1_REQUIRED_FIELDS
from apps.api.endpoints.brief_invariants_v1 import project_brief_to_v1, validate_brief_v1
from apps.api.services.synthesis_engine import build_daily_brief


BASE_PROPOSALS = {
    "p1": {
        "id": "p1",
        "type": "task_action",
        "title": "Morning prep",
        "description": "reference=p1; time_window=2026-04-16T08:00:00->2026-04-16T09:00:00",
        "priority": 5,
        "source_module": "task_module",
        "duration": 1,
        "effort": "medium",
        "category": "task",
        "normalized_priority": 5.0,
    },
    "p2": {
        "id": "p2",
        "type": "task_action",
        "title": "Review bills",
        "description": "reference=p2; time_window=2026-04-16T09:00:00->2026-04-16T10:00:00",
        "priority": 4,
        "source_module": "task_module",
        "duration": 1,
        "effort": "low",
        "category": "task",
        "normalized_priority": 4.0,
    },
    "p3": {
        "id": "p3",
        "type": "task_action",
        "title": "Call provider",
        "description": "reference=p3; time_window=2026-04-16T10:00:00->2026-04-16T11:00:00",
        "priority": 3,
        "source_module": "task_module",
        "duration": 1,
        "effort": "low",
        "category": "task",
        "normalized_priority": 3.0,
    },
}


def _orchestrator_output(proposal_ids: list[str]) -> dict[str, Any]:
    proposals = [deepcopy(BASE_PROPOSALS[pid]) for pid in proposal_ids]
    ordering_index = [{"proposal_id": pid, "position": i} for i, pid in enumerate(proposal_ids)]
    return {
        "proposals": proposals,
        "signals": [
            {
                "id": "s-events",
                "type": "events_today",
                "message": "events_today=1",
                "severity": "low",
                "source_module": "calendar_module",
            }
        ],
        "semantic_layer": {"ordering_index": ordering_index},
    }


def _decision_output_for_scenario(name: str) -> dict[str, Any]:
    if name == "busy_day":
        return {
            "scheduled_actions": [
                {
                    "proposal_id": "p1",
                    "score": 0.90,
                    "normalized_priority": 5.0,
                    "ordering_position": 0,
                    "bucket": "morning",
                },
                {
                    "proposal_id": "p2",
                    "score": 0.80,
                    "normalized_priority": 4.0,
                    "ordering_position": 1,
                    "bucket": "morning",
                },
            ],
            "unscheduled_actions": [],
            "priorities": [
                {
                    "rank": 1,
                    "proposal_id": "p1",
                    "score": 0.90,
                    "urgency_score": 0.6,
                    "context_score": 0.3,
                    "source_module": "task_module",
                },
                {
                    "rank": 2,
                    "proposal_id": "p2",
                    "score": 0.80,
                    "urgency_score": 0.5,
                    "context_score": 0.2,
                    "source_module": "task_module",
                },
            ],
            "warnings": [],
            "risks": [],
        }

    if name == "overloaded_day":
        return {
            "scheduled_actions": [
                {
                    "proposal_id": "p1",
                    "score": 0.91,
                    "normalized_priority": 5.0,
                    "ordering_position": 0,
                    "bucket": "morning",
                }
            ],
            "unscheduled_actions": [
                {
                    "proposal_id": "p2",
                    "score": 0.70,
                    "normalized_priority": 4.0,
                    "ordering_position": 1,
                    "unscheduled_reason": "capacity_exceeded",
                },
                {
                    "proposal_id": "p3",
                    "score": 0.60,
                    "normalized_priority": 3.0,
                    "ordering_position": 2,
                    "unscheduled_reason": "capacity_exceeded",
                },
            ],
            "priorities": [
                {
                    "rank": 1,
                    "proposal_id": "p1",
                    "score": 0.91,
                    "urgency_score": 0.7,
                    "context_score": 0.2,
                    "source_module": "task_module",
                },
                {
                    "rank": 2,
                    "proposal_id": "p2",
                    "score": 0.70,
                    "urgency_score": 0.5,
                    "context_score": 0.2,
                    "source_module": "task_module",
                },
                {
                    "rank": 3,
                    "proposal_id": "p3",
                    "score": 0.60,
                    "urgency_score": 0.4,
                    "context_score": 0.2,
                    "source_module": "task_module",
                },
            ],
            "warnings": [{"code": "W_CAP", "message": "Capacity exceeded", "severity": "medium"}],
            "risks": [{"code": "R_DELAY", "message": "Potential delay", "severity": "medium"}],
        }

    if name == "empty_day":
        return {
            "scheduled_actions": [],
            "unscheduled_actions": [],
            "priorities": [],
            "warnings": [],
            "risks": [],
        }

    raise ValueError(f"Unsupported scenario: {name}")


def _semantic_normalize(value: Any) -> Any:
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if key in {"proposal_id", "generated_at", "start_time", "end_time", "date"}:
                continue
            normalized[key] = _semantic_normalize(item)
        return normalized
    if isinstance(value, list):
        return [_semantic_normalize(item) for item in value]
    return value


@pytest.mark.parametrize(
    ("scenario", "proposal_ids"),
    [
        ("busy_day", ["p1", "p2"]),
        ("overloaded_day", ["p1", "p2", "p3"]),
        ("empty_day", []),
    ],
)
def test_brief_v1_contract_golden_scenarios(
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
    proposal_ids: list[str],
) -> None:
    decision_output = _decision_output_for_scenario(scenario)

    def _fake_decision_engine(*_args, **_kwargs):
        return deepcopy(decision_output)

    monkeypatch.setattr("apps.api.services.synthesis_engine.run_decision_engine_v2", _fake_decision_engine)

    orchestrator_output = _orchestrator_output(proposal_ids)

    first_brief = build_daily_brief(f"hh-brief-v1-{scenario}", orchestrator_output=orchestrator_output)
    second_brief = build_daily_brief(f"hh-brief-v1-{scenario}", orchestrator_output=orchestrator_output)

    first_validation = validate_brief_v1(first_brief, enabled=True, raise_on_error=False)
    second_validation = validate_brief_v1(second_brief, enabled=True, raise_on_error=False)

    assert first_validation["valid"], first_validation["errors"]
    assert second_validation["valid"], second_validation["errors"]

    brief_v1 = first_validation["brief_v1"]
    assert brief_v1 is not None
    assert set(brief_v1.keys()) == set(BRIEF_V1_REQUIRED_FIELDS)

    projected = project_brief_to_v1(first_brief)
    assert set(projected.keys()) == set(BRIEF_V1_REQUIRED_FIELDS)

    # Semantic determinism: repeated runs must match when ignoring IDs/timestamps.
    assert _semantic_normalize(first_validation["brief_v1"]) == _semantic_normalize(second_validation["brief_v1"])
