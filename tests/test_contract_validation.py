from __future__ import annotations

import pytest

from apps.api.services import decision_engine
from apps.api.services.decision_engine import run_decision_engine_v2
from apps.api.services.synthesis_engine import build_daily_brief
from modules.core.services.contract_registry import (
    validate_brief_output_contract,
    validate_module_output_contract,
)


def _proposal(proposal_id: str, *, priority: float) -> dict:
    return {
        "id": proposal_id,
        "type": "task_action",
        "title": proposal_id,
        "description": "test",
        "priority": priority,
        "normalized_priority": priority,
        "source_module": "task_module",
        "duration": 1,
        "effort": "medium",
        "category": "task",
    }


def _signal(signal_id: str, *, signal_type: str, message: str, severity: str = "medium") -> dict:
    return {
        "id": signal_id,
        "type": signal_type,
        "message": message,
        "severity": severity,
        "source_module": "calendar_module",
    }


def test_valid_module_output_contract_passes() -> None:
    output = {
        "module": "task_module",
        "proposals": [
            {
                "id": "p1",
                "type": "task_action",
                "title": "Do laundry",
                "description": "test",
                "priority": 4,
                "source_module": "task_module",
                "duration": 1,
                "effort": "low",
                "category": "task",
            }
        ],
        "signals": [
            {
                "id": "s1",
                "type": "events_today",
                "message": "events_today=1",
                "severity": "low",
                "source_module": "calendar_module",
            }
        ],
        "confidence": 0.9,
        "metadata": {},
    }

    assert validate_module_output_contract(output) == output


def test_invalid_proposal_schema_fails_fast() -> None:
    payload = {
        "proposals": [
            {
                "id": "bad-1",
                "type": "task_action",
                "title": "Bad",
                "description": "invalid source_module type",
                "priority": 5,
                "source_module": 123,
            }
        ],
        "signals": [_signal("s1", signal_type="events_today", message="events_today=1")],
    }

    with pytest.raises(ValueError):
        run_decision_engine_v2(payload)


def test_invalid_decision_output_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "proposals": [_proposal("p1", priority=9.0)],
        "signals": [_signal("s1", signal_type="events_today", message="events_today=0")],
    }

    def _broken_serialize_rows(rows: list[dict]) -> list[dict]:
        # Intentionally break output contract to verify boundary fail-fast.
        return [{"proposal_id": "p1"}] if rows else []

    monkeypatch.setattr(decision_engine_v2, "_serialize_rows", _broken_serialize_rows)

    with pytest.raises(ValueError):
        run_decision_engine_v2(payload)


def test_brief_output_matches_schema_exactly() -> None:
    orchestrator_output = {
        "proposals": [
            {
                "id": "p1",
                "type": "task_action",
                "title": "Pay bill",
                "description": "reference=p1; time_window=2026-04-16T10:00:00->2026-04-16T11:00:00",
                "priority": 5,
                "source_module": "task_module",
                "duration": 1,
                "effort": "medium",
                "category": "task",
                "normalized_priority": 5.0,
            }
        ],
        "signals": [_signal("s1", signal_type="events_today", message="events_today=1")],
        "semantic_layer": {"ordering_index": [{"proposal_id": "p1", "position": 0}]},
    }

    brief = build_daily_brief("hh-contract-brief", orchestrator_output=orchestrator_output)
    assert validate_brief_output_contract(brief) == brief

    expected_keys = {
        "household_id",
        "date",
        "schedule",
        "personal_agendas",
        "suggestions",
        "suggested_actions",
        "priorities",
        "warnings",
        "risks",
        "summary_text",
        "time_based_schedule",
        "financial",
        "meals",
        "interrupts",
        "meta",
    }
    assert set(brief.keys()) == expected_keys


def test_deterministic_run_is_identical_with_contract_layer() -> None:
    orchestrator_output = {
        "proposals": [
            {
                "id": "p1",
                "type": "task_action",
                "title": "Task one",
                "description": "test",
                "priority": 5,
                "source_module": "task_module",
                "duration": 1,
                "effort": "medium",
                "category": "task",
                "normalized_priority": 5.0,
            },
            {
                "id": "p2",
                "type": "task_action",
                "title": "Task two",
                "description": "test",
                "priority": 4,
                "source_module": "task_module",
                "duration": 1,
                "effort": "low",
                "category": "task",
                "normalized_priority": 4.0,
            },
        ],
        "signals": [
            _signal("s1", signal_type="events_today", message="events_today=1"),
            _signal("s2", signal_type="high_priority_events", message="high_priority_events=0", severity="low"),
        ],
        "semantic_layer": {
            "ordering_index": [
                {"proposal_id": "p1", "position": 0},
                {"proposal_id": "p2", "position": 1},
            ]
        },
    }

    first = build_daily_brief("hh-contract-determinism", orchestrator_output=orchestrator_output)
    second = build_daily_brief("hh-contract-determinism", orchestrator_output=orchestrator_output)

    assert first == second

