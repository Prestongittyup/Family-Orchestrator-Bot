from __future__ import annotations

from copy import deepcopy
from typing import Any

from apps.api.endpoints import brief_endpoint
from apps.api.endpoints.brief_invariants_v1 import validate_brief_v1
from apps.api.endpoints.brief_renderer_v1 import render_brief_v1


def _sample_brief_v1() -> dict[str, Any]:
    return {
        "scheduled_actions": [
            {
                "proposal_id": "sched-001",
                "title": "Morning school drop-off",
                "source_module": "task_module",
                "decision_type": "scheduled",
                "ordering_position": 0,
                "start_time": "2026-04-16T09:00:00",
                "end_time": "2026-04-16T09:30:00",
                "time_bucket": "morning",
            },
            {
                "proposal_id": "sched-002",
                "title": "Afternoon grocery pickup",
                "source_module": "task_module",
                "decision_type": "scheduled",
                "ordering_position": 1,
                "start_time": "2026-04-16T14:00:00",
                "end_time": "2026-04-16T14:30:00",
                "time_bucket": "afternoon",
            },
            {
                "proposal_id": "sched-003",
                "title": "Evening meal prep",
                "source_module": "task_module",
                "decision_type": "scheduled",
                "ordering_position": 2,
                "start_time": "2026-04-16T19:00:00",
                "end_time": "2026-04-16T19:45:00",
                "time_bucket": "evening",
            },
        ],
        "unscheduled_actions": [
            {
                "proposal_id": "defer-001",
                "title": "Renew insurance",
                "source_module": "task_module",
                "decision_type": "deferred",
                "reason": "capacity_exceeded",
                "ordering_position": 0,
                "normalized_priority": 4.0,
            }
        ],
        "priorities": [
            {
                "rank": 1,
                "proposal_id": "sched-001",
                "title": "Morning school drop-off",
                "source_module": "task_module",
                "normalized_priority": 0.91,
                "score": 0.91,
            },
            {
                "rank": 2,
                "proposal_id": "sched-002",
                "title": "Afternoon grocery pickup",
                "source_module": "task_module",
                "normalized_priority": 0.8,
                "score": 0.8,
            },
        ],
        "warnings": [
            {"code": "W_TIME", "message": "Tight time windows", "severity": "medium"}
        ],
        "risks": [
            {"code": "R_DELAY", "message": "Traffic risk", "severity": "low"}
        ],
        "summary": "3 scheduled actions, 1 deferred action, 1 warning, 1 risk.",
    }


def _validated_brief_v1() -> dict[str, Any]:
    validation = validate_brief_v1(_sample_brief_v1(), enabled=True, raise_on_error=False)
    assert validation["valid"], validation["errors"]
    return validation["brief_v1"]


def test_renderer_output_is_deterministic_for_same_input() -> None:
    brief_v1 = _validated_brief_v1()

    first = render_brief_v1(deepcopy(brief_v1))
    second = render_brief_v1(deepcopy(brief_v1))

    assert first == second


def test_renderer_hides_internal_ids_and_is_readable() -> None:
    brief_v1 = _validated_brief_v1()
    rendered = render_brief_v1(brief_v1)

    assert "proposal_id" not in rendered
    assert "trace_id" not in rendered
    assert "sched-001" not in rendered
    assert "defer-001" not in rendered

    assert "Today's Plan" in rendered
    assert "Unscheduled / Deferred" in rendered
    assert "Priorities" in rendered
    assert "Warnings" in rendered
    assert "Risks" in rendered

    assert "{" not in rendered
    assert "}" not in rendered


def test_renderer_groups_actions_by_morning_afternoon_evening() -> None:
    brief_v1 = _validated_brief_v1()
    rendered = render_brief_v1(brief_v1)

    morning_index = rendered.find("Morning")
    afternoon_index = rendered.find("Afternoon")
    evening_index = rendered.find("Evening")

    assert morning_index != -1
    assert afternoon_index != -1
    assert evening_index != -1
    assert morning_index < afternoon_index < evening_index

    assert "9:00 AM-9:30 AM | Morning school drop-off" in rendered
    assert "2:00 PM-2:30 PM | Afternoon grocery pickup" in rendered
    assert "7:00 PM-7:45 PM | Evening meal prep" in rendered


def test_renderer_markdown_format_supported() -> None:
    brief_v1 = _validated_brief_v1()
    rendered = render_brief_v1(brief_v1, output_format="markdown")

    assert "## Today's Plan" in rendered
    assert "### Morning" in rendered
    assert "## Unscheduled / Deferred" in rendered


def test_brief_endpoint_render_human_non_breaking(test_client) -> None:
    brief_endpoint._clear_brief_cache()

    default_response = test_client.get("/brief/hh-001")
    assert default_response.status_code == 200
    default_payload = default_response.json()

    rendered_response = test_client.get("/brief/hh-001?render_human=true")
    assert rendered_response.status_code == 200
    rendered_payload = rendered_response.json()

    assert rendered_payload["status"] == default_payload["status"]
    assert rendered_payload["brief"] == default_payload["brief"]
    assert isinstance(rendered_payload.get("rendered"), str)
    assert rendered_payload["rendered"].strip() != ""
