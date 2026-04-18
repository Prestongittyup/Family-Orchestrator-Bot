from __future__ import annotations

from datetime import datetime

from fastapi.testclient import TestClient

from apps.api import main
from apps.api.endpoints import brief_endpoint
from apps.api.services.calendar_service import create_recurring_event, schedule_event
from apps.api.services.synthesis_engine import build_daily_brief
from apps.api.services.task_service import create_task, update_task_metadata


def _overlaps(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    start_a = datetime.fromisoformat(a_start.replace("Z", "+00:00"))
    end_a = datetime.fromisoformat(a_end.replace("Z", "+00:00"))
    start_b = datetime.fromisoformat(b_start.replace("Z", "+00:00"))
    end_b = datetime.fromisoformat(b_end.replace("Z", "+00:00"))
    return start_a < end_b and start_b < end_a


def _collect_action_items(brief: dict) -> list[dict]:
    items: list[dict] = []
    items.extend(brief.get("schedule", []))
    items.extend(brief.get("suggestions", []))
    items.extend(brief.get("interrupts", []))

    personal_agendas = brief.get("personal_agendas", {})
    items.extend(personal_agendas.get("tasks", []))
    items.extend(personal_agendas.get("notifications", []))

    return items


def _assert_brief_contract(brief: dict) -> None:
    assert "personal_agendas" in brief
    assert "tasks" in brief["personal_agendas"]
    assert "schedule" in brief
    assert "suggested_actions" in brief
    assert "priorities" in brief
    assert "warnings" in brief
    assert "risks" in brief
    assert "summary_text" in brief
    assert "time_based_schedule" in brief

    time_schedule = brief["time_based_schedule"]
    assert "morning" in time_schedule
    assert "afternoon" in time_schedule
    assert "evening" in time_schedule


def test_system_integration_deterministic_brief_output() -> None:
    household_id = "hh-system-integration"

    brief_endpoint._clear_brief_cache()

    # Seed calendar events: one near-term baseline and one higher priority recurring.
    schedule_event(
        household_id=household_id,
        user_id="u-integration",
        title="Event A Near Term",
        description="Near-term baseline event",
    )
    create_recurring_event(
        household_id=household_id,
        user_id="u-integration",
        title="Event B High Priority",
        frequency="weekly",
        description="High-priority recurring event",
    )

    # Seed task mix with different priorities.
    task_high = create_task(
        household_id=household_id,
        title="Task High Priority",
        description="Due soon",
    )
    update_task_metadata(task_high.id, "high", "operations")

    task_medium = create_task(
        household_id=household_id,
        title="Task Medium Priority",
        description="Due later",
    )
    update_task_metadata(task_medium.id, "medium", "planning")

    task_low = create_task(
        household_id=household_id,
        title="Task Low Priority",
        description="Due tonight",
    )
    update_task_metadata(task_low.id, "low", "maintenance")

    client = TestClient(main.app)

    responses: list[dict] = []
    for _ in range(3):
        response = client.get(f"/brief/{household_id}")
        assert response.status_code == 200
        payload = response.json()
        assert payload.get("status") == "success"
        responses.append(payload)

    # Deterministic output requirement: identical payloads across repeated runs.
    assert responses[0] == responses[1] == responses[2]

    brief = responses[0]["brief"]
    _assert_brief_contract(brief)

    items = _collect_action_items(brief)

    # Multi-module integrity: both calendar + task derived items must exist.
    assert any(item.get("source_module") == "calendar_module" for item in items)
    assert any(item.get("source_module") == "task_module" for item in items)

    # No duplicates by proposal_id.
    proposal_ids = [str(item.get("proposal_id")) for item in items if item.get("proposal_id") is not None]
    assert len(proposal_ids) == len(set(proposal_ids))

    # No malformed action entries.
    for item in items:
        assert isinstance(item, dict)
        assert isinstance(item.get("title"), str)
        assert isinstance(item.get("description"), str)
        assert isinstance(item.get("source_module"), str)
        assert isinstance(item.get("decision_type"), str)

    scheduled_items = [row for row in brief.get("suggested_actions", []) if row.get("decision_type") == "scheduled"]
    for row in scheduled_items:
        assert isinstance(row.get("start_time"), str)
        assert isinstance(row.get("end_time"), str)

    for i in range(len(scheduled_items)):
        for j in range(i + 1, len(scheduled_items)):
            assert not _overlaps(
                str(scheduled_items[i]["start_time"]),
                str(scheduled_items[i]["end_time"]),
                str(scheduled_items[j]["start_time"]),
                str(scheduled_items[j]["end_time"]),
            )

    # Ranked priorities must exist and be deterministic in ordering.
    priorities = brief.get("priorities", [])
    assert isinstance(priorities, list)
    if priorities:
        normalized = [float(row.get("normalized_priority", 0.0)) for row in priorities]
        assert normalized == sorted(normalized, reverse=True)


def test_brief_output_fixed_fixture_shape_and_order() -> None:
    orchestrator_output = {
        "proposals": [
            {
                "id": "p-alpha",
                "type": "task_action",
                "title": "Alpha",
                "description": "reference=p-alpha; time_window=2026-04-16T09:00:00->2026-04-16T10:00:00",
                "priority": 5,
                "source_module": "task_module",
                "duration": 1,
                "effort": "medium",
                "category": "task",
                "normalized_priority": 5.0,
            },
            {
                "id": "p-beta",
                "type": "task_action",
                "title": "Beta",
                "description": "reference=p-beta; time_window=2026-04-16T10:00:00->2026-04-16T11:00:00",
                "priority": 4,
                "source_module": "task_module",
                "duration": 1,
                "effort": "low",
                "category": "task",
                "normalized_priority": 4.0,
            },
        ],
        "signals": [
            {
                "id": "s-events",
                "type": "events_today",
                "message": "events_today=1",
                "severity": "low",
                "source_module": "calendar_module",
            },
            {
                "id": "s-high",
                "type": "high_priority_events",
                "message": "high_priority_events=0",
                "severity": "low",
                "source_module": "calendar_module",
            },
        ],
        "semantic_layer": {
            "ordering_index": [
                {"proposal_id": "p-alpha", "position": 0},
                {"proposal_id": "p-beta", "position": 1},
            ]
        },
    }

    brief = build_daily_brief("hh-fixed-fixture", orchestrator_output=orchestrator_output)

    _assert_brief_contract(brief)
    assert [row.get("proposal_id") for row in brief.get("suggested_actions", [])] == ["p-alpha", "p-beta"]
    assert [row.get("proposal_id") for row in brief.get("personal_agendas", {}).get("tasks", [])] == ["p-alpha", "p-beta"]

    time_schedule = brief.get("time_based_schedule", {})
    assert [row.get("proposal_id") for row in time_schedule.get("morning", [])] == ["p-alpha", "p-beta"]
    assert time_schedule.get("afternoon", []) == []
    assert time_schedule.get("evening", []) == []
