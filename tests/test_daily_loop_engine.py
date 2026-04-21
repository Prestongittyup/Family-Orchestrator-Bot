from __future__ import annotations

from copy import deepcopy
from datetime import datetime

from fastapi.testclient import TestClient

from apps.api.main import app
from apps.assistant_core import assistant_router
from apps.assistant_core.planning_engine import _fallback_household_state
from assistant.daily_loop.daily_loop_engine import DEFAULT_DAILY_LOOP_QUERY, DailyLoopEngine
from assistant.daily_loop.time_slicer import parse_iso_datetime
from assistant.runtime.assistant_runtime import AssistantRuntimeEngine


def _schedule_ranges(schedule: list[dict]) -> list[tuple[datetime, datetime, int, int]]:
    rows = []
    for item in schedule:
        rows.append(
            (
                parse_iso_datetime(item["start"]),
                parse_iso_datetime(item["end"]),
                int(item["buffer_before_minutes"]),
                int(item["buffer_after_minutes"]),
            )
        )
    return rows


def test_daily_loop_output_is_deterministic() -> None:
    engine = DailyLoopEngine()
    state = _fallback_household_state("household-001")

    left = engine.generate(
        query=DEFAULT_DAILY_LOOP_QUERY,
        household_id="household-001",
        repeat_window_days=10,
        fitness_goal="fat loss",
        state=state,
    ).plan.model_dump()
    right = engine.generate(
        query=DEFAULT_DAILY_LOOP_QUERY,
        household_id="household-001",
        repeat_window_days=10,
        fitness_goal="fat loss",
        state=state,
    ).plan.model_dump()

    assert left == right


def test_daily_loop_has_no_overlapping_time_blocks() -> None:
    payload = DailyLoopEngine().generate(
        query="Plan today around appointments, dinner, and a workout after school pickup",
        household_id="household-001",
        repeat_window_days=10,
        fitness_goal="fat loss",
        state=_fallback_household_state("household-001"),
    ).plan.model_dump()

    rows = sorted(_schedule_ranges(payload["schedule"]), key=lambda item: item[0])
    for index in range(1, len(rows)):
        previous = rows[index - 1]
        current = rows[index]
        assert current[0] >= previous[1]


def test_daily_loop_places_meals_and_workouts() -> None:
    plan = DailyLoopEngine().generate(
        query="Plan today with dinner and a workout around the family schedule",
        household_id="household-001",
        repeat_window_days=10,
        fitness_goal="strength",
        state=_fallback_household_state("household-001"),
    ).plan

    assert len(plan.meals) >= 1
    assert len(plan.workouts) >= 1
    assert all(item.segment == "evening" for item in plan.meals)
    assert all(item.source_proposal_id for item in plan.workouts)


def test_daily_loop_preserves_buffer_time() -> None:
    payload = DailyLoopEngine().generate(
        query="Plan today with appointments, dinner, and a workout around the family schedule",
        household_id="household-001",
        repeat_window_days=10,
        fitness_goal="fat loss",
        state=_fallback_household_state("household-001"),
    ).plan.model_dump()

    rows = sorted(_schedule_ranges(payload["schedule"]), key=lambda item: item[0])
    for index in range(1, len(rows)):
        previous_end = rows[index - 1][1]
        current_start = rows[index][0]
        min_gap = min(rows[index - 1][3], rows[index][2])
        assert int((current_start - previous_end).total_seconds() // 60) >= min_gap


def test_daily_loop_consumes_runtime_plan_output() -> None:
    runtime_result = AssistantRuntimeEngine().run(
        query="Plan today with appointments, dinner, and a workout around the family schedule",
        household_id="household-001",
        repeat_window_days=10,
        fitness_goal="fat loss",
        state=_fallback_household_state("household-001"),
    )
    daily_result = DailyLoopEngine().build_from_runtime_plan(runtime_result.plan)

    assert daily_result.plan.approval_state.request_id == runtime_result.plan.request_id
    assert len(daily_result.plan.schedule) >= 1
    assert any(item.domain == "meal" for item in daily_result.plan.schedule)


def test_daily_loop_generation_is_read_only() -> None:
    engine = DailyLoopEngine()
    state = _fallback_household_state("household-001")
    original = deepcopy(state.as_dict())

    plan = engine.generate(
        query=DEFAULT_DAILY_LOOP_QUERY,
        household_id="household-001",
        repeat_window_days=10,
        fitness_goal="fat loss",
        state=state,
    ).plan

    assert state.as_dict() == original
    assert plan.approval_state.approved is False
    assert all(action.approval_status == "pending" for action in plan.approval_state.proposed_actions)


def test_daily_get_endpoint_does_not_persist(monkeypatch) -> None:
    def _fail(*_args, **_kwargs):
        raise AssertionError("GET /assistant/daily must not persist approval state")

    monkeypatch.setattr(assistant_router.request_store, "save", _fail)

    client = TestClient(app)
    response = client.get("/assistant/daily")

    assert response.status_code == 200
    payload = response.json()
    assert payload["approval_state"]["persisted"] is False


def test_daily_regenerate_endpoint_persists() -> None:
    client = TestClient(app)
    response = client.post(
        "/assistant/daily/regenerate",
        json={"query": DEFAULT_DAILY_LOOP_QUERY, "fitness_goal": "fat loss"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["approval_state"]["persisted"] is True
    assert payload["approval_state"]["approved"] is False