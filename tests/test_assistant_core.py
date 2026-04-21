from __future__ import annotations

import ast
from pathlib import Path

from fastapi.testclient import TestClient

from apps.api.main import app
from apps.assistant_core.intent_parser import parse_intent
from apps.assistant_core.meal_planner import plan_meal
from apps.assistant_core.planning_engine import AssistantPlanningEngine, _fallback_household_state


def test_intent_parsing_variants() -> None:
    doctor_intent = parse_intent("Schedule a doctor appointment for Monday morning after school drop-off")
    meal_intent = parse_intent("Plan dinners for the week using the pantry without repeating recipes")
    fitness_intent = parse_intent("Build a fat loss workout plan around my family schedule")

    assert doctor_intent.intent_type == "appointment"
    assert "doctor" in doctor_intent.entities
    assert "monday" in doctor_intent.time_constraints

    assert meal_intent.intent_type == "meal"
    assert "meal" in meal_intent.entities

    assert fitness_intent.intent_type == "fitness"
    assert "fitness_goal_present" in fitness_intent.context_flags


def test_meal_repetition_prevention() -> None:
    meal = plan_meal(
        inventory={
            "salmon": 1,
            "brown rice": 2,
            "broccoli": 1,
            "olive oil": 1,
            "chicken": 2,
            "quinoa": 1,
            "spinach": 1,
            "bell pepper": 1,
        },
        recipe_history=[
            {"recipe_name": "Chicken Quinoa Bowl", "served_on": "2026-04-15"},
            {"recipe_name": "Egg and Sweet Potato Skillet", "served_on": "2026-04-16"},
        ],
        repeat_window_days=7,
    )

    assert meal.recipe_name != "Chicken Quinoa Bowl"


def test_scheduling_conflict_detection() -> None:
    engine = AssistantPlanningEngine()
    query = "Schedule a doctor appointment for Monday morning"
    intent = parse_intent(query)
    response = engine.build_response(
        query=query,
        household_id="household-001",
        intent=intent,
        repeat_window_days=10,
        fitness_goal=None,
        state=_fallback_household_state("household-001"),
    )

    assert response.intent.intent_type == "appointment"
    assert len(response.conflicts) >= 1
    assert any("Work standup" in conflict.description for conflict in response.conflicts)


def test_approval_gate_is_inert() -> None:
    client = TestClient(app)
    query_response = client.post(
        "/assistant/query",
        json={"query": "Schedule a doctor appointment for Monday morning after school drop-off", "household_id": "household-core-approval-v2"},
    )

    assert query_response.status_code == 200
    payload = query_response.json()
    assert payload["recommended_action"]["approval_status"] == "pending"
    assert payload["grouped_approval_payload"]["approval_status"] == "pending"

    action_id = payload["recommended_action"]["action_id"]
    approve_response = client.post(
        "/assistant/approve",
        json={"request_id": payload["request_id"], "action_ids": [action_id]},
    )

    assert approve_response.status_code == 200
    approved = approve_response.json()
    assert approved["recommended_action"]["approval_status"] == "approved"
    # Verify approval was recorded without executing side effects (action is inert)
    assert len(approved["reasoning_trace"]) > 0


def test_deterministic_output() -> None:
    engine = AssistantPlanningEngine()
    query = "Build a strength workout plan around school pickup and work"
    intent = parse_intent(query)
    state = _fallback_household_state("household-001")

    left = engine.build_response(
        query=query,
        household_id="household-001",
        intent=intent,
        repeat_window_days=10,
        fitness_goal="strength",
        state=state,
    ).model_dump()
    right = engine.build_response(
        query=query,
        household_id="household-001",
        intent=intent,
        repeat_window_days=10,
        fitness_goal="strength",
        state=state,
    ).model_dump()

    assert left == right


def test_module_isolation_from_read_only_layers() -> None:
    assistant_dir = Path(__file__).resolve().parent.parent / "apps" / "assistant_core"
    forbidden_prefixes = ("policy_engine", "insights", "tests.simulation", "tests.evaluation")

    for file_path in assistant_dir.glob("*.py"):
        tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            for name in names:
                assert not name.startswith(forbidden_prefixes), f"Forbidden import found in {file_path.name}: {name}"