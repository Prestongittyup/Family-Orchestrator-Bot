from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from apps.api.main import app
from apps.assistant_core.planning_engine import _fallback_household_state
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.core.lifecycle_state import LifecycleState
from household_os.runtime.orchestrator import HouseholdOSOrchestrator


def _test_client_with_temp_runtime_store(tmp_path):
    import apps.api.assistant_runtime_router as assistant_runtime_router

    store = HouseholdStateGraphStore(graph_path=Path(tmp_path) / "behavior_feedback_graph.json")
    assistant_runtime_router.runtime_orchestrator = HouseholdOSOrchestrator(state_store=store)
    return TestClient(app), assistant_runtime_router.runtime_orchestrator


def test_rejection_affects_future_recommendations(tmp_path):
    client, orchestrator = _test_client_with_temp_runtime_store(tmp_path)
    household_id = "behavior-rejection-household"

    for _ in range(4):
        run_response = client.post(
            "/assistant/run",
            json={"message": "I need to start working out", "household_id": household_id},
        )
        assert run_response.status_code == 200
        action_id = run_response.json()["action_id"]

        reject_response = client.post(
            "/assistant/reject",
            json={"action_id": action_id, "household_id": household_id},
        )
        assert reject_response.status_code == 200
        assert reject_response.json()["status"] == "rejected"

    next_response = client.post(
        "/assistant/run",
        json={"message": "I need to start working out", "household_id": household_id},
    )

    assert next_response.status_code == 200
    payload = next_response.json()
    assert "tomorrow evening at 6:30 PM" in payload["recommendation"]

    graph = orchestrator.state_store.load_graph(household_id)
    records = graph["behavior_feedback"]["records"]
    assert sum(1 for item in records if item["status"] == LifecycleState.REJECTED and item["category"] == "fitness") >= 4


def test_preferred_time_is_reused(tmp_path):
    client, orchestrator = _test_client_with_temp_runtime_store(tmp_path)
    household_id = "behavior-preferred-time-household"
    state = _fallback_household_state(household_id)
    graph = orchestrator.state_store.refresh_graph(
        household_id=household_id,
        state=state,
        query="bootstrap",
        fitness_goal="consistency",
    )
    graph["behavior_feedback"]["records"].append(
        {
            "action_id": "prior-approved-evening",
            "status": LifecycleState.APPROVED,
            "executed": True,
            "timestamp": "2026-04-18T18:30:00Z",
            "category": "fitness",
            "scheduled_time": "2026-04-18 18:30-19:15",
            "actual_execution_time": "2026-04-18T18:30:00Z",
        }
    )
    orchestrator.state_store.save_graph(graph)

    response = client.post(
        "/assistant/run",
        json={"message": "I need to start working out", "household_id": household_id},
    )

    assert response.status_code == 200
    payload = response.json()
    assert "evening at 6:30 PM" in payload["recommendation"]


def test_ignored_actions_reduce_priority(tmp_path):
    client, orchestrator = _test_client_with_temp_runtime_store(tmp_path)
    household_id = "behavior-ignored-household"
    state = _fallback_household_state(household_id)
    graph = orchestrator.state_store.refresh_graph(
        household_id=household_id,
        state=state,
        query="bootstrap",
        fitness_goal="consistency",
    )
    for index in range(3):
        graph["behavior_feedback"]["records"].append(
            {
                "action_id": f"ignored-fitness-{index}",
                "status": LifecycleState.FAILED,
                "executed": False,
                "timestamp": f"2026-04-1{index}T06:00:00Z",
                "category": "fitness",
                "scheduled_time": f"2026-04-1{index} 06:00-06:45",
                "actual_execution_time": None,
            }
        )
    orchestrator.state_store.save_graph(graph)

    response = client.post(
        "/assistant/run",
        json={"message": "I need to start working out", "household_id": household_id},
    )

    assert response.status_code == 200
    payload = response.json()
    assert "30-minute workout" in payload["recommendation"]
