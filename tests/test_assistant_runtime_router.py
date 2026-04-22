from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from apps.api.main import app
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.core.lifecycle_state import LifecycleState
from household_os.runtime.orchestrator import HouseholdOSOrchestrator


def _test_client_with_temp_runtime_store(tmp_path):
    import apps.api.assistant_runtime_router as assistant_runtime_router

    store = HouseholdStateGraphStore(graph_path=Path(tmp_path) / "assistant_runtime_router_graph.json")
    assistant_runtime_router.runtime_orchestrator = HouseholdOSOrchestrator(state_store=store)
    return TestClient(app), assistant_runtime_router.runtime_orchestrator


def test_run_endpoint_returns_action(tmp_path):
    client, _orchestrator = _test_client_with_temp_runtime_store(tmp_path)

    response = client.post(
        "/assistant/run",
        json={
            "message": "I need to start working out",
            "household_id": "runtime-api-household",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["action_id"]
    assert "Schedule" in payload["recommendation"] or "Create" in payload["recommendation"] or "Adjust" in payload["recommendation"]
    assert isinstance(payload["why"], list)
    assert isinstance(payload["impact"], str)
    assert payload["approval_required"] is True


def test_approval_executes_action(tmp_path):
    client, orchestrator = _test_client_with_temp_runtime_store(tmp_path)
    run_response = client.post(
        "/assistant/run",
        json={
            "message": "I need to start working out",
            "household_id": "runtime-api-household",
        },
    )
    action_id = run_response.json()["action_id"]

    approve_response = client.post(
        "/assistant/approve",
        json={
            "action_id": action_id,
            "household_id": "runtime-api-household",
        },
    )

    assert approve_response.status_code == 200
    payload = approve_response.json()
    assert payload["status"] == "executed"
    assert len(payload["effects"]) == 1
    assert payload["effects"][0]["handler"] == "calendar_update"

    graph = orchestrator.state_store.load_graph("runtime-api-household")
    action = graph["action_lifecycle"]["actions"][action_id]
    assert action["current_state"] == LifecycleState.COMMITTED


def test_today_view_returns_state(tmp_path):
    client, _orchestrator = _test_client_with_temp_runtime_store(tmp_path)
    run_response = client.post(
        "/assistant/run",
        json={
            "message": "I need to start working out",
            "household_id": "runtime-api-household",
        },
    )
    action_id = run_response.json()["action_id"]

    before_approval = client.get("/assistant/today", params={"household_id": "runtime-api-household"})
    assert before_approval.status_code == 200
    before_payload = before_approval.json()
    assert any(item["action_id"] == action_id for item in before_payload["pending_actions"])
    assert before_payload["last_recommendation"] is not None

    approve_response = client.post(
        "/assistant/approve",
        json={
            "action_id": action_id,
            "household_id": "runtime-api-household",
        },
    )
    assert approve_response.status_code == 200

    after_approval = client.get("/assistant/today", params={"household_id": "runtime-api-household"})
    assert after_approval.status_code == 200
    after_payload = after_approval.json()
    assert not any(item["action_id"] == action_id for item in after_payload["pending_actions"])
    assert any(event["event_id"] == f"runtime-{action_id}" for event in after_payload["events"])