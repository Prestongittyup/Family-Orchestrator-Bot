from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.endpoints import evaluation_router


_REQUIRED_KEYS = {
    "timestamp",
    "household_id",
    "top_priorities",
    "schedule_actions",
    "conflicts",
    "system_notes",
}


def _assert_contract(payload: dict) -> None:
    assert set(payload.keys()) == _REQUIRED_KEYS
    assert isinstance(payload["timestamp"], str)
    assert isinstance(payload["household_id"], str)
    assert isinstance(payload["top_priorities"], list)
    assert isinstance(payload["schedule_actions"], list)
    assert isinstance(payload["conflicts"], list)
    assert isinstance(payload["system_notes"], list)

    for item in payload["top_priorities"]:
        assert set(item.keys()) == {"title", "priority_level", "reason"}
        assert item["priority_level"] in {"high", "medium", "low"}

    for item in payload["schedule_actions"]:
        assert set(item.keys()) == {"action", "time", "confidence"}
        assert isinstance(item["confidence"], float)
        assert 0.0 <= item["confidence"] <= 1.0

    for item in payload["conflicts"]:
        assert set(item.keys()) == {"conflict_type", "severity", "description"}
        assert item["severity"] in {"low", "medium", "high"}


def test_operational_api_contract_validity() -> None:
    with TestClient(app) as client:
        response = client.get("/operational/run", params={"household_id": "hh-contract"})

    assert response.status_code == 200
    _assert_contract(response.json())


def test_operational_pipeline_isolation_from_simulation_and_evaluation(monkeypatch) -> None:
    def _fail(*_args, **_kwargs):
        raise AssertionError("Evaluation or simulation paths should not be called in operational mode")

    monkeypatch.setattr(evaluation_router, "run_live_simulation", _fail)
    monkeypatch.setattr(evaluation_router, "run_stress_scenarios", _fail)

    with TestClient(app) as client:
        response = client.get("/operational/run", params={"household_id": "hh-isolation"})

    assert response.status_code == 200
    _assert_contract(response.json())


def test_operational_output_structure_is_deterministic() -> None:
    with TestClient(app) as client:
        first = client.get("/operational/context", params={"household_id": "hh-deterministic"})
        second = client.get("/operational/context", params={"household_id": "hh-deterministic"})

    assert first.status_code == 200
    assert second.status_code == 200

    first_payload = first.json()
    second_payload = second.json()

    _assert_contract(first_payload)
    _assert_contract(second_payload)

    assert set(first_payload.keys()) == set(second_payload.keys())
    assert [set(item.keys()) for item in first_payload["top_priorities"]] == [
        set(item.keys()) for item in second_payload["top_priorities"]
    ]
    assert [set(item.keys()) for item in first_payload["schedule_actions"]] == [
        set(item.keys()) for item in second_payload["schedule_actions"]
    ]
    assert [set(item.keys()) for item in first_payload["conflicts"]] == [
        set(item.keys()) for item in second_payload["conflicts"]
    ]


def test_operational_brief_executes_real_pipeline() -> None:
    with TestClient(app) as client:
        response = client.get("/operational/brief", params={"household_id": "hh-real-pipeline"})

    assert response.status_code == 200
    payload = response.json()

    _assert_contract(payload)
    assert payload["household_id"] == "hh-real-pipeline"
    assert len(payload["system_notes"]) > 0
