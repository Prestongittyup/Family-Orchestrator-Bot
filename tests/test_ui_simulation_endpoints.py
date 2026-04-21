from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.main import app


def test_simulation_run_endpoint_returns_structured_payload() -> None:
    with TestClient(app) as client:
        response = client.get(
            "/simulation/run",
            params={
                "seed": 44,
                "household_size": 4,
                "chaos_level": "medium",
                "event_density": 10,
                "scenario_preset": "after_school_rush",
            },
        )

    assert response.status_code == 200
    payload = response.json()

    assert "event_timeline" in payload
    assert "brief_outputs_over_time" in payload
    assert "decision_drift_metrics" in payload
    assert "stability_scores" in payload
    assert "failure_patterns" in payload
    assert "system_recovery_metrics" in payload


def test_simulation_stream_and_results_endpoints() -> None:
    events = [
        {
            "event_id": "evt-1",
            "timestamp": "2026-04-18T08:00:00Z",
            "type": "work_event",
            "title": "Work Sync",
            "start_time": "2026-04-18T09:00:00Z",
            "end_time": "2026-04-18T10:00:00Z",
            "participants": ["Member 1"],
            "payload": {
                "title": "Work Sync",
                "start_time": "2026-04-18T09:00:00Z",
                "end_time": "2026-04-18T10:00:00Z",
                "priority_hint": "normal",
            },
        }
    ]

    with TestClient(app) as client:
        stream_response = client.post(
            "/simulation/stream-events",
            json={
                "seed": 15,
                "household_size": 3,
                "chaos_level": "low",
                "scenario_preset": "streamed",
                "events": events,
            },
        )
        assert stream_response.status_code == 200

        results_response = client.get("/simulation/results")
        assert results_response.status_code == 200

    payload = results_response.json()
    assert "simulation_id" in payload
    assert "event_timeline" in payload
    assert isinstance(payload["event_timeline"], list)
