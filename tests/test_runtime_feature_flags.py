from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from apps.api.core.feature_flags import (
    _reset_feature_flags_for_tests,
    resolve_feature_flags,
)


@pytest.fixture(autouse=True)
def reset_runtime_flags():
    _reset_feature_flags_for_tests()
    yield
    _reset_feature_flags_for_tests()
def test_environment_override_affects_resolution(test_client: TestClient):
    response = test_client.put(
        "/config/feature-flags/environment/development",
        json={"ingestion_enabled": False, "tracing_enabled": False},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["environment"] == "development"
    assert data["effective"]["ingestion_enabled"] is False
    assert data["effective"]["tracing_enabled"] is False


def test_household_override_wins_over_environment(test_client: TestClient):
    set_env = test_client.put(
        "/config/feature-flags/environment/development",
        json={"ingestion_enabled": False},
    )
    assert set_env.status_code == 200

    set_household = test_client.put(
        "/config/feature-flags/household/hh-001",
        json={"ingestion_enabled": True},
    )
    assert set_household.status_code == 200

    resolved = resolve_feature_flags(household_id="hh-001", environment="development")
    assert resolved.ingestion_enabled is True


def test_ingestion_can_be_disabled_per_household(test_client: TestClient):
    set_household = test_client.put(
        "/config/feature-flags/household/hh-001",
        json={"ingestion_enabled": False},
    )
    assert set_household.status_code == 200

    payload = {
        "source": "calendar_api",
        "type": "event_created",
        "timestamp": "2026-04-15T10:30:00Z",
        "data": {"event_id": "evt-disabled"},
    }
    response = test_client.post("/ingest/webhook", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "disabled"
    assert "trace_id" not in data


def test_tracing_can_be_disabled_per_household(test_client: TestClient):
    set_household = test_client.put(
        "/config/feature-flags/household/hh-001",
        json={"tracing_enabled": False},
    )
    assert set_household.status_code == 200

    payload = {
        "source": "calendar_api",
        "type": "event_created",
        "timestamp": "2026-04-15T11:30:00Z",
        "data": {"event_id": "evt-notrace"},
    }
    response = test_client.post("/ingest/webhook", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] in {"success", "duplicate_ignored"}
    assert "trace_id" not in data


def test_debug_mode_adds_debug_payload(test_client: TestClient):
    set_household = test_client.put(
        "/config/feature-flags/household/hh-001",
        json={"debug_mode": True},
    )
    assert set_household.status_code == 200

    payload = {
        "source": "calendar_api",
        "type": "event_created",
        "timestamp": "2026-04-15T12:30:00Z",
        "data": {"event_id": "evt-debug"},
    }
    ingest_response = test_client.post("/ingest/webhook", json=payload)
    assert ingest_response.status_code == 200
    ingest_data = ingest_response.json()
    assert "debug" in ingest_data

    brief_response = test_client.get("/brief/hh-001")
    assert brief_response.status_code == 200
    brief_data = brief_response.json()
    assert "debug" in brief_data
    assert brief_data["debug"]["feature_flags"]["debug_mode"] is True
