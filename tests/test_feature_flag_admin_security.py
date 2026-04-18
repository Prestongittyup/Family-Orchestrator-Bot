from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


def test_unauthorized_write_rejected_in_prod(test_client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("FOB_ADMIN_TOKEN", "super-secret")

    response = test_client.put(
        "/config/feature-flags/household/hh-001",
        json={"debug_mode": True},
    )

    assert response.status_code == 403
    assert "authorization" in response.json()["detail"].lower()


def test_authorized_write_succeeds_in_prod(test_client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("FOB_ADMIN_TOKEN", "super-secret")

    response = test_client.put(
        "/config/feature-flags/household/hh-001",
        json={"debug_mode": True},
        headers={"X-Admin-Token": "super-secret"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["household_id"] == "hh-001"
    assert data["effective"]["debug_mode"] is True


def test_read_access_always_allowed(test_client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("FOB_ADMIN_TOKEN", "super-secret")

    response = test_client.get("/config/feature-flags?household_id=hh-001")
    assert response.status_code == 200
    data = response.json()
    assert data["household_id"] == "hh-001"
    assert "effective" in data


def test_control_plane_security_has_no_os1_determinism_impact(
    test_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("FOB_ADMIN_TOKEN", "super-secret")

    event_payload = {
        "household_id": "hh-001",
        "type": "task_created",
        "source": "test",
        "payload": {"title": "Admin guard isolation test"},
        "idempotency_key": "admin-guard-os1-determinism-key",
    }

    first = test_client.post("/event", json=event_payload)
    second = test_client.post("/event", json=event_payload)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["status"] == "processed"
    assert second.json()["status"] == "duplicate_ignored"
