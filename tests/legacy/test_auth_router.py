from __future__ import annotations
import pytest

import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

from app.main import app
import archive.apps.api.endpoints.auth_router as auth_router

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



@pytest.mark.integration
@pytest.mark.legacy
@pytest.mark.flaky
def test_oauth_stub_uses_request_scoped_services(monkeypatch) -> None:
    build_calls: list[int] = []

    class _FakeRepo:
        def __init__(self, serial: int) -> None:
            self.serial = serial

        def get_user_by_email(self, _email: str):
            return None

        def list_devices_for_user(self, _user_id: str):
            return []

    class _FakeIdentity:
        def __init__(self, serial: int) -> None:
            self.serial = serial

        def register_user(self, **_kwargs):
            return SimpleNamespace(user=SimpleNamespace(user_id=f"user-{self.serial}"))

        def register_device(self, **_kwargs):
            return SimpleNamespace(device=SimpleNamespace(device_id=f"device-{self.serial}"))

        def bootstrap_identity(self, **_kwargs):
            serial = self.serial

            class _BootstrapResult:
                def model_dump(self):
                    return {"service_instance": serial}

            return _BootstrapResult()

    class _FakeTokens:
        def __init__(self, serial: int) -> None:
            self.serial = serial

        def issue_token_pair(self, **_kwargs):
            now = datetime.now(timezone.utc)
            return SimpleNamespace(
                access_token=f"access-{self.serial}",
                refresh_token=f"refresh-{self.serial}",
                access_expires_at=now + timedelta(minutes=15),
                refresh_expires_at=now + timedelta(days=30),
            )

    def _fake_build_services():
        serial = len(build_calls) + 1
        build_calls.append(serial)
        return _FakeRepo(serial), _FakeIdentity(serial), _FakeTokens(serial)

    monkeypatch.setattr(auth_router, "_build_services", _fake_build_services)

    payload = {
        "household_id": "household-auth-1",
        "email": "adult@example.test",
        "display_name": "Adult One",
        "role": "ADULT",
        "device_name": "Web Browser",
        "platform": "Web",
        "user_agent": "pytest-agent",
    }

    with TestClient(app, raise_server_exceptions=True, follow_redirects=False) as client:
        first = client.post("/v1/auth/oauth/google/stub", json=payload)
        second = client.post(
            "/v1/auth/oauth/google/stub",
            json={**payload, "email": "adult2@example.test"},
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["service_instance"] == 1
    assert second.json()["service_instance"] == 2
    assert build_calls == [1, 2]


@pytest.mark.integration
@pytest.mark.legacy
@pytest.mark.flaky
def test_oauth_stub_creates_missing_household_before_registering_user(monkeypatch) -> None:
    created_households: list[tuple[str, str, str]] = []
    register_household_ids: list[str] = []

    class _FakeRepo:
        def __init__(self) -> None:
            self._household_exists = False

        def get_household(self, _household_id: str):
            return object() if self._household_exists else None

        def create_household(self, household_id: str, name: str, timezone: str):
            self._household_exists = True
            created_households.append((household_id, name, timezone))
            return SimpleNamespace(household_id=household_id, name=name, timezone=timezone)

        def get_user_by_email(self, _email: str):
            return None

        def list_devices_for_user(self, _user_id: str):
            return []

    class _FakeIdentity:
        def register_user(self, **kwargs):
            register_household_ids.append(kwargs["household_id"])
            return SimpleNamespace(user=SimpleNamespace(user_id="new-user"))

        def register_device(self, **_kwargs):
            return SimpleNamespace(device=SimpleNamespace(device_id="new-device"))

        def bootstrap_identity(self, **_kwargs):
            class _BootstrapResult:
                def model_dump(self):
                    return {"status": "ok"}

            return _BootstrapResult()

    class _FakeTokens:
        def issue_token_pair(self, **_kwargs):
            now = datetime.now(timezone.utc)
            return SimpleNamespace(
                access_token="access-token",
                refresh_token="refresh-token",
                access_expires_at=now + timedelta(minutes=15),
                refresh_expires_at=now + timedelta(days=30),
            )

    def _fake_build_services():
        return _FakeRepo(), _FakeIdentity(), _FakeTokens()

    monkeypatch.setattr(auth_router, "_build_services", _fake_build_services)

    payload = {
        "household_id": "household-auth-missing",
        "email": "adult@example.test",
        "display_name": "Adult One",
        "role": "ADULT",
        "device_name": "Web Browser",
        "platform": "Web",
        "user_agent": "pytest-agent",
    }

    with TestClient(app, raise_server_exceptions=True, follow_redirects=False) as client:
        response = client.post("/v1/auth/oauth/google/stub", json=payload)

    assert response.status_code == 200
    assert created_households == [
        (
            "household-auth-missing",
            "Household household-auth-missing",
            "UTC",
        )
    ]
    assert register_household_ids == ["household-auth-missing"]


@pytest.mark.integration
@pytest.mark.legacy
@pytest.mark.flaky
def test_refresh_token_uses_request_scoped_token_service(monkeypatch) -> None:
    build_calls: list[int] = []

    class _FakeTokenService:
        def __init__(self, serial: int) -> None:
            self.serial = serial

        def rotate_refresh_token(self, _refresh_token: str):
            now = datetime.now(timezone.utc)
            return SimpleNamespace(
                access_token=f"new-access-{self.serial}",
                refresh_token=f"new-refresh-{self.serial}",
                access_expires_at=now + timedelta(minutes=15),
                refresh_expires_at=now + timedelta(days=30),
            )

    def _fake_build_token_service():
        serial = len(build_calls) + 1
        build_calls.append(serial)
        return _FakeTokenService(serial)

    monkeypatch.setattr(auth_router, "_build_token_service", _fake_build_token_service)

    with TestClient(app, raise_server_exceptions=True, follow_redirects=False) as client:
        first = client.post("/v1/auth/token/refresh", json={"refresh_token": "refresh-a"})
        second = client.post("/v1/auth/token/refresh", json={"refresh_token": "refresh-b"})

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["access_token"] == "new-access-1"
    assert second.json()["access_token"] == "new-access-2"
    assert build_calls == [1, 2]


@pytest.mark.integration
@pytest.mark.legacy
@pytest.mark.flaky
def test_oauth_stub_recovers_from_duplicate_email_integrity_error(monkeypatch) -> None:
    existing_user = SimpleNamespace(user_id="existing-user", household_id="household-auth-1")
    register_calls: list[int] = []

    class _FakeRepo:
        def __init__(self) -> None:
            self.email_lookups = 0

        def get_user_by_email(self, _email: str):
            self.email_lookups += 1
            if self.email_lookups == 1:
                return None
            return existing_user

        def list_devices_for_user(self, _user_id: str):
            return []

    class _FakeIdentity:
        def register_user(self, **_kwargs):
            register_calls.append(1)
            raise IntegrityError(
                "INSERT INTO users (...) VALUES (...)",
                {"email": "beta+household-auth-1@hpal.local"},
                sqlite3.IntegrityError("UNIQUE constraint failed: users.email"),
            )

        def register_device(self, **_kwargs):
            return SimpleNamespace(device=SimpleNamespace(device_id="device-recovered"))

        def bootstrap_identity(self, **kwargs):
            class _BootstrapResult:
                def model_dump(self):
                    return {"resolved_user_id": kwargs["user_id"]}

            return _BootstrapResult()

    class _FakeTokens:
        def issue_token_pair(self, **_kwargs):
            now = datetime.now(timezone.utc)
            return SimpleNamespace(
                access_token="access-recovered",
                refresh_token="refresh-recovered",
                access_expires_at=now + timedelta(minutes=15),
                refresh_expires_at=now + timedelta(days=30),
            )

    def _fake_build_services():
        return _FakeRepo(), _FakeIdentity(), _FakeTokens()

    monkeypatch.setattr(auth_router, "_build_services", _fake_build_services)

    payload = {
        "household_id": "household-auth-1",
        "email": "beta+household-auth-1@hpal.local",
        "display_name": "Adult One",
        "role": "ADULT",
        "device_name": "Web Browser",
        "platform": "Web",
        "user_agent": "pytest-agent",
    }

    with TestClient(app, raise_server_exceptions=True, follow_redirects=False) as client:
        response = client.post("/v1/auth/oauth/google/stub", json=payload)

    assert response.status_code == 200
    assert response.json()["resolved_user_id"] == "existing-user"
    assert register_calls == [1]
