from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore
from apps.api.integration_core.google_calendar_provider import GoogleCalendarRealProvider
from apps.api.integration_core.orchestrator import Orchestrator
from apps.api.integration_core.state_builder import StateBuilder


def _clear_overrides(app) -> None:
    app.dependency_overrides.clear()


def test_missing_google_oauth_config_does_not_crash_system(monkeypatch):
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("GOOGLE_REDIRECT_URI", raising=False)

    from apps.api import main
    from apps.api.endpoints import integrations_router as ir

    store = InMemoryOAuthCredentialStore()
    main.app.dependency_overrides[ir.get_credential_store] = lambda: store
    main.app.dependency_overrides[ir.get_http_client] = lambda: None

    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)
    try:
        brief = client.get("/brief/hh-001")
        assert brief.status_code == 200

        connect = client.get("/integrations/google-calendar/connect/safe-boot-user")
        assert connect.status_code == 400
        assert connect.json() == {
            "status": "disabled",
            "integration": "google_calendar",
            "reason": "OAuth client not configured",
            "action": "set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET",
        }

        debug = client.get("/debug/google-calendar/safe-boot-user")
        assert debug.status_code == 200
        payload = debug.json()
        # New HouseholdState-based shape
        assert payload["calendar"]["window_7d"] == []
        assert payload["calendar"]["window_30d"] == []
        assert payload["calendar"]["window_90d"] == []
        assert len(payload["integrations"]) == 1
        health = payload["integrations"][0]
        assert health["state"] == "disabled"
        assert health["reason"] == "google_oauth_not_configured"
    finally:
        _clear_overrides(main.app)


def test_provider_fetch_returns_empty_and_disabled_when_oauth_not_configured(monkeypatch):
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("GOOGLE_REDIRECT_URI", raising=False)

    provider = GoogleCalendarRealProvider(
        credential_store=InMemoryOAuthCredentialStore(),
        http_client=None,
    )

    rows = provider.fetch_events(user_id="provider-disabled-user", max_results=10)

    assert rows == []
    assert provider.get_runtime_status()["status"] == "disabled"
    assert provider.get_runtime_status()["reason"] == "google_oauth_not_configured"


def test_orchestrator_continues_running_when_google_oauth_not_configured(monkeypatch):
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("GOOGLE_REDIRECT_URI", raising=False)

    credential_store = InMemoryOAuthCredentialStore()
    state_builder = StateBuilder(
        credential_store=credential_store,
        http_client=None,
    )
    orchestrator = Orchestrator(state_builder)

    state = orchestrator.build_household_state("orchestrator-disabled-user")

    assert state.brief()["event_count"] == 0
    assert state.brief()["events"] == []
    assert len(state.calendar.window_7d) == 0
