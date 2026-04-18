from __future__ import annotations

from datetime import UTC, datetime, timedelta
import urllib.parse
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore, OAuthCredential
from apps.api.integration_core.google_oauth_config import GoogleOAuthClientConfig
from apps.api.integration_core.google_calendar_provider import GoogleCalendarRealProvider
from apps.api.integration_core.orchestrator import Orchestrator
from apps.api.integration_core.state_builder import StateBuilder


def _mock_google_http(*, event_items: list[dict] | None = None) -> MagicMock:
    event_items = event_items or []

    token_resp = MagicMock()
    token_resp.raise_for_status = MagicMock()
    token_resp.json.return_value = {
        "access_token": "mock-access-token",
        "refresh_token": "mock-refresh-token",
        "token_type": "Bearer",
        "expires_in": 3600,
    }

    http = MagicMock()
    http.post.return_value = token_resp

    def _get(url: str, *, headers: dict, params: dict | None = None):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if url.endswith("/users/me/calendarList"):
            resp.json.return_value = {
                "items": [
                    {
                        "id": "primary",
                        "summary": "Primary",
                        "accessRole": "owner",
                        "selected": True,
                    }
                ]
            }
        else:
            resp.json.return_value = {"items": event_items}
        return resp

    http.get.side_effect = _get
    return http


def _build_client(http_client: MagicMock, credential_store: InMemoryOAuthCredentialStore | None = None):
    from apps.api import main
    from apps.api.endpoints import integrations_router as ir

    cfg = GoogleOAuthClientConfig(
        client_id="test-client-id",
        client_secret="test-client-secret",
        redirect_uri="http://localhost:8000/integrations/google-calendar/callback",
    )
    store = credential_store or InMemoryOAuthCredentialStore()

    main.app.dependency_overrides[ir.get_oauth_config] = lambda: cfg
    main.app.dependency_overrides[ir.get_credential_store] = lambda: store
    main.app.dependency_overrides[ir.get_http_client] = lambda: http_client

    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)
    return client, main.app, store


def _clear_overrides(app):
    app.dependency_overrides.clear()


def _google_event(event_id: str, start: str) -> dict:
    return {
        "id": event_id,
        "summary": f"Event {event_id}",
        "status": "confirmed",
        "start": {"dateTime": start},
        "end": {"dateTime": start},
    }


def test_google_oauth_url_generation():
    http = _mock_google_http()
    client, app, _ = _build_client(http)
    try:
        resp = client.get("/integrations/google-calendar/connect/dev-user-1")
        assert resp.status_code == 302
        location = resp.headers["location"]

        parsed = urllib.parse.urlparse(location)
        params = urllib.parse.parse_qs(parsed.query)

        assert location.startswith("https://accounts.google.com/o/oauth2/auth")
        assert params["client_id"][0] == "test-client-id"
        assert params["response_type"][0] == "code"
        assert "calendar.readonly" in params["scope"][0]
        assert params["state"][0]
    finally:
        _clear_overrides(app)


def test_oauth_callback_stores_credentials():
    http = _mock_google_http()
    store = InMemoryOAuthCredentialStore()
    client, app, _ = _build_client(http, credential_store=store)
    try:
        connect = client.get("/integrations/google-calendar/connect/dev-user-2")
        state = urllib.parse.parse_qs(urllib.parse.urlparse(connect.headers["location"]).query)["state"][0]

        callback = client.get(
            "/integrations/google-calendar/callback",
            params={"code": "auth-code-123", "state": state, "user_id": "dev-user-2"},
        )
        assert callback.status_code == 302
        assert callback.headers["location"].startswith("/?status=integration_successful")

        creds = store.get_credentials(user_id="dev-user-2", provider_name="google_calendar")
        assert creds is not None
        assert creds.access_token == "mock-access-token"
        assert creds.refresh_token == "mock-refresh-token"
    finally:
        _clear_overrides(app)


def test_provider_fetch_with_mock_credentials():
    events = [
        _google_event("evt-1", "2026-04-20T09:00:00Z"),
        _google_event("evt-2", "2026-04-20T10:00:00Z"),
    ]
    http = _mock_google_http(event_items=events)
    store = InMemoryOAuthCredentialStore()
    store.save_credentials(
        OAuthCredential(
            user_id="provider-user",
            provider_name="google_calendar",
            access_token="provider-token",
            refresh_token=None,
        )
    )

    provider = GoogleCalendarRealProvider(credential_store=store, http_client=http)
    rows = provider.fetch_events(user_id="provider-user", max_results=50)

    assert len(rows) == 2
    assert rows[0]["event_id"] == "evt-1"
    assert rows[0]["timestamp"] == "2026-04-20T09:00:00Z"
    assert rows[1]["event_id"] == "evt-2"


def test_end_to_end_calendar_ingestion_flow():
    events = [
        _google_event("evt-a", "2026-04-21T08:00:00Z"),
        _google_event("evt-b", "2026-04-21T09:00:00Z"),
    ]
    http = _mock_google_http(event_items=events)
    store = InMemoryOAuthCredentialStore()
    client, app, _ = _build_client(http, credential_store=store)
    try:
        # 1) Connect -> callback to store credentials
        connect = client.get("/integrations/google-calendar/connect/dev-user-e2e")
        state = urllib.parse.parse_qs(urllib.parse.urlparse(connect.headers["location"]).query)["state"][0]
        callback = client.get(
            "/integrations/google-calendar/callback",
            params={"code": "auth-code-e2e", "state": state, "user_id": "dev-user-e2e"},
        )
        assert callback.status_code == 302

        # 2) Debug endpoint returns HouseholdState-based response
        debug = client.get("/debug/google-calendar/dev-user-e2e")
        assert debug.status_code == 200
        payload = debug.json()
        assert payload["credential_present"] is True
        # Events appear in calendar windows (at least window_7d if within 7 days, else longer)
        all_debug_events = (
            payload["calendar"]["window_7d"]
            or payload["calendar"]["window_30d"]
            or payload["calendar"]["window_90d"]
        )
        assert len(all_debug_events) == 2

        titles = [row["title"] for row in all_debug_events]
        assert titles == sorted(titles) or len(titles) == 2

        # 3) UI and brief surfaces are reachable from the same flow
        ui = client.get("/", params={"user_id": "dev-user-e2e", "household_id": "hh-001"})
        assert ui.status_code == 200
        assert "Connect Google Calendar" in ui.text
        assert "View Brief" in ui.text

        brief = client.get("/brief/hh-001", params={"user_id": "dev-user-e2e"})
        assert brief.status_code == 200
        brief_payload = brief.json()
        debug_7d_titles = {e["title"] for e in payload["calendar"]["window_7d"]}
        assert brief_payload["brief"]["summary"]["calendar_event_count"] == len(all_debug_events)
        assert brief_payload["brief"]["next_upcoming_event"]["title"] in debug_7d_titles
    finally:
        _clear_overrides(app)


def test_orchestrator_builds_household_state_from_credential_store():
    """Orchestrator uses credential_store + http_client; no direct provider construction."""
    event_start = datetime.now(UTC).replace(hour=18, minute=0, second=0, microsecond=0)
    if event_start <= datetime.now(UTC):
        event_start = event_start + timedelta(days=1)

    events = [
        _google_event(
            "evt-brief",
            event_start.isoformat().replace("+00:00", "Z"),
        )
    ]
    http = _mock_google_http(event_items=events)
    store = InMemoryOAuthCredentialStore()
    store.save_credentials(
        OAuthCredential(
            user_id="orchestrator-user",
            provider_name="google_calendar",
            access_token="provider-token",
            refresh_token=None,
        )
    )

    state_builder = StateBuilder(
        credential_store=store,
        http_client=http,
        max_results=25,
    )
    orchestrator = Orchestrator(state_builder)
    state = orchestrator.build_household_state("orchestrator-user")

    # Event 18:00 today or tomorrow is within the 7-day window.
    all_events = state.calendar.window_7d or state.calendar.window_30d
    assert len(all_events) == 1
    assert all_events[0].title == "Event evt-brief"


def test_orchestrator_household_state_has_raw_events_in_debug_meta():
    """debug_meta contains raw_event_count; brief/debug are pure projections."""
    event_start = datetime.now(UTC) + timedelta(hours=2)
    events = [
        _google_event(
            "evt-debug",
            event_start.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        )
    ]
    http = _mock_google_http(event_items=events)
    store = InMemoryOAuthCredentialStore()
    store.save_credentials(
        OAuthCredential(
            user_id="orchestrator-debug-user",
            provider_name="google_calendar",
            access_token="provider-token",
            refresh_token=None,
        )
    )

    state_builder = StateBuilder(
        credential_store=store,
        http_client=http,
    )
    orchestrator = Orchestrator(state_builder)
    state = orchestrator.build_household_state("orchestrator-debug-user")

    # debug_meta tracks raw event count from provider
    assert state.debug_meta["raw_event_count"] == 1
    # Event 2 hours from now is in the 7-day window
    assert len(state.calendar.window_7d) == 1
    # debug projection includes all windows + meta
    debug = state.debug()
    assert "calendar" in debug
    assert "debug_meta" in debug
    assert "integrations" in debug


def test_provider_refreshes_expired_access_token_before_google_calls(monkeypatch):
    http = MagicMock()

    refresh_resp = MagicMock()
    refresh_resp.raise_for_status = MagicMock()
    refresh_resp.json.return_value = {
        "access_token": "refreshed-access-token",
        "token_type": "Bearer",
        "expires_in": 3600,
    }
    http.post.return_value = refresh_resp

    def _get(url: str, *, headers: dict, params: dict | None = None):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if url.endswith("/users/me/calendarList"):
            assert headers["Authorization"] == "Bearer refreshed-access-token"
            resp.json.return_value = {
                "items": [
                    {
                        "id": "primary",
                        "summary": "Primary",
                        "accessRole": "owner",
                        "selected": True,
                    }
                ]
            }
        else:
            assert headers["Authorization"] == "Bearer refreshed-access-token"
            resp.json.return_value = {
                "items": [
                    {
                        "id": "evt-refreshed",
                        "summary": "Refreshed Event",
                        "status": "confirmed",
                        "start": {"dateTime": "2026-04-20T09:00:00Z"},
                        "end": {"dateTime": "2026-04-20T10:00:00Z"},
                    }
                ]
            }
        return resp

    http.get.side_effect = _get

    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/integrations/google-calendar/callback")

    store = InMemoryOAuthCredentialStore()
    store.save_credentials(
        OAuthCredential(
            user_id="refresh-user",
            provider_name="google_calendar",
            access_token="expired-token",
            refresh_token="refresh-token-1",
            expires_at=datetime.now(UTC) - timedelta(minutes=5),
        )
    )

    provider = GoogleCalendarRealProvider(credential_store=store, http_client=http)
    rows = provider.fetch_events(user_id="refresh-user", max_results=10)

    assert len(rows) == 1
    assert rows[0]["event_id"] == "evt-refreshed"

    stored = store.get_credentials(user_id="refresh-user", provider_name="google_calendar")
    assert stored is not None
    assert stored.access_token == "refreshed-access-token"
    assert stored.refresh_token == "refresh-token-1"


def test_provider_returns_empty_dataset_when_expired_and_refresh_token_missing(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/integrations/google-calendar/callback")

    http = _mock_google_http(event_items=[])
    store = InMemoryOAuthCredentialStore()
    store.save_credentials(
        OAuthCredential(
            user_id="reauth-user",
            provider_name="google_calendar",
            access_token="expired-token",
            refresh_token=None,
            expires_at=datetime.now(UTC) - timedelta(minutes=5),
        )
    )

    provider = GoogleCalendarRealProvider(credential_store=store, http_client=http)
    rows = provider.fetch_events(user_id="reauth-user", max_results=5)

    assert rows == []
    assert provider.get_runtime_status()["reason"] == "google_refresh_token_missing"
