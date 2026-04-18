from __future__ import annotations

import importlib
import urllib.parse
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore, OAuthCredential
from apps.api.integration_core.google_calendar_provider import GoogleCalendarRealProvider


def _reload_module():
    import apps.api.integration_core.google_oauth_config as cfg
    return importlib.reload(cfg)


def test_missing_env_vars_report_soft_configuration_status(monkeypatch):
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("GOOGLE_REDIRECT_URI", raising=False)

    cfg = _reload_module()
    config = cfg.GoogleOAuthClientConfig.from_env()
    status = config.validate()

    assert status.configured is False
    assert status.missing_fields == [
        "GOOGLE_CLIENT_ID",
        "GOOGLE_CLIENT_SECRET",
        "GOOGLE_REDIRECT_URI",
    ]
    assert status.message == "OAuth client not configured"


def test_valid_env_vars_generate_oauth_url(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "env-client-id.apps.googleusercontent.com")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "env-client-secret")
    monkeypatch.setenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:8000/integrations/google-calendar/callback")

    cfg = _reload_module()
    config = cfg.GoogleOAuthClientConfig.from_env()
    status = config.validate()
    assert status.configured is True

    url = cfg.build_authorization_url(config=config, state="state-123")
    parsed = urllib.parse.urlparse(url)
    params = urllib.parse.parse_qs(parsed.query)

    assert url.startswith("https://accounts.google.com/o/oauth2/auth")
    assert params["client_id"][0] == "env-client-id.apps.googleusercontent.com"
    assert params["redirect_uri"][0] == "http://127.0.0.1:8000/integrations/google-calendar/callback"
    assert params["scope"][0] == "https://www.googleapis.com/auth/calendar.readonly"


def test_redirect_uri_used_exactly_as_configured(monkeypatch):
    expected_redirect = "http://127.0.0.1:8999/custom/callback"
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "client-x")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "secret-x")
    monkeypatch.setenv("GOOGLE_REDIRECT_URI", expected_redirect)

    cfg = _reload_module()
    config = cfg.GoogleOAuthClientConfig.from_env()
    status = config.validate()
    assert status.configured is True

    url = cfg.build_authorization_url(config=config, state="st")
    params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    assert params["redirect_uri"][0] == expected_redirect


def test_client_id_injected_from_environment_only(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "client-from-env")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "secret-from-env")
    monkeypatch.setenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:8000/integrations/google-calendar/callback")

    cfg = _reload_module()
    config = cfg.GoogleOAuthClientConfig.from_env()
    assert config.client_id == "client-from-env"


def test_token_refresh_flow(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "refresh-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "refresh-client-secret")
    monkeypatch.setenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:8000/integrations/google-calendar/callback")

    http = MagicMock()

    refresh_response = MagicMock()
    refresh_response.raise_for_status = MagicMock()
    refresh_response.json.return_value = {
        "access_token": "refreshed-access-token",
        "token_type": "Bearer",
        "expires_in": 3600,
    }
    http.post.return_value = refresh_response

    def _get(url: str, *, headers: dict, params: dict | None = None):
        response = MagicMock()
        response.raise_for_status = MagicMock()
        if url.endswith("/users/me/calendarList"):
            assert headers["Authorization"] == "Bearer refreshed-access-token"
            response.json.return_value = {
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
            response.json.return_value = {
                "items": [
                    {
                        "id": "evt-1",
                        "summary": "Refreshed Event",
                        "status": "confirmed",
                        "start": {"dateTime": "2026-04-20T09:00:00Z"},
                        "end": {"dateTime": "2026-04-20T10:00:00Z"},
                    }
                ]
            }
        return response

    http.get.side_effect = _get

    store = InMemoryOAuthCredentialStore()
    expired_at = datetime.now(UTC) - timedelta(minutes=5)
    store.save_credentials(
        OAuthCredential(
            user_id="refresh-user",
            provider_name="google_calendar",
            access_token="expired-access-token",
            refresh_token="persisted-refresh-token",
            scopes=("https://www.googleapis.com/auth/calendar.readonly",),
            expires_at=expired_at,
        )
    )

    provider = GoogleCalendarRealProvider(credential_store=store, http_client=http)
    rows = provider.fetch_events(user_id="refresh-user", max_results=10)

    assert len(rows) == 1
    assert rows[0]["event_id"] == "evt-1"

    updated = store.get_credentials(user_id="refresh-user", provider_name="google_calendar")
    assert updated is not None
    assert updated.access_token == "refreshed-access-token"
    assert updated.refresh_token == "persisted-refresh-token"
    assert updated.expires_at is not None
    assert updated.expires_at > datetime.now(UTC)

    assert http.post.call_count == 1
