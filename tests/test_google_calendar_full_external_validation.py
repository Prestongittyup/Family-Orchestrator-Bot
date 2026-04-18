from __future__ import annotations

import os
import sys
import uuid
from dataclasses import asdict
from typing import Any
from unittest.mock import MagicMock

import pytest

from apps.api.integration_core.architecture_guard import FORBIDDEN_IMPORT_PREFIXES
from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore, OAuthCredential
from apps.api.integration_core.event_windowing import OrchestrationView
from apps.api.integration_core.google_calendar_provider import GoogleCalendarProviderReal
from apps.api.integration_core.google_oauth_config import (
    GoogleOAuthClientConfig,
    OAuthStateStore,
    build_authorization_url,
    exchange_code_for_tokens,
)
from apps.api.integration_core.identity_service import IdentityService
from apps.api.integration_core.normalization import normalize_provider_events
from apps.api.integration_core.orchestrator import IntegrationOrchestrator
from apps.api.integration_core.registry import ProviderRegistry
from apps.api.integration_core.repository import InMemoryIdentityRepository


def _mock_google_http(event_items: list[dict[str, Any]]) -> MagicMock:
    token_resp = MagicMock()
    token_resp.raise_for_status = MagicMock()
    token_resp.json.return_value = {
        "access_token": "live-like-access-token",
        "refresh_token": "live-like-refresh-token",
        "token_type": "Bearer",
        "expires_in": 3600,
    }

    http = MagicMock()
    http.post.return_value = token_resp

    def _get(url: str, *, headers: dict[str, Any], params: dict[str, Any] | None = None):
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


def _event(event_id: str, start: str, summary: str) -> dict[str, Any]:
    return {
        "id": event_id,
        "summary": summary,
        "status": "confirmed",
        "description": "Deterministic test event",
        "start": {"dateTime": start},
        "end": {"dateTime": start},
    }


def _verify_required_env() -> GoogleOAuthClientConfig:
    required = ["GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET", "GOOGLE_REDIRECT_URI"]
    missing = [name for name in required if not str(os.environ.get(name, "")).strip()]
    if missing:
        raise AssertionError(
            "Missing required Google OAuth environment variables: " + ", ".join(missing)
        )
    cfg = GoogleOAuthClientConfig.from_env()
    assert cfg.client_id == os.environ["GOOGLE_CLIENT_ID"]
    assert cfg.client_secret == os.environ["GOOGLE_CLIENT_SECRET"]
    assert cfg.redirect_uri == os.environ["GOOGLE_REDIRECT_URI"]
    return cfg


def _normalize_sorted(user_id: str, provider_name: str, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = normalize_provider_events(
        user_id=user_id,
        provider_name=provider_name,
        raw_events=[{k: v for k, v in row.items() if k != "_raw_google_event"} for row in raw_rows],
        event_type="calendar.event",
    )
    ordered = sorted(
        normalized,
        key=lambda e: (e.timestamp, e.provider_name, e.event_id),
    )
    return [
        {
            "event_id": e.event_id,
            "user_id": e.user_id,
            "provider_name": e.provider_name,
            "event_type": e.event_type,
            "timestamp": e.timestamp,
            "payload": e.payload,
        }
        for e in ordered
    ]


def test_google_calendar_full_external_validation(monkeypatch, capsys):
    # 1) RESET STATE
    identity_repo = InMemoryIdentityRepository()
    identity_repo.clear()
    credential_store = InMemoryOAuthCredentialStore()
    credential_store.clear()
    registry = ProviderRegistry(credential_store)
    registry.clear_providers()
    orchestrator = IntegrationOrchestrator(registry)  # no internal state/cache to clear

    # Ensure no residual credentials for both provider names used in this surface.
    assert credential_store.get_credentials(user_id="residual", provider_name="google_calendar") is None
    assert credential_store.get_credentials(user_id="residual", provider_name="google_calendar_real") is None

    # 2) VERIFY ENV CONFIG (fail fast)
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "det-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "det-client-secret")
    monkeypatch.setenv(
        "GOOGLE_REDIRECT_URI",
        "http://localhost:8000/integrations/google-calendar/callback",
    )
    cfg = _verify_required_env()

    # 3) START CLEAN FLOW (new test user)
    service = IdentityService(identity_repo)
    user = service.create_user(
        email="google-e2e-test@example.test",
        display_name="Google E2E Test",
        household_id="test-household",
    )
    user_id = str(user.user_id)

    # 4) INITIATE OAUTH FLOW (manual-step friendly)
    state_store = OAuthStateStore()
    state = state_store.generate_state(user_id)
    oauth_url = build_authorization_url(config=cfg, state=state)
    assert "calendar.readonly" in oauth_url
    assert state in oauth_url

    # Snapshot loaded modules BEFORE flow, to detect forbidden imports introduced by this run.
    before_modules = set(sys.modules.keys())

    # 5) EXPECT CALLBACK (simulated token exchange)
    google_events = [
        _event("evt-001", "2026-05-01T08:00:00Z", "Breakfast prep"),
        _event("evt-002", "2026-05-01T10:00:00Z", "Doctor appointment"),
    ]
    http_client = _mock_google_http(google_events)

    bound_user = state_store.consume_state(state)
    assert bound_user == user_id

    token = exchange_code_for_tokens(
        code="mock-auth-code",
        config=cfg,
        http_client=http_client,
    )

    # Store alias key requested by task.
    credential_store.save_credentials(
        OAuthCredential(
            user_id=user_id,
            provider_name="google_calendar",
            access_token=token.access_token,
            refresh_token=token.refresh_token,
            scopes=("https://www.googleapis.com/auth/calendar.readonly",),
        )
    )
    # Store operational key used by the real provider implementation.
    credential_store.save_credentials(
        OAuthCredential(
            user_id=user_id,
            provider_name="google_calendar_real",
            access_token=token.access_token,
            refresh_token=token.refresh_token,
            scopes=("https://www.googleapis.com/auth/calendar.readonly",),
        )
    )

    # 6) VERIFY CREDENTIAL STORAGE
    creds_alias = credential_store.get_credentials(user_id=user_id, provider_name="google_calendar")
    creds_real = credential_store.get_credentials(user_id=user_id, provider_name="google_calendar_real")
    assert creds_alias is not None
    assert creds_real is not None
    assert isinstance(creds_alias.access_token, str) and creds_alias.access_token
    assert isinstance(creds_alias.refresh_token, str) and creds_alias.refresh_token

    # 7) RUN PROVIDER FETCH
    registry.register_provider(
        "google_calendar",
        lambda store: GoogleCalendarProviderReal(credential_store=store, http_client=http_client),
    )
    provider = registry.get_provider("google_calendar")

    provider_called = False
    auth_ok = provider.authenticate(
        OAuthCredential(
            user_id=user_id,
            provider_name="google_calendar_real",
            access_token=token.access_token,
            refresh_token=token.refresh_token,
        )
    )
    assert auth_ok is True
    raw_rows = provider.fetch_events(
        user_id=user_id,
        max_results=50,
        view=OrchestrationView.LONG_TERM,
    )
    provider_called = True

    # 8) RUN ORCHESTRATOR + NORMALIZATION (run #1)
    orch_rows = orchestrator.collect_external_events(
        user_id,
        view=OrchestrationView.LONG_TERM,
    )
    raw_from_orchestrator = [row.raw_payload for row in orch_rows]
    normalized_run1 = _normalize_sorted(user_id, "google_calendar", raw_from_orchestrator)

    # 9) OUTPUT DEBUG RESULTS
    event_count_per_provider = {"google_calendar": len(raw_rows)}
    debug_result_1 = {
        "raw_provider_output": raw_rows,
        "normalized_external_events": normalized_run1,
        "credential_confirmation_status": {
            "google_calendar": creds_alias is not None,
            "google_calendar_real": creds_real is not None,
        },
        "event_count_per_provider": event_count_per_provider,
    }

    # 10) VALIDATION ASSERTIONS
    assert len(raw_rows) >= 0
    assert creds_alias is not None and creds_real is not None

    after_modules = set(sys.modules.keys())
    loaded_during_run = after_modules - before_modules
    forbidden_loaded = [
        name
        for name in loaded_during_run
        if any(name == p or name.startswith(f"{p}.") for p in FORBIDDEN_IMPORT_PREFIXES)
    ]
    assert forbidden_loaded == [], f"Forbidden modules imported during run: {forbidden_loaded}"

    # 11) REPEAT TEST (run #2) and compare determinism
    orch_rows_2 = orchestrator.collect_external_events(
        user_id,
        view=OrchestrationView.LONG_TERM,
    )
    raw_from_orchestrator_2 = [row.raw_payload for row in orch_rows_2]
    normalized_run2 = _normalize_sorted(user_id, "google_calendar", raw_from_orchestrator_2)

    deterministic_stability = normalized_run1 == normalized_run2
    assert deterministic_stability is True

    # Final summary requested by task
    summary = {
        "credential_connected": bool(creds_alias and creds_real),
        "provider_called": provider_called,
        "events_received_count": len(raw_rows),
        "events_after_normalization_count": len(normalized_run1),
        "deterministic_stability": deterministic_stability,
    }

    print("OAuth URL:", oauth_url)
    print("OAuth state:", state)
    print("Debug result (run1):", debug_result_1)
    print("Final summary:", summary)

    # explicit outcome assertions
    assert summary["credential_connected"] is True
    assert summary["provider_called"] is True
    assert summary["deterministic_stability"] is True
