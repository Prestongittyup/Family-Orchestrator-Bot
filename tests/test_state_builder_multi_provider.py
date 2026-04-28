from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from archive.apps.api.integration_core.credentials import InMemoryOAuthCredentialStore, OAuthCredential
from archive.apps.api.integration_core.state_builder import StateBuilder


class _StubProvider:
    def __init__(self, events: list[dict[str, Any]], runtime_status: dict[str, Any]) -> None:
        self._events = list(events)
        self._runtime_status = dict(runtime_status)

    def fetch_events(self, **_kwargs) -> list[dict[str, Any]]:
        return list(self._events)

    def get_runtime_status(self) -> dict[str, Any]:
        return dict(self._runtime_status)


def _event_row(event_id: str, title: str, days_from_now: int, provider: str) -> dict[str, str]:
    start = datetime.now(UTC) + timedelta(days=days_from_now)
    end = start + timedelta(hours=1)
    return {
        "event_id": event_id,
        "title": title,
        "timestamp": start.isoformat().replace("+00:00", "Z"),
        "end_timestamp": end.isoformat().replace("+00:00", "Z"),
        "source_provider": provider,
    }


def test_state_builder_aggregates_connected_calendar_providers(monkeypatch) -> None:
    monkeypatch.setenv("INTEGRATION_CORE_CALENDAR_PROVIDERS", "google_calendar,outlook_calendar")

    store = InMemoryOAuthCredentialStore()
    store.save_credentials(
        OAuthCredential(
            user_id="multi-user",
            provider_name="google_calendar",
            access_token="google-token",
            refresh_token=None,
        )
    )
    store.save_credentials(
        OAuthCredential(
            user_id="multi-user",
            provider_name="outlook_calendar",
            access_token="outlook-token",
            refresh_token=None,
        )
    )

    providers = {
        "google_calendar": _StubProvider(
            [_event_row("g-1", "Google Event", 2, "google_calendar")],
            {"status": "ok", "reason": None},
        ),
        "outlook_calendar": _StubProvider(
            [_event_row("o-1", "Outlook Event", 3, "outlook_calendar")],
            {"status": "ok", "reason": None},
        ),
    }

    builder = StateBuilder(credential_store=store)

    def _fake_build_provider(*, provider_name: str | None = None):
        assert provider_name is not None
        return providers[provider_name]

    monkeypatch.setattr(builder, "_build_provider", _fake_build_provider)

    state = builder.build("multi-user")

    assert {event.title for event in state.calendar.window_7d} == {"Google Event", "Outlook Event"}
    assert {health.integration for health in state.integrations} == {"google_calendar", "outlook_calendar"}
    assert state.debug_meta["provider_count"] == 2
    assert state.debug_meta["provider_event_counts"] == {
        "google_calendar": 1,
        "outlook_calendar": 1,
    }


def test_state_builder_keeps_google_fallback_when_no_provider_credentials(monkeypatch) -> None:
    monkeypatch.setenv("INTEGRATION_CORE_CALENDAR_PROVIDERS", "google_calendar,outlook_calendar")

    store = InMemoryOAuthCredentialStore()
    builder = StateBuilder(credential_store=store)

    disabled_google = _StubProvider(
        [],
        {
            "status": "disabled",
            "reason": "google_oauth_not_configured",
        },
    )

    def _fake_build_provider(*, provider_name: str | None = None):
        assert provider_name == "google_calendar"
        return disabled_google

    monkeypatch.setattr(builder, "_build_provider", _fake_build_provider)

    state = builder.build("fallback-user")

    assert len(state.integrations) == 1
    health = state.integrations[0]
    assert health.integration == "google_calendar"
    assert health.state == "disabled"
    assert health.reason == "google_oauth_not_configured"
