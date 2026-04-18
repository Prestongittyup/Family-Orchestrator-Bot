from __future__ import annotations

from apps.api.integration_core import InMemoryOAuthCredentialStore, OAuthCredential, ProviderRegistry
from apps.api.integration_core.providers import GmailProviderMock, GoogleCalendarProviderMock


def test_provider_registration() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    registry = ProviderRegistry(store)

    registry.register_provider("gmail", lambda injected_store: GmailProviderMock(credential_store=injected_store))
    registry.register_provider(
        "google_calendar",
        lambda injected_store: GoogleCalendarProviderMock(credential_store=injected_store),
    )

    listed = registry.list_providers()
    assert listed == ("gmail", "google_calendar")


def test_provider_retrieval() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    registry = ProviderRegistry(store)
    registry.register_provider("gmail", lambda injected_store: GmailProviderMock(credential_store=injected_store))

    provider = registry.get_provider("gmail")
    assert provider.provider_name == "gmail"
    assert provider.health_check()["healthy"] is True


def test_mock_fetch_events_returns_deterministic_data() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    registry = ProviderRegistry(store)
    registry.register_provider(
        "google_calendar",
        lambda injected_store: GoogleCalendarProviderMock(credential_store=injected_store),
    )

    provider = registry.get_provider("google_calendar")
    provider.authenticate(
        OAuthCredential(
            user_id="u-1",
            provider_name="google_calendar",
            access_token="mock-access",
            refresh_token="mock-refresh",
            scopes=("calendar.read",),
        )
    )

    first = provider.fetch_events(user_id="u-1")
    second = provider.fetch_events(user_id="u-1")

    assert first == second
    assert len(first) == 2
    assert first[0]["provider"] == "google_calendar"
