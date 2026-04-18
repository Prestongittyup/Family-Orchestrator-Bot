from __future__ import annotations

from datetime import datetime

from apps.api.integration_core import InMemoryOAuthCredentialStore, OAuthCredential


def test_store_and_retrieve_credentials() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    credentials = OAuthCredential(
        user_id="user-1",
        provider_name="gmail",
        access_token="access-1",
        refresh_token="refresh-1",
        scopes=("gmail.read",),
        expires_at=datetime(2030, 1, 1, 0, 0, 0),
    )

    store.save_credentials(credentials)
    loaded = store.get_credentials(user_id="user-1", provider_name="gmail")

    assert loaded is not None
    assert loaded.user_id == "user-1"
    assert loaded.provider_name == "gmail"
    assert loaded.access_token == "access-1"
    assert loaded.refresh_token == "refresh-1"
    assert loaded.scopes == ("gmail.read",)


def test_overwrite_credentials() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    first = OAuthCredential(
        user_id="user-1",
        provider_name="google_calendar",
        access_token="old-access",
        refresh_token="old-refresh",
        scopes=("calendar.read",),
    )
    second = OAuthCredential(
        user_id="user-1",
        provider_name="google_calendar",
        access_token="new-access",
        refresh_token="new-refresh",
        scopes=("calendar.read", "calendar.events"),
    )

    store.save_credentials(first)
    store.save_credentials(second)

    loaded = store.get_credentials(user_id="user-1", provider_name="google_calendar")
    assert loaded is not None
    assert loaded.access_token == "new-access"
    assert loaded.refresh_token == "new-refresh"
    assert loaded.scopes == ("calendar.read", "calendar.events")


def test_delete_credentials() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    credentials = OAuthCredential(
        user_id="user-2",
        provider_name="gmail",
        access_token="token",
        refresh_token=None,
    )

    store.save_credentials(credentials)
    assert store.get_credentials(user_id="user-2", provider_name="gmail") is not None

    deleted = store.delete_credentials(user_id="user-2", provider_name="gmail")
    assert deleted is True
    assert store.get_credentials(user_id="user-2", provider_name="gmail") is None


def test_supports_multiple_providers_per_user() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    gmail = OAuthCredential(
        user_id="user-3",
        provider_name="gmail",
        access_token="gmail-token",
        refresh_token="gmail-refresh",
    )
    calendar = OAuthCredential(
        user_id="user-3",
        provider_name="google_calendar",
        access_token="calendar-token",
        refresh_token="calendar-refresh",
    )

    store.save_credentials(gmail)
    store.save_credentials(calendar)

    gmail_loaded = store.get_credentials(user_id="user-3", provider_name="gmail")
    calendar_loaded = store.get_credentials(user_id="user-3", provider_name="google_calendar")

    assert gmail_loaded is not None
    assert calendar_loaded is not None
    assert gmail_loaded.access_token == "gmail-token"
    assert calendar_loaded.access_token == "calendar-token"