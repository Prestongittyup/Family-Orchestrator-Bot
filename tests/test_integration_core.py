from __future__ import annotations

from dataclasses import dataclass

import pytest

from apps.api.integration_core import (
    InMemoryOAuthCredentialStore,
    OAuthToken,
    ProviderRegistry,
    UserIdentity,
    build_default_provider_registry,
)


def test_user_identity_model_is_strongly_typed() -> None:
    user = UserIdentity(
        user_id="u-1",
        household_id="hh-1",
        email="user@example.com",
        display_name="Test User",
    )

    assert user.user_id == "u-1"
    assert user.household_id == "hh-1"
    assert user.timezone == "UTC"
    assert user.is_active is True


def test_mock_token_supported_in_test_mode() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    token = store.issue_mock_token(user_id="u-1", provider_id="gmail", scope=("scope:read",))

    assert token.access_token == "mock-access-gmail-u-1"
    loaded = store.get_token(user_id="u-1", provider_id="gmail")
    assert loaded is not None
    assert loaded.access_token == token.access_token
    assert loaded.scope == ("scope:read",)


def test_mock_token_not_allowed_when_not_in_test_mode() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=False)
    with pytest.raises(RuntimeError):
        store.issue_mock_token(user_id="u-1", provider_id="gmail")


def test_credential_store_uses_encryption_ready_cipher_interface() -> None:
    class SpyCipher:
        def __init__(self) -> None:
            self.encrypt_calls = 0
            self.decrypt_calls = 0

        def encrypt(self, plaintext: str) -> str:
            self.encrypt_calls += 1
            return f"enc::{plaintext}"

        def decrypt(self, ciphertext: str) -> str:
            self.decrypt_calls += 1
            assert ciphertext.startswith("enc::")
            return ciphertext.replace("enc::", "", 1)

    cipher = SpyCipher()
    store = InMemoryOAuthCredentialStore(test_mode=True, cipher=cipher)
    store.upsert_token(
        user_id="u-1",
        provider_id="google_calendar",
        token=OAuthToken(access_token="a", refresh_token="r"),
    )

    loaded = store.get_token(user_id="u-1", provider_id="google_calendar")
    assert loaded is not None
    assert loaded.access_token == "a"
    assert loaded.refresh_token == "r"
    assert cipher.encrypt_calls >= 2
    assert cipher.decrypt_calls >= 2


def test_default_provider_registry_has_gmail_and_google_calendar() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    registry = build_default_provider_registry(store)
    registered = registry.list_registered()

    assert "gmail" in registered
    assert "google_calendar" in registered


def test_provider_registry_supports_dependency_injection() -> None:
    @dataclass(frozen=True)
    class FakeProvider:
        provider_id: str = "fake"

        def required_scopes(self) -> tuple[str, ...]:
            return ()

        def status(self, *, user: UserIdentity, credential_store: InMemoryOAuthCredentialStore) -> dict:
            return {"provider_id": self.provider_id, "connected": False, "mode": "injected"}

    store = InMemoryOAuthCredentialStore(test_mode=True)
    registry = ProviderRegistry(store)
    registry.register("fake", lambda _store: FakeProvider())

    provider = registry.create("fake")
    result = provider.status(
        user=UserIdentity(user_id="u-1", household_id="hh-1", email="u@example.com"),
        credential_store=store,
    )
    assert result["mode"] == "injected"


def test_placeholder_provider_status_is_deterministic_and_no_network() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    registry = build_default_provider_registry(store)
    user = UserIdentity(user_id="u-1", household_id="hh-1", email="u@example.com")

    gmail = registry.create("gmail")
    calendar = registry.create("google_calendar")

    first_gmail = gmail.status(user=user, credential_store=store)
    first_calendar = calendar.status(user=user, credential_store=store)
    second_gmail = gmail.status(user=user, credential_store=store)
    second_calendar = calendar.status(user=user, credential_store=store)

    assert first_gmail == second_gmail
    assert first_calendar == second_calendar
    assert first_gmail["connected"] is False
    assert first_calendar["connected"] is False
