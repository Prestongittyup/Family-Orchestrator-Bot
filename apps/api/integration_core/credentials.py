from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol


class CredentialCipher(Protocol):
    def encrypt(self, plaintext: str) -> str:
        ...

    def decrypt(self, ciphertext: str) -> str:
        ...


class NoopCredentialCipher:
    """Encryption-ready placeholder. Real encryption can be plugged in later."""

    def encrypt(self, plaintext: str) -> str:
        return plaintext

    def decrypt(self, ciphertext: str) -> str:
        return ciphertext


@dataclass(frozen=True)
class OAuthToken:
    access_token: str
    refresh_token: str | None
    token_type: str = "Bearer"
    expires_at: datetime | None = None
    scope: tuple[str, ...] = ()


@dataclass(frozen=True)
class OAuthCredentialRecord:
    user_id: str
    provider_id: str
    encrypted_access_token: str
    encrypted_refresh_token: str | None
    token_type: str
    expires_at: datetime | None
    scope: tuple[str, ...]


@dataclass(frozen=True)
class OAuthCredential:
    user_id: str
    provider_name: str
    access_token: str
    refresh_token: str | None
    scopes: tuple[str, ...] = ()
    expires_at: datetime | None = None


class CredentialStore(Protocol):
    test_mode: bool

    def save_credentials(self, credentials: OAuthCredential) -> OAuthCredential:
        ...

    def get_credentials(self, *, user_id: str, provider_name: str) -> OAuthCredential | None:
        ...

    def delete_credentials(self, *, user_id: str, provider_name: str) -> bool:
        ...


class OAuthCredentialStore(Protocol):
    test_mode: bool

    def upsert_token(self, *, user_id: str, provider_id: str, token: OAuthToken) -> OAuthCredentialRecord:
        ...

    def get_token(self, *, user_id: str, provider_id: str) -> OAuthToken | None:
        ...

    def issue_mock_token(self, *, user_id: str, provider_id: str, scope: tuple[str, ...] = ()) -> OAuthToken:
        ...


class InMemoryOAuthCredentialStore:
    def __init__(self, *, test_mode: bool = False, cipher: CredentialCipher | None = None) -> None:
        self.test_mode = test_mode
        self._cipher = cipher or NoopCredentialCipher()
        self._records: dict[tuple[str, str], OAuthCredentialRecord] = {}

    def upsert_token(self, *, user_id: str, provider_id: str, token: OAuthToken) -> OAuthCredentialRecord:
        key = (str(user_id), str(provider_id))
        record = OAuthCredentialRecord(
            user_id=key[0],
            provider_id=key[1],
            encrypted_access_token=self._cipher.encrypt(token.access_token),
            encrypted_refresh_token=(self._cipher.encrypt(token.refresh_token) if token.refresh_token else None),
            token_type=token.token_type,
            expires_at=token.expires_at,
            scope=tuple(token.scope),
        )
        self._records[key] = record
        return record

    def save_credentials(self, credentials: OAuthCredential) -> OAuthCredential:
        token = OAuthToken(
            access_token=credentials.access_token,
            refresh_token=credentials.refresh_token,
            expires_at=credentials.expires_at,
            scope=tuple(credentials.scopes),
        )
        self.upsert_token(
            user_id=credentials.user_id,
            provider_id=credentials.provider_name,
            token=token,
        )
        return credentials

    def get_token(self, *, user_id: str, provider_id: str) -> OAuthToken | None:
        record = self._records.get((str(user_id), str(provider_id)))
        if record is None:
            return None
        return OAuthToken(
            access_token=self._cipher.decrypt(record.encrypted_access_token),
            refresh_token=(self._cipher.decrypt(record.encrypted_refresh_token) if record.encrypted_refresh_token else None),
            token_type=record.token_type,
            expires_at=record.expires_at,
            scope=record.scope,
        )

    def get_credentials(self, *, user_id: str, provider_name: str) -> OAuthCredential | None:
        token = self.get_token(user_id=user_id, provider_id=provider_name)
        if token is None:
            return None
        return OAuthCredential(
            user_id=str(user_id),
            provider_name=str(provider_name),
            access_token=token.access_token,
            refresh_token=token.refresh_token,
            scopes=tuple(token.scope),
            expires_at=token.expires_at,
        )

    def delete_credentials(self, *, user_id: str, provider_name: str) -> bool:
        key = (str(user_id), str(provider_name))
        return self._records.pop(key, None) is not None

    def issue_mock_token(self, *, user_id: str, provider_id: str, scope: tuple[str, ...] = ()) -> OAuthToken:
        if not self.test_mode:
            raise RuntimeError("mock token issuance requires test_mode=True")

        token = OAuthToken(
            access_token=f"mock-access-{provider_id}-{user_id}",
            refresh_token=f"mock-refresh-{provider_id}-{user_id}",
            expires_at=datetime.utcnow() + timedelta(hours=1),
            scope=tuple(scope),
        )
        self.upsert_token(user_id=user_id, provider_id=provider_id, token=token)
        return token

    def clear(self) -> None:
        """Reset all in-memory credential records (test/reset helper)."""
        self._records.clear()
