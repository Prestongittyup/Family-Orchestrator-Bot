from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
    def __init__(
        self,
        *,
        test_mode: bool = False,
        cipher: CredentialCipher | None = None,
        persistence_path: str | None = None,
    ) -> None:
        self.test_mode = test_mode
        self._cipher = cipher or NoopCredentialCipher()
        self._records: dict[tuple[str, str], OAuthCredentialRecord] = {}
        self._lock = threading.RLock()
        self._persistence_path = self._normalize_persistence_path(persistence_path)
        self._load_from_disk()

    @staticmethod
    def _normalize_persistence_path(path: str | None) -> str | None:
        text = (path or "").strip()
        return text or None

    @staticmethod
    def _serialize_datetime(value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.isoformat()

    @staticmethod
    def _parse_datetime(raw: object) -> datetime | None:
        if raw is None:
            return None

        text = str(raw).strip()
        if not text:
            return None

        # Accept common UTC suffix form used by external systems.
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"

        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    @staticmethod
    def _normalize_scope(raw_scope: object) -> tuple[str, ...]:
        if isinstance(raw_scope, (list, tuple)):
            return tuple(str(item) for item in raw_scope if str(item).strip())
        if isinstance(raw_scope, str):
            return tuple(segment for segment in raw_scope.split(" ") if segment.strip())
        return ()

    def _persist_locked(self) -> None:
        if not self._persistence_path:
            return

        payload = {
            "records": [
                {
                    "user_id": record.user_id,
                    "provider_id": record.provider_id,
                    "encrypted_access_token": record.encrypted_access_token,
                    "encrypted_refresh_token": record.encrypted_refresh_token,
                    "token_type": record.token_type,
                    "expires_at": self._serialize_datetime(record.expires_at),
                    "scope": list(record.scope),
                }
                for record in self._records.values()
            ]
        }

        try:
            directory = os.path.dirname(os.path.abspath(self._persistence_path))
            if directory:
                os.makedirs(directory, exist_ok=True)

            temp_path = f"{self._persistence_path}.tmp"
            with open(temp_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, separators=(",", ":"))
            os.replace(temp_path, self._persistence_path)
        except Exception:
            # Persistence is best-effort; keep runtime behavior available.
            return

    def _load_from_disk(self) -> None:
        if not self._persistence_path:
            return

        if not os.path.exists(self._persistence_path):
            return

        try:
            with open(self._persistence_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception:
            return

        records_payload = payload.get("records") if isinstance(payload, dict) else None
        if not isinstance(records_payload, list):
            return

        loaded_records: dict[tuple[str, str], OAuthCredentialRecord] = {}
        for row in records_payload:
            if not isinstance(row, dict):
                continue

            user_id = str(row.get("user_id") or "").strip()
            provider_id = str(row.get("provider_id") or "").strip()
            encrypted_access_token = str(row.get("encrypted_access_token") or "").strip()
            if not user_id or not provider_id or not encrypted_access_token:
                continue

            encrypted_refresh_token_raw = row.get("encrypted_refresh_token")
            encrypted_refresh_token = (
                str(encrypted_refresh_token_raw).strip() if encrypted_refresh_token_raw is not None else None
            )
            token_type = str(row.get("token_type") or "Bearer")
            expires_at = self._parse_datetime(row.get("expires_at"))
            scope = self._normalize_scope(row.get("scope"))

            loaded_records[(user_id, provider_id)] = OAuthCredentialRecord(
                user_id=user_id,
                provider_id=provider_id,
                encrypted_access_token=encrypted_access_token,
                encrypted_refresh_token=encrypted_refresh_token,
                token_type=token_type,
                expires_at=expires_at,
                scope=scope,
            )

        with self._lock:
            self._records = loaded_records

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
        with self._lock:
            self._records[key] = record
            self._persist_locked()
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
        key = (str(user_id), str(provider_id))
        with self._lock:
            record = self._records.get(key)
            if record is None and self._persistence_path:
                # Another process may have updated the persisted store.
                # Reload on miss so runtime reads converge without restart.
                self._load_from_disk()
                record = self._records.get(key)
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
        with self._lock:
            deleted = self._records.pop(key, None) is not None
            if deleted:
                self._persist_locked()
            return deleted

    def issue_mock_token(self, *, user_id: str, provider_id: str, scope: tuple[str, ...] = ()) -> OAuthToken:
        if not self.test_mode:
            raise RuntimeError("mock token issuance requires test_mode=True")

        token = OAuthToken(
            access_token=f"mock-access-{provider_id}-{user_id}",
            refresh_token=f"mock-refresh-{provider_id}-{user_id}",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
            scope=tuple(scope),
        )
        self.upsert_token(user_id=user_id, provider_id=provider_id, token=token)
        return token

    def clear(self) -> None:
        """Reset all in-memory credential records (test/reset helper)."""
        with self._lock:
            self._records.clear()
            self._persist_locked()
