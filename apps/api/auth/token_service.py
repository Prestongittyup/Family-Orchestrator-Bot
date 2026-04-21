from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from threading import Lock
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Literal

from sqlalchemy.exc import SQLAlchemyError, TimeoutError as SQLAlchemyTimeoutError

from apps.api.identity.repository import IdentityRepository
from apps.api.observability.metrics import metrics


class AuthValidationSystemError(RuntimeError):
    """Raised when token validation cannot complete because a system dependency failed."""


@dataclass(frozen=True)
class _CachedValidation:
    claims: dict
    valid_until_monotonic: float


@dataclass(frozen=True)
class TokenPair:
    access_token: str
    refresh_token: str
    access_expires_at: datetime
    refresh_expires_at: datetime


class TokenService:
    """Server-verified token issuance/validation with refresh rotation + revocation."""

    _cache_lock = Lock()
    _validation_cache: dict[str, _CachedValidation] = {}

    def __init__(
        self,
        repository: IdentityRepository | None = None,
        repository_factory: Callable[[], IdentityRepository] | None = None,
    ) -> None:
        self._repo = repository
        self._repository_factory = repository_factory
        self._secret = os.getenv("AUTH_TOKEN_SECRET", "dev-insecure-secret-change-me")
        self._issuer = os.getenv("AUTH_TOKEN_ISSUER", "hpal")
        self._access_minutes = int(os.getenv("AUTH_ACCESS_MINUTES", "15"))
        self._refresh_days = int(os.getenv("AUTH_REFRESH_DAYS", "30"))
        configured_ttl = int(os.getenv("AUTH_VALIDATION_CACHE_TTL_SECONDS", "60"))
        self._validation_cache_ttl_seconds = max(30, min(120, configured_ttl))

    def issue_token_pair(
        self,
        *,
        household_id: str,
        user_id: str,
        device_id: str,
        role: Literal["ADMIN", "ADULT", "CHILD", "VIEW_ONLY"],
    ) -> TokenPair:
        now = datetime.now(timezone.utc)
        access_exp = now + timedelta(minutes=self._access_minutes)
        refresh_exp = now + timedelta(days=self._refresh_days)

        access_claims = self._base_claims(
            household_id=household_id,
            user_id=user_id,
            device_id=device_id,
            role=role,
            token_type="access",
            exp=access_exp,
        )
        refresh_claims = self._base_claims(
            household_id=household_id,
            user_id=user_id,
            device_id=device_id,
            role=role,
            token_type="refresh",
            exp=refresh_exp,
        )

        access_token = self._encode_jws(access_claims)
        refresh_token = self._encode_jws(refresh_claims)

        self._persist_token(access_token, access_claims)
        self._persist_token(refresh_token, refresh_claims)

        return TokenPair(
            access_token=access_token,
            refresh_token=refresh_token,
            access_expires_at=access_exp,
            refresh_expires_at=refresh_exp,
        )

    def validate_access_token(self, token: str) -> dict | None:
        claims = self._decode_and_verify(token)
        if claims is None:
            return None
        if claims.get("typ") != "access":
            return None
        token_hash = self._token_hash(token)
        cached_claims = self._get_cached_claims(token_hash)
        if cached_claims is not None:
            metrics.increment("auth_validation_cache_hits_total")
            return cached_claims

        metrics.increment("auth_validation_cache_misses_total")
        try:
            if not self._is_persisted_and_valid(token_hash):
                return None
        except SQLAlchemyTimeoutError as exc:
            metrics.note_db_pool_rejection()
            raise AuthValidationSystemError("access_token_validation_failed") from exc
        except SQLAlchemyError as exc:
            raise AuthValidationSystemError("access_token_validation_failed") from exc

        self._cache_valid_claims(token_hash, claims)
        return claims

    def validate_refresh_token(self, token: str) -> dict | None:
        claims = self._decode_and_verify(token)
        if claims is None:
            return None
        if claims.get("typ") != "refresh":
            return None
        token_hash = self._token_hash(token)
        cached_claims = self._get_cached_claims(token_hash)
        if cached_claims is not None:
            metrics.increment("auth_validation_cache_hits_total")
            return cached_claims

        metrics.increment("auth_validation_cache_misses_total")
        try:
            if not self._is_persisted_and_valid(token_hash):
                return None
        except SQLAlchemyTimeoutError as exc:
            metrics.note_db_pool_rejection()
            raise AuthValidationSystemError("refresh_token_validation_failed") from exc
        except SQLAlchemyError as exc:
            raise AuthValidationSystemError("refresh_token_validation_failed") from exc

        self._cache_valid_claims(token_hash, claims)
        return claims

    def rotate_refresh_token(self, refresh_token: str) -> TokenPair | None:
        claims = self.validate_refresh_token(refresh_token)
        if claims is None:
            return None

        # Revoke old refresh token before issuing a new pair
        self.revoke_token(refresh_token)

        role = claims.get("role", "VIEW_ONLY")
        return self.issue_token_pair(
            household_id=str(claims["household_id"]),
            user_id=str(claims["user_id"]),
            device_id=str(claims["device_id"]),
            role=role,
        )

    def revoke_token(self, token: str) -> None:
        token_hash = self._token_hash(token)
        self._get_repository().invalidate_session_token(token_hash)
        self._invalidate_cached_claims(token_hash)

    def revoke_user_tokens(self, user_id: str) -> int:
        return self._get_repository().invalidate_all_user_tokens(user_id)

    def revoke_device_tokens(self, device_id: str) -> int:
        return self._get_repository().invalidate_all_device_tokens(device_id)

    def _base_claims(
        self,
        *,
        household_id: str,
        user_id: str,
        device_id: str,
        role: str,
        token_type: str,
        exp: datetime,
    ) -> dict:
        now = datetime.now(timezone.utc)
        return {
            "iss": self._issuer,
            "jti": str(uuid.uuid4()),
            "typ": token_type,
            "household_id": household_id,
            "user_id": user_id,
            "device_id": device_id,
            "role": role,
            "iat": int(now.timestamp()),
            "exp": int(exp.timestamp()),
        }

    def _persist_token(self, token: str, claims: dict) -> None:
        token_hash = self._token_hash(token)
        expires_at = datetime.fromtimestamp(int(claims["exp"]), tz=timezone.utc).replace(tzinfo=None)
        self._get_repository().create_session_token(
            token_id=token_hash,
            household_id=str(claims["household_id"]),
            user_id=str(claims["user_id"]),
            device_id=str(claims["device_id"]),
            role=str(claims["role"]),
            session_claims=json.dumps(claims, sort_keys=True),
            expires_at=expires_at,
        )
        self._cache_valid_claims(token_hash, claims)

    def _is_persisted_and_valid(self, token_hash: str) -> bool:
        row = self._get_repository().get_session_token(token_hash)
        if row is None or row.is_valid is False:
            return False
        return row.expires_at >= datetime.utcnow()

    def _get_repository(self) -> IdentityRepository:
        if self._repo is not None:
            return self._repo
        if self._repository_factory is not None:
            return self._repository_factory()
        raise RuntimeError("TokenService requires a repository or repository_factory")

    def _cache_valid_claims(self, token_hash: str, claims: dict) -> None:
        valid_for_seconds = max(0.0, float(int(claims.get("exp", 0)) - int(datetime.now(timezone.utc).timestamp())))
        if valid_for_seconds <= 0:
            self._invalidate_cached_claims(token_hash)
            return

        cache_ttl_seconds = min(valid_for_seconds, float(self._validation_cache_ttl_seconds))
        with self._cache_lock:
            self._validation_cache[token_hash] = _CachedValidation(
                claims=dict(claims),
                valid_until_monotonic=time.monotonic() + cache_ttl_seconds,
            )

    def _get_cached_claims(self, token_hash: str) -> dict | None:
        now = time.monotonic()
        with self._cache_lock:
            cached = self._validation_cache.get(token_hash)
            if cached is None:
                return None
            if cached.valid_until_monotonic <= now:
                self._validation_cache.pop(token_hash, None)
                return None
            return dict(cached.claims)

    def _invalidate_cached_claims(self, token_hash: str) -> None:
        with self._cache_lock:
            self._validation_cache.pop(token_hash, None)

    @staticmethod
    def _token_hash(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def _encode_jws(self, claims: dict) -> str:
        header = {"alg": "HS256", "typ": "JWT"}
        header_b64 = self._b64url(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
        payload_b64 = self._b64url(json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("utf-8"))
        signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
        signature = hmac.new(self._secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
        sig_b64 = self._b64url(signature)
        return f"{header_b64}.{payload_b64}.{sig_b64}"

    def _decode_and_verify(self, token: str) -> dict | None:
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None
            header_b64, payload_b64, sig_b64 = parts
            signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
            expected_sig = hmac.new(self._secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
            actual_sig = self._b64url_decode(sig_b64)
            if not hmac.compare_digest(expected_sig, actual_sig):
                return None

            payload = json.loads(self._b64url_decode(payload_b64).decode("utf-8"))
            if int(payload.get("exp", 0)) < int(datetime.now(timezone.utc).timestamp()):
                return None
            return payload
        except Exception:
            return None

    @staticmethod
    def _b64url(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    @staticmethod
    def _b64url_decode(data: str) -> bytes:
        padding = "=" * ((4 - len(data) % 4) % 4)
        return base64.urlsafe_b64decode((data + padding).encode("utf-8"))
