"""
google_oauth_config.py
-----------------------
Configuration stub and state management for the Google Calendar OAuth flow.

Responsibilities
----------------
- Hold GoogleOAuthClientConfig (client_id, client_secret, redirect_uri)
- Generate and validate one-time state tokens bound to a user_id
- Build the Google OAuth consent URL
- Exchange an authorisation code for tokens (injectable HTTP client for tests)

Safety constraints
------------------
- No OS-1 / OS-2 imports
- Read-only scopes only
- State tokens are single-use (consumed on first validation)
- Optional runtime persistence is limited to OAuth state context only
"""
from __future__ import annotations

import base64
import json
import hashlib
import hmac
import os
import secrets
import threading
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import HTTPException


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"  # noqa: S105 (not a secret)

CALENDAR_READONLY_SCOPE = "https://www.googleapis.com/auth/calendar.readonly"
GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
GOOGLE_READONLY_SCOPES: tuple[str, ...] = (
    CALENDAR_READONLY_SCOPE,
    GMAIL_READONLY_SCOPE,
)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OAuthConfigStatus:
    configured: bool
    missing_fields: list[str]
    message: str


@dataclass(frozen=True)
class OAuthStateContext:
    """Server-side context associated with a one-time OAuth state token."""

    user_id: str
    redirect_base_url: str | None = None
    household_id: str | None = None


@dataclass(frozen=True)
class OAuthProcessedState:
    """Result of an OAuth callback that already consumed a state token."""

    context: OAuthStateContext
    outcome: str
    processed_at_epoch_s: int


@dataclass(frozen=True)
class GoogleOAuthClientConfig:
    """
    Google OAuth 2.0 client configuration.

    Values are read from environment variables by ``from_env()`` so that no
    credentials are hard-coded in source.  Tests may supply values directly.
    """

    client_id: str
    client_secret: str
    redirect_uri: str

    @classmethod
    def from_env(cls) -> "GoogleOAuthClientConfig":
        client_id = os.environ.get("GOOGLE_CLIENT_ID", "")
        client_secret = os.environ.get("GOOGLE_CLIENT_SECRET", "")
        redirect_uri = os.environ.get("GOOGLE_REDIRECT_URI", "")
        return cls(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
        )

    def status(self) -> OAuthConfigStatus:
        missing_fields: list[str] = []
        if not str(self.client_id).strip():
            missing_fields.append("GOOGLE_CLIENT_ID")
        if not str(self.client_secret).strip():
            missing_fields.append("GOOGLE_CLIENT_SECRET")
        if not str(self.redirect_uri).strip():
            missing_fields.append("GOOGLE_REDIRECT_URI")

        configured = len(missing_fields) == 0
        message = "Google OAuth configured" if configured else "OAuth client not configured"
        return OAuthConfigStatus(
            configured=configured,
            missing_fields=missing_fields,
            message=message,
        )

    def is_configured(self) -> bool:
        return self.status().configured

    def validate(self) -> OAuthConfigStatus:
        return self.status()

    def require_valid_config_or_raise_for_connect(self) -> OAuthConfigStatus:
        status = self.status()
        if not status.configured:
            raise HTTPException(status_code=400, detail="OAuth not configured")
        return status


# ---------------------------------------------------------------------------
# State store — thread-safe, in-memory, single-use tokens
# ---------------------------------------------------------------------------


class OAuthStateStore:
    """
    Thread-safe store of pending OAuth state tokens.

    Each state token is a URL-safe random string bound to a user_id.
    Tokens are consumed (removed) upon first validation to prevent replay.
    """

    def __init__(
        self,
        *,
        persistence_path: str | None = None,
        state_ttl_seconds: int | None = None,
    ) -> None:
        self._store: dict[str, OAuthStateContext] = {}
        self._created_at_epoch_s: dict[str, int] = {}
        self._processed_states: dict[str, OAuthProcessedState] = {}
        self._lock = threading.RLock()
        self._persistence_path = self._normalize_persistence_path(persistence_path)
        self._state_ttl_seconds = self._normalize_state_ttl_seconds(state_ttl_seconds)
        self._load_from_disk()

    @staticmethod
    def _resolve_state_signing_secret() -> str:
        explicit = os.getenv("HPAL_OAUTH_STATE_SIGNING_KEY", "").strip()
        if explicit:
            return explicit

        client_secret = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
        if client_secret:
            return client_secret

        # Deterministic dev fallback to keep local callback flow functional.
        return "hpal-dev-oauth-state-signing-key"

    @staticmethod
    def _b64url_encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")

    @staticmethod
    def _b64url_decode(text: str) -> bytes | None:
        value = str(text or "").strip()
        if not value:
            return None

        padding = "=" * ((4 - (len(value) % 4)) % 4)
        try:
            return base64.urlsafe_b64decode(f"{value}{padding}")
        except Exception:
            return None

    def _build_signed_state(
        self,
        *,
        user_id: str,
        redirect_base_url: str | None,
        household_id: str | None,
    ) -> str:
        payload_obj = {
            "n": secrets.token_urlsafe(16),
            "u": str(user_id),
            "iat": int(time.time()),
        }
        if redirect_base_url:
            payload_obj["r"] = str(redirect_base_url)
        if household_id:
            payload_obj["h"] = str(household_id)

        payload_json = json.dumps(payload_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
        payload_b64 = self._b64url_encode(payload_json)
        signature = hmac.new(
            self._resolve_state_signing_secret().encode("utf-8"),
            payload_b64.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return f"v1.{payload_b64}.{signature}"

    def _decode_signed_state(self, state: str) -> OAuthStateContext | None:
        state_text = str(state or "").strip()
        parts = state_text.split(".")
        if len(parts) != 3:
            return None

        version, payload_b64, signature = parts
        if version != "v1":
            return None

        expected_signature = hmac.new(
            self._resolve_state_signing_secret().encode("utf-8"),
            payload_b64.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected_signature, signature):
            return None

        payload_bytes = self._b64url_decode(payload_b64)
        if payload_bytes is None:
            return None

        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception:
            return None

        if not isinstance(payload, dict):
            return None

        user_id = str(payload.get("u") or "").strip()
        if not user_id:
            return None

        issued_at_raw = payload.get("iat")
        try:
            issued_at_epoch_s = int(issued_at_raw)
        except (TypeError, ValueError):
            return None

        now_epoch_s = int(time.time())
        if issued_at_epoch_s + self._state_ttl_seconds < now_epoch_s:
            return None

        redirect_base_url_raw = payload.get("r")
        household_id_raw = payload.get("h")
        return OAuthStateContext(
            user_id=user_id,
            redirect_base_url=(str(redirect_base_url_raw) if redirect_base_url_raw is not None else None),
            household_id=(str(household_id_raw) if household_id_raw is not None else None),
        )

    @staticmethod
    def _normalize_persistence_path(path: str | None) -> str | None:
        text = (path or "").strip()
        return text or None

    @staticmethod
    def _normalize_state_ttl_seconds(ttl_seconds: int | None) -> int:
        if ttl_seconds is None:
            return 15 * 60
        try:
            parsed = int(ttl_seconds)
        except (TypeError, ValueError):
            return 15 * 60
        return max(60, parsed)

    def _prune_expired_locked(self, *, now_epoch_s: int | None = None) -> bool:
        now = int(now_epoch_s or time.time())
        expired_tokens = [
            token
            for token, created_at in self._created_at_epoch_s.items()
            if created_at + self._state_ttl_seconds < now
        ]
        changed = False
        for token in expired_tokens:
            self._store.pop(token, None)
            self._created_at_epoch_s.pop(token, None)

        if expired_tokens:
            changed = True

        expired_processed_tokens = [
            token
            for token, record in self._processed_states.items()
            if record.processed_at_epoch_s + self._state_ttl_seconds < now
        ]
        for token in expired_processed_tokens:
            self._processed_states.pop(token, None)

        if expired_processed_tokens:
            changed = True

        return changed

    def _persist_locked(self) -> None:
        if not self._persistence_path:
            return

        payload = {
            "state_ttl_seconds": self._state_ttl_seconds,
            "states": [
                {
                    "state": token,
                    "created_at_epoch_s": self._created_at_epoch_s.get(token, int(time.time())),
                    "user_id": context.user_id,
                    "redirect_base_url": context.redirect_base_url,
                    "household_id": context.household_id,
                }
                for token, context in self._store.items()
            ],
            "processed_states": [
                {
                    "state": token,
                    "processed_at_epoch_s": record.processed_at_epoch_s,
                    "outcome": record.outcome,
                    "user_id": record.context.user_id,
                    "redirect_base_url": record.context.redirect_base_url,
                    "household_id": record.context.household_id,
                }
                for token, record in self._processed_states.items()
            ],
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
            # Persistence is best-effort; runtime safety takes priority.
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

        if not isinstance(payload, dict):
            return

        states_payload = payload.get("states")
        if not isinstance(states_payload, list):
            return

        processed_states_payload = payload.get("processed_states")
        if processed_states_payload is not None and not isinstance(processed_states_payload, list):
            processed_states_payload = []

        loaded_store: dict[str, OAuthStateContext] = {}
        loaded_created: dict[str, int] = {}
        loaded_processed: dict[str, OAuthProcessedState] = {}
        for row in states_payload:
            if not isinstance(row, dict):
                continue

            token = str(row.get("state") or "").strip()
            user_id = str(row.get("user_id") or "").strip()
            if not token or not user_id:
                continue

            redirect_base_url_raw = row.get("redirect_base_url")
            household_id_raw = row.get("household_id")
            created_raw = row.get("created_at_epoch_s")
            try:
                created_epoch = int(created_raw) if created_raw is not None else int(time.time())
            except (TypeError, ValueError):
                created_epoch = int(time.time())

            loaded_store[token] = OAuthStateContext(
                user_id=user_id,
                redirect_base_url=(str(redirect_base_url_raw) if redirect_base_url_raw is not None else None),
                household_id=(str(household_id_raw) if household_id_raw is not None else None),
            )
            loaded_created[token] = created_epoch

        for row in processed_states_payload or []:
            if not isinstance(row, dict):
                continue

            token = str(row.get("state") or "").strip()
            user_id = str(row.get("user_id") or "").strip()
            outcome = str(row.get("outcome") or "").strip().lower()
            if not token or not user_id or outcome not in {"success", "failed"}:
                continue

            redirect_base_url_raw = row.get("redirect_base_url")
            household_id_raw = row.get("household_id")
            processed_raw = row.get("processed_at_epoch_s")
            try:
                processed_at_epoch_s = int(processed_raw) if processed_raw is not None else int(time.time())
            except (TypeError, ValueError):
                processed_at_epoch_s = int(time.time())

            loaded_processed[token] = OAuthProcessedState(
                context=OAuthStateContext(
                    user_id=user_id,
                    redirect_base_url=(str(redirect_base_url_raw) if redirect_base_url_raw is not None else None),
                    household_id=(str(household_id_raw) if household_id_raw is not None else None),
                ),
                outcome=outcome,
                processed_at_epoch_s=processed_at_epoch_s,
            )

        with self._lock:
            self._store = loaded_store
            self._created_at_epoch_s = loaded_created
            self._processed_states = loaded_processed
            changed = self._prune_expired_locked()
            if changed:
                self._persist_locked()

    def generate_state(
        self,
        user_id: str,
        *,
        redirect_base_url: str | None = None,
        household_id: str | None = None,
    ) -> str:
        """Create and store a fresh state token for *user_id*."""
        token = self._build_signed_state(
            user_id=user_id,
            redirect_base_url=redirect_base_url,
            household_id=household_id,
        )
        context = OAuthStateContext(
            user_id=str(user_id),
            redirect_base_url=redirect_base_url,
            household_id=household_id,
        )
        with self._lock:
            self._prune_expired_locked()
            self._store[token] = context
            self._created_at_epoch_s[token] = int(time.time())
            self._persist_locked()
        return token

    def validate_and_consume(self, state: str, user_id: str) -> bool:
        """
        Return True and remove the token if *state* maps to *user_id*.
        Return False for unknown tokens or mismatched user_id (state mismatch).
        """
        with self._lock:
            self._prune_expired_locked()
            stored_context = self._store.get(state)
            if stored_context is None:
                return False
            if stored_context.user_id != str(user_id):
                return False
            del self._store[state]
            self._created_at_epoch_s.pop(state, None)
            self._persist_locked()
            return True

    def consume_state(self, state: str) -> str | None:
        """Consume *state* and return the bound user_id, or None if unknown."""
        context = self.consume_state_context(state)
        if context is None:
            return None
        return context.user_id

    def consume_state_context(self, state: str) -> OAuthStateContext | None:
        """Consume *state* and return the full bound context, or None if unknown."""
        with self._lock:
            self._prune_expired_locked()
            context = self._store.pop(state, None)
            if context is not None:
                self._created_at_epoch_s.pop(state, None)
                self._persist_locked()
            return context

    def record_processed_state(self, state: str, context: OAuthStateContext, outcome: str) -> None:
        """Record callback outcome for a consumed state token."""
        normalized_outcome = str(outcome).strip().lower()
        if normalized_outcome not in {"success", "failed"}:
            return

        with self._lock:
            self._prune_expired_locked()
            self._processed_states[state] = OAuthProcessedState(
                context=context,
                outcome=normalized_outcome,
                processed_at_epoch_s=int(time.time()),
            )
            self._persist_locked()

    def peek_processed_state(self, state: str) -> OAuthProcessedState | None:
        """Return processed callback record for *state* if still in retention window."""
        with self._lock:
            self._prune_expired_locked()
            return self._processed_states.get(state)

    def decode_state_context(self, state: str) -> OAuthStateContext | None:
        """Decode state context from signed state token when pending-store lookup misses."""
        return self._decode_signed_state(state)

    def peek(self, state: str) -> str | None:
        """Return the user_id for *state* without consuming it (for testing)."""
        context = self.peek_context(state)
        if context is None:
            return None
        return context.user_id

    def peek_context(self, state: str) -> OAuthStateContext | None:
        """Return full context for *state* without consuming it (for testing)."""
        with self._lock:
            self._prune_expired_locked()
            return self._store.get(state)

    def clear(self) -> None:
        """Remove all pending tokens (for testing)."""
        with self._lock:
            self._store.clear()
            self._created_at_epoch_s.clear()
            self._processed_states.clear()
            self._persist_locked()

    def __len__(self) -> int:
        with self._lock:
            self._prune_expired_locked()
            return len(self._store)


# Module-level singleton — injected into the router at startup.
def _resolve_default_state_store_path() -> str | None:
    configured = os.getenv("HPAL_OAUTH_STATE_STORE_PATH", "").strip()
    if configured:
        return configured

    root_dir = Path(__file__).resolve().parents[3]
    return str(root_dir / "data" / "runtime" / "oauth" / "state_store.json")


def _resolve_default_state_ttl_seconds() -> int:
    configured = os.getenv("HPAL_OAUTH_STATE_TTL_SECONDS", "").strip()
    if not configured:
        return 15 * 60

    try:
        parsed = int(configured)
    except ValueError:
        return 15 * 60
    return max(60, parsed)


_default_state_store = OAuthStateStore(
    persistence_path=_resolve_default_state_store_path(),
    state_ttl_seconds=_resolve_default_state_ttl_seconds(),
)


def get_state_store() -> OAuthStateStore:
    return _default_state_store


# ---------------------------------------------------------------------------
# URL builder
# ---------------------------------------------------------------------------


def build_authorization_url(
    *,
    config: GoogleOAuthClientConfig,
    state: str,
) -> str:
    """
    Build the Google OAuth consent URL.

    Parameters are deterministic for a given (config, state) pair.
    """
    params = {
        "client_id": config.client_id,
        "redirect_uri": config.redirect_uri,
        "response_type": "code",
        "scope": " ".join(GOOGLE_READONLY_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    return f"{GOOGLE_AUTH_URL}?{urllib.parse.urlencode(params)}"


# ---------------------------------------------------------------------------
# Token exchange
# ---------------------------------------------------------------------------


@dataclass
class OAuthTokenResponse:
    access_token: str
    refresh_token: str | None
    token_type: str = "Bearer"
    expires_in: int | None = None
    scope: str = ""


def _google_error_detail(response: Any) -> str:
    """Return a concise error detail string from a Google OAuth error response."""
    status_code = getattr(response, "status_code", None)
    parts: list[str] = []
    if status_code is not None:
        parts.append(f"status={status_code}")

    payload: dict[str, Any] | None = None
    try:
        parsed = response.json()
        if isinstance(parsed, dict):
            payload = parsed
    except Exception:
        payload = None

    if payload is not None:
        error_code = payload.get("error")
        error_description = payload.get("error_description")
        if error_code:
            parts.append(f"error={error_code}")
        if error_description:
            parts.append(f"description={error_description}")

    if len(parts) == 0:
        try:
            body_text = str(getattr(response, "text", "")).strip()
            if body_text:
                compact = body_text.replace("\n", " ")[:240]
                parts.append(f"response={compact}")
        except Exception:
            pass

    if len(parts) == 0:
        return "no response details"
    return ", ".join(parts)


def _post_token_exchange(
    *,
    payload: dict[str, Any],
    http_client: Any,
) -> OAuthTokenResponse:
    response = http_client.post(GOOGLE_TOKEN_URL, data=payload)
    try:
        response.raise_for_status()
    except Exception as exc:
        details = _google_error_detail(response)
        raise RuntimeError(f"google_token_exchange_failed: {details}") from exc
    data: dict[str, Any] = response.json()

    return OAuthTokenResponse(
        access_token=data["access_token"],
        refresh_token=data.get("refresh_token"),
        token_type=data.get("token_type", "Bearer"),
        expires_in=data.get("expires_in"),
        scope=data.get("scope", ""),
    )


def _resolve_default_http_client() -> Any:
    """Return a default HTTP client module for OAuth requests."""
    try:
        import httpx  # noqa: PLC0415

        return httpx
    except ImportError:
        try:
            import requests  # noqa: PLC0415

            return requests
        except ImportError as exc:
            raise RuntimeError(
                "Google OAuth token exchange requires either 'httpx' or 'requests': pip install httpx"
            ) from exc


def exchange_code_for_tokens(
    *,
    code: str,
    config: GoogleOAuthClientConfig,
    http_client: Any = None,
) -> OAuthTokenResponse:
    """
    Exchange an authorisation code for access + refresh tokens.

    Parameters
    ----------
    code:
        The ``code`` query parameter received from the Google callback.
    config:
        OAuth client configuration.
    http_client:
        Injectable HTTP client.  Must expose
        ``post(url, *, data) → response`` where response has
        ``.raise_for_status()`` and ``.json() → dict``.
        When ``None``, uses ``httpx`` (or ``requests`` as fallback).
    """
    if http_client is None:
        http_client = _resolve_default_http_client()

    payload = {
        "code": code,
        "client_id": config.client_id,
        "client_secret": config.client_secret,
        "redirect_uri": config.redirect_uri,
        "grant_type": "authorization_code",
    }
    return _post_token_exchange(payload=payload, http_client=http_client)


def refresh_access_token(
    *,
    refresh_token: str,
    config: GoogleOAuthClientConfig,
    http_client: Any = None,
) -> OAuthTokenResponse:
    """Exchange a refresh token for a new access token."""
    if http_client is None:
        http_client = _resolve_default_http_client()

    payload = {
        "refresh_token": refresh_token,
        "client_id": config.client_id,
        "client_secret": config.client_secret,
        "grant_type": "refresh_token",
    }
    return _post_token_exchange(payload=payload, http_client=http_client)
