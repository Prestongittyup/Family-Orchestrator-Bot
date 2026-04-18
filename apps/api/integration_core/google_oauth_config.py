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
- No persistence beyond in-memory state store (CredentialStore handles tokens)
- Read-only scopes only
- State tokens are single-use (consumed on first validation)
"""
from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import threading
import urllib.parse
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"  # noqa: S105 (not a secret)

CALENDAR_READONLY_SCOPE = "https://www.googleapis.com/auth/calendar.readonly"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OAuthConfigStatus:
    configured: bool
    missing_fields: list[str]
    message: str


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

    def __init__(self) -> None:
        self._store: dict[str, str] = {}  # state_token → user_id
        self._lock = threading.Lock()

    def generate_state(self, user_id: str) -> str:
        """Create and store a fresh state token for *user_id*."""
        token = secrets.token_urlsafe(32)
        with self._lock:
            self._store[token] = str(user_id)
        return token

    def validate_and_consume(self, state: str, user_id: str) -> bool:
        """
        Return True and remove the token if *state* maps to *user_id*.
        Return False for unknown tokens or mismatched user_id (state mismatch).
        """
        with self._lock:
            stored_user = self._store.get(state)
            if stored_user is None:
                return False
            if stored_user != str(user_id):
                return False
            del self._store[state]
            return True

    def consume_state(self, state: str) -> str | None:
        """Consume *state* and return the bound user_id, or None if unknown."""
        with self._lock:
            stored_user = self._store.pop(state, None)
            if stored_user is None:
                return None
            return stored_user

    def peek(self, state: str) -> str | None:
        """Return the user_id for *state* without consuming it (for testing)."""
        with self._lock:
            return self._store.get(state)

    def clear(self) -> None:
        """Remove all pending tokens (for testing)."""
        with self._lock:
            self._store.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)


# Module-level singleton — injected into the router at startup.
_default_state_store = OAuthStateStore()


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
        "scope": CALENDAR_READONLY_SCOPE,
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


def _post_token_exchange(
    *,
    payload: dict[str, Any],
    http_client: Any,
) -> OAuthTokenResponse:
    response = http_client.post(GOOGLE_TOKEN_URL, data=payload)
    response.raise_for_status()
    data: dict[str, Any] = response.json()

    return OAuthTokenResponse(
        access_token=data["access_token"],
        refresh_token=data.get("refresh_token"),
        token_type=data.get("token_type", "Bearer"),
        expires_in=data.get("expires_in"),
        scope=data.get("scope", ""),
    )


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
        When ``None``, uses the ``requests`` library.
    """
    if http_client is None:
        try:
            import requests  # noqa: PLC0415
            http_client = requests
        except ImportError as exc:
            raise RuntimeError(
                "Token exchange requires the 'requests' library: pip install requests"
            ) from exc

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
        try:
            import requests  # noqa: PLC0415
            http_client = requests
        except ImportError as exc:
            raise RuntimeError(
                "Token refresh requires the 'requests' library: pip install requests"
            ) from exc

    payload = {
        "refresh_token": refresh_token,
        "client_id": config.client_id,
        "client_secret": config.client_secret,
        "grant_type": "refresh_token",
    }
    return _post_token_exchange(payload=payload, http_client=http_client)
