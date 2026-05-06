# ARCHIVE MODULE - NOT PART OF ACTIVE RUNTIME
# DO NOT IMPORT INTO app/

"""
integrations_router.py
-----------------------
FastAPI router for Integration Core OAuth connection endpoints.

Endpoints
---------
GET /integrations/google-calendar/connect/{user_id}
    Redirects the user to the Google OAuth consent screen.

GET /integrations/google-calendar/callback
    Receives the OAuth callback, exchanges the code for tokens,
    stores credentials in the CredentialStore, and returns a
    success response.

Constraints
-----------
- No OS-1 imports
- No OS-2 imports
- No persistence beyond CredentialStore
- No agent logic / background workers
"""
from __future__ import annotations

import html
import json
import logging
import os
import urllib.parse
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from archive.apps.api.integration_core.credentials import InMemoryOAuthCredentialStore, OAuthCredential
from archive.apps.api.integration_core.orchestrator import create_orchestrator
from archive.apps.api.integration_core.models.household_state import HouseholdState
from archive.apps.api.integration_core.google_oauth_config import (
    GMAIL_READONLY_SCOPE,
    GOOGLE_READONLY_SCOPES,
    GoogleOAuthClientConfig,
    OAuthStateStore,
    OAuthTokenResponse,
    build_authorization_url,
    exchange_code_for_tokens,
    get_state_store,
    refresh_access_token,
)
from archive.apps.api.ingestion.adapters.provider_email_adapter import ProviderEmailAdapter
from archive.apps.api.ingestion.models import IngestionError
from archive.apps.api.ingestion.service import ingest_email


logger = logging.getLogger(__name__)


ui_router = APIRouter(tags=["ui-lite"])

router = APIRouter(prefix="/integrations", tags=["integrations"])

# ---------------------------------------------------------------------------
# Module-level shared objects
# Swappable at test time via the dependency-override or direct assignment.
# ---------------------------------------------------------------------------


def _resolve_oauth_credential_store_path() -> str | None:
    configured = os.getenv("HPAL_OAUTH_CREDENTIAL_STORE_PATH", "").strip()
    if configured:
        return configured

    root_dir = Path(__file__).resolve().parents[3]
    return str(root_dir / "data" / "runtime" / "oauth" / "credentials_store.json")

# The credential store used by these endpoints. Defaults to a file-backed
# runtime store so OAuth links survive backend restarts in local/dev runs.
_credential_store: InMemoryOAuthCredentialStore = InMemoryOAuthCredentialStore(
    persistence_path=_resolve_oauth_credential_store_path(),
)

# Injectable HTTP client (None = use real requests).
_http_client: Any = None

PROVIDER_NAME = "google_calendar"
GMAIL_API_BASE_URL = "https://gmail.googleapis.com/gmail/v1"

# Last debug snapshot keyed by user_id, shown on the minimal UI page.
_last_debug_snapshot: dict[str, dict[str, Any]] = {}


def _allowed_return_hosts() -> set[str]:
    hosts = {"127.0.0.1", "localhost"}
    configured_hosts = os.getenv("GOOGLE_OAUTH_ALLOWED_RETURN_HOSTS", "")
    for raw_host in configured_hosts.split(","):
        host = raw_host.strip().lower()
        if host:
            hosts.add(host)
    return hosts


def _normalise_return_base_url(candidate: str | None) -> str | None:
    text = (candidate or "").strip()
    if not text:
        return None

    try:
        parsed = urllib.parse.urlparse(text)
    except Exception:
        return None

    if parsed.scheme not in {"http", "https"}:
        return None
    if not parsed.hostname:
        return None

    host = parsed.hostname.lower()
    if host not in _allowed_return_hosts():
        logger.warning("Ignoring OAuth return_base host=%s because it is not allowlisted", host)
        return None

    try:
        port = f":{parsed.port}" if parsed.port else ""
    except ValueError:
        return None

    path = parsed.path.rstrip("/")
    return f"{parsed.scheme}://{host}{port}{path}"


def _resolve_return_base_from_request(request: Request) -> str | None:
    origin_header = request.headers.get("origin")
    normalized_origin = _normalise_return_base_url(origin_header)
    if normalized_origin:
        return normalized_origin

    referer_header = request.headers.get("referer")
    normalized_referer = _normalise_return_base_url(referer_header)
    if normalized_referer:
        return normalized_referer

    return None


def _oauth_success_query(*, user_id: str, household_id: str | None = None) -> str:
    query_params: dict[str, str] = {
        "status": "integration_successful",
        "user_id": user_id,
    }
    if household_id:
        query_params["familyId"] = household_id
    return urllib.parse.urlencode(query_params)


def _resolve_oauth_success_redirect_url(
    *,
    user_id: str,
    household_id: str | None = None,
    return_base_url: str | None = None,
) -> str:
    """
    Build the post-OAuth redirect URL.

    Default behavior remains the current relative path for compatibility and tests.
    Set GOOGLE_OAUTH_SUCCESS_REDIRECT_BASE_URL to route users back to the
    frontend app (for example: http://127.0.0.1:5173).
    """
    query = _oauth_success_query(user_id=user_id, household_id=household_id)

    normalized_return_base = _normalise_return_base_url(return_base_url)
    if normalized_return_base:
        return f"{normalized_return_base.rstrip('/')}/?{query}"

    configured_base = _normalise_return_base_url(os.getenv("GOOGLE_OAUTH_SUCCESS_REDIRECT_BASE_URL", ""))
    if not configured_base:
        return f"/?{query}"

    return f"{configured_base.rstrip('/')}/?{query}"


def _token_exchange_hint_html(*, error_text: str, redirect_uri: str) -> str:
    normalized = error_text.lower()
    hints = [
        f"<li>Configured redirect URI: <code>{html.escape(redirect_uri)}</code></li>",
        "<li>Try the Connect button again; Google authorization codes are single-use.</li>",
    ]

    if "invalid_grant" in normalized or "bad request" in normalized:
        hints.append(
            "<li>In Google Cloud Console, Authorized redirect URI must match exactly (including host, port, and path).</li>"
        )
        hints.append(
            "<li>Confirm your local URL style is consistent: use either <code>127.0.0.1</code> or <code>localhost</code>, not both.</li>"
        )
    if "invalid_client" in normalized or "unauthorized_client" in normalized:
        hints.append(
            "<li>Verify GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET belong to the same OAuth Web application client.</li>"
        )

    return "<ul>" + "".join(hints) + "</ul>"


def get_credential_store() -> InMemoryOAuthCredentialStore:
    """Dependency accessor for the credential store (overridable in tests)."""
    return _credential_store


def get_oauth_config() -> GoogleOAuthClientConfig:
    """Dependency accessor for the OAuth config (overridable in tests)."""
    # Read from environment on each access so runtime changes are visible
    # and tests remain deterministic regardless of import order.
    return GoogleOAuthClientConfig.from_env()


def get_oauth_state_store() -> OAuthStateStore:
    """Dependency accessor for one-time OAuth state token context."""
    return get_state_store()


def get_http_client() -> Any:
    """Dependency accessor for the HTTP client (overridable in tests)."""
    return _http_client


def _resolve_google_http_client(http_client: Any) -> Any:
    if http_client is not None:
        return http_client

    try:
        import httpx  # noqa: PLC0415

        return httpx
    except ImportError:
        try:
            import requests  # noqa: PLC0415

            return requests
        except ImportError as exc:
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "google_http_client_missing",
                    "detail": {
                        "hint": "Install httpx or requests in the backend runtime.",
                    },
                },
            ) from exc


def _is_token_expiring(expires_at: datetime | None, *, skew_seconds: int = 60) -> bool:
    if expires_at is None:
        return False

    timestamp = expires_at
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)

    return timestamp <= (datetime.now(UTC) + timedelta(seconds=skew_seconds))


def _upstream_error_detail(response: Any) -> str:
    status_code = getattr(response, "status_code", None)
    details: list[str] = []
    if status_code is not None:
        details.append(f"status={status_code}")

    try:
        payload = response.json()
    except Exception:
        payload = None

    if isinstance(payload, dict):
        error_obj = payload.get("error")
        if isinstance(error_obj, dict):
            message = str(error_obj.get("message") or "").strip()
            if message:
                details.append(f"error={message}")
            status = str(error_obj.get("status") or "").strip()
            if status:
                details.append(f"upstream_status={status}")
        elif isinstance(error_obj, str) and error_obj.strip():
            details.append(f"error={error_obj.strip()}")
    elif payload is not None:
        details.append(f"error={str(payload)}")

    if not details:
        return "no upstream error detail"
    return ", ".join(details)


def _resolve_google_credentials(
    *,
    user_id: str,
    credential_store: InMemoryOAuthCredentialStore,
    config: GoogleOAuthClientConfig,
    http_client: Any,
) -> OAuthCredential:
    credentials = credential_store.get_credentials(
        user_id=user_id,
        provider_name=PROVIDER_NAME,
    )
    if credentials is None:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "google_calendar_not_connected",
                "detail": {
                    "hint": "Connect Google Calendar before syncing email.",
                },
            },
        )

    if not _is_token_expiring(credentials.expires_at):
        return credentials

    if not credentials.refresh_token:
        raise HTTPException(
            status_code=412,
            detail={
                "message": "google_reauth_required",
                "detail": {
                    "hint": "Reconnect Google integration to restore refresh credentials.",
                },
            },
        )

    try:
        refreshed = refresh_access_token(
            refresh_token=credentials.refresh_token,
            config=config,
            http_client=http_client,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "google_token_refresh_failed",
                "detail": {
                    "error": str(exc),
                },
            },
        ) from exc

    updated_scopes = tuple(credentials.scopes or GOOGLE_READONLY_SCOPES)
    updated_credentials = OAuthCredential(
        user_id=credentials.user_id,
        provider_name=credentials.provider_name,
        access_token=refreshed.access_token,
        refresh_token=refreshed.refresh_token or credentials.refresh_token,
        scopes=updated_scopes,
        expires_at=(
            datetime.now(UTC) + timedelta(seconds=int(refreshed.expires_in or 0))
            if refreshed.expires_in is not None
            else credentials.expires_at
        ),
    )
    credential_store.save_credentials(updated_credentials)
    return updated_credentials


@ui_router.get("/", response_class=HTMLResponse)
def ui_home(user_id: str = "test-user", household_id: str = "hh-001", status: str | None = None) -> HTMLResponse:
    """Single-page Integration Control Panel for local OAuth/debug/brief validation."""
    logger.warning("ROOT ROUTE HIT: integrations_router.py ui_home")
    safe_user_id = html.escape(user_id, quote=True)
    safe_household_id = html.escape(household_id, quote=True)
    safe_status = html.escape(status or "", quote=True)
    snapshot = _last_debug_snapshot.get(user_id)
    pretty_snapshot = json.dumps(snapshot, indent=2, default=str) if snapshot is not None else "{}"

    page = f"""
<html>
    <body style="font-family:Arial,sans-serif;max-width:1000px;margin:24px auto;padding:0 8px;line-height:1.45;">
        <h1>Integration Control Panel</h1>
        <p style="margin-top:0;">Minimal localhost surface for Google Calendar OAuth and Integration Core validation.</p>

        <section style="border:1px solid #ddd;border-radius:8px;padding:12px;margin-bottom:12px;">
            <h2 style="margin:0 0 10px 0;">Integrations</h2>
            <label>User ID:
                <input id="userId" type="text" value="{safe_user_id}" style="margin-right:12px;"/>
            </label>
            <label>Household ID:
                <input id="householdId" type="text" value="{safe_household_id}" style="margin-right:12px;"/>
            </label>
            <button id="connectGoogleBtn" type="button">Connect Google Calendar</button>
            <button id="debugBtn" type="button">View Calendar Debug Data</button>
            <button id="briefBtn" type="button">Refresh Brief (View Brief)</button>
            <div id="uiStatus" style="margin-top:10px;color:#444;"></div>
        </section>

        <section style="border:1px solid #ddd;border-radius:8px;padding:12px;margin-bottom:12px;">
            <h3 style="margin:0 0 8px 0;">System Status</h3>
            <div>Google Calendar: <strong id="googleStatus">Unknown</strong></div>
            <div id="lastAction" style="margin-top:6px;color:#555;"></div>
        </section>

        <section style="border:1px solid #ddd;border-radius:8px;padding:12px;margin-bottom:12px;">
            <h3 style="margin:0 0 8px 0;">Calendar Debug</h3>
            <details open>
                <summary>raw + normalized JSON</summary>
                <pre id="debugJson" style="background:#f6f6f6;padding:10px;border-radius:6px;overflow:auto;max-height:340px;">{html.escape(pretty_snapshot)}</pre>
            </details>
        </section>

        <section style="border:1px solid #ddd;border-radius:8px;padding:12px;margin-bottom:12px;">
            <h3 style="margin:0 0 8px 0;">Brief Output</h3>
            <pre id="briefOutput" style="white-space:pre-wrap;background:#f6f6f6;padding:10px;border-radius:6px;overflow:auto;max-height:340px;">(click Refresh Brief)</pre>
        </section>

        <script>
            (function() {{
                const userInput = document.getElementById('userId');
                const householdInput = document.getElementById('householdId');
                const statusEl = document.getElementById('googleStatus');
                const actionEl = document.getElementById('lastAction');
                const debugPre = document.getElementById('debugJson');
                const briefPre = document.getElementById('briefOutput');
                const uiStatus = document.getElementById('uiStatus');

                function logAction(message, isError) {{
                    actionEl.textContent = message;
                    actionEl.style.color = isError ? '#a00' : '#555';
                    if (isError) console.error(message);
                }}

                async function fetchDebug() {{
                    const userId = encodeURIComponent(userInput.value || 'test-user');
                    try {{
                        const response = await fetch(`/debug/google-calendar/${{userId}}`);
                        if (!response.ok) throw new Error(`Debug request failed: ${{response.status}}`);
                        const data = await response.json();
                        debugPre.textContent = JSON.stringify(data, null, 2);
                        statusEl.textContent = data.credential_present ? 'Connected' : 'Not Connected';
                        statusEl.style.color = data.credential_present ? '#0a7a00' : '#8a6d00';
                        logAction('Calendar debug data refreshed.', false);
                        return data;
                    }} catch (err) {{
                        logAction(`Error loading debug data: ${{err.message}}`, true);
                        uiStatus.textContent = `Error loading debug data: ${{err.message}}`;
                        uiStatus.style.color = '#a00';
                        return null;
                    }}
                }}

                async function fetchBrief() {{
                    const userId = encodeURIComponent(userInput.value || 'test-user');
                    const householdId = encodeURIComponent(householdInput.value || 'hh-001');
                    try {{
                        const response = await fetch(`/brief/${{householdId}}?user_id=${{userId}}`);
                        if (!response.ok) throw new Error(`Brief request failed: ${{response.status}}`);
                        const data = await response.json();
                        const rendered = data.rendered || (data.brief ? JSON.stringify(data.brief, null, 2) : JSON.stringify(data, null, 2));
                        briefPre.textContent = typeof rendered === 'string' ? rendered : JSON.stringify(rendered, null, 2);
                        logAction('Brief refreshed.', false);
                    }} catch (err) {{
                        briefPre.textContent = `Error: ${{err.message}}`;
                        logAction(`Error loading brief: ${{err.message}}`, true);
                    }}
                }}

                document.getElementById('connectGoogleBtn').addEventListener('click', function() {{
                    const userId = encodeURIComponent(userInput.value || 'test-user');
                    window.location.href = `/integrations/google-calendar/connect/${{userId}}`;
                }});

                document.getElementById('debugBtn').addEventListener('click', fetchDebug);
                document.getElementById('briefBtn').addEventListener('click', fetchBrief);

                const qpStatus = '{safe_status}';
                if (qpStatus) {{
                    uiStatus.textContent = `Status: ${{qpStatus}}`;
                    uiStatus.style.color = '#0a7a00';
                }}

                // Initial status refresh on page load.
                fetchDebug();
            }})();
        </script>
    </body>
</html>
"""
    return HTMLResponse(content=page, status_code=200)


# ---------------------------------------------------------------------------
# Connect endpoint
# ---------------------------------------------------------------------------


@router.get(
    "/google-calendar/connect/{user_id}",
    summary="Start Google Calendar OAuth flow",
    response_class=RedirectResponse,
    response_model=None,
    status_code=302,
)
def connect_google_calendar(
    user_id: str,
    request: Request,
    return_base: str | None = Query(
        None,
        description="Optional frontend base URL to return to after OAuth callback.",
    ),
    household_id: str | None = Query(
        None,
        description="Optional household identifier to round-trip into callback success redirect.",
    ),
    config: GoogleOAuthClientConfig = Depends(get_oauth_config),
    state_store: OAuthStateStore = Depends(get_oauth_state_store),
) -> Any:
    """
    Redirect the user to Google's OAuth consent screen.

    - Generates a secure state token bound to *user_id*.
    - Builds the consent URL with ``calendar.readonly`` scope.
    - Returns a 302 redirect when configured.
    """
    try:
        config.require_valid_config_or_raise_for_connect()
    except HTTPException:
        return JSONResponse(
            status_code=400,
            content={
                "status": "disabled",
                "integration": "google_calendar",
                "reason": "OAuth client not configured",
                "action": "set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET",
            },
        )

    normalized_household_id = household_id.strip() if household_id and household_id.strip() else None
    normalized_return_base = _normalise_return_base_url(return_base) or _resolve_return_base_from_request(request)
    state = state_store.generate_state(
        user_id=user_id,
        redirect_base_url=normalized_return_base,
        household_id=normalized_household_id,
    )
    url = build_authorization_url(config=config, state=state)
    return RedirectResponse(url=url, status_code=302)


# ---------------------------------------------------------------------------
# Callback endpoint
# ---------------------------------------------------------------------------


@router.get(
    "/google-calendar/callback",
    summary="Google Calendar OAuth callback",
    response_class=HTMLResponse,
)
def google_calendar_callback(
    code: str = Query(..., description="Authorisation code from Google"),
    state: str = Query(..., description="State from Google callback"),
    user_id: str | None = Query(None, description="Optional user ID. If provided, must match the state token."),
    config: GoogleOAuthClientConfig = Depends(get_oauth_config),
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
    state_store: OAuthStateStore = Depends(get_oauth_state_store),
) -> RedirectResponse:
    """
    Receive the authorisation code, validate state, exchange for tokens,
    store credentials, and return a minimal success page.

    Rejects mismatched state to prevent CSRF.
    """
    # 1) Validate state <-> user binding via one-time server-side context.
    consumed_context = state_store.consume_state_context(state)
    if consumed_context is None:
        processed_state = state_store.peek_processed_state(state)
        if processed_state is not None:
            processed_user_id = processed_state.context.user_id
            if user_id is not None and str(user_id) != processed_user_id:
                return HTMLResponse(
                    status_code=400,
                    content=(
                        "<html><body><h3>OAuth state mismatch</h3>"
                        "<p>The callback state does not match the provided user_id.</p>"
                        "</body></html>"
                    ),
                )

            if processed_state.outcome == "success":
                return RedirectResponse(
                    url=_resolve_oauth_success_redirect_url(
                        user_id=processed_state.context.user_id,
                        household_id=processed_state.context.household_id,
                        return_base_url=processed_state.context.redirect_base_url,
                    ),
                    status_code=302,
                )

            return HTMLResponse(
                status_code=400,
                content=(
                    "<html><body><h3>OAuth callback already processed</h3>"
                    "<p>This callback state was already handled and cannot be reused. "
                    "Please restart the Google connection flow.</p>"
                    "</body></html>"
                ),
            )

        decoded_context = state_store.decode_state_context(state)
        if decoded_context is not None:
            consumed_context = decoded_context
        else:
            return HTMLResponse(
                status_code=400,
                content=(
                    "<html><body><h3>OAuth state mismatch</h3>"
                    "<p>The callback state was not recognized or has already been used.</p>"
                    "</body></html>"
                ),
            )

    target_user_id = consumed_context.user_id
    if user_id is not None and str(user_id) != target_user_id:
        state_store.record_processed_state(state, consumed_context, "failed")
        return HTMLResponse(
            status_code=400,
            content=(
                "<html><body><h3>OAuth state mismatch</h3>"
                "<p>The callback state does not match the provided user_id.</p>"
                "</body></html>"
            ),
        )

    # 2) Exchange code for tokens
    try:
        token_response: OAuthTokenResponse = exchange_code_for_tokens(
            code=code,
            config=config,
            http_client=http_client,
        )
    except Exception as exc:
        state_store.record_processed_state(state, consumed_context, "failed")
        error_text = str(exc)
        hint_html = _token_exchange_hint_html(error_text=error_text, redirect_uri=config.redirect_uri)
        return HTMLResponse(
            status_code=502,
            content=(
                "<html><body><h3>Google OAuth token exchange failed</h3>"
                f"<p>{html.escape(error_text)}</p>"
                f"{hint_html}"
                "</body></html>"
            ),
        )

    # 3) Store credentials under user_id + provider_name
    existing_credentials = credential_store.get_credentials(
        user_id=target_user_id,
        provider_name=PROVIDER_NAME,
    )
    effective_refresh_token = token_response.refresh_token
    if not effective_refresh_token and existing_credentials and existing_credentials.refresh_token:
        logger.debug(
            "Google OAuth callback omitted refresh token; preserving existing refresh token for user_id=%s",
            target_user_id,
        )
        effective_refresh_token = existing_credentials.refresh_token

    if not effective_refresh_token:
        logger.warning(
            "Google OAuth callback did not provide a refresh token for user_id=%s; future re-auth may be required",
            target_user_id,
        )

    credential = OAuthCredential(
        user_id=target_user_id,
        provider_name=PROVIDER_NAME,
        access_token=token_response.access_token,
        refresh_token=effective_refresh_token,
        scopes=GOOGLE_READONLY_SCOPES,
        expires_at=(
            datetime.now(UTC) + timedelta(seconds=int(token_response.expires_in or 0))
            if token_response.expires_in is not None
            else None
        ),
    )
    credential_store.save_credentials(credential)
    state_store.record_processed_state(state, consumed_context, "success")

    # 4) Redirect back to the configured user-facing UI.
    return RedirectResponse(
        url=_resolve_oauth_success_redirect_url(
            user_id=target_user_id,
            household_id=consumed_context.household_id,
            return_base_url=consumed_context.redirect_base_url,
        ),
        status_code=302,
    )


@router.post("/google-email/sync/{user_id}")
def sync_google_email(
    user_id: str,
    household_id: str = Query(..., description="Household ID to attribute ingested email events to."),
    max_results: int = Query(100, ge=1, le=100, description="Maximum inbox messages to fetch and ingest."),
    config: GoogleOAuthClientConfig = Depends(get_oauth_config),
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
) -> dict[str, Any]:
    normalized_household_id = household_id.strip()
    if not normalized_household_id:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "household_id_required",
            },
        )

    resolved_http_client = _resolve_google_http_client(http_client)
    credentials = _resolve_google_credentials(
        user_id=user_id,
        credential_store=credential_store,
        config=config,
        http_client=resolved_http_client,
    )

    headers = {
        "Authorization": f"Bearer {credentials.access_token}",
        "Accept": "application/json",
    }

    list_response = resolved_http_client.get(
        f"{GMAIL_API_BASE_URL}/users/me/messages",
        headers=headers,
        params={
            "labelIds": "INBOX",
            "maxResults": max_results,
        },
    )

    try:
        list_response.raise_for_status()
    except Exception as exc:
        upstream_status = int(getattr(list_response, "status_code", 502) or 502)
        upstream_error = _upstream_error_detail(list_response)
        if upstream_status in {401, 403}:
            raise HTTPException(
                status_code=412,
                detail={
                    "message": "gmail_scope_or_token_invalid",
                    "detail": {
                        "hint": "Reconnect Google integration to grant Gmail read access.",
                        "required_scope": GMAIL_READONLY_SCOPE,
                        "upstream": upstream_error,
                    },
                },
            ) from exc
        raise HTTPException(
            status_code=502,
            detail={
                "message": "gmail_list_failed",
                "detail": {
                    "upstream": upstream_error,
                },
            },
        ) from exc

    try:
        list_payload = list_response.json()
    except Exception:
        list_payload = {}

    message_rows = list_payload.get("messages", []) if isinstance(list_payload, dict) else []
    if not isinstance(message_rows, list):
        message_rows = []

    adapter = ProviderEmailAdapter(provider_name="gmail")
    results: list[dict[str, Any]] = []
    processed_count = 0
    ignored_count = 0
    failed_count = 0
    seen_thread_ids: set[str] = set()

    for row in message_rows:
        message_id = ""
        if isinstance(row, dict):
            message_id = str(row.get("id") or "").strip()
        if not message_id:
            continue

        detail_response = resolved_http_client.get(
            f"{GMAIL_API_BASE_URL}/users/me/messages/{urllib.parse.quote(message_id, safe='')}",
            headers=headers,
            params={"format": "full"},
        )

        try:
            detail_response.raise_for_status()
            raw_message = detail_response.json()
        except Exception:
            failed_count += 1
            results.append(
                {
                    "message_id": message_id,
                    "status": "failed",
                    "error": {
                        "message": "gmail_message_fetch_failed",
                        "detail": {
                            "upstream": _upstream_error_detail(detail_response),
                        },
                    },
                }
            )
            continue

        if not isinstance(raw_message, dict):
            raw_message = {}

        thread_id = str(raw_message.get("threadId") or raw_message.get("thread_id") or "").strip()
        if thread_id and thread_id in seen_thread_ids:
            ignored_count += 1
            results.append(
                {
                    "message_id": message_id,
                    "status": "ignored",
                    "result": {
                        "status": "thread_superseded",
                        "event_id": "",
                    },
                }
            )
            continue
        if thread_id:
            seen_thread_ids.add(thread_id)

        thread_messages: list[str] = []
        if thread_id:
            thread_response = resolved_http_client.get(
                f"{GMAIL_API_BASE_URL}/users/me/threads/{urllib.parse.quote(thread_id, safe='')}",
                headers=headers,
                params={"format": "metadata"},
            )
            try:
                thread_response.raise_for_status()
                thread_payload = thread_response.json()
                if isinstance(thread_payload, dict):
                    messages = thread_payload.get("messages")
                    if isinstance(messages, list):
                        for message in messages[-3:]:
                            if not isinstance(message, dict):
                                continue
                            snippet = str(message.get("snippet") or "").strip()
                            if snippet:
                                thread_messages.append(snippet)
            except Exception:
                thread_messages = []

        parsed = adapter.parse_message(raw_message)
        received_at = str(parsed.received_at or "").strip() or datetime.now(UTC).isoformat().replace("+00:00", "Z")
        subject = str(parsed.subject or "").strip() or "Email update"
        sender = str(parsed.sender or "").strip() or "unknown@unknown"
        recipient = str(parsed.recipient or "").strip()
        body = str(parsed.body or "").strip() or str(raw_message.get("snippet") or "")
        email_id_value = str(parsed.email_id or "").strip() or message_id
        latest_message_id = str(parsed.latest_message_id or email_id_value).strip() or email_id_value

        try:
            ingest_result = ingest_email(
                email_id=email_id_value,
                sender=sender,
                recipient=recipient,
                subject=subject,
                body=body,
                received_at=received_at,
                provider="gmail",
                household_id=normalized_household_id,
                thread_id=thread_id or parsed.thread_id,
                latest_message_id=latest_message_id,
                thread_messages=thread_messages or list(parsed.thread_messages or []),
                to_me=parsed.to_me,
                cc_me=parsed.cc_me,
            )
            ingest_status = str(ingest_result.get("status") or "success").strip().lower() or "success"
            result_status = "processed"
            if ingest_status in {"ignored_junk", "duplicate_ignored"}:
                ignored_count += 1
                result_status = "ignored"
            elif ingest_status == "success":
                processed_count += 1
            else:
                failed_count += 1
                result_status = "failed"

            results.append(
                {
                    "message_id": message_id,
                    "status": result_status,
                    "result": {
                        "status": ingest_status,
                        "event_id": str(ingest_result.get("event_id") or ""),
                    },
                }
            )
        except IngestionError as exc:
            failed_count += 1
            results.append(
                {
                    "message_id": message_id,
                    "status": "failed",
                    "error": {
                        "message": exc.message,
                        "detail": exc.detail,
                        "status_code": exc.status_code,
                    },
                }
            )

    return {
        "status": "ok",
        "provider": "gmail",
        "count": len(results),
        "processed_count": processed_count,
        "ignored_count": ignored_count,
        "failed_count": failed_count,
        "results": results,
    }


@router.get("/google-calendar/status/{user_id}")
def google_calendar_status(
    user_id: str,
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
) -> dict[str, Any]:
    """Return non-sensitive Google Calendar connection status for the UI."""
    credentials = credential_store.get_credentials(
        user_id=user_id,
        provider_name=PROVIDER_NAME,
    )
    connected = credentials is not None
    return {
        "user_id": user_id,
        "provider_name": PROVIDER_NAME,
        "connected": connected,
        "expires_at": credentials.expires_at.isoformat() if connected and credentials.expires_at else None,
        "scopes": list(credentials.scopes) if connected else [],
    }


@ui_router.get("/debug/google-calendar/{user_id}")
def debug_google_calendar(
    user_id: str,
    max_results: int = 25,
    mode: str | None = None,
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
) -> dict[str, Any]:
    """
    Return a full HouseholdState debug projection for *user_id*.

    Provider selection is handled internally by Orchestrator.
    Endpoints must not import or construct provider classes directly.
    """
    provider_key = "google_calendar"
    creds = credential_store.get_credentials(user_id=user_id, provider_name=provider_key)
    credential_present = creds is not None

    orchestrator = create_orchestrator(
        credential_store=credential_store,
        http_client=http_client,
        max_results=max_results,
        provider_mode=mode,
    )
    state: HouseholdState = orchestrator.build_household_state(user_id)

    selected_mode = str(mode or "real").lower()
    response = {
        "user_id": user_id,
        "mode": selected_mode,
        "provider_name": provider_key,
        "credential_present": credential_present,
        **state.debug(),
    }
    _last_debug_snapshot[user_id] = response
    return response

