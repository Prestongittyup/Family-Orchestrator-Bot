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
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore, OAuthCredential
from apps.api.integration_core.orchestrator import create_orchestrator
from apps.api.integration_core.models.household_state import HouseholdState
from apps.api.integration_core.google_oauth_config import (
    CALENDAR_READONLY_SCOPE,
    GoogleOAuthClientConfig,
    OAuthTokenResponse,
    build_authorization_url,
    exchange_code_for_tokens,
)


logger = logging.getLogger(__name__)


ui_router = APIRouter(tags=["ui-lite"])

router = APIRouter(prefix="/integrations", tags=["integrations"])

# ---------------------------------------------------------------------------
# Module-level shared objects
# Swappable at test time via the dependency-override or direct assignment.
# ---------------------------------------------------------------------------

# The credential store used by these endpoints.  In production this should
# be replaced with a persistent store via the dependency system.
_credential_store: InMemoryOAuthCredentialStore = InMemoryOAuthCredentialStore()

# The OAuth client config.  Reads from env vars by default.
_oauth_config: GoogleOAuthClientConfig = GoogleOAuthClientConfig.from_env()

# Injectable HTTP client (None = use real requests).
_http_client: Any = None

PROVIDER_NAME = "google_calendar"

# Last debug snapshot keyed by user_id, shown on the minimal UI page.
_last_debug_snapshot: dict[str, dict[str, Any]] = {}


def get_credential_store() -> InMemoryOAuthCredentialStore:
    """Dependency accessor for the credential store (overridable in tests)."""
    return _credential_store


def get_oauth_config() -> GoogleOAuthClientConfig:
    """Dependency accessor for the OAuth config (overridable in tests)."""
    return _oauth_config


def get_http_client() -> Any:
    """Dependency accessor for the HTTP client (overridable in tests)."""
    return _http_client


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
    config: GoogleOAuthClientConfig = Depends(get_oauth_config),
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

    # Contract: state carries user_id for callback binding.
    state = str(user_id)
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
    state: str = Query(..., description="State from Google callback (bound to user_id)"),
    user_id: str | None = Query(None, description="Optional user ID. If provided, must match the state token."),
    config: GoogleOAuthClientConfig = Depends(get_oauth_config),
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
) -> RedirectResponse:
    """
    Receive the authorisation code, validate state, exchange for tokens,
    store credentials, and return a minimal success page.

    Rejects mismatched state to prevent CSRF.
    """
    # 1) Validate state <-> user binding (state must encode user_id)
    target_user_id = str(state)
    if user_id is not None and str(user_id) != target_user_id:
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
        return HTMLResponse(
            status_code=502,
            content=(
                "<html><body><h3>Google OAuth token exchange failed</h3>"
                f"<p>{html.escape(str(exc))}</p>"
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
        scopes=(CALENDAR_READONLY_SCOPE,),
        expires_at=(
            datetime.now(UTC) + timedelta(seconds=int(token_response.expires_in or 0))
            if token_response.expires_in is not None
            else None
        ),
    )
    credential_store.save_credentials(credential)

    # 4) Redirect back to UI-lite surface
    return RedirectResponse(
        url=f"/?status=integration_successful&user_id={target_user_id}",
        status_code=302,
    )


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
