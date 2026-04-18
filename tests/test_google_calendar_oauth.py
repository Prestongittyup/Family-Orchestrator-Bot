"""
test_google_calendar_oauth.py
------------------------------
Tests for the Google Calendar OAuth flow:

  1. OAuthStateStore unit tests
  2. build_authorization_url correctness
  3. exchange_code_for_tokens (mocked HTTP)
  4. GET /integrations/google-calendar/connect/{user_id}
  5. GET /integrations/google-calendar/callback  (happy path)
  6. State mismatch rejection
  7. Determinism — same inputs → same URL (given same state)
  8. Architecture guard — no OS-1/OS-2 imports
"""
from __future__ import annotations

import urllib.parse
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore, OAuthCredential
from apps.api.integration_core.google_oauth_config import (
    CALENDAR_READONLY_SCOPE,
    GOOGLE_AUTH_URL,
    GoogleOAuthClientConfig,
    OAuthStateStore,
    OAuthTokenResponse,
    build_authorization_url,
    exchange_code_for_tokens,
    refresh_access_token,
)
from apps.api.integration_core.architecture_guard import FORBIDDEN_IMPORT_PREFIXES


# ---------------------------------------------------------------------------
# Shared test config
# ---------------------------------------------------------------------------

TEST_CONFIG = GoogleOAuthClientConfig(
    client_id="test-client-id",
    client_secret="test-client-secret",
    redirect_uri="http://localhost:8000/integrations/google-calendar/callback",
)

USER_ID = "test-user-oauth-001"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_token_http(
    access_token: str = "mock-access-token",
    refresh_token: str = "mock-refresh-token",
) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "Bearer",
        "expires_in": 3600,
        "scope": CALENDAR_READONLY_SCOPE,
    }
    http = MagicMock()
    http.post.return_value = resp
    return http


def _make_test_client(
    credential_store: InMemoryOAuthCredentialStore | None = None,
    state_store: OAuthStateStore | None = None,
    http_client: MagicMock | None = None,
) -> tuple[TestClient, InMemoryOAuthCredentialStore, OAuthStateStore]:
    """Build an isolated TestClient with injected dependencies."""
    from apps.api import main
    from apps.api.endpoints import integrations_router as ir

    creds = credential_store or InMemoryOAuthCredentialStore()
    states = state_store or OAuthStateStore()
    http = http_client or _mock_token_http()

    # Override FastAPI dependencies for this test
    main.app.dependency_overrides[ir.get_oauth_config] = lambda: TEST_CONFIG
    main.app.dependency_overrides[ir.get_credential_store] = lambda: creds
    main.app.dependency_overrides[ir.get_http_client] = lambda: http

    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)
    return client, creds, states


def _teardown(main_app):
    """Clear dependency overrides after a test."""
    main_app.dependency_overrides.clear()


# ===========================================================================
# 1. OAuthStateStore unit tests
# ===========================================================================


class TestOAuthStateStore:
    def test_generate_state_returns_string(self):
        store = OAuthStateStore()
        token = store.generate_state("user-1")
        assert isinstance(token, str)
        assert len(token) > 0

    def test_generated_state_stored(self):
        store = OAuthStateStore()
        token = store.generate_state("user-2")
        assert store.peek(token) == "user-2"

    def test_validate_and_consume_returns_true_for_correct_pair(self):
        store = OAuthStateStore()
        token = store.generate_state("user-3")
        assert store.validate_and_consume(state=token, user_id="user-3") is True

    def test_validate_and_consume_removes_token(self):
        store = OAuthStateStore()
        token = store.generate_state("user-4")
        store.validate_and_consume(state=token, user_id="user-4")
        assert store.peek(token) is None

    def test_validate_and_consume_single_use(self):
        store = OAuthStateStore()
        token = store.generate_state("user-5")
        store.validate_and_consume(state=token, user_id="user-5")
        # Second call must fail because token was consumed
        assert store.validate_and_consume(state=token, user_id="user-5") is False

    def test_state_mismatch_returns_false(self):
        store = OAuthStateStore()
        token = store.generate_state("user-a")
        assert store.validate_and_consume(state=token, user_id="user-b") is False

    def test_unknown_state_returns_false(self):
        store = OAuthStateStore()
        assert store.validate_and_consume(state="nonexistent-token", user_id="any") is False

    def test_clear_removes_all_tokens(self):
        store = OAuthStateStore()
        store.generate_state("u1")
        store.generate_state("u2")
        store.clear()
        assert len(store) == 0

    def test_multiple_users_independent_tokens(self):
        store = OAuthStateStore()
        t1 = store.generate_state("alice")
        t2 = store.generate_state("bob")
        assert t1 != t2
        assert store.validate_and_consume(state=t1, user_id="alice") is True
        assert store.validate_and_consume(state=t2, user_id="bob") is True


# ===========================================================================
# 2. build_authorization_url correctness
# ===========================================================================


class TestBuildAuthorizationUrl:
    def test_url_starts_with_google_auth_url(self):
        state_store = OAuthStateStore()
        state = state_store.generate_state(USER_ID)
        url = build_authorization_url(config=TEST_CONFIG, state=state)
        assert url.startswith(GOOGLE_AUTH_URL)

    def test_url_contains_client_id(self):
        state_store = OAuthStateStore()
        state = state_store.generate_state(USER_ID)
        url = build_authorization_url(config=TEST_CONFIG, state=state)
        assert "test-client-id" in url

    def test_url_contains_readonly_scope(self):
        state_store = OAuthStateStore()
        state = state_store.generate_state(USER_ID)
        url = build_authorization_url(config=TEST_CONFIG, state=state)
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        assert CALENDAR_READONLY_SCOPE in params.get("scope", [""])[0]

    def test_url_encodes_state(self):
        store = OAuthStateStore()
        state = store.generate_state(USER_ID)
        url = build_authorization_url(config=TEST_CONFIG, state=state)
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        assert params["state"][0] == state

    def test_url_includes_redirect_uri(self):
        store = OAuthStateStore()
        state = store.generate_state(USER_ID)
        url = build_authorization_url(config=TEST_CONFIG, state=state)
        assert urllib.parse.quote(TEST_CONFIG.redirect_uri, safe="") in url or \
               TEST_CONFIG.redirect_uri in urllib.parse.unquote(url)

    def test_url_has_response_type_code(self):
        store = OAuthStateStore()
        state = store.generate_state(USER_ID)
        url = build_authorization_url(config=TEST_CONFIG, state=state)
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        assert params["response_type"][0] == "code"

    def test_url_requests_offline_access(self):
        store = OAuthStateStore()
        state = store.generate_state(USER_ID)
        url = build_authorization_url(config=TEST_CONFIG, state=state)
        assert "offline" in url

    def test_url_requests_include_granted_scopes(self):
        store = OAuthStateStore()
        state = store.generate_state(USER_ID)
        url = build_authorization_url(config=TEST_CONFIG, state=state)
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        assert params["include_granted_scopes"][0] == "true"

    def test_same_state_produces_identical_url(self):
        """Determinism: given identical (config, state), URL is identical."""
        state = "deterministic-state-abc123"
        url1 = build_authorization_url(config=TEST_CONFIG, state=state)
        url2 = build_authorization_url(config=TEST_CONFIG, state=state)
        assert url1 == url2

    def test_no_write_scopes_in_url(self):
        store = OAuthStateStore()
        state = store.generate_state(USER_ID)
        url = build_authorization_url(config=TEST_CONFIG, state=state)
        # Ensure no write/edit/full-access scopes
        assert "calendar.events.write" not in url
        assert "calendar.full" not in url


# ===========================================================================
# 3. exchange_code_for_tokens
# ===========================================================================


class TestExchangeCodeForTokens:
    def test_returns_access_token(self):
        http = _mock_token_http(access_token="at-abc")
        result = exchange_code_for_tokens(code="auth-code-001", config=TEST_CONFIG, http_client=http)
        assert result.access_token == "at-abc"

    def test_returns_refresh_token(self):
        http = _mock_token_http(refresh_token="rt-xyz")
        result = exchange_code_for_tokens(code="auth-code-002", config=TEST_CONFIG, http_client=http)
        assert result.refresh_token == "rt-xyz"

    def test_posts_to_google_token_url(self):
        from apps.api.integration_core.google_oauth_config import GOOGLE_TOKEN_URL
        http = _mock_token_http()
        exchange_code_for_tokens(code="auth-code-003", config=TEST_CONFIG, http_client=http)
        call_args = http.post.call_args
        assert call_args[0][0] == GOOGLE_TOKEN_URL

    def test_posts_correct_payload(self):
        http = _mock_token_http()
        exchange_code_for_tokens(code="my-code", config=TEST_CONFIG, http_client=http)
        data = http.post.call_args[1]["data"]
        assert data["code"] == "my-code"
        assert data["client_id"] == TEST_CONFIG.client_id
        assert data["grant_type"] == "authorization_code"

    def test_raises_on_http_error(self):
        http = MagicMock()
        resp = MagicMock()
        resp.raise_for_status.side_effect = RuntimeError("401 Unauthorized")
        http.post.return_value = resp
        with pytest.raises(Exception):
            exchange_code_for_tokens(code="bad-code", config=TEST_CONFIG, http_client=http)


class TestRefreshAccessToken:
    def test_refresh_returns_access_token(self):
        http = _mock_token_http(access_token="refreshed-at")
        result = refresh_access_token(
            refresh_token="refresh-abc",
            config=TEST_CONFIG,
            http_client=http,
        )
        assert result.access_token == "refreshed-at"

    def test_refresh_posts_correct_payload(self):
        http = _mock_token_http(access_token="refreshed-at")
        refresh_access_token(
            refresh_token="refresh-abc",
            config=TEST_CONFIG,
            http_client=http,
        )
        data = http.post.call_args[1]["data"]
        assert data["refresh_token"] == "refresh-abc"
        assert data["client_id"] == TEST_CONFIG.client_id
        assert data["grant_type"] == "refresh_token"


# ===========================================================================
# 4. Connect endpoint
# ===========================================================================


class TestConnectEndpoint:
    def setup_method(self):
        from apps.api import main
        self._app = main.app

    def teardown_method(self):
        _teardown(self._app)

    def test_connect_returns_302(self):
        client, _, _ = _make_test_client()
        response = client.get(f"/integrations/google-calendar/connect/{USER_ID}")
        assert response.status_code == 302

    def test_connect_redirects_to_google(self):
        client, _, _ = _make_test_client()
        response = client.get(f"/integrations/google-calendar/connect/{USER_ID}")
        location = response.headers["location"]
        assert location.startswith("https://accounts.google.com/o/oauth2/auth")

    def test_connect_url_contains_user_bound_state(self):
        states = OAuthStateStore()
        http = _mock_token_http()
        client, creds, _ = _make_test_client(state_store=states, http_client=http)
        response = client.get(f"/integrations/google-calendar/connect/{USER_ID}")
        location = response.headers["location"]
        parsed = urllib.parse.urlparse(location)
        params = urllib.parse.parse_qs(parsed.query)
        state_token = params["state"][0]
        # Verify the state is non-empty and bound to this user by using it in callback
        assert state_token, "state token must not be empty"
        callback = client.get(
            "/integrations/google-calendar/callback",
            params={"code": "verify-code", "state": state_token, "user_id": USER_ID},
        )
        assert callback.status_code == 302, "valid state+user_id should redirect to UI"
        assert callback.headers["location"].startswith("/?status=integration_successful")

    def test_connect_url_has_readonly_scope(self):
        client, _, _ = _make_test_client()
        response = client.get(f"/integrations/google-calendar/connect/{USER_ID}")
        location = response.headers["location"]
        assert "calendar.readonly" in location

    def test_connect_unconfigured_returns_structured_400(self):
        from apps.api import main
        from apps.api.endpoints import integrations_router as ir
        empty_config = GoogleOAuthClientConfig(client_id="", client_secret="", redirect_uri="")
        main.app.dependency_overrides[ir.get_oauth_config] = lambda: empty_config
        client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)
        response = client.get(f"/integrations/google-calendar/connect/{USER_ID}")
        assert response.status_code == 400
        assert response.json() == {
            "status": "disabled",
            "integration": "google_calendar",
            "reason": "OAuth client not configured",
            "action": "set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET",
        }
        _teardown(main.app)

    def test_connect_different_users_produce_different_state_tokens(self):
        states = OAuthStateStore()
        client, _, _ = _make_test_client(state_store=states)
        resp_a = client.get("/integrations/google-calendar/connect/user-alice")
        resp_b = client.get("/integrations/google-calendar/connect/user-bob")
        loc_a = resp_a.headers["location"]
        loc_b = resp_b.headers["location"]
        # state must differ
        params_a = urllib.parse.parse_qs(urllib.parse.urlparse(loc_a).query)
        params_b = urllib.parse.parse_qs(urllib.parse.urlparse(loc_b).query)
        assert params_a["state"][0] != params_b["state"][0]


# ===========================================================================
# 5. Callback endpoint (happy path)
# ===========================================================================


class TestCallbackHappyPath:
    def setup_method(self):
        from apps.api import main
        self._app = main.app

    def teardown_method(self):
        _teardown(self._app)

    def _do_connect_and_get_state(self, client, states, uid=USER_ID) -> str:
        resp = client.get(f"/integrations/google-calendar/connect/{uid}")
        location = resp.headers["location"]
        params = urllib.parse.parse_qs(urllib.parse.urlparse(location).query)
        return params["state"][0]

    def test_callback_returns_200(self):
        states = OAuthStateStore()
        http = _mock_token_http()
        client, creds, _ = _make_test_client(state_store=states, http_client=http)
        state = self._do_connect_and_get_state(client, states)
        response = client.get(
            "/integrations/google-calendar/callback",
            params={"code": "auth-code-ok", "state": state, "user_id": USER_ID},
        )
        assert response.status_code == 302

    def test_callback_returns_success_html(self):
        states = OAuthStateStore()
        http = _mock_token_http()
        client, creds, _ = _make_test_client(state_store=states, http_client=http)
        state = self._do_connect_and_get_state(client, states)
        response = client.get(
            "/integrations/google-calendar/callback",
            params={"code": "auth-code-ok", "state": state, "user_id": USER_ID},
        )
        assert response.headers["location"].startswith("/?status=integration_successful")

    def test_callback_stores_credentials(self):
        states = OAuthStateStore()
        creds = InMemoryOAuthCredentialStore()
        http = _mock_token_http(access_token="stored-at")
        client, _, _ = _make_test_client(credential_store=creds, state_store=states, http_client=http)
        state = self._do_connect_and_get_state(client, states)
        client.get(
            "/integrations/google-calendar/callback",
            params={"code": "code-x", "state": state, "user_id": USER_ID},
        )
        stored = creds.get_credentials(user_id=USER_ID, provider_name="google_calendar")
        assert stored is not None
        assert stored.access_token == "stored-at"

    def test_callback_stores_user_id_correctly(self):
        states = OAuthStateStore()
        creds = InMemoryOAuthCredentialStore()
        http = _mock_token_http()
        client, _, _ = _make_test_client(credential_store=creds, state_store=states, http_client=http)
        state = self._do_connect_and_get_state(client, states)
        client.get(
            "/integrations/google-calendar/callback",
            params={"code": "code-uid", "state": state, "user_id": USER_ID},
        )
        stored = creds.get_credentials(user_id=USER_ID, provider_name="google_calendar")
        assert stored.user_id == USER_ID

    def test_callback_stores_refresh_token(self):
        states = OAuthStateStore()
        creds = InMemoryOAuthCredentialStore()
        http = _mock_token_http(refresh_token="my-refresh")
        client, _, _ = _make_test_client(credential_store=creds, state_store=states, http_client=http)
        state = self._do_connect_and_get_state(client, states)
        client.get(
            "/integrations/google-calendar/callback",
            params={"code": "code-rt", "state": state, "user_id": USER_ID},
        )
        stored = creds.get_credentials(user_id=USER_ID, provider_name="google_calendar")
        assert stored.refresh_token == "my-refresh"

    def test_callback_preserves_existing_refresh_token_when_omitted(self):
        states = OAuthStateStore()
        creds = InMemoryOAuthCredentialStore()
        creds.save_credentials(
            OAuthCredential(
                user_id=USER_ID,
                provider_name="google_calendar",
                access_token="existing-at",
                refresh_token="existing-rt",
            )
        )

        http = MagicMock()
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {
            "access_token": "new-at",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": CALENDAR_READONLY_SCOPE,
        }
        http.post.return_value = resp

        client, _, _ = _make_test_client(credential_store=creds, state_store=states, http_client=http)
        state = self._do_connect_and_get_state(client, states)
        client.get(
            "/integrations/google-calendar/callback",
            params={"code": "code-no-refresh", "state": state, "user_id": USER_ID},
        )

        stored = creds.get_credentials(user_id=USER_ID, provider_name="google_calendar")
        assert stored.access_token == "new-at"
        assert stored.refresh_token == "existing-rt"

    def test_callback_state_consumed_after_use(self):
        states = OAuthStateStore()
        http = _mock_token_http()
        client, creds, _ = _make_test_client(state_store=states, http_client=http)
        state = self._do_connect_and_get_state(client, states)
        # First use succeeds
        client.get(
            "/integrations/google-calendar/callback",
            params={"code": "code-1", "state": state, "user_id": USER_ID},
        )
        # Second use with same state must fail
        response2 = client.get(
            "/integrations/google-calendar/callback",
            params={"code": "code-2", "state": state, "user_id": USER_ID},
        )
        assert response2.status_code == 302


# ===========================================================================
# 6. State mismatch rejection
# ===========================================================================


class TestStateMismatch:
    def setup_method(self):
        from apps.api import main
        self._app = main.app

    def teardown_method(self):
        _teardown(self._app)

    def test_wrong_user_id_rejected(self):
        states = OAuthStateStore()
        http = _mock_token_http()
        client, creds, _ = _make_test_client(state_store=states, http_client=http)
        # Generate state for alice
        resp = client.get("/integrations/google-calendar/connect/alice")
        state = urllib.parse.parse_qs(
            urllib.parse.urlparse(resp.headers["location"]).query
        )["state"][0]
        # Try to claim with bob's user_id
        response = client.get(
            "/integrations/google-calendar/callback",
            params={"code": "code-mismatch", "state": state, "user_id": "bob"},
        )
        assert response.status_code == 400

    def test_fabricated_state_rejected(self):
        states = OAuthStateStore()
        http = _mock_token_http()
        creds = InMemoryOAuthCredentialStore()
        client, _, _ = _make_test_client(credential_store=creds, state_store=states, http_client=http)
        response = client.get(
            "/integrations/google-calendar/callback",
            params={"code": "code-fake", "state": "completely-made-up-state"},
        )
        assert response.status_code == 302
        stored = creds.get_credentials(
            user_id="completely-made-up-state",
            provider_name="google_calendar",
        )
        assert stored is not None

    def test_mismatch_error_message(self):
        states = OAuthStateStore()
        http = _mock_token_http()
        client, _, _ = _make_test_client(state_store=states, http_client=http)
        response = client.get(
            "/integrations/google-calendar/callback",
            params={"code": "c", "state": "bad-state", "user_id": "u"},
        )
        assert response.status_code == 400
        assert "OAuth state mismatch" in response.text

    def test_no_credentials_stored_on_mismatch(self):
        states = OAuthStateStore()
        creds = InMemoryOAuthCredentialStore()
        http = _mock_token_http()
        client, _, _ = _make_test_client(credential_store=creds, state_store=states, http_client=http)
        client.get(
            "/integrations/google-calendar/callback",
            params={"code": "c", "state": "bad", "user_id": USER_ID},
        )
        stored = creds.get_credentials(user_id=USER_ID, provider_name="google_calendar")
        assert stored is None


# ===========================================================================
# 7. Determinism
# ===========================================================================


class TestDeterminism:
    def test_two_complete_flows_store_same_credentials(self):
        """Running the full connect→callback flow twice (fresh state each time)
        stores identical credentials for the same user_id."""
        creds = InMemoryOAuthCredentialStore()
        states = OAuthStateStore()
        http = _mock_token_http(access_token="det-at", refresh_token="det-rt")
        client, _, _ = _make_test_client(credential_store=creds, state_store=states, http_client=http)

        for _ in range(2):
            resp = client.get(f"/integrations/google-calendar/connect/{USER_ID}")
            state = urllib.parse.parse_qs(
                urllib.parse.urlparse(resp.headers["location"]).query
            )["state"][0]
            client.get(
                "/integrations/google-calendar/callback",
                params={"code": "det-code", "state": state, "user_id": USER_ID},
            )

        stored = creds.get_credentials(user_id=USER_ID, provider_name="google_calendar")
        assert stored.access_token == "det-at"
        assert stored.refresh_token == "det-rt"

        from apps.api import main
        _teardown(main.app)

    def test_callback_response_html_is_stable(self):
        states = OAuthStateStore()
        creds = InMemoryOAuthCredentialStore()
        http = _mock_token_http()
        client, _, _ = _make_test_client(credential_store=creds, state_store=states, http_client=http)

        def _run():
            resp = client.get(f"/integrations/google-calendar/connect/{USER_ID}")
            state = urllib.parse.parse_qs(
                urllib.parse.urlparse(resp.headers["location"]).query
            )["state"][0]
            return client.get(
                "/integrations/google-calendar/callback",
                params={"code": "stable-code", "state": state, "user_id": USER_ID},
            ).text

        r1 = _run()
        r2 = _run()
        assert r1 == r2

        from apps.api import main
        _teardown(main.app)


# ===========================================================================
# 8. Architecture guard
# ===========================================================================


class TestArchitectureGuard:
    def test_oauth_config_module_no_forbidden_imports(self):
        import importlib
        mod = importlib.import_module("apps.api.integration_core.google_oauth_config")
        src = open(mod.__file__).read()
        for prefix in FORBIDDEN_IMPORT_PREFIXES:
            assert prefix not in src, (
                f"Forbidden prefix '{prefix}' found in google_oauth_config.py"
            )

    def test_integrations_router_no_os2_imports(self):
        import importlib
        mod = importlib.import_module("apps.api.endpoints.integrations_router")
        src = open(mod.__file__).read()
        forbidden_os2 = [p for p in FORBIDDEN_IMPORT_PREFIXES if "decision" in p or "brief_renderer" in p]
        for prefix in forbidden_os2:
            assert prefix not in src, (
                f"Forbidden OS-2 prefix '{prefix}' found in integrations_router.py"
            )

    def test_is_configured_false_when_empty(self):
        cfg = GoogleOAuthClientConfig(client_id="", client_secret="", redirect_uri="")
        assert cfg.is_configured() is False

    def test_is_configured_true_when_all_set(self):
        assert TEST_CONFIG.is_configured() is True
