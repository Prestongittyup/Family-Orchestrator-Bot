from __future__ import annotations

import importlib


def test_load_environment_populates_google_oauth_env_from_dotenv(tmp_path, monkeypatch):
    dotenv_text = "\n".join(
        [
            "GOOGLE_CLIENT_ID=dotenv-client-id.apps.googleusercontent.com",
            "GOOGLE_CLIENT_SECRET=dotenv-client-secret",
            "GOOGLE_REDIRECT_URI=http://127.0.0.1:8000/integrations/google-calendar/callback",
        ]
    )
    (tmp_path / ".env").write_text(dotenv_text, encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("GOOGLE_REDIRECT_URI", raising=False)

    import apps.api.core.env_bootstrap as env_bootstrap
    env_bootstrap = importlib.reload(env_bootstrap)

    env_bootstrap.load_environment(force=True)

    import apps.api.integration_core.google_oauth_config as google_oauth_config
    google_oauth_config = importlib.reload(google_oauth_config)

    config = google_oauth_config.GoogleOAuthClientConfig.from_env()

    assert config.client_id == "dotenv-client-id.apps.googleusercontent.com"
    assert config.client_secret == "dotenv-client-secret"
    assert config.redirect_uri == "http://127.0.0.1:8000/integrations/google-calendar/callback"


def test_get_missing_google_oauth_env_vars_reports_missing_keys(monkeypatch):
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("GOOGLE_REDIRECT_URI", raising=False)

    import apps.api.core.env_bootstrap as env_bootstrap
    missing = env_bootstrap.get_missing_google_oauth_env_vars()

    assert "GOOGLE_CLIENT_ID" in missing
    assert "GOOGLE_CLIENT_SECRET" in missing
    assert "GOOGLE_REDIRECT_URI" in missing
