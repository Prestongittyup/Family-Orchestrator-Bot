from archive.apps.api.core.asgi_admission import (
    _is_identity_bootstrap_critical_path,
    _is_oauth_critical_path,
)


def test_oauth_critical_paths_are_bypassed() -> None:
    assert _is_oauth_critical_path("/integrations/google-calendar/connect/test-user") is True
    assert _is_oauth_critical_path("/integrations/google-calendar/callback") is True
    assert _is_oauth_critical_path("/integrations/google-calendar/status/test-user") is True


def test_non_oauth_paths_are_not_bypassed() -> None:
    assert _is_oauth_critical_path("/v1/ui/bootstrap") is False
    assert _is_oauth_critical_path("/integrations/google-calendar/debug/test-user") is False


def test_identity_bootstrap_paths_are_bypassed() -> None:
    assert _is_identity_bootstrap_critical_path("/v1/identity/household/create") is True
    assert _is_identity_bootstrap_critical_path("/v1/identity/user/register") is True
    assert _is_identity_bootstrap_critical_path("/v1/identity/device/register") is True
    assert _is_identity_bootstrap_critical_path("/v1/identity/bootstrap") is True
    assert _is_identity_bootstrap_critical_path("/v1/identity/session/validate") is True
    assert _is_identity_bootstrap_critical_path("/v1/ui/bootstrap") is True


def test_identity_bootstrap_paths_are_bypassed_with_api_prefix_and_slash() -> None:
    assert _is_identity_bootstrap_critical_path("/api/v1/identity/household/create") is True
    assert _is_identity_bootstrap_critical_path("/api/v1/identity/household/create/") is True
    assert _is_identity_bootstrap_critical_path("/api/v1/ui/bootstrap/") is True


def test_non_bootstrap_identity_paths_are_not_bypassed() -> None:
    assert _is_identity_bootstrap_critical_path("/v1/identity/user/test-user") is False
    assert _is_identity_bootstrap_critical_path("/v1/identity/session/logout") is False


def test_oauth_paths_are_bypassed_with_api_prefix_and_slash() -> None:
    assert _is_oauth_critical_path("/api/integrations/google-calendar/connect/test-user") is True
    assert _is_oauth_critical_path("/api/integrations/google-calendar/callback/") is True
    assert _is_oauth_critical_path("/api/integrations/google-calendar/status/test-user/") is True
