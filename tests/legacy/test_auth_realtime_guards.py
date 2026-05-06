from __future__ import annotations
import pytest

from fastapi.testclient import TestClient
from starlette.requests import Request

from archive.apps.api.core.auth_middleware import _extract_request_token

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



@pytest.mark.integration
@pytest.mark.legacy
def test_session_validate_not_rejected_as_duplicate(test_client: TestClient) -> None:
    payload = {"session_token": "invalid-test-token"}

    first = test_client.post("/v1/identity/session/validate", json=payload)
    second = test_client.post("/v1/identity/session/validate", json=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["is_valid"] is False
    assert second.json()["is_valid"] is False


@pytest.mark.integration
@pytest.mark.legacy
def test_realtime_stream_rejects_missing_auth(test_client: TestClient) -> None:
    response = test_client.get(
        "/v1/realtime/stream",
        params={"household_id": "family-auth-missing"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "missing_bearer_token"


@pytest.mark.integration
@pytest.mark.legacy
def test_realtime_query_token_extraction_is_supported() -> None:
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/v1/realtime/stream",
            "query_string": b"household_id=family-1&session_token=token-123",
            "headers": [],
        }
    )

    token, had_bearer_header = _extract_request_token(request)

    assert token == "token-123"
    assert had_bearer_header is False


@pytest.mark.integration
@pytest.mark.legacy
def test_non_realtime_routes_ignore_query_session_token() -> None:
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/v1/ui/bootstrap",
            "query_string": b"session_token=token-123",
            "headers": [],
        }
    )

    token, had_bearer_header = _extract_request_token(request)

    assert token is None
    assert had_bearer_header is False