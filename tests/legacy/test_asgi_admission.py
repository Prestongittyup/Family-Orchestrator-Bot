import asyncio

import pytest
from archive.apps.api.core.asgi_admission import (
    AdmissionGateASGI,
    _is_identity_bootstrap_critical_path,
    _is_oauth_critical_path,
    get_runtime_metrics_snapshot,
)

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



@pytest.mark.unit
@pytest.mark.legacy
def test_oauth_critical_paths_are_bypassed() -> None:
    assert _is_oauth_critical_path("/integrations/google-calendar/connect/test-user") is True
    assert _is_oauth_critical_path("/integrations/google-calendar/callback") is True
    assert _is_oauth_critical_path("/integrations/google-calendar/status/test-user") is True


@pytest.mark.unit
@pytest.mark.legacy
def test_non_oauth_paths_are_not_bypassed() -> None:
    assert _is_oauth_critical_path("/v1/ui/bootstrap") is False
    assert _is_oauth_critical_path("/integrations/google-calendar/debug/test-user") is False


@pytest.mark.unit
@pytest.mark.legacy
def test_identity_bootstrap_paths_are_bypassed() -> None:
    assert _is_identity_bootstrap_critical_path("/v1/identity/household/create") is True
    assert _is_identity_bootstrap_critical_path("/v1/identity/user/register") is True
    assert _is_identity_bootstrap_critical_path("/v1/identity/device/register") is True
    assert _is_identity_bootstrap_critical_path("/v1/identity/bootstrap") is True
    assert _is_identity_bootstrap_critical_path("/v1/identity/session/validate") is True
    assert _is_identity_bootstrap_critical_path("/v1/ui/bootstrap") is True


@pytest.mark.unit
@pytest.mark.legacy
def test_identity_bootstrap_paths_are_bypassed_with_api_prefix_and_slash() -> None:
    assert _is_identity_bootstrap_critical_path("/api/v1/identity/household/create") is True
    assert _is_identity_bootstrap_critical_path("/api/v1/identity/household/create/") is True
    assert _is_identity_bootstrap_critical_path("/api/v1/ui/bootstrap/") is True


@pytest.mark.unit
@pytest.mark.legacy
def test_non_bootstrap_identity_paths_are_not_bypassed() -> None:
    assert _is_identity_bootstrap_critical_path("/v1/identity/user/test-user") is False
    assert _is_identity_bootstrap_critical_path("/v1/identity/session/logout") is False


@pytest.mark.unit
@pytest.mark.legacy
def test_oauth_paths_are_bypassed_with_api_prefix_and_slash() -> None:
    assert _is_oauth_critical_path("/api/integrations/google-calendar/connect/test-user") is True
    assert _is_oauth_critical_path("/api/integrations/google-calendar/callback/") is True
    assert _is_oauth_critical_path("/api/integrations/google-calendar/status/test-user/") is True


def _response_status(messages: list[dict[str, object]]) -> int:
    for message in messages:
        if message.get("type") == "http.response.start":
            return int(message.get("status", 0))
    return 0


async def _invoke(gate: AdmissionGateASGI, path: str) -> list[dict[str, object]]:
    messages: list[dict[str, object]] = []

    async def _receive() -> dict[str, object]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def _send(message: dict[str, object]) -> None:
        messages.append(message)

    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": b"",
        "headers": [(b"user-agent", b"pytest-asgi-admission")],
        "client": ("127.0.0.1", 12345),
    }
    await gate(scope, _receive, _send)
    return messages


@pytest.mark.unit
@pytest.mark.legacy
def test_inflight_drains_after_concurrency_and_health_is_accessible(monkeypatch: pytest.MonkeyPatch) -> None:
    from archive.apps.api.runtime.backpressure_controller import backpressure

    monkeypatch.setattr(backpressure, "multiplier", lambda: 1.0)

    async def _inner(scope, receive, send):
        path = str(scope.get("path", ""))
        if path == "/health":
            body = b'{"status":"ok"}'
        else:
            await asyncio.sleep(0.02)
            body = b'{"status":"work"}'
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"content-length", str(len(body)).encode("ascii")],
                ],
            }
        )
        await send({"type": "http.response.body", "body": body, "more_body": False})

    gate = AdmissionGateASGI(_inner)

    async def _run() -> tuple[list[list[dict[str, object]]], list[dict[str, object]], list[dict[str, object]]]:
        concurrent_results = await asyncio.gather(*[_invoke(gate, "/work") for _ in range(10)], _invoke(gate, "/health"))
        work_results = concurrent_results[:-1]
        health_during_load = concurrent_results[-1]
        health_after_load = await _invoke(gate, "/health")
        return work_results, health_during_load, health_after_load

    work_results, health_during_load, health_after_load = asyncio.run(_run())

    assert all(_response_status(messages) == 200 for messages in work_results)
    assert _response_status(health_during_load) == 200
    assert _response_status(health_after_load) == 200

    metrics = get_runtime_metrics_snapshot(gate)
    assert metrics["inflight_current"] == 0
    assert metrics["INFLIGHT_CURRENT"] == 0
    assert metrics["rejected_total"] == 0


@pytest.mark.unit
@pytest.mark.legacy
def test_no_capacity_exceeded_when_below_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    from archive.apps.api.runtime.backpressure_controller import backpressure

    monkeypatch.setattr(backpressure, "multiplier", lambda: 1.0)

    async def _inner(scope, receive, send):
        await asyncio.sleep(0.01)
        body = b'{"status":"ok"}'
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"content-length", str(len(body)).encode("ascii")],
                ],
            }
        )
        await send({"type": "http.response.body", "body": body, "more_body": False})

    gate = AdmissionGateASGI(_inner)

    async def _run() -> list[list[dict[str, object]]]:
        return await asyncio.gather(*[_invoke(gate, "/work") for _ in range(5)])

    results = asyncio.run(_run())
    assert all(_response_status(messages) == 200 for messages in results)

    metrics = get_runtime_metrics_snapshot(gate)
    assert metrics["inflight_current"] == 0
    assert metrics["rejected_total"] == 0


@pytest.mark.unit
@pytest.mark.legacy
def test_repeated_startup_rebinds_state_without_accumulation(monkeypatch: pytest.MonkeyPatch) -> None:
    from archive.apps.api.runtime.backpressure_controller import backpressure

    monkeypatch.setattr(backpressure, "multiplier", lambda: 1.0)

    async def _inner(scope, receive, send):
        body = b'{"status":"ok"}'
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"content-length", str(len(body)).encode("ascii")],
                ],
            }
        )
        await send({"type": "http.response.body", "body": body, "more_body": False})

    gate_first = AdmissionGateASGI(_inner)

    async def _run_first() -> None:
        await _invoke(gate_first, "/work")
        await _invoke(gate_first, "/work")

    asyncio.run(_run_first())
    first_metrics = get_runtime_metrics_snapshot(gate_first)
    assert first_metrics["accepted_total"] == 2
    assert first_metrics["inflight_current"] == 0

    # Simulate reload/startup by wrapping the same app callable again.
    gate_second = AdmissionGateASGI(_inner)
    second_metrics_before = get_runtime_metrics_snapshot(gate_second)
    assert second_metrics_before["accepted_total"] == 0
    assert second_metrics_before["inflight_current"] == 0

    second_response = asyncio.run(_invoke(gate_second, "/health"))
    assert _response_status(second_response) == 200

    second_metrics_after = get_runtime_metrics_snapshot(gate_second)
    assert second_metrics_after["accepted_total"] == 1
    assert second_metrics_after["inflight_current"] == 0