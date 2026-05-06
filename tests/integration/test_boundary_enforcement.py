from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from household_os.core.lifecycle_state import LifecycleState, enforce_boundary_state, parse_lifecycle_state

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate]



app = FastAPI()


@app.post("/boundary-action")
def boundary_action(payload: dict[str, Any]) -> dict[str, str]:
    try:
        state = enforce_boundary_state(payload.get("state"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"state": state.value}


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _background_job_input(value: Any) -> LifecycleState:
    return enforce_boundary_state(value)


def _scheduler_input(value: Any) -> LifecycleState:
    return enforce_boundary_state(value)


def _cli_input(value: Any) -> LifecycleState:
    return enforce_boundary_state(value)


@pytest.mark.integration
def test_api_rejects_invalid_state(client: TestClient) -> None:
    response = client.post("/boundary-action", json={"state": "executed"})
    assert response.status_code in (400, 422)


@pytest.mark.integration
def test_boundary_parses_valid_state() -> None:
    state = parse_lifecycle_state(LifecycleState.COMMITTED.value)
    assert state == LifecycleState.COMMITTED


@pytest.mark.integration
def test_boundary_rejects_invalid_type() -> None:
    with pytest.raises(TypeError):
        parse_lifecycle_state(123)


@pytest.mark.parametrize("boundary", [_background_job_input, _scheduler_input, _cli_input])
@pytest.mark.integration
def test_non_api_boundaries_reject_legacy(boundary) -> None:
    with pytest.raises(ValueError):
        boundary("ignored")


@pytest.mark.parametrize("boundary", [_background_job_input, _scheduler_input, _cli_input])
@pytest.mark.integration
def test_non_api_boundaries_accept_valid(boundary) -> None:
    assert boundary(LifecycleState.APPROVED.value) == LifecycleState.APPROVED