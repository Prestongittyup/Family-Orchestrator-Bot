from __future__ import annotations
import pytest

import subprocess

from fastapi.testclient import TestClient

import app.main as main
from archive.apps.api.endpoints import evaluation_router

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



@pytest.mark.integration
@pytest.mark.legacy
def test_evaluation_results_artifact_endpoint_serves_json() -> None:
    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)
    response = client.get("/evaluation_results.json")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    payload = response.json()
    assert "scenarios" in payload
    assert "aggregate" in payload


@pytest.mark.integration
@pytest.mark.legacy
def test_evaluation_run_endpoint_returns_expected_contract(monkeypatch) -> None:
    def _fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["pytest", "test_brief_evaluation.py", "-s"],
            returncode=0,
            stdout="DECISION_FEEDBACK_COMPLETE\n1 passed",
            stderr="",
        )

    monkeypatch.setattr(evaluation_router.subprocess, "run", _fake_run)

    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)
    response = client.get("/evaluation/run")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "success"
    assert "DECISION_FEEDBACK_COMPLETE" in payload["summary"]
    assert payload["artifact_path"] == "evaluation_results.json"
