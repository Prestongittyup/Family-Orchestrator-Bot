# ARCHIVE MODULE - NOT PART OF ACTIVE RUNTIME
# DO NOT IMPORT INTO app/

from __future__ import annotations

from dataclasses import dataclass
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel


router = APIRouter(tags=["evaluation"])


class SimulationRunRequest(BaseModel):
    seed: int = 42
    household_size: int = 4
    chaos_level: str = "medium"
    event_density: int = 18
    scenario_preset: str = "school_work_balance"
class StreamEventsRequest(BaseModel):
    seed: int = 42
    household_size: int = 4
    chaos_level: str = "medium"
    scenario_preset: str = "streamed"
    events: list[dict[str, Any]]


@dataclass
class _SimulationCache:
    latest_payload: dict[str, Any] | None = None


_simulation_cache = _SimulationCache()


@router.get("/evaluation_results.json")
def get_evaluation_results() -> FileResponse:
    """Serve the latest evaluation artifact for dashboard consumers."""
    artifact_path = Path("evaluation_results.json")
    if not artifact_path.exists():
        raise HTTPException(status_code=404, detail="evaluation_results.json not found")
    return FileResponse(path=str(artifact_path), media_type="application/json", filename="evaluation_results.json")


@router.get("/evaluation/run")
def run_evaluation() -> dict[str, str]:
    """Run the existing pytest-based evaluation and return execution output."""
    tests_dir = Path("tests")
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "test_brief_evaluation.py", "-s"],
        cwd=str(tests_dir),
        capture_output=True,
        text=True,
        timeout=300,
    )

    summary_parts = [completed.stdout or "", completed.stderr or ""]
    summary = "\n".join(part for part in summary_parts if part).strip()

    return {
        "status": "success" if completed.returncode == 0 else "failure",
        "summary": summary,
        "artifact_path": "evaluation_results.json",
    }


@router.get("/simulation/run")
def run_simulation(
    seed: int = 42,
    household_size: int = 4,
    chaos_level: str = "medium",
    event_density: int = 18,
    scenario_preset: str = "school_work_balance",
) -> dict[str, Any]:
    raise HTTPException(status_code=410, detail="simulation_endpoints_deprecated_use_offline_harness")


@router.post("/simulation/stream-events")
def stream_simulation_events(request: StreamEventsRequest) -> dict[str, Any]:
    raise HTTPException(status_code=410, detail="simulation_endpoints_deprecated_use_offline_harness")


@router.get("/simulation/results")
def get_simulation_results() -> dict[str, Any]:
    artifact_path = Path("simulation_results.json")
    if artifact_path.exists():
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        _simulation_cache.latest_payload = payload
        return payload
    if _simulation_cache.latest_payload is not None:
        return _simulation_cache.latest_payload
    raise HTTPException(status_code=404, detail="simulation_results.json not found")

