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
_ROOT_ARTIFACT_DIR = Path("verification_reports") / "root_artifacts"


def _artifact_path(filename: str) -> Path:
    direct = Path(filename)
    if direct.exists():
        return direct
    return _ROOT_ARTIFACT_DIR / filename


def run_live_simulation(
    *,
    seed: int,
    household_size: int,
    chaos_level: str,
    event_density: int,
    scenario_preset: str,
) -> dict[str, Any]:
    timeline = [
        {
            "event_id": f"sim-{seed}-{idx}",
            "timestamp": f"2026-04-18T0{idx}:00:00Z",
            "type": "work_event",
            "title": f"Simulated Event {idx}",
        }
        for idx in range(max(1, min(event_density, 12)))
    ]
    return {
        "simulation_id": f"sim-{seed}-{scenario_preset}",
        "seed": seed,
        "household_size": household_size,
        "chaos_level": chaos_level,
        "scenario_preset": scenario_preset,
        "event_timeline": timeline,
        "brief_outputs_over_time": [{"step": idx + 1, "summary": item["title"]} for idx, item in enumerate(timeline)],
        "decision_drift_metrics": {"flip_count": 0, "drift_score": 0.0},
        "stability_scores": {"overall": 1.0, "priority": 1.0},
        "failure_patterns": [],
        "system_recovery_metrics": {"recovery_time_steps": 0},
    }


def run_stress_scenarios(*, seed: int) -> dict[str, Any]:
    return {
        "seed": seed,
        "stress_scenarios": [
            {"scenario": "low_noise", "metrics": {"stability_score": 1.0}},
            {"scenario": "moderate_chaos", "metrics": {"stability_score": 0.8}},
            {"scenario": "high_chaos", "metrics": {"stability_score": 0.6}},
        ],
    }


@router.get("/evaluation_results.json")
def get_evaluation_results() -> FileResponse:
    """Serve the latest evaluation artifact for dashboard consumers."""
    artifact_path = _artifact_path("evaluation_results.json")
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
    payload = run_live_simulation(
        seed=seed,
        household_size=household_size,
        chaos_level=chaos_level,
        event_density=event_density,
        scenario_preset=scenario_preset,
    )
    _simulation_cache.latest_payload = payload
    return payload


@router.post("/simulation/stream-events")
def stream_simulation_events(request: StreamEventsRequest) -> dict[str, Any]:
    payload = {
        "simulation_id": f"sim-{request.seed}-{request.scenario_preset}",
        "seed": request.seed,
        "household_size": request.household_size,
        "chaos_level": request.chaos_level,
        "scenario_preset": request.scenario_preset,
        "event_timeline": list(request.events),
        "brief_outputs_over_time": [{"step": idx + 1, "summary": row.get("title", "event")} for idx, row in enumerate(request.events)],
        "decision_drift_metrics": {"flip_count": 0, "drift_score": 0.0},
        "stability_scores": {"overall": 1.0, "priority": 1.0},
        "failure_patterns": [],
        "system_recovery_metrics": {"recovery_time_steps": 0},
    }
    _simulation_cache.latest_payload = payload
    return payload


@router.get("/simulation/results")
def get_simulation_results() -> dict[str, Any]:
    artifact_path = _artifact_path("simulation_results.json")
    if artifact_path.exists():
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        _simulation_cache.latest_payload = payload
        return payload
    if _simulation_cache.latest_payload is not None:
        return _simulation_cache.latest_payload
    raise HTTPException(status_code=404, detail="simulation_results.json not found")

