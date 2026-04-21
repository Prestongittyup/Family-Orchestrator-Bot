"""Lightweight bridge API for the Evaluation Dashboard.

Run with:
    uvicorn ui.server:app --port 8765 --reload

or via scripts/start_dashboard.ps1
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

_ROOT = Path(__file__).parent.parent  # project root
_RESULTS_PATH = _ROOT / "evaluation_results.json"
_RUNS_DIR = _ROOT / "evaluation_runs"

app = FastAPI(title="Evaluation Dashboard Bridge API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/api/results")
def get_results() -> dict:
    """Return latest evaluation_results.json."""
    if not _RESULTS_PATH.exists():
        return JSONResponse({"error": "No evaluation results found. Run the evaluation first."}, status_code=404)
    return json.loads(_RESULTS_PATH.read_text(encoding="utf-8"))


@app.post("/api/run")
def run_evaluation() -> dict:
    """Archive previous results, run pytest, return fresh results."""
    # Archive current results before the run
    if _RESULTS_PATH.exists():
        _RUNS_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        shutil.copy(_RESULTS_PATH, _RUNS_DIR / f"evaluation_{ts}.json")

    proc = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            "tests/test_brief_evaluation.py", "-s", "--tb=short",
        ],
        cwd=str(_ROOT),
        capture_output=True,
        text=True,
        timeout=180,
    )

    results_data: dict = {}
    if _RESULTS_PATH.exists():
        results_data = json.loads(_RESULTS_PATH.read_text(encoding="utf-8"))

    return {
        "success": proc.returncode == 0,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "results": results_data,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/history")
def get_history() -> list:
    """Return list of archived evaluation runs (most recent first)."""
    if not _RUNS_DIR.exists():
        return []
    files = sorted(_RUNS_DIR.glob("evaluation_*.json"), reverse=True)
    return [
        {"filename": f.name, "label": f.stem.replace("evaluation_", "").replace("_", " ")}
        for f in files[:30]
    ]


@app.get("/api/history/{filename}")
def get_history_run(filename: str) -> dict:
    """Return a specific archived run by filename."""
    # Sanitize: reject paths with traversal or non-JSON extensions
    if any(c in filename for c in ("/", "\\", "..")) or not filename.endswith(".json"):
        return JSONResponse({"error": "Invalid filename"}, status_code=400)
    file_path = _RUNS_DIR / filename
    if not file_path.exists():
        return JSONResponse({"error": "Run not found"}, status_code=404)
    return json.loads(file_path.read_text(encoding="utf-8"))
