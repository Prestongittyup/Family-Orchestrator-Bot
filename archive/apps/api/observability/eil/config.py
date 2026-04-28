"""
EIL Configuration
-----------------
Central configuration for the Execution Intelligence Layer.
All settings can be overridden via environment variables.

Environment variables:
  EIL_ENABLE_TRACING      - "1" / "true" / "yes" to enable (default: "1")
  EIL_STORAGE_BACKEND     - "jsonl" or "sqlite" (default: "jsonl")
  EIL_TRACE_OUTPUT_DIR    - directory for trace artifacts (default: "data/execution_traces")
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class EILConfig:
    enable_tracing: bool
    storage_backend: str          # "jsonl" | "sqlite"
    trace_output_dir: Path
    jsonl_path: Path
    sqlite_path: Path


def _bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if raw in {"1", "true", "yes"}:
        return True
    if raw in {"0", "false", "no"}:
        return False
    return default


def get_config() -> EILConfig:
    output_dir = Path(os.environ.get("EIL_TRACE_OUTPUT_DIR", "data/execution_traces"))
    return EILConfig(
        enable_tracing=_bool_env("EIL_ENABLE_TRACING", default=True),
        storage_backend=os.environ.get("EIL_STORAGE_BACKEND", "jsonl").strip().lower(),
        trace_output_dir=output_dir,
        jsonl_path=output_dir / "runtime_traces.jsonl",
        sqlite_path=output_dir / "traces.db",
    )
