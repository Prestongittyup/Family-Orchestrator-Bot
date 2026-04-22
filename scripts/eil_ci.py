#!/usr/bin/env python3
"""
scripts/eil_ci.py
-----------------
CI integration entry-point for the Execution Intelligence Layer.

What it does:
  1. Loads all traces from the configured storage backend plus any JSON/JSONL
     files found in data/execution_traces/.
  2. Builds the execution map.
  3. Compares against the baseline map (data/execution_traces/baseline_map.json)
     to detect regressions.
  4. Writes updated EXECUTION_MAP.md and execution_map.json.
  5. Optionally saves the current map as the new baseline (--save-baseline).
  6. Exits non-zero if a regression is detected (CI gate).

Usage:
    python scripts/eil_ci.py
    python scripts/eil_ci.py --save-baseline
    python scripts/eil_ci.py --sources runtime,pytest
    EIL_STORAGE_BACKEND=sqlite python scripts/eil_ci.py

Exit codes:
    0  — no regressions, analysis complete
    1  — regression detected (CI should fail)
    2  — usage / configuration error
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.observability.eil import (
    bootstrap_eil,
    build_execution_map,
    build_module_heatmap,
    detect_regression,
    get_storage_backend,
    render_execution_map_markdown,
)
from apps.api.observability.eil.storage import JSONLStorageBackend

TRACE_DIR = ROOT / "data" / "execution_traces"
EXECUTION_MAP_JSON = TRACE_DIR / "execution_map.json"
EXECUTION_MAP_MD = ROOT / "EXECUTION_MAP.md"
BASELINE_MAP_JSON = TRACE_DIR / "baseline_map.json"


def _load_extra_source_files(
    trace_dir: Path,
    allowed_sources: set[str] | None,
) -> list[dict]:
    """Load JSON/JSONL source files from trace_dir (excluding the runtime JSONL log)."""
    loader = JSONLStorageBackend(trace_dir / "__unused__")
    traces: list[dict] = []
    for path in sorted(trace_dir.glob("*.json")):
        if path.name in {"execution_map.json", "baseline_map.json", "execution_traces.json"}:
            continue
        source_name = path.stem
        if allowed_sources and source_name not in allowed_sources:
            continue
        loaded = loader.load_source_file(path)
        for item in loaded:
            item.setdefault("source", source_name)
        traces.extend(loaded)
    return traces


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="EIL CI analysis runner")
    parser.add_argument(
        "--save-baseline",
        action="store_true",
        help="Save current execution map as new regression baseline",
    )
    parser.add_argument(
        "--sources",
        default="",
        help="Comma-separated list of source names to include (default: all). "
             "Example: --sources runtime,pytest",
    )
    parser.add_argument(
        "--no-fail-on-regression",
        action="store_true",
        help="Report regressions but do not exit non-zero (informational mode)",
    )
    args = parser.parse_args(argv)

    allowed_sources: set[str] | None = None
    if args.sources.strip():
        allowed_sources = {s.strip() for s in args.sources.split(",") if s.strip()}

    # Bootstrap storage (reads from env for backend selection)
    backend = bootstrap_eil()

    # Load traces
    storage_filters = None
    if allowed_sources and "runtime" not in allowed_sources:
        # Exclude runtime traces if not in allowed sources
        storage_filters = None  # can't filter by source easily, load all and filter below
    runtime_traces = backend.load_traces()
    if allowed_sources:
        runtime_traces = [t for t in runtime_traces if t.get("source", "runtime") in allowed_sources]

    extra_traces = _load_extra_source_files(TRACE_DIR, allowed_sources)
    all_traces = runtime_traces + extra_traces

    print(f"[EIL CI] Loaded {len(all_traces)} traces "
          f"({len(runtime_traces)} runtime + {len(extra_traces)} from source files)")

    # Build execution map
    execution_map = build_execution_map(all_traces)
    module_heatmap = build_module_heatmap(execution_map)

    # Write artifacts
    TRACE_DIR.mkdir(parents=True, exist_ok=True)
    EXECUTION_MAP_JSON.write_text(
        json.dumps(execution_map, indent=2, sort_keys=True), encoding="utf-8"
    )
    EXECUTION_MAP_MD.write_text(
        render_execution_map_markdown(execution_map, module_heatmap=module_heatmap),
        encoding="utf-8",
    )
    print(f"[EIL CI] Wrote {EXECUTION_MAP_JSON.relative_to(ROOT)}")
    print(f"[EIL CI] Wrote {EXECUTION_MAP_MD.relative_to(ROOT)}")

    # Regression detection
    regression_exit_code = 0
    if BASELINE_MAP_JSON.exists():
        baseline_map = json.loads(BASELINE_MAP_JSON.read_text(encoding="utf-8"))
        report = detect_regression(baseline_map, execution_map)
        print(f"\n[EIL CI] Regression check:\n{report.summary()}")
        if report.has_regression and not args.no_fail_on_regression:
            regression_exit_code = 1
    else:
        print("[EIL CI] No baseline map found — skipping regression check.")
        print(f"         Run with --save-baseline to create one.")

    # Save baseline if requested
    if args.save_baseline:
        BASELINE_MAP_JSON.write_text(
            json.dumps(execution_map, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(f"[EIL CI] Saved baseline: {BASELINE_MAP_JSON.relative_to(ROOT)}")

    return regression_exit_code


if __name__ == "__main__":
    sys.exit(main())
