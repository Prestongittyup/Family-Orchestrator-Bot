from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.observability.eil import (
    bootstrap_eil,
    build_execution_map,
    build_module_heatmap,
    get_storage_backend,
    render_execution_map_markdown,
)
from apps.api.observability.eil.storage import JSONLStorageBackend


TRACE_DIR = ROOT / "data" / "execution_traces"
RUNTIME_EXPORT = TRACE_DIR / "execution_traces.json"
EXECUTION_MAP_JSON = TRACE_DIR / "execution_map.json"
EXECUTION_MAP_MD = ROOT / "EXECUTION_MAP.md"

_EXTRA_SOURCE_NAMES = [
    "pytest_traces",
    "orchestrator_simulation_traces",
    "event_replay_traces",
]


def main() -> None:
    TRACE_DIR.mkdir(parents=True, exist_ok=True)

    backend = bootstrap_eil()

    # Export stored runtime traces
    runtime_traces = backend.load_traces()
    RUNTIME_EXPORT.write_text(
        json.dumps(runtime_traces, indent=2, sort_keys=True), encoding="utf-8"
    )

    # Load additional source files (pytest, orchestrator simulation, event replay)
    loader = JSONLStorageBackend(TRACE_DIR / "__unused__")
    extra_traces: list[dict] = []
    for name in _EXTRA_SOURCE_NAMES:
        path = TRACE_DIR / f"{name}.json"
        loaded = loader.load_source_file(path)
        for item in loaded:
            item.setdefault("source", name)
        extra_traces.extend(loaded)

    all_traces = runtime_traces + extra_traces
    execution_map = build_execution_map(all_traces)
    module_heatmap = build_module_heatmap(execution_map)

    EXECUTION_MAP_JSON.write_text(
        json.dumps(execution_map, indent=2, sort_keys=True), encoding="utf-8"
    )
    EXECUTION_MAP_MD.write_text(
        render_execution_map_markdown(execution_map, module_heatmap=module_heatmap),
        encoding="utf-8",
    )

    print(f"Wrote runtime export: {RUNTIME_EXPORT.relative_to(ROOT)}")
    print(f"Wrote execution map JSON: {EXECUTION_MAP_JSON.relative_to(ROOT)}")
    print(f"Wrote execution map markdown: {EXECUTION_MAP_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
