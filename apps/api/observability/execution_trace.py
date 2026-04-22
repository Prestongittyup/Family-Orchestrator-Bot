"""
Backward-compatibility shim for apps.api.observability.execution_trace
-----------------------------------------------------------------------
All symbols are now implemented in the eil/ package.  This module re-exports
them so existing imports continue to work without any changes.

New code should import directly from apps.api.observability.eil.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# Wire storage backend to tracer on first import
from apps.api.observability.eil import bootstrap_eil
from apps.api.observability.eil.config import get_config
from apps.api.observability.eil.tracer import (
    TRACE_TARGETS,
    TraceEvent,
    TraceSession,
    end_trace,
    get_current_trace_id,
    get_instrumented_functions,
    start_trace,
    trace_event,
    trace_function,
)
from apps.api.observability.eil.storage import (
    JSONLStorageBackend,
    get_storage_backend,
)
from apps.api.observability.eil.analysis import (
    build_execution_map as _build_execution_map,
    build_module_heatmap,
    render_execution_map_markdown,
)

bootstrap_eil()

_cfg = get_config()
TRACE_OUTPUT_DIR = _cfg.trace_output_dir
TRACE_JSONL_PATH = _cfg.jsonl_path


# ---------------------------------------------------------------------------
# Compatibility wrappers
# ---------------------------------------------------------------------------

# load_runtime_trace_log — delegates to the active storage backend
def load_runtime_trace_log() -> list[dict[str, Any]]:
    return get_storage_backend().load_traces()


# load_trace_source_file — delegates to JSONLStorageBackend helper
def load_trace_source_file(path: str | Path) -> list[dict[str, Any]]:
    from apps.api.observability.eil.config import get_config as _get_cfg
    loader = JSONLStorageBackend(_get_cfg().jsonl_path)
    return loader.load_source_file(Path(path))


# build_execution_map — legacy signature with synthetic_sources kwarg
def build_execution_map(
    *,
    synthetic_sources: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    traces = load_runtime_trace_log()
    if synthetic_sources:
        for source_name, source_traces in synthetic_sources.items():
            for trace in source_traces:
                merged = dict(trace)
                merged.setdefault("source", source_name)
                traces.append(merged)
    return _build_execution_map(traces)


class ExecutionTraceExporter:
    """Legacy exporter class.  Wraps the new EIL storage backend."""

    def export_trace(self, trace_id: str) -> dict[str, Any]:
        rows = get_storage_backend().load_traces(trace_id=trace_id)
        return rows[0] if rows else {}

    def export_all(self) -> list[dict[str, Any]]:
        return get_storage_backend().load_traces()

    def export_trace_file(
        self, output_path: str | Path, trace_id: str | None = None
    ) -> Path:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        if trace_id:
            payload: Any = self.export_trace(trace_id)
        else:
            payload = self.export_all()
        output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return output
