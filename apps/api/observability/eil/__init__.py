"""
Execution Intelligence Layer (EIL)
===================================
Package entry-point.  Wire up the storage backend to the tracer at import time.

Typical usage in application startup (e.g. apps/api/main.py):
    from apps.api.observability.eil import bootstrap_eil
    bootstrap_eil()

Or import individual components:
    from apps.api.observability.eil.tracer import trace_function
    from apps.api.observability.eil.analysis import build_execution_map
    from apps.api.observability.eil.storage import get_storage_backend
"""

from __future__ import annotations

from apps.api.observability.eil.config import EILConfig, get_config
from apps.api.observability.eil.tracer import (
    TRACE_TARGETS,
    TraceEvent,
    TraceSession,
    end_trace,
    get_current_trace_id,
    get_instrumented_functions,
    set_persist_callback,
    start_trace,
    trace_event,
    trace_function,
)
from apps.api.observability.eil.storage import (
    JSONLStorageBackend,
    SQLiteStorageBackend,
    StorageBackend,
    get_storage_backend,
    reset_storage_backend,
)
from apps.api.observability.eil.analysis import (
    RegressionReport,
    build_execution_map,
    build_module_heatmap,
    detect_regression,
    load_all_traces,
    render_execution_map_markdown,
)

__all__ = [
    # config
    "EILConfig",
    "get_config",
    # tracer
    "TRACE_TARGETS",
    "TraceEvent",
    "TraceSession",
    "trace_function",
    "start_trace",
    "end_trace",
    "trace_event",
    "get_current_trace_id",
    "get_instrumented_functions",
    "set_persist_callback",
    # storage
    "StorageBackend",
    "JSONLStorageBackend",
    "SQLiteStorageBackend",
    "get_storage_backend",
    "reset_storage_backend",
    # analysis
    "build_execution_map",
    "build_module_heatmap",
    "render_execution_map_markdown",
    "detect_regression",
    "RegressionReport",
    "load_all_traces",
    # bootstrap
    "bootstrap_eil",
]


def bootstrap_eil(config: EILConfig | None = None) -> StorageBackend:
    """Wire storage backend into the tracer.  Call once at application startup.

    This is idempotent — safe to call multiple times.

    Returns the active StorageBackend for optional direct use.
    """
    cfg = config or get_config()
    if not cfg.enable_tracing:
        return get_storage_backend(cfg)
    backend = get_storage_backend(cfg)
    set_persist_callback(backend.persist)
    return backend
