"""
EIL Tracer
----------
Pure instrumentation layer: decorator, context propagation, trace events.

This module ONLY produces trace data. It knows nothing about storage or analysis.
Storage is injected via a callback registered with `set_persist_callback`.

Usage:
    from apps.api.observability.eil.tracer import trace_function

    @trace_function(entrypoint="mymodule.my_route", actor_type="api_user")
    async def my_route(request):
        ...
"""

from __future__ import annotations

import contextvars
import functools
import inspect
import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Callable, TypeVar, cast

from apps.api.observability.eil.config import get_config


# ---------------------------------------------------------------------------
# Known instrumentation targets (used by analysis to classify un-called fns)
# ---------------------------------------------------------------------------
TRACE_TARGETS: set[str] = {
    "apps.api.main.create_app.<locals>.ingest_event",
    "apps.api.assistant_runtime_router.run_assistant",
    "apps.api.assistant_runtime_router.approve_assistant_action",
    "apps.api.assistant_runtime_router.assistant_today",
    "apps.api.assistant_runtime_router.reject_assistant_action",
    "apps.api.endpoints.operational_router._run_pipeline",
    "apps.api.endpoints.operational_router.run_operational_mode",
    "apps.api.endpoints.operational_router.get_operational_context",
    "apps.api.endpoints.operational_router.get_operational_brief",
    "apps.api.ingestion.service.ingest_webhook",
    "apps.api.ingestion.service.ingest_email",
    "apps.api.services.event_replay_service.replay_events",
    "apps.api.services.event_replay_service.replay_events_for_household",
    "household_os.runtime.orchestrator.HouseholdOSOrchestrator.tick",
    "household_os.runtime.orchestrator.HouseholdOSOrchestrator.approve_and_execute",
    "household_os.runtime.action_pipeline.ActionPipeline.register_proposed_action",
    "household_os.runtime.action_pipeline.ActionPipeline.approve_actions",
    "household_os.runtime.action_pipeline.ActionPipeline.reject_actions",
    "household_os.runtime.action_pipeline.ActionPipeline.reject_action_timeout",
    "household_os.runtime.action_pipeline.ActionPipeline.execute_approved_actions",
    "household_os.runtime.state_reducer.replay_events",
}

# ---------------------------------------------------------------------------
# In-process registry
# ---------------------------------------------------------------------------
_current_trace_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "eil_trace_id", default=None
)
_current_depth: contextvars.ContextVar[int] = contextvars.ContextVar(
    "eil_trace_depth", default=0
)
_TRACE_LOCK = threading.RLock()
_TRACE_REGISTRY: dict[str, "TraceSession"] = {}
_INSTRUMENTED_FUNCTIONS: set[str] = set()

# Pluggable persistence callback: (TraceSession) -> None
_persist_callback: Callable[["TraceSession"], None] | None = None


def set_persist_callback(callback: Callable[["TraceSession"], None] | None) -> None:
    """Register a storage backend.  Called once at application startup."""
    global _persist_callback
    _persist_callback = callback


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class TraceEvent:
    event_type: str        # function_entry | function_exit | function_error
    module: str
    function: str
    timestamp: str
    depth: int
    status: str = "ok"
    error_type: str | None = None
    error_message: str | None = None


@dataclass(slots=True)
class TraceSession:
    trace_id: str
    actor_type: str
    entrypoint: str
    started_at: str
    source: str = "runtime"
    ended_at: str | None = None
    events: list[TraceEvent] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _extract_actor_type(
    args: tuple[Any, ...], kwargs: dict[str, Any], fallback: str
) -> str:
    for key in ("actor_type", "actor", "source"):
        value = kwargs.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for arg in args:
        if isinstance(arg, str) and arg.strip():
            lowered = arg.strip().lower()
            if lowered in {"api_user", "system_worker", "event_bus", "test_harness", "user", "system"}:
                return lowered
    return fallback


# ---------------------------------------------------------------------------
# Public API: trace lifecycle
# ---------------------------------------------------------------------------
def start_trace(*, entrypoint: str, actor_type: str, source: str = "runtime") -> str:
    trace_id = f"trace-{uuid.uuid4()}"
    session = TraceSession(
        trace_id=trace_id,
        actor_type=actor_type,
        entrypoint=entrypoint,
        source=source,
        started_at=_utc_now_iso(),
    )
    with _TRACE_LOCK:
        _TRACE_REGISTRY[trace_id] = session
    _current_trace_id.set(trace_id)
    _current_depth.set(0)
    return trace_id


def end_trace(trace_id: str | None = None) -> None:
    active_id = trace_id or _current_trace_id.get()
    if not active_id:
        return
    with _TRACE_LOCK:
        session = _TRACE_REGISTRY.get(active_id)
        if session is None:
            return
        if session.ended_at is None:
            session.ended_at = _utc_now_iso()
            if _persist_callback is not None:
                try:
                    _persist_callback(session)
                except Exception:
                    pass  # never let persistence kill the traced call
    current_id = _current_trace_id.get()
    if current_id == active_id:
        _current_trace_id.set(None)
        _current_depth.set(0)


def get_current_trace_id() -> str | None:
    return _current_trace_id.get()


def trace_event(
    *,
    module: str,
    function: str,
    event_type: str,
    depth: int,
    status: str = "ok",
    error: BaseException | None = None,
) -> None:
    trace_id = _current_trace_id.get()
    if not trace_id:
        return
    event = TraceEvent(
        event_type=event_type,
        module=module,
        function=function,
        timestamp=_utc_now_iso(),
        depth=depth,
        status=status,
        error_type=None if error is None else type(error).__name__,
        error_message=None if error is None else str(error),
    )
    with _TRACE_LOCK:
        session = _TRACE_REGISTRY.get(trace_id)
        if session is None:
            return
        session.events.append(event)


def get_instrumented_functions() -> set[str]:
    return set(_INSTRUMENTED_FUNCTIONS)


def get_active_traces() -> list[TraceSession]:
    with _TRACE_LOCK:
        return list(_TRACE_REGISTRY.values())


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------
F = TypeVar("F", bound=Callable[..., Any])


def trace_function(
    *,
    entrypoint: str | None = None,
    actor_type: str = "system_worker",
    source: str = "runtime",
) -> Callable[[F], F]:
    """Decorator that wraps a function with tracing.

    When ENABLE_TRACING is False, returns the original function unchanged so
    overhead is zero in production when tracing is disabled.
    """

    def decorator(func: F) -> F:
        if not get_config().enable_tracing:
            # Tracing disabled — return unwrapped function immediately
            return func

        function_path = f"{func.__module__}.{func.__qualname__}"
        _INSTRUMENTED_FUNCTIONS.add(function_path)

        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                created_trace = False
                trace_id = _current_trace_id.get()
                if not trace_id:
                    created_trace = True
                    trace_id = start_trace(
                        entrypoint=entrypoint or function_path,
                        actor_type=_extract_actor_type(args, kwargs, actor_type),
                        source=source,
                    )
                depth = _current_depth.get()
                trace_event(module=func.__module__, function=func.__qualname__,
                            event_type="function_entry", depth=depth)
                _current_depth.set(depth + 1)
                try:
                    result = await func(*args, **kwargs)
                    trace_event(module=func.__module__, function=func.__qualname__,
                                event_type="function_exit", depth=depth)
                    return result
                except Exception as exc:
                    trace_event(module=func.__module__, function=func.__qualname__,
                                event_type="function_error", depth=depth,
                                status="error", error=exc)
                    raise
                finally:
                    _current_depth.set(depth)
                    if created_trace:
                        end_trace(trace_id)

            return cast(F, async_wrapper)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            created_trace = False
            trace_id = _current_trace_id.get()
            if not trace_id:
                created_trace = True
                trace_id = start_trace(
                    entrypoint=entrypoint or function_path,
                    actor_type=_extract_actor_type(args, kwargs, actor_type),
                    source=source,
                )
            depth = _current_depth.get()
            trace_event(module=func.__module__, function=func.__qualname__,
                        event_type="function_entry", depth=depth)
            _current_depth.set(depth + 1)
            try:
                result = func(*args, **kwargs)
                trace_event(module=func.__module__, function=func.__qualname__,
                            event_type="function_exit", depth=depth)
                return result
            except Exception as exc:
                trace_event(module=func.__module__, function=func.__qualname__,
                            event_type="function_error", depth=depth,
                            status="error", error=exc)
                raise
            finally:
                _current_depth.set(depth)
                if created_trace:
                    end_trace(trace_id)

        return cast(F, wrapper)

    return decorator
