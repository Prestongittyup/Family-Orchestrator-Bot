from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import Request
from starlette.responses import Response

from apps.api.core.audit_bypass import is_audit_bypass_request
from apps.api.runtime.execution_fairness import fairness_gate, RequestClass, get_loop_local_resource
from apps.api.runtime.loop_tracing import register_loop_resource, trace_loop_binding, trace_loop_context

_edge_logger = logging.getLogger("uvicorn.error")

# Keep symbol for compatibility with existing imports (for example, SSE router).
_global_limiter = None


def _get_audit_bootstrap_semaphore() -> asyncio.Semaphore:
    key = "audit_bootstrap_semaphore"
    trace_loop_context("backpressure_middleware._get_audit_bootstrap_semaphore")

    def _factory() -> asyncio.Semaphore:
        semaphore = asyncio.Semaphore(max(1, int(os.getenv("AUDIT_BOOTSTRAP_CONCURRENCY", "1"))))
        register_loop_resource(semaphore, "CREATE: apps/api/core/backpressure_middleware.py:_get_audit_bootstrap_semaphore")
        return semaphore

    resource = get_loop_local_resource(key, _factory)
    assert isinstance(resource, asyncio.Semaphore)  # noqa: S101
    trace_loop_binding(resource, "USE: apps/api/core/backpressure_middleware.py:_get_audit_bootstrap_semaphore")
    # Runtime assertion if binding already materialized.
    loop = asyncio.get_running_loop()
    bound_loop = getattr(resource, "_loop", None)
    if bound_loop is not None:
        assert bound_loop is loop  # noqa: S101
    return resource

_SSE_PATHS = frozenset({"/v1/realtime/stream"})

_SHORT_PREFIXES = (
    "/v1/system/",
    "/v1/auth/",
    "/health",
    "/v1/system/health",
)


def _classify(path: str) -> RequestClass:
    if any(path.startswith(p) for p in _SHORT_PREFIXES):
        return "SHORT"
    return "LONG"


def install_request_backpressure_middleware(app: Any) -> None:
    # Diagnostic probe + execution fairness guard.
    # Admission (hard cap) is enforced at ASGI boundary; this layer adds
    # per-class semaphore fairness for non-SSE requests.

    @app.middleware("http")
    async def request_backpressure_guard(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        trace_loop_context(f"backpressure_middleware.request_backpressure_guard:{request.url.path}")
        path = request.url.path
        _edge_logger.info(
            json.dumps(
                {
                    "marker": "BACKPRESSURE_MW_ENTRY",
                    "classification": "ENTERED_APP",
                    "ts": round(time.time(), 6),
                    "path": path,
                    "client_ip": request.client.host if request.client else "unknown",
                    "pid": os.getpid(),
                },
                sort_keys=True,
            )
        )

        # SSE requests are governed by sse_guard + fairness inside the route handler.
        if path in _SSE_PATHS:
            return await call_next(request)

        request_headers = {key.lower(): value for key, value in request.headers.items()}
        if is_audit_bypass_request(path, request_headers):
            _edge_logger.info(
                json.dumps(
                    {
                        "marker": "AUDIT_BYPASS_ACTIVE",
                        "layer": "fairness_gate",
                        "ts": round(time.time(), 6),
                        "path": path,
                        "client_ip": request.client.host if request.client else "unknown",
                        "pid": os.getpid(),
                        "audit_mode": request_headers.get("x-audit-mode", "unknown"),
                        "quota_pool": "audit_bootstrap",
                    },
                    sort_keys=True,
                )
            )
            async with _get_audit_bootstrap_semaphore():
                return await call_next(request)

        cls = _classify(path)
        async with fairness_gate.acquire(cls):
            return await call_next(request)