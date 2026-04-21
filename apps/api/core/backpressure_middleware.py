from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from threading import Lock
from typing import Any

from fastapi import Request
from starlette.responses import JSONResponse, Response

from apps.api.observability.metrics import metrics


class _InFlightLimiter:
    def __init__(self, max_inflight: int) -> None:
        self._max_inflight = max(1, max_inflight)
        self._message_cap = max(1, min(self._max_inflight, 25))
        self._sse_cap = max(1, min(self._max_inflight, 25))
        self._lock = Lock()
        self._inflight = 0

    def _cap_for_path(self, path: str) -> int:
        if path.startswith("/v1/ui/message"):
            return self._message_cap
        if path.startswith("/v1/realtime/stream"):
            return self._sse_cap
        return self._max_inflight

    def try_acquire(self, path: str) -> bool:
        cap = self._cap_for_path(path)
        with self._lock:
            if self._inflight >= cap:
                return False
            self._inflight += 1
            metrics.gauge_set("inflight_request_count", float(self._inflight))
            return True

    def release(self) -> None:
        with self._lock:
            self._inflight = max(0, self._inflight - 1)
            metrics.gauge_set("inflight_request_count", float(self._inflight))


def install_request_backpressure_middleware(app: Any) -> None:
    # Temporary validation cap for Phase 1 stabilization verification.
    limiter = _InFlightLimiter(40)

    @app.middleware("http")
    async def request_backpressure_guard(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if not limiter.try_acquire(request.url.path):
            metrics.note_request_rejection("max_inflight")
            return JSONResponse({"detail": "too_many_inflight_requests"}, status_code=429)

        try:
            return await call_next(request)
        finally:
            limiter.release()