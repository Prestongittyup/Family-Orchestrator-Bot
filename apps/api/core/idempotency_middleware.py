from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
import hashlib

from fastapi import Request
from starlette.responses import JSONResponse, Response

from apps.api.services import idempotency_key_service


_WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


def install_idempotency_middleware(app: Any) -> None:
    """
    Enforce idempotency for all mutating /v1 routes.

    Contract:
      - Clients send x-idempotency-key for write operations.
      - Duplicate key for same scoped path/household returns 409 and no side effects.
      - Key reservation is released on 5xx responses to allow safe retries.
    """

    @app.middleware("http")
    async def idempotency_guard(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        if not request.url.path.startswith("/v1/"):
            return await call_next(request)
        if request.method.upper() not in _WRITE_METHODS:
            return await call_next(request)

        # Auth endpoints are excluded from idempotency constraints.
        if request.url.path.startswith("/v1/auth/"):
            return await call_next(request)

        idem_header = request.headers.get("x-idempotency-key")
        if not idem_header:
            # Compatibility mode: derive deterministic key from request payload.
            body = await request.body()
            canonical = (
                f"{request.method}:{request.url.path}:{request.query_params}:{body.decode('utf-8', errors='ignore')}"
            )
            idem_header = f"auto-{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:24]}"

        household_scope = (
            request.headers.get("x-hpal-household-id")
            or request.query_params.get("family_id")
            or request.query_params.get("household_id")
            or "global"
        )
        scoped_key = f"{household_scope}:{request.url.path}:{idem_header}"

        reserved = idempotency_key_service.reserve(
            key=scoped_key,
            household_id=household_scope,
            event_type=request.url.path,
        )
        if not reserved:
            return JSONResponse({"detail": "duplicate_request", "idempotency_key": idem_header}, status_code=409)

        request.state.idempotency_key = scoped_key
        response = await call_next(request)

        # On server failure, release reservation so retries can proceed safely.
        if response.status_code >= 500:
            idempotency_key_service.release(scoped_key)

        return response
