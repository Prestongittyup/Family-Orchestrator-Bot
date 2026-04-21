from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import Request
from starlette.responses import JSONResponse, Response

from apps.api.auth.token_service import AuthValidationSystemError, TokenService
from apps.api.identity.sqlalchemy_repository import SQLAlchemyIdentityRepository
from apps.api.observability.logging import log_error, log_event
from apps.api.observability.metrics import metrics


_PUBLIC_PATHS = {
    "/v1/auth/oauth/google/stub",
    "/v1/auth/token/refresh",
    "/v1/auth/logout",
    "/v1/auth/magic/request",
    "/v1/auth/magic/verify",
    "/v1/identity/household/create",
    "/v1/identity/user/register",
    "/v1/identity/device/register",
    "/v1/identity/bootstrap",
    "/v1/identity/session/validate",
    "/v1/system/boot-status",
    "/v1/system/boot-probe",
    "/v1/system/health",
}

_TOKEN_SERVICE = TokenService(repository_factory=SQLAlchemyIdentityRepository)

def install_auth_middleware(app: Any) -> None:
    """Install server-validated bearer token middleware for all /v1 routes."""

    @app.middleware("http")
    async def auth_guard(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        path = request.url.path
        if not path.startswith("/v1/"):
            return await call_next(request)

        if path in _PUBLIC_PATHS:
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        had_bearer_header = auth_header.lower().startswith("bearer ")
        if not auth_header.lower().startswith("bearer "):
            metrics.record_auth_result(False, "missing_bearer_token")
            log_event("auth_invalid_token", path=path, reason="missing_bearer_token")
            return JSONResponse({"detail": "missing_bearer_token"}, status_code=401)

        token = auth_header.split(" ", 1)[1].strip()
        token_invalid = False
        try:
            claims = _TOKEN_SERVICE.validate_access_token(token)
        except AuthValidationSystemError as exc:
            metrics.record_auth_result(False, "system_failure")
            log_error("auth_system_failure", exc, path=path)
            return JSONResponse({"detail": "token_validation_unavailable"}, status_code=503)

        if claims is None:
            token_invalid = True

        # Final auth guard: invalid token must never execute downstream logic.
        if token_invalid:
            metrics.record_auth_result(False, "invalid_token")
            log_event("auth_invalid_token", path=path, reason="invalid_or_expired_token")
            return JSONResponse({"detail": "invalid_or_expired_token"}, status_code=401)

        metrics.record_auth_result(True)

        # Household scope guard to prevent cross-household leakage
        request_household = (
            request.headers.get("x-hpal-household-id")
            or request.query_params.get("family_id")
            or request.query_params.get("household_id")
        )
        token_household = str(claims.get("household_id", ""))
        if request_household and request_household != token_household:
            return JSONResponse({"detail": "household_scope_mismatch"}, status_code=403)

        request.state.auth_claims = claims
        response = await call_next(request)
        if had_bearer_header and response.status_code in {200, 500, 503} and getattr(request.state, "auth_claims", None) is None:
            metrics.note_invalid_token_non_401()
            return JSONResponse({"detail": "invalid_or_expired_token"}, status_code=401)
        return response
