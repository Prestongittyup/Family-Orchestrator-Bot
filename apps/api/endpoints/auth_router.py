"""
Authentication Router (P0)
==========================
Provides server-issued authentication abstraction for frontend runtime.

For closed beta we support an OAuth stub flow:
  POST /v1/auth/oauth/google/stub

This endpoint:
  1) finds or creates a user by email in the given household
  2) registers device if needed
  3) issues/refreshed server-side session token via IdentityService.bootstrap_identity

This replaces frontend mock token issuance while keeping identity-layer contracts.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import os

from pydantic import BaseModel, ConfigDict
from fastapi import APIRouter, HTTPException, Request

from apps.api.auth.token_service import TokenService
from apps.api.identity.service import IdentityService
from apps.api.identity.sqlalchemy_repository import SQLAlchemyIdentityRepository
from apps.api.identity.contracts import IdentityBootstrapResponse

router = APIRouter(prefix="/v1/auth", tags=["auth"])
_repo = SQLAlchemyIdentityRepository()
_identity = IdentityService(repository=_repo)
_tokens = TokenService(repository=_repo)


class OAuthStubRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    household_id: str
    email: str
    display_name: str
    role: str = "ADULT"
    device_name: str
    platform: str
    user_agent: str


class TokenRefreshRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    refresh_token: str


class LogoutRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    access_token: str | None = None
    refresh_token: str | None = None


class MagicLinkRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    household_id: str
    email: str
    display_name: str


class MagicLinkVerifyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: str
    household_id: str
    email: str
    display_name: str
    device_name: str
    platform: str
    user_agent: str


@router.post("/oauth/google/stub")
def oauth_google_stub(request: OAuthStubRequest) -> dict:
    """
    OAuth placeholder for closed beta.

    The caller provides household/email/device details and the server returns a
    real identity bootstrap payload with server-issued session token.
    """
    try:
        role = request.role if request.role in {"ADMIN", "ADULT", "CHILD", "VIEW_ONLY"} else "ADULT"

        user = _repo.get_user_by_email(request.email)
        if user is None:
            reg = _identity.register_user(
                household_id=request.household_id,
                name=request.display_name,
                role=role,
                email=request.email,
            )
            user_id = reg.user.user_id
        else:
            if user.household_id != request.household_id:
                raise ValueError("email already belongs to a different household")
            user_id = user.user_id

        # Reuse existing deterministic device if the user already has one with same label/platform.
        existing_devices = _repo.list_devices_for_user(user_id)
        matched = next(
            (
                d for d in existing_devices
                if d.device_name == request.device_name and d.platform == request.platform
            ),
            None,
        )

        if matched is not None:
            device_id = matched.device_id
        else:
            dev = _identity.register_device(
                user_id=user_id,
                household_id=request.household_id,
                device_name=request.device_name,
                platform=request.platform,
                user_agent_hash=request.user_agent,
            )
            device_id = dev.device.device_id

        role_literal = role  # validated above
        pair = _tokens.issue_token_pair(
            household_id=request.household_id,
            user_id=user_id,
            device_id=device_id,
            role=role_literal,  # type: ignore[arg-type]
        )
        bootstrap = _identity.bootstrap_identity(
            household_id=request.household_id,
            user_id=user_id,
            device_id=device_id,
            session_token=pair.access_token,
        )
        return {
            **bootstrap.model_dump(),
            "access_token": pair.access_token,
            "refresh_token": pair.refresh_token,
            "access_expires_at": pair.access_expires_at.isoformat(),
            "refresh_expires_at": pair.refresh_expires_at.isoformat(),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"oauth_stub_failed: {exc}")


@router.post("/token/refresh")
def refresh_token(request: TokenRefreshRequest) -> dict:
    pair = _tokens.rotate_refresh_token(request.refresh_token)
    if pair is None:
        raise HTTPException(status_code=401, detail="invalid_refresh_token")
    return {
        "access_token": pair.access_token,
        "refresh_token": pair.refresh_token,
        "access_expires_at": pair.access_expires_at.isoformat(),
        "refresh_expires_at": pair.refresh_expires_at.isoformat(),
    }


@router.post("/logout")
def logout(request: LogoutRequest) -> dict:
    if request.access_token:
        _tokens.revoke_token(request.access_token)
    if request.refresh_token:
        _tokens.revoke_token(request.refresh_token)
    return {"status": "logged_out"}


@router.post("/revoke/device/{device_id}")
def revoke_device_tokens(device_id: str) -> dict:
    count = _tokens.revoke_device_tokens(device_id)
    return {"revoked": count, "scope": "device", "device_id": device_id}


@router.post("/magic/request")
def request_magic_link(request: MagicLinkRequest) -> dict:
    """
    Production-safe magic-link issuance placeholder.

    In production this token should be emailed; for local/dev we return it only
    if AUTH_EXPOSE_MAGIC_CODE=true.
    """
    ttl_minutes = int(os.getenv("AUTH_MAGIC_LINK_TTL_MIN", "10"))
    expires = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
    payload = f"{request.household_id}|{request.email}|{int(expires.timestamp())}"
    secret = os.getenv("AUTH_TOKEN_SECRET", "dev-insecure-secret-change-me")
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    code = f"{payload}|{digest}"

    if os.getenv("AUTH_EXPOSE_MAGIC_CODE", "false").lower() == "true":
        return {"status": "issued", "code": code, "expires_at": expires.isoformat()}
    return {"status": "issued", "expires_at": expires.isoformat()}


@router.post("/magic/verify")
def verify_magic_link(request: MagicLinkVerifyRequest) -> dict:
    try:
        household_id, email, exp_raw, digest = request.code.rsplit("|", 3)
        if household_id != request.household_id or email != request.email:
            raise ValueError("magic_code_mismatch")
        payload = f"{household_id}|{email}|{exp_raw}"
        secret = os.getenv("AUTH_TOKEN_SECRET", "dev-insecure-secret-change-me")
        expected = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, digest):
            raise ValueError("magic_code_signature_invalid")
        if int(exp_raw) < int(datetime.now(timezone.utc).timestamp()):
            raise ValueError("magic_code_expired")
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    except Exception:
        raise HTTPException(status_code=401, detail="invalid_magic_code")

    # Reuse OAuth bootstrap path for identity/device provisioning + token pair.
    return oauth_google_stub(
        OAuthStubRequest(
            household_id=request.household_id,
            email=request.email,
            display_name=request.display_name,
            role="ADULT",
            device_name=request.device_name,
            platform=request.platform,
            user_agent=request.user_agent,
        )
    )
