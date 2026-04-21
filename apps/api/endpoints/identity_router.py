"""
Identity API endpoints (FastAPI router).

Exposes identity bootstrap, registration, device linking, and session management.
All responses are UI-safe contracts with no internal orchestration details.
"""

from __future__ import annotations

import logging
import traceback

from fastapi import APIRouter, HTTPException, Query

from apps.api.auth.token_service import TokenService
from apps.api.identity.auth import build_identity_context
from apps.api.identity.service import IdentityService
from apps.api.identity.sqlalchemy_repository import SQLAlchemyIdentityRepository
from apps.api.identity.contracts import (
    IdentityBootstrapRequest,
    IdentityBootstrapResponse,
    UserRegistrationRequest,
    UserRegistrationResponse,
    DeviceLinkingRequest,
    DeviceLinkingResponse,
    HouseholdCreationRequest,
    HouseholdCreationResponse,
    SessionValidationRequest,
    SessionValidationResponse,
    IdentityErrorResponse,
)


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/identity", tags=["identity"])
_repository = SQLAlchemyIdentityRepository()
_token_service = TokenService(_repository)
_identity_service = IdentityService(repository=_repository, token_service=_token_service)


# =========================================================================
# Household Management
# =========================================================================

@router.post("/household/create")
def create_household(request: HouseholdCreationRequest) -> HouseholdCreationResponse:
    """
    Create a new household with an optional founder user.
    
    Returns the household info and founder user if provided.
    """
    try:
        logger.info(f"Creating household: name={request.name}, timezone={request.timezone}, founder={request.founder_user_name}")
        response = _identity_service.create_household(
            name=request.name,
            timezone=request.timezone,
            founder_name=request.founder_user_name,
            founder_email=request.founder_email,
        )
        logger.info(f"[OK] Created household: {response.household.household_id}")
        return response
    except ValueError as exc:
        logger.warning(f"[Household Create] Validation error: {exc}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc))
    except KeyError as exc:
        logger.error(f"[Household Create] Missing required field: {exc}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Missing field: {str(exc)}")
    except Exception as exc:
        error_detail = f"household_creation_failed: {str(exc)}"
        logger.error(f"[Household Create] FAILED with exception: {type(exc).__name__}", exc_info=True)
        logger.error(f"[Household Create] Full traceback:\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=error_detail)


@router.get("/household/{household_id}")
def get_household(household_id: str):
    """Retrieve household info."""
    try:
        household_info = _identity_service.get_household(household_id)
        if not household_info:
            raise HTTPException(status_code=404, detail="Household not found")
        return household_info
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"household_lookup_failed: {exc}")


# =========================================================================
# User Registration
# =========================================================================

@router.post("/user/register")
def register_user(request: UserRegistrationRequest) -> UserRegistrationResponse:
    """
    Register a new user in a household.
    
    Returns the new user info and household info.
    """
    try:
        response = _identity_service.register_user(
            household_id=request.household_id,
            name=request.name,
            role=request.role,
            email=request.email,
        )
        return response
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"user_registration_failed: {exc}")


@router.get("/user/{user_id}")
def get_user(user_id: str):
    """Retrieve user info."""
    try:
        user_info = _identity_service.get_user_info(user_id)
        if not user_info:
            raise HTTPException(status_code=404, detail="User not found")
        return user_info
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"user_lookup_failed: {exc}")


# =========================================================================
# Device Registration
# =========================================================================

@router.post("/device/register")
def register_device(request: DeviceLinkingRequest) -> DeviceLinkingResponse:
    """
    Register a new device for a user.
    
    Device ID should be pre-computed deterministically by the frontend
    from userId + userAgent + platform and validated here.
    
    Returns the new device info.
    """
    try:
        response = _identity_service.register_device(
            user_id=request.user_id,
            household_id=request.household_id,
            device_name=request.device_name,
            platform=request.platform,
            user_agent_hash=request.user_agent,
        )
        return response
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"device_registration_failed: {exc}")


@router.get("/device/{device_id}")
def get_device(device_id: str):
    """Retrieve device info."""
    try:
        device_info = _identity_service.get_device_info(device_id)
        if not device_info:
            raise HTTPException(status_code=404, detail="Device not found")
        return device_info
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"device_lookup_failed: {exc}")


# =========================================================================
# Bootstrap (Identity Resolution)
# =========================================================================

@router.post("/bootstrap")
def bootstrap_identity(request: IdentityBootstrapRequest) -> IdentityBootstrapResponse:
    """
    Bootstrap identity from stored context or restore from session token.
    
    Resolves (household_id, user_id, device_id) deterministically:
    1. If session_token provided: validate and refresh
    2. If user_id + device_id provided: resolve from storage
    3. Otherwise: use first user/device in household
    
    Returns resolved identity context and new/refreshed session token.
    """
    try:
        logger.info(f"Bootstrapping identity: household={request.household_id}, user={request.user_id}")
        response = _identity_service.bootstrap_identity(
            household_id=request.household_id,
            user_id=request.user_id,
            device_id=request.device_id,
            session_token=request.session_token,
        )
        logger.info(f"✓ Bootstrap complete: user={response.user.user_id}, device={response.device.device_id}")
        return response
    except ValueError as exc:
        logger.warning(f"Bootstrap validation error: {exc}")
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error(f"Bootstrap failed: {exc}")
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"bootstrap_failed: {str(exc)}")


# =========================================================================
# Session Management
# =========================================================================

@router.post("/session/validate")
def validate_session(request: SessionValidationRequest) -> SessionValidationResponse:
    """
    Validate and refresh a session token.
    
    Returns validity status, identity context (if valid), and refreshed token.
    """
    try:
        claims = _token_service.validate_access_token(request.session_token)
        if claims is not None:
            return SessionValidationResponse(
                is_valid=True,
                identity_context=build_identity_context(
                    household_id=str(claims["household_id"]),
                    user_id=str(claims["user_id"]),
                    device_id=str(claims["device_id"]),
                    user_role=str(claims["role"]),
                ),
                refreshed_token=request.session_token,
            )
        response = _identity_service.validate_session(request.session_token)
        return response
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"session_validation_failed: {exc}")


@router.post("/session/logout")
def logout_session(
    session_token: str = Query(..., description="Token to invalidate"),
):
    """
    Invalidate a session token (logout).
    """
    try:
        # Hash for lookup
        import hashlib
        token_hash = hashlib.sha256(session_token.encode("utf-8")).hexdigest()
        
        # Invalidate via repository
        from apps.api.identity.sqlalchemy_repository import SQLAlchemyIdentityRepository
        repo = SQLAlchemyIdentityRepository()
        repo.invalidate_session_token(token_hash)
        
        return {"status": "logged_out"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"logout_failed: {exc}")
