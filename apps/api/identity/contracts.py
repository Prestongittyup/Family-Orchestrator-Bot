"""
Identity layer API contracts (request/response models).

UI-safe contract definitions for identity bootstrap, registration, device linking,
and session binding. No internal orchestration or implementation details leaked.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class HouseholdInfo(BaseModel):
    """Public household information."""
    model_config = ConfigDict(extra="forbid")

    household_id: str
    name: str
    timezone: str
    member_count: int


class UserInfo(BaseModel):
    """Public user information."""
    model_config = ConfigDict(extra="forbid")

    user_id: str
    household_id: str
    name: str
    email: str | None = None
    role: Literal["ADMIN", "ADULT", "CHILD", "VIEW_ONLY"]
    is_active: bool


class DeviceInfo(BaseModel):
    """Public device information."""
    model_config = ConfigDict(extra="forbid")

    device_id: str
    user_id: str
    household_id: str
    device_name: str
    platform: Literal["iOS", "Android", "Web"]
    is_active: bool
    last_seen_at: datetime | None = None


class IdentityContext(BaseModel):
    """Complete identity context resolved from session."""
    model_config = ConfigDict(extra="forbid")

    household_id: str
    user_id: str
    device_id: str
    user_role: Literal["ADMIN", "ADULT", "CHILD", "VIEW_ONLY"]
    
    # Permissions resolved from role
    can_chat: bool
    can_execute_actions: bool
    can_override_conflicts: bool
    can_view_sensitive_cards: bool


class SessionClaims(BaseModel):
    """Claims embedded in session token."""
    model_config = ConfigDict(extra="forbid")

    household_id: str
    user_id: str
    device_id: str
    user_role: Literal["ADMIN", "ADULT", "CHILD", "VIEW_ONLY"]
    token_created_at: datetime
    token_expires_at: datetime


class IdentityBootstrapRequest(BaseModel):
    """Request to bootstrap identity from stored device/user context."""
    model_config = ConfigDict(extra="forbid")

    household_id: str = Field(..., description="Household identifier")
    user_id: str | None = Field(default=None, description="Optional: pre-known user ID")
    device_id: str | None = Field(
        default=None, 
        description="Optional: pre-known device ID for rehydration"
    )
    session_token: str | None = Field(
        default=None,
        description="Optional: existing session token to restore"
    )


class IdentityBootstrapResponse(BaseModel):
    """Response with resolved identity and new session token."""
    model_config = ConfigDict(extra="forbid")

    household: HouseholdInfo
    user: UserInfo
    device: DeviceInfo
    identity_context: IdentityContext
    session_token: str = Field(..., description="New/refreshed session token")


class UserRegistrationRequest(BaseModel):
    """Request to register a new user in a household."""
    model_config = ConfigDict(extra="forbid")

    household_id: str = Field(..., description="Household to join")
    name: str = Field(..., description="User's full name")
    email: str | None = Field(default=None, description="Optional: email address")
    role: Literal["ADMIN", "ADULT", "CHILD", "VIEW_ONLY"] = Field(
        default="CHILD", 
        description="Initial role in household"
    )


class UserRegistrationResponse(BaseModel):
    """Response with newly registered user."""
    model_config = ConfigDict(extra="forbid")

    user: UserInfo
    household: HouseholdInfo


class DeviceLinkingRequest(BaseModel):
    """Request to link a device to a user."""
    model_config = ConfigDict(extra="forbid")

    household_id: str = Field(..., description="Household scope")
    user_id: str = Field(..., description="User to link device to")
    device_name: str = Field(..., description="Human-readable device name (e.g. 'Jane's iPhone')")
    platform: Literal["iOS", "Android", "Web"] = Field(..., description="Device platform")
    user_agent: str = Field(..., description="Device user agent hash")


class DeviceLinkingResponse(BaseModel):
    """Response with newly linked device."""
    model_config = ConfigDict(extra="forbid")

    device: DeviceInfo


class HouseholdCreationRequest(BaseModel):
    """Request to create a new household."""
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Household name")
    timezone: str = Field(default="UTC", description="Default timezone")
    founder_user_name: str = Field(..., description="Name of household founder")
    founder_email: str | None = Field(default=None, description="Optional: founder email")


class HouseholdCreationResponse(BaseModel):
    """Response with newly created household and founder user."""
    model_config = ConfigDict(extra="forbid")

    household: HouseholdInfo
    founder_user: UserInfo


class SessionValidationRequest(BaseModel):
    """Request to validate and refresh session token."""
    model_config = ConfigDict(extra="forbid")

    session_token: str = Field(..., description="Token to validate")


class SessionValidationResponse(BaseModel):
    """Response with validity and refreshed token if valid."""
    model_config = ConfigDict(extra="forbid")

    is_valid: bool
    identity_context: IdentityContext | None = None
    refreshed_token: str | None = None


class IdentityErrorResponse(BaseModel):
    """Standard error response for identity operations."""
    model_config = ConfigDict(extra="forbid")

    error: str
    error_code: str
    detail: str | None = None
