"""
Identity service layer.

High-level operations for identity management: household creation, user registration,
device linking, bootstrap, and session handling. Uses repository + auth layers.
"""

from __future__ import annotations

import logging
from uuid import uuid4
from datetime import datetime
from typing import Literal

from apps.api.identity.repository import IdentityRepository
from apps.api.identity.sqlalchemy_repository import SQLAlchemyIdentityRepository
from apps.api.auth.token_service import TokenService
from apps.api.identity.auth import (
    build_identity_context,
)
from apps.api.identity.contracts import (
    HouseholdInfo,
    UserInfo,
    DeviceInfo,
    IdentityContext,
    IdentityBootstrapResponse,
    UserRegistrationResponse,
    DeviceLinkingResponse,
    HouseholdCreationResponse,
    SessionValidationResponse,
)


logger = logging.getLogger(__name__)


class IdentityService:
    """
    High-level identity operations service.
    
    Coordinates household creation, user registration, device linking,
    bootstrap, and session management.
    """

    def __init__(
        self,
        repository: IdentityRepository | None = None,
        token_service: TokenService | None = None,
    ):
        """Initialize with optional repository and token service."""
        self._repository = repository or SQLAlchemyIdentityRepository()
        self._token_service = token_service or TokenService(self._repository)

    # =========================================================================
    # Household Operations
    # =========================================================================

    def create_household(
        self,
        name: str,
        timezone: str = "UTC",
        founder_name: str | None = None,
        founder_email: str | None = None,
    ) -> HouseholdCreationResponse:
        """Create a new household with optional founder user."""
        logger.debug(f"[create_household] Starting: name={name}, tz={timezone}, founder={founder_name}")
        
        try:
            if founder_email:
                existing = self._repository.get_user_by_email(founder_email)
                if existing is not None:
                    raise ValueError(
                        f"founder_email_already_exists: {founder_email}"
                    )

            # Generate household ID
            household_id = str(uuid4())
            logger.debug(f"[create_household] Generated household_id: {household_id}")
            
            # Create household
            logger.debug(f"[create_household] Creating household in repository...")
            household = self._repository.create_household(
                household_id=household_id,
                name=name,
                timezone=timezone,
            )
            logger.debug(f"[create_household] ✓ Household created: {household}")
            
            household_info = HouseholdInfo(
                household_id=household.household_id,
                name=household.name,
                timezone=household.timezone,
                member_count=0,
            )
            logger.debug(f"[create_household] Household info: {household_info}")
            
            # Create founder user if provided
            founder_user = None
            if founder_name:
                logger.debug(f"[create_household] Creating founder user: {founder_name}")
                founder_user = self.register_user(
                    household_id=household_id,
                    name=founder_name,
                    email=founder_email,
                    role="ADMIN",
                )
                logger.debug(f"[create_household] ✓ Founder user created: {founder_user.user.user_id}")
            
            response = HouseholdCreationResponse(
                household=household_info,
                founder_user=founder_user.user if founder_user else None,
            )
            logger.debug(f"[create_household] ✓ Complete: response={response}")
            return response
        except Exception as exc:
            logger.error(f"[create_household] FAILED: {exc}", exc_info=True)
            raise

    def get_household(self, household_id: str) -> HouseholdInfo | None:
        """Retrieve household info."""
        household = self._repository.get_household(household_id)
        if not household:
            return None
        
        # Count members
        members = self._repository.list_users_in_household(household_id)
        
        return HouseholdInfo(
            household_id=household.household_id,
            name=household.name,
            timezone=household.timezone,
            member_count=len(members),
        )

    # =========================================================================
    # User Operations
    # =========================================================================

    def register_user(
        self,
        household_id: str,
        name: str,
        role: Literal["ADMIN", "ADULT", "CHILD", "VIEW_ONLY"] = "CHILD",
        email: str | None = None,
    ) -> UserRegistrationResponse:
        """Register a new user in a household."""
        # Generate user ID
        user_id = str(uuid4())
        
        # Create user
        user = self._repository.create_user(
            user_id=user_id,
            household_id=household_id,
            name=name,
            role=role,
            email=email,
        )
        
        # Create membership record
        membership_id = str(uuid4())
        self._repository.create_membership(
            membership_id=membership_id,
            household_id=household_id,
            user_id=user_id,
            role=role,
        )
        
        user_info = UserInfo(
            user_id=user.user_id,
            household_id=user.household_id,
            name=user.name,
            email=user.email,
            role=user.role,
            is_active=user.is_active,
        )
        
        household_info = self.get_household(household_id)
        
        return UserRegistrationResponse(
            user=user_info,
            household=household_info,
        )

    def get_user_info(self, user_id: str) -> UserInfo | None:
        """Retrieve user info."""
        user = self._repository.get_user(user_id)
        if not user:
            return None
        
        return UserInfo(
            user_id=user.user_id,
            household_id=user.household_id,
            name=user.name,
            email=user.email,
            role=user.role,
            is_active=user.is_active,
        )

    # =========================================================================
    # Device Operations
    # =========================================================================

    def register_device(
        self,
        user_id: str,
        household_id: str,
        device_name: str,
        platform: Literal["iOS", "Android", "Web"],
        user_agent_hash: str,
    ) -> DeviceLinkingResponse:
        """Register a new device for a user."""
        # Generate device ID from deterministic hash
        # (In practice, this would be computed by frontend and validated here)
        device_id = str(uuid4())
        
        # Create device
        device = self._repository.create_device(
            device_id=device_id,
            user_id=user_id,
            household_id=household_id,
            device_name=device_name,
            platform=platform,
            user_agent=user_agent_hash,
        )
        
        device_info = DeviceInfo(
            device_id=device.device_id,
            user_id=device.user_id,
            household_id=device.household_id,
            device_name=device.device_name,
            platform=device.platform,
            is_active=device.is_active,
            last_seen_at=device.last_seen_at,
        )
        
        return DeviceLinkingResponse(device=device_info)

    def update_device_last_seen(self, device_id: str) -> DeviceInfo | None:
        """Update device last_seen timestamp."""
        device = self._repository.update_device(
            device_id=device_id,
            last_seen_at=datetime.utcnow(),
        )
        
        if not device:
            return None
        
        return DeviceInfo(
            device_id=device.device_id,
            user_id=device.user_id,
            household_id=device.household_id,
            device_name=device.device_name,
            platform=device.platform,
            is_active=device.is_active,
            last_seen_at=device.last_seen_at,
        )

    def get_device_info(self, device_id: str) -> DeviceInfo | None:
        """Retrieve device info."""
        device = self._repository.get_device(device_id)
        if not device:
            return None
        
        return DeviceInfo(
            device_id=device.device_id,
            user_id=device.user_id,
            household_id=device.household_id,
            device_name=device.device_name,
            platform=device.platform,
            is_active=device.is_active,
            last_seen_at=device.last_seen_at,
        )

    # =========================================================================
    # Bootstrap and Identity Resolution
    # =========================================================================

    def bootstrap_identity(
        self,
        household_id: str,
        user_id: str | None = None,
        device_id: str | None = None,
        session_token: str | None = None,
    ) -> IdentityBootstrapResponse:
        """
        Bootstrap identity from stored context or restore from token.
        
        Returns JWT access token for use with protected endpoints.
        Deterministic resolution: same input → same output.
        """
        # Option 1: Validate existing JWT token via token service
        if session_token:
            claims = self._token_service.validate_access_token(session_token)
            if claims:
                # JWT is still valid, use claims to reload identity
                user_id = claims.get("user_id")
                device_id = claims.get("device_id")
                # Fall through to Option 2 to resolve and issue fresh token
            else:
                # Invalid token, fall through to resolution by IDs
                pass
        
        # Option 2: Resolve from user_id + device_id
        if user_id and device_id:
            user = self._repository.get_user(user_id)
            device = self._repository.get_device(device_id)
            
            if user and device and user.household_id == household_id:
                # Issue new JWT token pair
                token_pair = self._token_service.issue_token_pair(
                    household_id=household_id,
                    user_id=user_id,
                    device_id=device_id,
                    role=user.role,
                )
                
                identity_context = build_identity_context(
                    household_id=household_id,
                    user_id=user_id,
                    device_id=device_id,
                    user_role=user.role,
                )
                
                household = self._repository.get_household(household_id)
                
                return IdentityBootstrapResponse(
                    household=self._household_to_info(household),
                    user=self._user_to_info(user),
                    device=self._device_to_info(device),
                    identity_context=identity_context,
                    session_token=token_pair.access_token,
                )
        
        # Option 3: Fallback - use any user in household (or specified user)
        # Try to use specified user_id first, fallback to first user in household
        if user_id:
            user = self._repository.get_user(user_id)
            if not user or user.household_id != household_id:
                raise ValueError(
                    f"Cannot resolve identity: user {user_id} not found or not in household"
                )
        else:
            users = self._repository.list_users_in_household(household_id)
            if not users:
                raise ValueError(
                    f"Cannot resolve identity for household {household_id}: no users found"
                )
            user = users[0]
        
        # Resolved user, get or create device
        user_id = user.user_id
        
        # Look for existing device, or create placeholder device if none exists
        devices = self._repository.list_devices_for_user(user_id)
        if devices:
            device = devices[0]
            logger.debug(f"[bootstrap_identity] Using existing device: {device.device_id}")
        else:
            # Auto-create placeholder device for first-time bootstrap
            logger.debug(f"[bootstrap_identity] Creating placeholder device for user {user_id}")
            device_id = str(uuid4())
            device = self._repository.create_device(
                device_id=device_id,
                user_id=user_id,
                household_id=household_id,
                device_name="Primary Device",
                platform="Web",
                user_agent="bootstrap-placeholder",
            )
            logger.debug(f"[bootstrap_identity] ✓ Created placeholder device: {device.device_id}")
        
        # Issue new JWT token pair
        token_pair = self._token_service.issue_token_pair(
            household_id=household_id,
            user_id=user_id,
            device_id=device.device_id,
            role=user.role,
        )
        
        identity_context = build_identity_context(
            household_id=household_id,
            user_id=user_id,
            device_id=device.device_id,
            user_role=user.role,
        )
        
        household = self._repository.get_household(household_id)
        
        return IdentityBootstrapResponse(
            household=self._household_to_info(household),
            user=self._user_to_info(user),
            device=self._device_to_info(device),
            identity_context=identity_context,
            session_token=token_pair.access_token,
        )

    # =========================================================================
    # Session Management
    # =========================================================================

    def validate_session(self, session_token: str) -> SessionValidationResponse:
        """
        Validate JWT access token and optionally rotate with refresh token.
        
        Returns validity status, identity context, and optionally refreshed token.
        """
        # Validate access token via TokenService
        claims = self._token_service.validate_access_token(session_token)
        
        if not claims:
            # Token is invalid or expired
            return SessionValidationResponse(is_valid=False)
        
        # Build identity context from JWT claims
        identity_context = build_identity_context(
            household_id=claims.get("household_id", ""),
            user_id=claims.get("user_id", ""),
            device_id=claims.get("device_id", ""),
            user_role=claims.get("role", "VIEW_ONLY"),
        )
        
        # For now, return valid status with identity context
        # (Token rotation would require explicit refresh endpoint)
        return SessionValidationResponse(
            is_valid=True,
            identity_context=identity_context,
            refreshed_token=session_token,  # Return same token; use refresh endpoint for rotation
        )

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _household_to_info(self, household) -> HouseholdInfo:
        """Convert model to info contract."""
        members = self._repository.list_users_in_household(household.household_id)
        return HouseholdInfo(
            household_id=household.household_id,
            name=household.name,
            timezone=household.timezone,
            member_count=len(members),
        )

    def _user_to_info(self, user) -> UserInfo:
        """Convert model to info contract."""
        return UserInfo(
            user_id=user.user_id,
            household_id=user.household_id,
            name=user.name,
            email=user.email,
            role=user.role,
            is_active=user.is_active,
        )

    def _device_to_info(self, device) -> DeviceInfo:
        """Convert model to info contract."""
        return DeviceInfo(
            device_id=device.device_id,
            user_id=device.user_id,
            household_id=device.household_id,
            device_name=device.device_name,
            platform=device.platform,
            is_active=device.is_active,
            last_seen_at=device.last_seen_at,
        )
