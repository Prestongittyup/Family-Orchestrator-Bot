"""
Repository layer for identity operations.

Abstract storage interface enabling flexible backend swaps (SQLite, PostgreSQL, Cosmos DB, etc.).
All identity persistence and resolution happens here; no business logic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Protocol

from apps.api.models.identity import (
    Household, User, Device, Membership, SessionToken
)


class IdentityRepository(ABC):
    """
    Abstract identity storage interface.
    
    Implementations can swap SQLAlchemy, MongoDB, Cosmos DB, etc. without
    affecting business logic or consumers.
    """

    # =========================================================================
    # Household Operations
    # =========================================================================

    @abstractmethod
    def create_household(
        self,
        household_id: str,
        name: str,
        timezone: str = "UTC",
    ) -> Household:
        """Create a new household."""
        pass

    @abstractmethod
    def get_household(self, household_id: str) -> Household | None:
        """Retrieve household by ID."""
        pass

    @abstractmethod
    def get_household_by_name(self, name: str) -> Household | None:
        """Retrieve household by name."""
        pass

    @abstractmethod
    def update_household(
        self,
        household_id: str,
        name: str | None = None,
        timezone: str | None = None,
    ) -> Household | None:
        """Update household metadata."""
        pass

    @abstractmethod
    def list_households(self) -> list[Household]:
        """List all households."""
        pass

    # =========================================================================
    # User Operations
    # =========================================================================

    @abstractmethod
    def create_user(
        self,
        user_id: str,
        household_id: str,
        name: str,
        role: str,
        email: str | None = None,
    ) -> User:
        """Create a new user in a household."""
        pass

    @abstractmethod
    def get_user(self, user_id: str) -> User | None:
        """Retrieve user by ID."""
        pass

    @abstractmethod
    def get_user_by_email(self, email: str) -> User | None:
        """Retrieve user by email."""
        pass

    @abstractmethod
    def list_users_in_household(self, household_id: str) -> list[User]:
        """List all users in a household."""
        pass

    @abstractmethod
    def update_user(
        self,
        user_id: str,
        name: str | None = None,
        email: str | None = None,
        role: str | None = None,
        is_active: bool | None = None,
    ) -> User | None:
        """Update user metadata."""
        pass

    @abstractmethod
    def deactivate_user(self, user_id: str) -> User | None:
        """Mark user as inactive."""
        pass

    # =========================================================================
    # Device Operations
    # =========================================================================

    @abstractmethod
    def create_device(
        self,
        device_id: str,
        user_id: str,
        household_id: str,
        device_name: str,
        platform: str,
        user_agent: str,
    ) -> Device:
        """Register a new device for a user."""
        pass

    @abstractmethod
    def get_device(self, device_id: str) -> Device | None:
        """Retrieve device by ID."""
        pass

    @abstractmethod
    def list_devices_for_user(self, user_id: str) -> list[Device]:
        """List all devices registered to a user."""
        pass

    @abstractmethod
    def list_devices_in_household(self, household_id: str) -> list[Device]:
        """List all devices in a household."""
        pass

    @abstractmethod
    def update_device(
        self,
        device_id: str,
        device_name: str | None = None,
        is_active: bool | None = None,
        last_seen_at: datetime | None = None,
    ) -> Device | None:
        """Update device metadata."""
        pass

    @abstractmethod
    def deactivate_device(self, device_id: str) -> Device | None:
        """Mark device as inactive."""
        pass

    # =========================================================================
    # Membership Operations
    # =========================================================================

    @abstractmethod
    def create_membership(
        self,
        membership_id: str,
        household_id: str,
        user_id: str,
        role: str,
        invited_by: str | None = None,
    ) -> Membership:
        """Create membership record linking user to household."""
        pass

    @abstractmethod
    def get_membership(self, membership_id: str) -> Membership | None:
        """Retrieve membership by ID."""
        pass

    @abstractmethod
    def get_membership_by_household_user(
        self, household_id: str, user_id: str
    ) -> Membership | None:
        """Retrieve membership record for specific household+user pair."""
        pass

    @abstractmethod
    def list_memberships_for_household(self, household_id: str) -> list[Membership]:
        """List all memberships in a household."""
        pass

    @abstractmethod
    def list_memberships_for_user(self, user_id: str) -> list[Membership]:
        """List all household memberships for a user."""
        pass

    @abstractmethod
    def update_membership(
        self,
        membership_id: str,
        role: str | None = None,
        is_active: bool | None = None,
    ) -> Membership | None:
        """Update membership metadata."""
        pass

    @abstractmethod
    def accept_membership_invite(self, membership_id: str) -> Membership | None:
        """Mark membership invite as accepted."""
        pass

    # =========================================================================
    # Session Token Operations
    # =========================================================================

    @abstractmethod
    def create_session_token(
        self,
        token_id: str,
        household_id: str,
        user_id: str,
        device_id: str,
        role: str,
        session_claims: str,  # JSON encoded
        expires_at: datetime,
    ) -> SessionToken:
        """Create a new session token mapping."""
        pass

    @abstractmethod
    def get_session_token(self, token_id: str) -> SessionToken | None:
        """Retrieve session token by ID (token hash)."""
        pass

    @abstractmethod
    def list_session_tokens_for_device(self, device_id: str) -> list[SessionToken]:
        """List all valid session tokens for a device."""
        pass

    @abstractmethod
    def list_session_tokens_for_user(self, user_id: str) -> list[SessionToken]:
        """List all valid session tokens for a user."""
        pass

    @abstractmethod
    def invalidate_session_token(self, token_id: str) -> SessionToken | None:
        """Mark a session token as invalid."""
        pass

    @abstractmethod
    def invalidate_all_device_tokens(self, device_id: str) -> int:
        """Invalidate all session tokens for a device (returns count)."""
        pass

    @abstractmethod
    def invalidate_all_user_tokens(self, user_id: str) -> int:
        """Invalidate all session tokens for a user (returns count)."""
        pass

    @abstractmethod
    def cleanup_expired_tokens(self) -> int:
        """Remove expired tokens from storage (returns count deleted)."""
        pass

    # =========================================================================
    # Transactional Operations
    # =========================================================================

    @abstractmethod
    def begin_transaction(self) -> None:
        """Begin a database transaction."""
        pass

    @abstractmethod
    def commit_transaction(self) -> None:
        """Commit current transaction."""
        pass

    @abstractmethod
    def rollback_transaction(self) -> None:
        """Rollback current transaction."""
        pass
