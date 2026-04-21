"""
Persistent household identity, user, device, and membership models.

These models form the source-of-truth for household identity across sessions,
device reinstalls, and multi-user scenarios. No orchestration logic here;
pure persistence and resolution only.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from sqlalchemy import DateTime, String, Boolean, Index, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from apps.api.core.database import Base


class Household(Base):
    """
    Persistent household record.
    
    A household is the top-level scope containing multiple users and devices.
    Survives full frontend reinstalls and device switching.
    """
    __tablename__ = "households"

    # Primary identity
    household_id: Mapped[str] = mapped_column(String, primary_key=True)
    
    # Household metadata
    name: Mapped[str] = mapped_column(String, nullable=False)
    timezone: Mapped[str] = mapped_column(String, nullable=False, default="UTC")
    
    # Lifecycle
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    
    # Indexes for query performance
    __table_args__ = (
        Index("idx_household_id_created", "household_id", "created_at"),
    )


class User(Base):
    """
    Persistent user record.
    
    A user is a person within the household system. Each user has a stable
    user_id that persists across device reinstalls and session resets.
    """
    __tablename__ = "users"

    # Primary identity
    user_id: Mapped[str] = mapped_column(String, primary_key=True)
    household_id: Mapped[str] = mapped_column(
        String, ForeignKey("households.household_id"), nullable=False
    )
    
    # User metadata
    name: Mapped[str] = mapped_column(String, nullable=False)
    email: Mapped[str | None] = mapped_column(String, nullable=True, unique=True)
    
    # Role within household
    role: Mapped[str] = mapped_column(
        String, nullable=False, default="CHILD"
    )  # ADMIN, ADULT, CHILD, VIEW_ONLY
    
    # Account status
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    
    # Lifecycle
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    
    # Indexes for query performance
    __table_args__ = (
        Index("idx_user_household", "household_id"),
        Index("idx_user_email", "email"),
        Index("idx_user_created", "user_id", "created_at"),
    )


class Device(Base):
    """
    Persistent device record.
    
    A device is a physical phone, tablet, or other client where HPAL is installed.
    Device ID is deterministically derived from hardware + user identification,
    ensuring rehydration after reinstall.
    """
    __tablename__ = "devices"

    # Primary identity (deterministic hash of userId + userAgent + platform)
    device_id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.user_id"), nullable=False
    )
    household_id: Mapped[str] = mapped_column(
        String, ForeignKey("households.household_id"), nullable=False
    )
    
    # Device metadata
    device_name: Mapped[str] = mapped_column(String, nullable=False)  # e.g. "Jane's iPhone"
    platform: Mapped[str] = mapped_column(String, nullable=False)  # iOS, Android, Web
    user_agent: Mapped[str] = mapped_column(String, nullable=False)  # full user agent hash
    
    # Device status
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    
    # Lifecycle
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    
    # Indexes for query performance
    __table_args__ = (
        Index("idx_device_user", "user_id"),
        Index("idx_device_household", "household_id"),
        Index("idx_device_last_seen", "last_seen_at"),
        Index("idx_device_created", "device_id", "created_at"),
    )


class Membership(Base):
    """
    Persistent household membership record.
    
    Tracks which users are members of which households and their roles.
    Enables multi-household support and explicit membership management.
    """
    __tablename__ = "memberships"

    # Primary identity (composite)
    membership_id: Mapped[str] = mapped_column(String, primary_key=True)
    household_id: Mapped[str] = mapped_column(
        String, ForeignKey("households.household_id"), nullable=False
    )
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.user_id"), nullable=False
    )
    
    # Membership metadata
    role: Mapped[str] = mapped_column(
        String, nullable=False, default="CHILD"
    )  # ADMIN, ADULT, CHILD, VIEW_ONLY (may differ from user.role)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    
    # Invite/approval tracking
    invited_by: Mapped[str | None] = mapped_column(String, nullable=True)  # user_id of inviter
    invite_accepted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    # Lifecycle
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    
    # Indexes for query performance
    __table_args__ = (
        Index("idx_membership_household", "household_id"),
        Index("idx_membership_user", "user_id"),
        Index("idx_membership_household_user", "household_id", "user_id"),
        Index("idx_membership_created", "membership_id", "created_at"),
    )


class SessionToken(Base):
    """
    Persistent session token mapping.
    
    Maps session tokens to their resolved identity (user_id, household_id, device_id).
    Enables deterministic session rehydration and token validation.
    """
    __tablename__ = "session_tokens"

    # Primary identity (the token itself, or a hash)
    token_id: Mapped[str] = mapped_column(String, primary_key=True)  # session_token encoded
    
    # Identity resolution
    household_id: Mapped[str] = mapped_column(
        String, ForeignKey("households.household_id"), nullable=False
    )
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.user_id"), nullable=False
    )
    device_id: Mapped[str] = mapped_column(
        String, ForeignKey("devices.device_id"), nullable=False
    )
    
    # Session metadata
    role: Mapped[str] = mapped_column(String, nullable=False)  # ADMIN, ADULT, CHILD, VIEW_ONLY
    session_claims: Mapped[str] = mapped_column(String, nullable=False)  # JSON encoded claims
    
    # Session validity
    is_valid: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    
    # Lifecycle
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    
    # Indexes for query performance
    __table_args__ = (
        Index("idx_session_household", "household_id"),
        Index("idx_session_user", "user_id"),
        Index("idx_session_device", "device_id"),
        Index("idx_session_valid_expires", "is_valid", "expires_at"),
        Index("idx_session_created", "token_id", "created_at"),
    )
