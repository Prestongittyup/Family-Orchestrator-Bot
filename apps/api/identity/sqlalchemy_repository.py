"""
SQLAlchemy implementation of identity repository.

Implements the IdentityRepository interface using SQLAlchemy ORM.
Returns concrete model instances from database operations.
"""

from __future__ import annotations

from datetime import datetime
import logging

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.core.database import SessionLocal
from apps.api.models.identity import (
    Household, User, Device, Membership, SessionToken
)
from apps.api.identity.repository import IdentityRepository


logger = logging.getLogger(__name__)


class SQLAlchemyIdentityRepository(IdentityRepository):
    """SQLAlchemy-backed identity storage implementation."""

    def __init__(self, session: Session | None = None):
        """Initialize with optional session; creates new if not provided."""
        self._session = session
        self._owns_session = session is None

    def _get_session(self) -> Session:
        """Get or create session."""
        if self._session is None:
            self._session = SessionLocal()
            self._owns_session = True
        return self._session

    def _close_session(self) -> None:
        """Close session if we own it."""
        if self._owns_session and self._session:
            self._session.close()
            self._session = None

    # =========================================================================
    # Household Operations
    # =========================================================================

    def create_household(
        self,
        household_id: str,
        name: str,
        timezone: str = "UTC",
    ) -> Household:
        session = self._get_session()
        try:
            household = Household(
                household_id=household_id,
                name=name,
                timezone=timezone,
            )
            session.add(household)
            session.flush()
            session.commit()
            session.refresh(household)
            return household
        except SQLAlchemyError as exc:
            session.rollback()
            logger.error("create_household failed", exc_info=True)
            raise
        finally:
            if self._owns_session:
                self._close_session()

    def get_household(self, household_id: str) -> Household | None:
        session = self._get_session()
        try:
            return session.query(Household).filter(
                Household.household_id == household_id
            ).first()
        finally:
            if self._owns_session:
                self._close_session()

    def get_household_by_name(self, name: str) -> Household | None:
        session = self._get_session()
        try:
            return session.query(Household).filter(
                Household.name == name
            ).first()
        finally:
            if self._owns_session:
                self._close_session()

    def update_household(
        self,
        household_id: str,
        name: str | None = None,
        timezone: str | None = None,
    ) -> Household | None:
        session = self._get_session()
        try:
            household = session.get(Household, household_id)
            if household is None:
                return None
            if name is not None:
                household.name = name
            if timezone is not None:
                household.timezone = timezone
            session.commit()
            session.refresh(household)
            return household
        finally:
            if self._owns_session:
                self._close_session()

    def list_households(self) -> list[Household]:
        session = self._get_session()
        try:
            return session.query(Household).order_by(Household.created_at).all()
        finally:
            if self._owns_session:
                self._close_session()

    # =========================================================================
    # User Operations
    # =========================================================================

    def create_user(
        self,
        user_id: str,
        household_id: str,
        name: str,
        role: str,
        email: str | None = None,
    ) -> User:
        session = self._get_session()
        try:
            user = User(
                user_id=user_id,
                household_id=household_id,
                name=name,
                email=email,
                role=role,
            )
            session.add(user)
            session.flush()
            session.commit()
            session.refresh(user)
            return user
        except SQLAlchemyError as exc:
            session.rollback()
            logger.error("create_user failed", exc_info=True)
            raise
        finally:
            if self._owns_session:
                self._close_session()

    def get_user(self, user_id: str) -> User | None:
        session = self._get_session()
        try:
            return session.get(User, user_id)
        finally:
            if self._owns_session:
                self._close_session()

    def get_user_by_email(self, email: str) -> User | None:
        session = self._get_session()
        try:
            return session.query(User).filter(User.email == email).first()
        finally:
            if self._owns_session:
                self._close_session()

    def list_users_in_household(self, household_id: str) -> list[User]:
        session = self._get_session()
        try:
            return session.query(User).filter(
                User.household_id == household_id
            ).order_by(User.created_at).all()
        finally:
            if self._owns_session:
                self._close_session()

    def update_user(
        self,
        user_id: str,
        name: str | None = None,
        email: str | None = None,
        role: str | None = None,
        is_active: bool | None = None,
    ) -> User | None:
        session = self._get_session()
        try:
            user = session.get(User, user_id)
            if user is None:
                return None
            if name is not None:
                user.name = name
            if email is not None:
                user.email = email
            if role is not None:
                user.role = role
            if is_active is not None:
                user.is_active = is_active
            session.commit()
            session.refresh(user)
            return user
        finally:
            if self._owns_session:
                self._close_session()

    def deactivate_user(self, user_id: str) -> User | None:
        return self.update_user(user_id, is_active=False)

    # =========================================================================
    # Device Operations
    # =========================================================================

    def create_device(
        self,
        device_id: str,
        user_id: str,
        household_id: str,
        device_name: str,
        platform: str,
        user_agent: str,
    ) -> Device:
        session = self._get_session()
        try:
            device = Device(
                device_id=device_id,
                user_id=user_id,
                household_id=household_id,
                device_name=device_name,
                platform=platform,
                user_agent=user_agent,
            )
            session.add(device)
            session.flush()
            session.commit()
            session.refresh(device)
            return device
        except SQLAlchemyError as exc:
            session.rollback()
            logger.error("create_device failed", exc_info=True)
            raise
        finally:
            if self._owns_session:
                self._close_session()

    def get_device(self, device_id: str) -> Device | None:
        session = self._get_session()
        try:
            return session.get(Device, device_id)
        finally:
            if self._owns_session:
                self._close_session()

    def list_devices_for_user(self, user_id: str) -> list[Device]:
        session = self._get_session()
        try:
            return session.query(Device).filter(
                Device.user_id == user_id
            ).order_by(Device.created_at).all()
        finally:
            if self._owns_session:
                self._close_session()

    def list_devices_in_household(self, household_id: str) -> list[Device]:
        session = self._get_session()
        try:
            return session.query(Device).filter(
                Device.household_id == household_id
            ).order_by(Device.created_at).all()
        finally:
            if self._owns_session:
                self._close_session()

    def update_device(
        self,
        device_id: str,
        device_name: str | None = None,
        is_active: bool | None = None,
        last_seen_at: datetime | None = None,
    ) -> Device | None:
        session = self._get_session()
        try:
            device = session.get(Device, device_id)
            if device is None:
                return None
            if device_name is not None:
                device.device_name = device_name
            if is_active is not None:
                device.is_active = is_active
            if last_seen_at is not None:
                device.last_seen_at = last_seen_at
            session.commit()
            session.refresh(device)
            return device
        finally:
            if self._owns_session:
                self._close_session()

    def deactivate_device(self, device_id: str) -> Device | None:
        return self.update_device(device_id, is_active=False)

    # =========================================================================
    # Membership Operations
    # =========================================================================

    def create_membership(
        self,
        membership_id: str,
        household_id: str,
        user_id: str,
        role: str,
        invited_by: str | None = None,
    ) -> Membership:
        session = self._get_session()
        try:
            membership = Membership(
                membership_id=membership_id,
                household_id=household_id,
                user_id=user_id,
                role=role,
                invited_by=invited_by,
            )
            session.add(membership)
            session.flush()
            session.commit()
            session.refresh(membership)
            return membership
        except SQLAlchemyError as exc:
            session.rollback()
            logger.error("create_membership failed", exc_info=True)
            raise
        finally:
            if self._owns_session:
                self._close_session()

    def get_membership(self, membership_id: str) -> Membership | None:
        session = self._get_session()
        try:
            return session.get(Membership, membership_id)
        finally:
            if self._owns_session:
                self._close_session()

    def get_membership_by_household_user(
        self, household_id: str, user_id: str
    ) -> Membership | None:
        session = self._get_session()
        try:
            return session.query(Membership).filter(
                (Membership.household_id == household_id) &
                (Membership.user_id == user_id)
            ).first()
        finally:
            if self._owns_session:
                self._close_session()

    def list_memberships_for_household(self, household_id: str) -> list[Membership]:
        session = self._get_session()
        try:
            return session.query(Membership).filter(
                Membership.household_id == household_id
            ).order_by(Membership.created_at).all()
        finally:
            if self._owns_session:
                self._close_session()

    def list_memberships_for_user(self, user_id: str) -> list[Membership]:
        session = self._get_session()
        try:
            return session.query(Membership).filter(
                Membership.user_id == user_id
            ).order_by(Membership.created_at).all()
        finally:
            if self._owns_session:
                self._close_session()

    def update_membership(
        self,
        membership_id: str,
        role: str | None = None,
        is_active: bool | None = None,
    ) -> Membership | None:
        session = self._get_session()
        try:
            membership = session.get(Membership, membership_id)
            if membership is None:
                return None
            if role is not None:
                membership.role = role
            if is_active is not None:
                membership.is_active = is_active
            session.commit()
            session.refresh(membership)
            return membership
        finally:
            if self._owns_session:
                self._close_session()

    def accept_membership_invite(self, membership_id: str) -> Membership | None:
        session = self._get_session()
        try:
            membership = session.get(Membership, membership_id)
            if membership is None:
                return None
            membership.invite_accepted_at = datetime.utcnow()
            session.commit()
            session.refresh(membership)
            return membership
        finally:
            if self._owns_session:
                self._close_session()

    # =========================================================================
    # Session Token Operations
    # =========================================================================

    def create_session_token(
        self,
        token_id: str,
        household_id: str,
        user_id: str,
        device_id: str,
        role: str,
        session_claims: str,
        expires_at: datetime,
    ) -> SessionToken:
        session = self._get_session()
        try:
            token = SessionToken(
                token_id=token_id,
                household_id=household_id,
                user_id=user_id,
                device_id=device_id,
                role=role,
                session_claims=session_claims,
                expires_at=expires_at,
            )
            session.add(token)
            session.flush()
            session.commit()
            session.refresh(token)
            return token
        except SQLAlchemyError as exc:
            session.rollback()
            logger.error("create_session_token failed", exc_info=True)
            raise
        finally:
            if self._owns_session:
                self._close_session()

    def get_session_token(self, token_id: str) -> SessionToken | None:
        session = self._get_session()
        try:
            return session.get(SessionToken, token_id)
        finally:
            if self._owns_session:
                self._close_session()

    def list_session_tokens_for_device(self, device_id: str) -> list[SessionToken]:
        session = self._get_session()
        try:
            return session.query(SessionToken).filter(
                (SessionToken.device_id == device_id) &
                (SessionToken.is_valid == True)
            ).order_by(SessionToken.created_at).all()
        finally:
            if self._owns_session:
                self._close_session()

    def list_session_tokens_for_user(self, user_id: str) -> list[SessionToken]:
        session = self._get_session()
        try:
            return session.query(SessionToken).filter(
                (SessionToken.user_id == user_id) &
                (SessionToken.is_valid == True)
            ).order_by(SessionToken.created_at).all()
        finally:
            if self._owns_session:
                self._close_session()

    def invalidate_session_token(self, token_id: str) -> SessionToken | None:
        session = self._get_session()
        try:
            token = session.get(SessionToken, token_id)
            if token is None:
                return None
            token.is_valid = False
            session.commit()
            session.refresh(token)
            return token
        finally:
            if self._owns_session:
                self._close_session()

    def invalidate_all_device_tokens(self, device_id: str) -> int:
        session = self._get_session()
        try:
            count = session.query(SessionToken).filter(
                (SessionToken.device_id == device_id) &
                (SessionToken.is_valid == True)
            ).update({"is_valid": False})
            session.commit()
            return count
        finally:
            if self._owns_session:
                self._close_session()

    def invalidate_all_user_tokens(self, user_id: str) -> int:
        session = self._get_session()
        try:
            count = session.query(SessionToken).filter(
                (SessionToken.user_id == user_id) &
                (SessionToken.is_valid == True)
            ).update({"is_valid": False})
            session.commit()
            return count
        finally:
            if self._owns_session:
                self._close_session()

    def cleanup_expired_tokens(self) -> int:
        session = self._get_session()
        try:
            count = session.query(SessionToken).filter(
                SessionToken.expires_at < datetime.utcnow()
            ).delete()
            session.commit()
            return count
        finally:
            if self._owns_session:
                self._close_session()

    # =========================================================================
    # Transactional Operations
    # =========================================================================

    def begin_transaction(self) -> None:
        session = self._get_session()
        session.begin()

    def commit_transaction(self) -> None:
        session = self._get_session()
        session.commit()

    def rollback_transaction(self) -> None:
        session = self._get_session()
        session.rollback()
