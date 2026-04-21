"""
Comprehensive tests for persistent identity layer.

Tests deterministic identity resolution, session rehydration, isolation enforcement,
device switching, and multi-user household integrity.
"""

from __future__ import annotations

import pytest
from datetime import datetime

from apps.api.identity.sqlalchemy_repository import SQLAlchemyIdentityRepository
from apps.api.identity.service import IdentityService
from apps.api.identity.auth import (
    encode_session_token,
    decode_session_token,
    validate_session_token,
    issue_session_token,
    refresh_session_token,
    build_identity_context,
)


class TestPersistenceDeterminism:
    """Test that identity operations are deterministic across sessions."""

    def test_encode_decode_determinism(self):
        """Same input → same encoded token → same decoded claims."""
        household_id = "hh-001"
        user_id = "user-001"
        device_id = "dev-001"
        role = "ADMIN"
        
        # Encode same input twice
        token1 = encode_session_token(household_id, user_id, device_id, role)
        token2 = encode_session_token(household_id, user_id, device_id, role)
        
        # Must produce identical tokens
        assert token1 == token2, "Same input must produce same token"
        
        # Decode both
        claims1 = decode_session_token(token1)
        claims2 = decode_session_token(token2)
        
        # Claims must be identical
        assert claims1 == claims2
        assert claims1["household_id"] == household_id
        assert claims1["user_id"] == user_id
        assert claims1["device_id"] == device_id
        assert claims1["user_role"] == role

    def test_household_creation_persistence(self):
        """Household created once is retrievable deterministically."""
        repo = SQLAlchemyIdentityRepository()
        
        household_id = "hh-test-001"
        household = repo.create_household(
            household_id=household_id,
            name="Test Household",
            timezone="UTC",
        )
        
        # Retrieve immediately
        retrieved1 = repo.get_household(household_id)
        assert retrieved1 is not None
        assert retrieved1.household_id == household_id
        assert retrieved1.name == "Test Household"
        
        # Retrieve again (simulating new session)
        retrieved2 = repo.get_household(household_id)
        assert retrieved2 is not None
        assert retrieved2.household_id == household_id
        assert retrieved2.name == "Test Household"
        
        # Must be identical
        assert retrieved1.created_at == retrieved2.created_at

    def test_user_creation_persistence(self):
        """User created once is retrievable with same attributes."""
        repo = SQLAlchemyIdentityRepository()
        service = IdentityService(repo)
        
        # Create household and user
        household_resp = service.create_household(
            name="Test Household",
            founder_name="Alice",
            founder_email="alice@example.com",
        )
        household_id = household_resp.household.household_id
        user_id = household_resp.founder_user.user_id
        
        # Retrieve user
        user_info = service.get_user_info(user_id)
        assert user_info is not None
        assert user_info.household_id == household_id
        assert user_info.name == "Alice"
        assert user_info.email == "alice@example.com"
        assert user_info.role == "ADMIN"


class TestSessionRehydration:
    """Test session token rehydration across device reinstalls."""

    def test_session_token_encodes_claims(self):
        """Session token encodes household/user/device deterministically."""
        household_id = "hh-001"
        user_id = "user-001"
        device_id = "dev-001"
        role = "CHILD"
        
        token = encode_session_token(household_id, user_id, device_id, role)
        
        # Decode to verify claims
        claims = decode_session_token(token)
        assert claims is not None
        assert claims["household_id"] == household_id
        assert claims["user_id"] == user_id
        assert claims["device_id"] == device_id
        assert claims["user_role"] == role

    def test_session_rehydration_from_token(self):
        """Can rehydrate identity from persisted session token."""
        repo = SQLAlchemyIdentityRepository()
        service = IdentityService(repo)
        
        # Create household, user, device
        hh_resp = service.create_household(
            name="Test Household",
            founder_name="Bob",
        )
        household_id = hh_resp.household.household_id
        user_id = hh_resp.founder_user.user_id
        
        # Register device
        dev_resp = service.register_device(
            user_id=user_id,
            household_id=household_id,
            device_name="Bob's iPhone",
            platform="iOS",
            user_agent_hash="agent-hash-123",
        )
        device_id = dev_resp.device.device_id
        
        # Issue session token
        token = issue_session_token(
            repository=repo,
            household_id=household_id,
            user_id=user_id,
            device_id=device_id,
            user_role="ADMIN",
        )
        
        # Validate token (simulating device rehydration)
        is_valid, identity = validate_session_token(repo, token)
        assert is_valid is True
        assert identity is not None
        assert identity.household_id == household_id
        assert identity.user_id == user_id
        assert identity.device_id == device_id

    def test_token_refresh_preserves_identity(self):
        """Refreshed token resolves to same identity."""
        repo = SQLAlchemyIdentityRepository()
        service = IdentityService(repo)
        
        # Create full identity tree
        hh_resp = service.create_household(name="Test", founder_name="Carol")
        household_id = hh_resp.household.household_id
        user_id = hh_resp.founder_user.user_id
        
        dev_resp = service.register_device(
            user_id=user_id,
            household_id=household_id,
            device_name="Device",
            platform="Android",
            user_agent_hash="agent-456",
        )
        device_id = dev_resp.device.device_id
        
        # Issue original token
        token1 = issue_session_token(
            repository=repo,
            household_id=household_id,
            user_id=user_id,
            device_id=device_id,
            user_role="ADULT",
        )
        
        # Validate and get identity
        is_valid1, identity1 = validate_session_token(repo, token1)
        assert is_valid1 is True
        
        # Refresh token
        token2 = refresh_session_token(repo, token1)
        assert token2 is not None
        assert token1 != token2  # Different token string
        
        # Validate refreshed token
        is_valid2, identity2 = validate_session_token(repo, token2)
        assert is_valid2 is True
        assert identity2 is not None
        
        # Identities must be the same
        assert identity1.household_id == identity2.household_id
        assert identity1.user_id == identity2.user_id
        assert identity1.device_id == identity2.device_id
        assert identity1.user_role == identity2.user_role


class TestHouseholdIsolation:
    """Test that household isolation is enforced."""

    def test_users_isolated_between_households(self):
        """Users in one household are isolated from another."""
        repo = SQLAlchemyIdentityRepository()
        
        # Create household 1
        hh1 = repo.create_household("Household 1", "UTC")
        user1 = repo.create_user(
            user_id="user-1",
            household_id=hh1.household_id,
            name="Alice",
            role="ADMIN",
        )
        
        # Create household 2
        hh2 = repo.create_household("Household 2", "UTC")
        user2 = repo.create_user(
            user_id="user-2",
            household_id=hh2.household_id,
            name="Bob",
            role="ADMIN",
        )
        
        # List users in household 1
        users_hh1 = repo.list_users_in_household(hh1.household_id)
        assert len(users_hh1) == 1
        assert users_hh1[0].user_id == "user-1"
        
        # List users in household 2
        users_hh2 = repo.list_users_in_household(hh2.household_id)
        assert len(users_hh2) == 1
        assert users_hh2[0].user_id == "user-2"
        
        # Cross-household lookup must not leak
        retrieved = repo.get_user("user-1")
        assert retrieved.household_id == hh1.household_id
        assert retrieved.household_id != hh2.household_id

    def test_devices_isolated_between_users(self):
        """Devices are bound to specific users."""
        repo = SQLAlchemyIdentityRepository()
        
        # Create household with two users
        hh = repo.create_household("Test", "UTC")
        user1 = repo.create_user("user-1", hh.household_id, "Alice", "ADMIN")
        user2 = repo.create_user("user-2", hh.household_id, "Bob", "ADULT")
        
        # Register device for user 1
        dev1 = repo.create_device(
            device_id="dev-1",
            user_id=user1.user_id,
            household_id=hh.household_id,
            device_name="Alice's Phone",
            platform="iOS",
            user_agent="agent-1",
        )
        
        # Register device for user 2
        dev2 = repo.create_device(
            device_id="dev-2",
            user_id=user2.user_id,
            household_id=hh.household_id,
            device_name="Bob's Phone",
            platform="Android",
            user_agent="agent-2",
        )
        
        # Devices for user 1 should not include user 2's device
        user1_devices = repo.list_devices_for_user(user1.user_id)
        assert len(user1_devices) == 1
        assert user1_devices[0].device_id == "dev-1"
        
        # Devices for user 2 should not include user 1's device
        user2_devices = repo.list_devices_for_user(user2.user_id)
        assert len(user2_devices) == 1
        assert user2_devices[0].device_id == "dev-2"


class TestDeviceSwitching:
    """Test device consistency during user device switching."""

    def test_device_registration_consistency(self):
        """Device registered for user is retrievable consistently."""
        repo = SQLAlchemyIdentityRepository()
        service = IdentityService(repo)
        
        # Create household and user
        hh_resp = service.create_household(name="Test", founder_name="Dave")
        household_id = hh_resp.household.household_id
        user_id = hh_resp.founder_user.user_id
        
        # Register device
        dev_resp = service.register_device(
            user_id=user_id,
            household_id=household_id,
            device_name="Dave's Tablet",
            platform="Web",
            user_agent_hash="agent-web-001",
        )
        device_id = dev_resp.device.device_id
        
        # Retrieve device immediately
        dev_info1 = service.get_device_info(device_id)
        assert dev_info1 is not None
        assert dev_info1.user_id == user_id
        assert dev_info1.device_name == "Dave's Tablet"
        
        # Retrieve again (simulating new session)
        dev_info2 = service.get_device_info(device_id)
        assert dev_info2 is not None
        assert dev_info2.user_id == user_id
        assert dev_info2.device_name == "Dave's Tablet"

    def test_last_seen_tracking(self):
        """Device last_seen timestamp is updated deterministically."""
        repo = SQLAlchemyIdentityRepository()
        
        # Create minimal identity tree
        hh = repo.create_household("Test", "UTC")
        user = repo.create_user("user-x", hh.household_id, "Eve", "CHILD")
        dev = repo.create_device(
            "dev-x",
            user.user_id,
            hh.household_id,
            "Device",
            "iOS",
            "agent-x",
        )
        
        # Initially, last_seen is None
        assert dev.last_seen_at is None
        
        # Update last_seen
        before = datetime.utcnow()
        updated_dev = repo.update_device(dev.device_id, last_seen_at=before)
        assert updated_dev is not None
        assert updated_dev.last_seen_at is not None
        
        # Retrieve again
        retrieved_dev = repo.get_device(dev.device_id)
        assert retrieved_dev.last_seen_at is not None


class TestMultiUserHouseholdIntegrity:
    """Test multi-user household membership and role enforcement."""

    def test_multi_user_household_membership(self):
        """Multiple users can join same household with different roles."""
        repo = SQLAlchemyIdentityRepository()
        service = IdentityService(repo)
        
        # Create household
        hh_resp = service.create_household(
            name="Family",
            founder_name="Parent",
        )
        household_id = hh_resp.household.household_id
        
        # Add more users with different roles
        child_resp = service.register_user(
            household_id=household_id,
            name="Child",
            role="CHILD",
        )
        
        adult_resp = service.register_user(
            household_id=household_id,
            name="Adult",
            role="ADULT",
        )
        
        # Verify all users are in household
        users = repo.list_users_in_household(household_id)
        assert len(users) == 3  # Parent + Child + Adult
        
        roles = {u.role for u in users}
        assert roles == {"ADMIN", "CHILD", "ADULT"}

    def test_role_based_permissions(self):
        """Different roles have different permissions."""
        # ADMIN permissions
        admin_ctx = build_identity_context("hh", "user", "dev", "ADMIN")
        assert admin_ctx.can_chat is True
        assert admin_ctx.can_execute_actions is True
        assert admin_ctx.can_override_conflicts is True
        assert admin_ctx.can_view_sensitive_cards is True
        
        # ADULT permissions
        adult_ctx = build_identity_context("hh", "user", "dev", "ADULT")
        assert adult_ctx.can_chat is True
        assert adult_ctx.can_execute_actions is True
        assert adult_ctx.can_override_conflicts is False
        assert adult_ctx.can_view_sensitive_cards is True
        
        # CHILD permissions
        child_ctx = build_identity_context("hh", "user", "dev", "CHILD")
        assert child_ctx.can_chat is True
        assert child_ctx.can_execute_actions is False
        assert child_ctx.can_override_conflicts is False
        assert child_ctx.can_view_sensitive_cards is False
        
        # VIEW_ONLY permissions
        view_ctx = build_identity_context("hh", "user", "dev", "VIEW_ONLY")
        assert view_ctx.can_chat is False
        assert view_ctx.can_execute_actions is False
        assert view_ctx.can_override_conflicts is False
        assert view_ctx.can_view_sensitive_cards is False

    def test_bootstrap_resolves_to_any_user_in_household(self):
        """Bootstrap without explicit user_id resolves to any user in household."""
        repo = SQLAlchemyIdentityRepository()
        service = IdentityService(repo)
        
        # Create household with two users
        hh_resp = service.create_household(name="Test", founder_name="Frank")
        household_id = hh_resp.household.household_id
        
        second_user_resp = service.register_user(
            household_id=household_id,
            name="Grace",
            role="ADULT",
        )
        
        # Bootstrap without explicit user_id
        bootstrap_resp = service.bootstrap_identity(household_id=household_id)
        
        # Should resolve to one of the users
        resolved_user_id = bootstrap_resp.user.user_id
        assert resolved_user_id in [
            hh_resp.founder_user.user_id,
            second_user_resp.user.user_id,
        ]


class TestTokenExpiration:
    """Test session token expiration handling."""

    def test_expired_token_rejected(self):
        """Expired token is rejected on validation."""
        repo = SQLAlchemyIdentityRepository()
        
        # Create expired token (manual insertion for testing)
        from datetime import timedelta
        past_time = datetime.utcnow() - timedelta(days=1)
        
        import hashlib
        token = encode_session_token("hh", "user", "dev", "ADMIN")
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        
        repo.create_session_token(
            token_id=token_hash,
            household_id="hh",
            user_id="user",
            device_id="dev",
            role="ADMIN",
            session_claims="{}",
            expires_at=past_time,  # Expired!
        )
        
        # Validation should fail
        is_valid, identity = validate_session_token(repo, token)
        assert is_valid is False
        assert identity is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
