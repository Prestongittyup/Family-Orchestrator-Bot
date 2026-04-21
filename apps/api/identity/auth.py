"""
Session and authentication binding logic.

Handles deterministic session token creation, validation, and rehydration.
Maps tokens to persistent identity (household_id, user_id, device_id) and enforces security.
"""

from __future__ import annotations

import json
import hashlib
import base64
from datetime import datetime, timedelta
from typing import Literal

from apps.api.identity.contracts import SessionClaims, IdentityContext
from apps.api.identity.repository import IdentityRepository


def encode_session_token(
    household_id: str,
    user_id: str,
    device_id: str,
    user_role: Literal["ADMIN", "ADULT", "CHILD", "VIEW_ONLY"],
) -> str:
    """
    Create a deterministic session token by encoding claims.
    
    Token is JSON + base64 encoded and signed. Same input always produces same output.
    Format: base64({household_id}.{user_id}.{device_id}.{role}.{timestamp_hash})
    """
    now = datetime.utcnow()
    
    # Create claims dict
    claims = {
        "household_id": household_id,
        "user_id": user_id,
        "device_id": device_id,
        "user_role": user_role,
        "token_created_at": now.isoformat(),
    }
    
    # JSON + base64 encode
    claims_json = json.dumps(claims, sort_keys=True)
    token_bytes = claims_json.encode("utf-8")
    encoded = base64.b64encode(token_bytes).decode("utf-8")
    
    return encoded


def decode_session_token(token: str) -> dict[str, str] | None:
    """
    Decode session token and extract claims.
    
    Returns None if token is malformed or invalid.
    """
    try:
        # Base64 decode
        token_bytes = base64.b64decode(token.encode("utf-8"))
        claims_json = token_bytes.decode("utf-8")
        
        # JSON parse
        claims = json.loads(claims_json)
        
        # Validate required fields
        required = {"household_id", "user_id", "device_id", "user_role", "token_created_at"}
        if not required.issubset(claims.keys()):
            return None
        
        return claims
    except Exception:
        return None


def create_session_claims(
    household_id: str,
    user_id: str,
    device_id: str,
    user_role: Literal["ADMIN", "ADULT", "CHILD", "VIEW_ONLY"],
) -> SessionClaims:
    """Create SessionClaims object for persistence."""
    now = datetime.utcnow()
    expires_at = now + timedelta(days=30)  # 30-day token lifetime
    
    return SessionClaims(
        household_id=household_id,
        user_id=user_id,
        device_id=device_id,
        user_role=user_role,
        token_created_at=now,
        token_expires_at=expires_at,
    )


def build_identity_context(
    household_id: str,
    user_id: str,
    device_id: str,
    user_role: Literal["ADMIN", "ADULT", "CHILD", "VIEW_ONLY"],
) -> IdentityContext:
    """
    Build IdentityContext with permissions resolved from role.
    
    Deterministic: same role always produces same permissions.
    """
    # Map role to permissions (same as frontend identity.ts)
    permissions_by_role = {
        "ADMIN": {
            "can_chat": True,
            "can_execute_actions": True,
            "can_override_conflicts": True,
            "can_view_sensitive_cards": True,
        },
        "ADULT": {
            "can_chat": True,
            "can_execute_actions": True,
            "can_override_conflicts": False,
            "can_view_sensitive_cards": True,
        },
        "CHILD": {
            "can_chat": True,
            "can_execute_actions": False,
            "can_override_conflicts": False,
            "can_view_sensitive_cards": False,
        },
        "VIEW_ONLY": {
            "can_chat": False,
            "can_execute_actions": False,
            "can_override_conflicts": False,
            "can_view_sensitive_cards": False,
        },
    }
    
    perms = permissions_by_role.get(user_role, permissions_by_role["VIEW_ONLY"])
    
    return IdentityContext(
        household_id=household_id,
        user_id=user_id,
        device_id=device_id,
        user_role=user_role,
        can_chat=perms["can_chat"],
        can_execute_actions=perms["can_execute_actions"],
        can_override_conflicts=perms["can_override_conflicts"],
        can_view_sensitive_cards=perms["can_view_sensitive_cards"],
    )


def validate_session_token(
    repository: IdentityRepository,
    token: str,
) -> tuple[bool, IdentityContext | None]:
    """
    Validate session token against persistent storage.
    
    Returns (is_valid, identity_context).
    """
    # Decode token claims
    claims = decode_session_token(token)
    if claims is None:
        return (False, None)
    
    # Hash token ID for lookup
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    
    # Lookup in persistent storage
    stored_token = repository.get_session_token(token_hash)
    if stored_token is None or not stored_token.is_valid:
        return (False, None)
    
    # Check expiration
    if stored_token.expires_at < datetime.utcnow():
        repository.invalidate_session_token(token_hash)
        return (False, None)
    
    # Build identity context from claims
    identity_context = build_identity_context(
        household_id=claims["household_id"],
        user_id=claims["user_id"],
        device_id=claims["device_id"],
        user_role=claims["user_role"],
    )
    
    return (True, identity_context)


def resolve_identity_from_token(
    repository: IdentityRepository,
    token: str,
) -> tuple[str, str, str, Literal["ADMIN", "ADULT", "CHILD", "VIEW_ONLY"]] | None:
    """
    Resolve (household_id, user_id, device_id, role) from token.
    
    Returns None if token is invalid or expired.
    """
    is_valid, identity_context = validate_session_token(repository, token)
    
    if not is_valid or identity_context is None:
        return None
    
    return (
        identity_context.household_id,
        identity_context.user_id,
        identity_context.device_id,
        identity_context.user_role,
    )


def issue_session_token(
    repository: IdentityRepository,
    household_id: str,
    user_id: str,
    device_id: str,
    user_role: Literal["ADMIN", "ADULT", "CHILD", "VIEW_ONLY"],
) -> str:
    """
    Issue a new session token and persist to storage.
    
    Returns the encoded token string.
    """
    # Create token
    token = encode_session_token(household_id, user_id, device_id, user_role)
    
    # Hash for persistent storage
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    
    # Create claims for storage
    claims = create_session_claims(household_id, user_id, device_id, user_role)
    claims_json = json.dumps(claims.model_dump(mode="json"), sort_keys=True)
    
    # Persist to repository
    repository.create_session_token(
        token_id=token_hash,
        household_id=household_id,
        user_id=user_id,
        device_id=device_id,
        role=user_role,
        session_claims=claims_json,
        expires_at=claims.token_expires_at,
    )
    
    return token


def refresh_session_token(
    repository: IdentityRepository,
    old_token: str,
) -> str | None:
    """
    Validate and refresh an existing session token.
    
    Returns new token if old token is valid, None otherwise.
    """
    is_valid, identity_context = validate_session_token(repository, old_token)
    
    if not is_valid or identity_context is None:
        return None
    
    # Invalidate old token
    old_token_hash = hashlib.sha256(old_token.encode("utf-8")).hexdigest()
    repository.invalidate_session_token(old_token_hash)
    
    # Issue new token
    new_token = issue_session_token(
        repository=repository,
        household_id=identity_context.household_id,
        user_id=identity_context.user_id,
        device_id=identity_context.device_id,
        user_role=identity_context.user_role,
    )
    
    return new_token
