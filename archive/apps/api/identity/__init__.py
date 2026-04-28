"""
Identity module — persistent household identity, device registry, and session binding.

This module provides the durable backend foundation for HPAL's multi-user, multi-device
household support:

- Models: Household, User, Device, Membership, SessionToken
- Contracts: UI-safe request/response models
- Repository: Abstract storage interface (SQLite, PostgreSQL, Cosmos DB, etc.)
- Auth: Session token encoding, validation, and rehydration
- Service: High-level identity operations (bootstrap, register, link device, etc.)
- Endpoints: FastAPI router with /v1/identity/* routes

All identity resolution is deterministic:
- Same input → same output (no randomness)
- Identity survives full frontend reinstalls
- Device rehydration from stored user_agent hash
- Session tokens map persistently to household/user/device
- Multi-user/multi-household isolation enforced at repository level
"""

__all__ = [
    "IdentityRepository",
    "SQLAlchemyIdentityRepository",
    "IdentityService",
    "encode_session_token",
    "decode_session_token",
    "validate_session_token",
    "issue_session_token",
    "refresh_session_token",
    "build_identity_context",
]
