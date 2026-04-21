"""
Boot path diagnostics and validation.

Validates that all required services are properly initialized and callable.
Used during startup and exposed via diagnostic endpoints.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any

from sqlalchemy import inspect, text

from apps.api.core.database import engine, SessionLocal
from apps.api.models.identity import Household, User, Device, SessionToken
from apps.api.auth.token_service import TokenService
from apps.api.identity.sqlalchemy_repository import SQLAlchemyIdentityRepository
from apps.api.core.auth_middleware import _PUBLIC_PATHS
from apps.api.realtime.broadcaster import broadcaster


logger = logging.getLogger(__name__)


class BootStatus(str, Enum):
    """Boot component status."""
    OK = "ok"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass
class BootDiagnostics:
    """Full boot diagnostics report."""
    database: BootStatus
    identity_repo: BootStatus
    household_repo: BootStatus
    token_service: BootStatus
    auth_middleware: BootStatus
    broadcaster: BootStatus
    overall: BootStatus = BootStatus.UNKNOWN
    
    database_error: str | None = None
    identity_repo_error: str | None = None
    household_repo_error: str | None = None
    token_service_error: str | None = None
    auth_middleware_error: str | None = None
    broadcaster_error: str | None = None
    migration_mode: str = "metadata_create_all"
    pool_status: str | None = None
    required_tables: list[str] | None = None
    required_columns: dict[str, list[str]] | None = None

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "database": self.database.value,
            "database_error": self.database_error,
            "identity_repo": self.identity_repo.value,
            "identity_repo_error": self.identity_repo_error,
            "household_repo": self.household_repo.value,
            "household_repo_error": self.household_repo_error,
            "token_service": self.token_service.value,
            "token_service_error": self.token_service_error,
            "auth_middleware": self.auth_middleware.value,
            "auth_middleware_error": self.auth_middleware_error,
            "broadcaster": self.broadcaster.value,
            "broadcaster_error": self.broadcaster_error,
            "overall": self.overall.value,
            "migration_mode": self.migration_mode,
            "pool_status": self.pool_status,
            "required_tables": self.required_tables or [],
            "required_columns": self.required_columns or {},
        }


REQUIRED_SCHEMA: dict[str, list[str]] = {
    "households": ["household_id", "name", "timezone", "created_at", "updated_at"],
    "users": ["user_id", "household_id", "name", "email", "role", "is_active", "created_at", "updated_at"],
    "devices": ["device_id", "user_id", "household_id", "device_name", "platform", "user_agent", "is_active", "created_at", "updated_at", "last_seen_at"],
    "memberships": ["membership_id", "household_id", "user_id", "role", "is_active", "created_at", "updated_at"],
    "session_tokens": ["token_id", "household_id", "user_id", "device_id", "role", "session_claims", "is_valid", "created_at", "expires_at"],
    "idempotency_keys": ["key", "household_id", "event_type", "created_at", "expires_at"],
}


def validate_database(required_schema: dict[str, list[str]]) -> tuple[BootStatus, str | None, str | None]:
    """Validate database connectivity and table existence."""
    try:
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        pool_status = None
        if hasattr(engine, "pool") and hasattr(engine.pool, "status"):
            pool_status = str(engine.pool.status())

        inspector = inspect(engine)
        existing_tables = set(inspector.get_table_names())
        required_tables = list(required_schema.keys())
        missing = [name for name in required_tables if name not in existing_tables]
        if missing:
            return (BootStatus.FAILED, f"Missing required tables: {', '.join(missing)}", pool_status)

        missing_columns: dict[str, list[str]] = {}
        for table_name, required_columns in required_schema.items():
            existing_columns = {col["name"] for col in inspector.get_columns(table_name)}
            missing_for_table = [col for col in required_columns if col not in existing_columns]
            if missing_for_table:
                missing_columns[table_name] = missing_for_table
        if missing_columns:
            return (BootStatus.FAILED, f"Missing required columns: {missing_columns}", pool_status)

        # Lightweight query checks for ORM availability
        session = SessionLocal()
        try:
            session.query(Household).first()
            session.query(User).first()
            session.query(Device).first()
            session.query(SessionToken).first()
        finally:
            session.close()

        return (BootStatus.OK, None, pool_status)
    except Exception as exc:
        return (BootStatus.FAILED, f"Database connection failed: {str(exc)}", None)


def validate_identity_repository() -> tuple[BootStatus, str | None]:
    """Validate identity repository can be instantiated."""
    try:
        repo = SQLAlchemyIdentityRepository()
        # Try a simple read operation
        households = repo.list_households()
        return (BootStatus.OK, None)
    except Exception as exc:
        return (BootStatus.FAILED, f"Repository instantiation failed: {str(exc)}")


def validate_household_repository() -> tuple[BootStatus, str | None]:
    """Validate household repository operations."""
    try:
        repo = SQLAlchemyIdentityRepository()
        # Check that we can list households (even if empty)
        _ = repo.list_households()
        return (BootStatus.OK, None)
    except Exception as exc:
        return (BootStatus.FAILED, f"Household repository failed: {str(exc)}")


def validate_token_service() -> tuple[BootStatus, str | None]:
    """Validate token service can be instantiated."""
    try:
        repo = SQLAlchemyIdentityRepository()
        token_service = TokenService(repo)
        # Verify token service has required methods
        assert hasattr(token_service, "issue_token_pair")
        assert hasattr(token_service, "validate_access_token")
        return (BootStatus.OK, None)
    except Exception as exc:
        return (BootStatus.FAILED, f"Token service validation failed: {str(exc)}")


def validate_auth_middleware() -> tuple[BootStatus, str | None]:
    """Validate auth middleware is properly configured."""
    try:
        # Check that TokenService can be instantiated for auth checks
        repo = SQLAlchemyIdentityRepository()
        token_service = TokenService(repo)
        if token_service.validate_access_token("invalid_token") is not None:
            return (BootStatus.FAILED, "Invalid token unexpectedly validated")

        required_public = {
            "/v1/identity/household/create",
            "/v1/identity/bootstrap",
            "/v1/system/boot-status",
            "/v1/system/boot-probe",
            "/v1/system/health",
        }
        missing_public = sorted(required_public - _PUBLIC_PATHS)
        if missing_public:
            return (BootStatus.FAILED, f"Missing required PUBLIC_PATHS: {', '.join(missing_public)}")

        return (BootStatus.OK, None)
    except Exception as exc:
        return (BootStatus.FAILED, f"Auth middleware validation failed: {str(exc)}")


def validate_broadcaster() -> tuple[BootStatus, str | None]:
    """Validate broadcaster subsystem."""
    try:
        if not hasattr(broadcaster, "subscribe") or not hasattr(broadcaster, "publish"):
            return (BootStatus.FAILED, "Broadcaster missing required methods")
        if not hasattr(broadcaster, "_transport"):
            return (BootStatus.FAILED, "Broadcaster transport not initialized")
        return (BootStatus.OK, None)
    except Exception as exc:
        return (BootStatus.FAILED, f"Broadcaster validation failed: {str(exc)}")


def validate_repository_fresh_transaction() -> tuple[BootStatus, str | None]:
    """Validate repository operations against a fresh DB transaction."""
    try:
        session = SessionLocal()
        try:
            session.execute(text("SELECT COUNT(1) FROM households"))
            session.commit()
        finally:
            session.close()
        return (BootStatus.OK, None)
    except Exception as exc:
        return (BootStatus.FAILED, f"Fresh transaction failed: {str(exc)}")


def validate_sse_internal_probe() -> tuple[BootStatus, str | None]:
    """Lightweight SSE probe by opening an internal subscription and reading one frame."""

    async def _probe() -> str:
        probe_household = f"boot-probe-{uuid.uuid4().hex[:8]}"
        stream = broadcaster.subscribe(probe_household, last_watermark=None)
        try:
            chunk = await asyncio.wait_for(stream.__anext__(), timeout=1.5)
            return chunk
        finally:
            await stream.aclose()

    try:
        payload = asyncio.run(_probe())
        if "event: connected" not in payload:
            return (BootStatus.FAILED, "SSE probe did not receive connected event")
        return (BootStatus.OK, None)
    except Exception as exc:
        return (BootStatus.FAILED, f"SSE internal probe failed: {str(exc)}")


def run_boot_diagnostics() -> BootDiagnostics:
    """Run all boot diagnostics and return status report."""
    logger.info("Running boot diagnostics...")
    
    diags = BootDiagnostics(
        database=BootStatus.UNKNOWN,
        identity_repo=BootStatus.UNKNOWN,
        household_repo=BootStatus.UNKNOWN,
        token_service=BootStatus.UNKNOWN,
        auth_middleware=BootStatus.UNKNOWN,
        broadcaster=BootStatus.UNKNOWN,
        required_tables=list(REQUIRED_SCHEMA.keys()),
        required_columns=REQUIRED_SCHEMA,
    )
    
    # Validate database
    db_status, db_error, pool_status = validate_database(diags.required_columns or REQUIRED_SCHEMA)
    diags.database = db_status
    diags.database_error = db_error
    diags.pool_status = pool_status
    logger.info(f"Database: {db_status.value} {f'({db_error})' if db_error else ''}")
    
    # Skip other checks if database failed
    if db_status != BootStatus.OK:
        logger.error("Database validation failed, skipping other checks")
        diags.overall = BootStatus.FAILED
        return diags
    
    # Validate identity repository
    repo_status, repo_error = validate_identity_repository()
    diags.identity_repo = repo_status
    diags.identity_repo_error = repo_error
    logger.info(f"Identity Repository: {repo_status.value} {f'({repo_error})' if repo_error else ''}")
    
    # Validate household repository
    hh_status, hh_error = validate_household_repository()
    diags.household_repo = hh_status
    diags.household_repo_error = hh_error
    logger.info(f"Household Repository: {hh_status.value} {f'({hh_error})' if hh_error else ''}")
    
    # Validate token service
    ts_status, ts_error = validate_token_service()
    diags.token_service = ts_status
    diags.token_service_error = ts_error
    logger.info(f"Token Service: {ts_status.value} {f'({ts_error})' if ts_error else ''}")
    
    # Validate auth middleware
    am_status, am_error = validate_auth_middleware()
    diags.auth_middleware = am_status
    diags.auth_middleware_error = am_error
    logger.info(f"Auth Middleware: {am_status.value} {f'({am_error})' if am_error else ''}")
    
    # Validate broadcaster
    bc_status, bc_error = validate_broadcaster()
    diags.broadcaster = bc_status
    diags.broadcaster_error = bc_error
    logger.info(f"Broadcaster: {bc_status.value} {f'({bc_error})' if bc_error else ''}")
    
    # Calculate overall status
    all_ok = all([
        diags.database == BootStatus.OK,
        diags.identity_repo == BootStatus.OK,
        diags.household_repo == BootStatus.OK,
        diags.token_service == BootStatus.OK,
        diags.auth_middleware == BootStatus.OK,
        diags.broadcaster == BootStatus.OK,
    ])
    diags.overall = BootStatus.OK if all_ok else BootStatus.FAILED
    
    logger.info(f"Boot diagnostics complete: overall={diags.overall.value}")
    return diags


def assert_boot_invariants() -> BootDiagnostics:
    """Run diagnostics and raise RuntimeError when any invariant fails."""
    diags = run_boot_diagnostics()
    if diags.overall != BootStatus.OK:
        raise RuntimeError(f"boot_invariant_violation: {diags.to_dict()}")
    return diags


def run_boot_probe() -> dict[str, Any]:
    """
    Externalized live probe that re-validates boot path components on demand.

    This function intentionally avoids cached/in-memory assumptions by re-querying
    database state, re-instantiating auth/repository paths, and opening a fresh SSE probe.
    """
    diags = run_boot_diagnostics()
    repo_tx_status, repo_tx_error = validate_repository_fresh_transaction()
    sse_status, sse_error = validate_sse_internal_probe()

    probe_ok = (
        diags.overall == BootStatus.OK
        and repo_tx_status == BootStatus.OK
        and sse_status == BootStatus.OK
    )

    return {
        "overall": "ok" if probe_ok else "failed",
        "checked_live": True,
        "database": diags.database.value,
        "identity_repo": diags.identity_repo.value,
        "household_repo": diags.household_repo.value,
        "auth_middleware": diags.auth_middleware.value,
        "broadcaster": diags.broadcaster.value,
        "repository_fresh_transaction": repo_tx_status.value,
        "repository_fresh_transaction_error": repo_tx_error,
        "sse_internal_probe": sse_status.value,
        "sse_internal_probe_error": sse_error,
        "migration_mode": diags.migration_mode,
        "pool_status": diags.pool_status,
        "required_tables": diags.required_tables or [],
        "required_columns": diags.required_columns or {},
    }
