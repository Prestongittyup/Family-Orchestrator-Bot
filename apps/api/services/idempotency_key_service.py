from __future__ import annotations

from datetime import datetime
from sqlalchemy.exc import IntegrityError

from apps.api.core.database import SessionLocal
from apps.api.models.idempotency_key import IdempotencyKey
from apps.api.observability.metrics import metrics
from apps.api.observability.logging import log_event, log_error
from apps.api.observability.alerts import check_error_spike


def exists(key: str) -> bool:
    """Return True if the idempotency key is already present."""
    session = SessionLocal()
    try:
        return (
            session.query(IdempotencyKey.key)
            .filter(IdempotencyKey.key == key)
            .first()
            is not None
        )
    finally:
        session.close()


def record(key: str, household_id: str, event_type: str) -> None:
    """
    Persist an idempotency key.

    Duplicate keys are ignored safely and never raise to callers.
    """
    session = SessionLocal()
    try:
        session.add(
            IdempotencyKey(
                key=key,
                household_id=household_id,
                event_type=event_type,
            )
        )
        session.commit()
    except IntegrityError:
        session.rollback()
    finally:
        session.close()


def reserve(key: str, household_id: str, event_type: str) -> bool:
    """
    Attempt to reserve an idempotency key.

    Returns:
        True if key was newly reserved or has expired.
        False if key already exists and is not expired (duplicate request).
    """
    session = SessionLocal()
    try:
        existing = session.query(IdempotencyKey).filter(IdempotencyKey.key == key).first()

        if existing:
            if existing.expires_at <= datetime.utcnow():
                session.delete(existing)
                session.commit()
                session.add(
                    IdempotencyKey(
                        key=key,
                        household_id=household_id,
                        event_type=event_type,
                    )
                )
                session.commit()
                metrics.increment("idempotency_misses_total", household_id=household_id)
                log_event("idempotency_key_expired_reused", household_id=household_id,
                          event_type=event_type, key=key)
                return True
            else:
                metrics.increment("idempotency_hits_total", household_id=household_id)
                log_event("idempotency_duplicate_rejected", household_id=household_id,
                          event_type=event_type, key=key)
                return False
        else:
            session.add(
                IdempotencyKey(
                    key=key,
                    household_id=household_id,
                    event_type=event_type,
                )
            )
            session.commit()
            metrics.increment("idempotency_misses_total", household_id=household_id)
            log_event("idempotency_key_reserved", household_id=household_id,
                      event_type=event_type, key=key)
            return True
    except Exception as exc:
        session.rollback()
        metrics.increment("errors_total")
        check_error_spike()
        log_error("idempotency_reserve_failed", exc, household_id=household_id, key=key)
        raise
    finally:
        session.close()


def release(key: str) -> None:
    """Release a reserved key when request fails with 5xx so retries can proceed."""
    session = SessionLocal()
    try:
        session.query(IdempotencyKey).filter(IdempotencyKey.key == key).delete()
        session.commit()
    finally:
        session.close()


def cleanup_expired() -> int:
    """
    Remove expired idempotency keys.
    
    Returns:
        Number of keys deleted.
    """
    session = SessionLocal()
    try:
        count = session.query(IdempotencyKey).filter(
            IdempotencyKey.expires_at <= datetime.utcnow()
        ).delete()
        session.commit()
        return count
    finally:
        session.close()
