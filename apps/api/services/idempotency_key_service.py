from __future__ import annotations

from sqlalchemy.exc import IntegrityError

from apps.api.core.database import SessionLocal
from apps.api.models.idempotency_key import IdempotencyKey


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
