"""
Pure persistence and audit layer for event logging.

This service is responsible ONLY for:
- Writing events to the EventLog table
- Retrieving events for audit/replay

It does NOT trigger business logic, call event bus, or invoke handlers.
"""

from __future__ import annotations

from uuid import uuid4

from apps.api.core.database import SessionLocal
from apps.api.models.event_log import EventLog
from apps.api.schemas.event import SystemEvent


def internal_only(func):
    """Marker decorator for internal-only mutations excluded from router.emit enforcement."""
    return func


@internal_only
def log_system_event(event: SystemEvent) -> EventLog:
    """
    Persist an incoming SystemEvent to the event log.
    
    Internal-only mutation for Event logging.
    Pure persistence: writes to DB and returns record.
    No side effects, no business logic.
    
    Args:
        event: The SystemEvent to audit log
        
    Returns:
        The persisted EventLog record
    """
    session = SessionLocal()
    try:
        persisted_payload = dict(event.payload or {})
        persisted_payload.setdefault("event_id", event.event_id)
        persisted_payload.setdefault("source", event.source)
        persisted_payload.setdefault("severity", event.severity)
        if event.idempotency_key is not None:
            persisted_payload.setdefault("idempotency_key", event.idempotency_key)
        if event.signature is not None:
            persisted_payload.setdefault("signature", event.signature)

        entry = EventLog(
            id=event.event_id or str(uuid4()),
            household_id=event.household_id,
            type=event.type,
            source=event.source,
            payload=persisted_payload,
            severity=event.severity,
            idempotency_key=event.idempotency_key,
        )

        session.add(entry)
        session.commit()
        session.refresh(entry)

        return entry

    finally:
        session.close()


def get_event_logs(
    household_id: str, 
    event_type: str | None = None, 
    limit: int = 100
) -> list[EventLog]:
    """
    Retrieve event logs for a household, optionally filtered by type.
    
    Pure read: retrieves events for audit and replay.
    
    Args:
        household_id: The household ID to retrieve logs for
        event_type: Optional event type filter
        limit: Maximum number of records to return (default: 100)
        
    Returns:
        List of EventLog records, most recent first
    """
    session = SessionLocal()
    try:
        query = session.query(EventLog).filter(EventLog.household_id == household_id)
        
        if event_type:
            query = query.filter(EventLog.type == event_type)
        
        logs = query.order_by(EventLog.created_at.desc()).limit(limit).all()
        
        # Detach from session
        for log in logs:
            session.expunge(log)
        
        return logs
        
    finally:
        session.close()


def idempotency_key_exists(key: str) -> bool:
    """
    Check whether an idempotency key has already been persisted.
    """
    session = SessionLocal()
    try:
        return (
            session.query(EventLog.id)
            .filter(EventLog.idempotency_key == key)
            .first()
            is not None
        )
    finally:
        session.close()
