"""
Event replay service for deterministic event reprocessing.

Enables replaying past events from the audit log through the event bus.
Does NOT duplicate persistence (no log_system_event on replay).
"""

from __future__ import annotations

from apps.api.core.database import SessionLocal
from apps.api.models.event_log import EventLog
from apps.api.observability.execution_trace import trace_function
from apps.api.schemas.event import SystemEvent
from apps.api.services.canonical_event_adapter import CanonicalEventAdapter
from apps.api.services.canonical_event_router import canonical_event_router
from apps.api.schemas.canonical_event import is_registered_event_type


def _validate_replay_envelope(envelope, persisted_idempotency_key: str | None) -> None:
    if not is_registered_event_type(envelope.event_type):
        raise ValueError(f"Replay rejected for unregistered event_type: {envelope.event_type}")
    if persisted_idempotency_key != envelope.idempotency_key:
        raise ValueError(
            "Replay rejected due to idempotency_key mismatch between persisted log and envelope"
        )
    if envelope.signature:
        # Canonical replay currently validates signature presence and format provenance.
        if not isinstance(envelope.signature, str) or len(envelope.signature) < 16:
            raise ValueError("Replay rejected due to invalid signature format")


def _route_replay_log_entry(log: EventLog) -> object | None:
    event = SystemEvent(
        household_id=log.household_id,
        event_id=log.id,
        type=log.type,
        source=log.source,
        payload=log.payload,
        severity=log.severity,
        idempotency_key=log.idempotency_key,
        signature=(log.payload.get("signature") if isinstance(log.payload, dict) else None),
    )
    envelope = CanonicalEventAdapter.to_envelope(event)
    _validate_replay_envelope(envelope, log.idempotency_key)
    # STRICT INVARIANCE: Replay is transport-agnostic.
    # Broadcaster is sole authority for SSE emission.
    # Replay re-enters canonical pipeline like all events (Adapter→Router→Broadcaster).
    return canonical_event_router.route(
        envelope,
        persist=False,
        dispatch=True,
    )


@trace_function(entrypoint="event_replay.global", actor_type="system_worker", source="event_replay")
def replay_events(limit: int = 10) -> list[object]:
    """
    Replay the most recent EventLog entries through the event bus.
    
    Reconstructs SystemEvent objects from persisted logs and re-publishes them
    to handlers without duplicating audit log entries.
    
    Args:
        limit: Number of recent events to replay (default: 10)
        
    Returns:
        List of aggregated results from all replayed event handlers
    """
    session = SessionLocal()
    try:
        # Fetch most recent events, oldest first (for chronological replay)
        logs = (
            session.query(EventLog)
            .order_by(EventLog.created_at.asc())
            .limit(limit)
            .all()
        )
        
        all_results: list[object] = []
        
        for log in logs:
            results = _route_replay_log_entry(log)
            
            if results:
                all_results.extend(results)
        
        return all_results
        
    finally:
        session.close()


@trace_function(entrypoint="event_replay.household", actor_type="system_worker", source="event_replay")
def replay_events_for_household(
    household_id: str,
    event_type: str | None = None,
    limit: int = 10,
) -> list[object]:
    """
    Replay events for a specific household.
    
    Args:
        household_id: The household to replay events for
        event_type: Optional event type filter
        limit: Maximum number of events to replay (default: 10)
        
    Returns:
        List of aggregated results from replayed event handlers
    """
    session = SessionLocal()
    try:
        query = session.query(EventLog).filter(EventLog.household_id == household_id)
        
        if event_type:
            query = query.filter(EventLog.type == event_type)
        
        # Fetch oldest first for chronological replay
        logs = query.order_by(EventLog.created_at.asc()).limit(limit).all()
        
        all_results: list[object] = []
        
        for log in logs:
            results = _route_replay_log_entry(log)
            
            if results:
                all_results.extend(results)
        
        return all_results
        
    finally:
        session.close()
