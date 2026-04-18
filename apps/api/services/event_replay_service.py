"""
Event replay service for deterministic event reprocessing.

Enables replaying past events from the audit log through the event bus.
Does NOT duplicate persistence (no log_system_event on replay).
"""

from __future__ import annotations

from apps.api.core.database import SessionLocal
from apps.api.core.event_registry import event_bus
from apps.api.models.event_log import EventLog
from apps.api.schemas.event import SystemEvent


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
            # Reconstruct SystemEvent from persisted audit log
            event = SystemEvent(
                household_id=log.household_id,
                type=log.type,
                source=log.source,
                payload=log.payload,
                severity=log.severity,
            )
            
            # Publish to event_bus (triggers handlers WITHOUT re-logging)
            results = event_bus.publish(event)
            
            if results:
                all_results.extend(results)
        
        return all_results
        
    finally:
        session.close()


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
            # Reconstruct SystemEvent from persisted audit log
            event = SystemEvent(
                household_id=log.household_id,
                type=log.type,
                source=log.source,
                payload=log.payload,
                severity=log.severity,
            )
            
            # Publish to event_bus (triggers handlers WITHOUT re-logging)
            results = event_bus.publish(event)
            
            if results:
                all_results.extend(results)
        
        return all_results
        
    finally:
        session.close()
