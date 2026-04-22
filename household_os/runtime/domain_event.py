"""
Domain Event Model - Immutable Events for Lifecycle State Sourcing

All domain events are immutable and represent facts about what has occurred
in the system. State is never stored directly; instead, it is derived by
replaying the sequence of events.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from household_os.core.lifecycle_state import LifecycleState

# Event type definitions for lifecycle domain
LIFECYCLE_EVENT_TYPES = {
    "ACTION_PROPOSED": "action_proposed",
    "ACTION_APPROVED": "action_approved",
    "ACTION_REJECTED": "action_rejected",
    "ACTION_COMMITTED": "action_committed",
    "ACTION_FAILED": "action_failed",
}


@dataclass(frozen=True)
class DomainEvent:
    """
    Immutable domain event.

    Represents a fact about something that occurred in the system.
    All fields are frozen to prevent mutation after creation.

    Fields:
        event_id: Unique identifier for this event
        aggregate_id: ID of the aggregate (action/task) this event belongs to
        event_type: Type of event (e.g., "action_proposed")
        timestamp: When the event occurred (UTC)
        payload: Optional structured data associated with the event
        metadata: Additional contextual information (request_id, user_id, etc.)
    """

    event_id: str
    aggregate_id: str
    event_type: str
    timestamp: datetime
    payload: dict[str, Any]
    metadata: dict[str, Any]

    @staticmethod
    def create(
        aggregate_id: str,
        event_type: str,
        timestamp: datetime | None = None,
        payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DomainEvent:
        """
        Factory method to create a new domain event.

        Args:
            aggregate_id: ID of the aggregate (e.g., action_id)
            event_type: Type of event (use LIFECYCLE_EVENT_TYPES)
            timestamp: When event occurred (defaults to now in UTC)
            payload: Event-specific data
            metadata: Contextual data (request_id, user_id, etc.)

        Returns:
            Immutable DomainEvent
        """
        resolved_payload = payload or {}
        payload_state = resolved_payload.get("state") if isinstance(resolved_payload, dict) else None
        if payload_state is not None and not isinstance(payload_state, LifecycleState):
            raise TypeError("DomainEvent payload 'state' must be LifecycleState")

        return DomainEvent(
            event_id=str(uuid.uuid4()),
            aggregate_id=aggregate_id,
            event_type=event_type,
            timestamp=timestamp or datetime.now(UTC),
            payload=resolved_payload,
            metadata=metadata or {},
        )

    def __hash__(self) -> int:
        """Events are hashable (immutable)."""
        return hash((self.event_id, self.aggregate_id, self.event_type))

    def __repr__(self) -> str:
        timestamp_str = self.timestamp.isoformat()
        return (
            f"DomainEvent("
            f"event_id={self.event_id[:8]}..., "
            f"aggregate_id={self.aggregate_id}, "
            f"event_type={self.event_type}, "
            f"timestamp={timestamp_str}"
            f")"
        )


# Lifecycle state alias uses the canonical enum.
LifecycleEventState = LifecycleState


@dataclass(frozen=True)
class LifecycleSnapshot:
    """
    Snapshot of lifecycle state at a point in time.

    Computed by replaying events; useful for caching or analysis.
    """

    aggregate_id: str
    current_state: LifecycleEventState
    last_event_id: str
    last_event_timestamp: datetime
    event_count: int

    def __repr__(self) -> str:
        return f"LifecycleSnapshot(aggregate_id={self.aggregate_id}, state={self.current_state})"
