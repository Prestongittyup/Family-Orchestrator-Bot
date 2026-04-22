"""
Event Store - Append-Only, Immutable Event Log

Provides an abstraction for storing domain events with the guarantee that:
- Events are NEVER modified or deleted
- Events are appended sequentially
- Order is strictly preserved
- The store is the single source of truth for lifecycle state
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

from household_os.core.lifecycle_state import LifecycleState
from household_os.runtime.domain_event import DomainEvent


class EventStoreError(Exception):
    """Base exception for event store operations."""

    pass


class AggregateNotFoundError(EventStoreError):
    """Raised when an aggregate has no events."""

    pass


class EventStore(ABC):
    """
    Abstract base class for event stores.

    Guarantees:
    - Append-only: events can only be added, never modified or deleted
    - Deterministic replay: the same events always produce the same state
    - Ordered: events are stored and retrieved in their insertion order
    """

    @abstractmethod
    def append(self, event: DomainEvent) -> None:
        """
        Append an event to the store.

        This is the ONLY way to write to the event store.
        Once appended, an event can never be modified or deleted.

        Args:
            event: Immutable DomainEvent to append

        Raises:
            EventStoreError: If append fails (e.g., storage backend error)
        """
        ...

    @abstractmethod
    def get_events(self, aggregate_id: str) -> list[DomainEvent]:
        """
        Retrieve all events for an aggregate, in order.

        Args:
            aggregate_id: ID of the aggregate (e.g., action_id)

        Returns:
            List of events in insertion order (earliest first)

        Raises:
            AggregateNotFoundError: If no events exist for this aggregate_id
        """
        ...

    @abstractmethod
    def get_events_since(self, aggregate_id: str, timestamp: datetime) -> list[DomainEvent]:
        """
        Retrieve events for an aggregate since a specific timestamp.

        Args:
            aggregate_id: ID of the aggregate
            timestamp: Retrieve events after this time (exclusive)

        Returns:
            List of events after timestamp, in order

        Raises:
            AggregateNotFoundError: If no events exist for this aggregate_id
        """
        ...

    @abstractmethod
    def get_all_aggregates(self) -> list[str]:
        """
        Get list of all aggregate IDs in the store.

        Useful for batch operations or scanning.

        Returns:
            List of aggregate IDs
        """
        ...

    @abstractmethod
    def clear(self) -> None:
        """Clear all events from store (for testing only)."""
        ...


class InMemoryEventStore(EventStore):
    """
    In-memory event store implementation.

    Suitable for:
    - Unit tests
    - Development
    - Temporary event sourcing during request lifecycle

    Not suitable for:
    - Production persistent storage
    - Recovery after process restart

    Thread-safety: NOT guaranteed in current implementation.
    For threaded access, wrap with locks.
    """

    def __init__(self) -> None:
        """Initialize empty in-memory store."""
        # aggregate_id -> list of events (always append-only)
        self._events: dict[str, list[DomainEvent]] = {}

    def append(self, event: DomainEvent) -> None:
        """
        Append an event to the store.

        Args:
            event: Immutable DomainEvent

        Raises:
            EventStoreError: If event_id already exists (duplicate)
        """
        aggregate_id = event.aggregate_id

        payload_state = event.payload.get("state") if isinstance(event.payload, dict) else None
        if payload_state is not None and not isinstance(payload_state, LifecycleState):
            raise EventStoreError("event.payload['state'] must be LifecycleState")

        if aggregate_id not in self._events:
            self._events[aggregate_id] = []

        # Prevent duplicate event IDs (safety check)
        existing_ids = {e.event_id for e in self._events[aggregate_id]}
        if event.event_id in existing_ids:
            raise EventStoreError(
                f"Duplicate event_id {event.event_id} for aggregate {aggregate_id}"
            )

        # Append only - never modify
        self._events[aggregate_id].append(event)

    def get_events(self, aggregate_id: str) -> list[DomainEvent]:
        """
        Retrieve all events for an aggregate.

        Args:
            aggregate_id: ID of the aggregate

        Returns:
            List of events in insertion order

        Raises:
            AggregateNotFoundError: If aggregate has no events
        """
        if aggregate_id not in self._events or not self._events[aggregate_id]:
            raise AggregateNotFoundError(f"No events found for aggregate {aggregate_id}")
        # Return a copy to prevent external modification
        return list(self._events[aggregate_id])

    def get_events_since(self, aggregate_id: str, timestamp: datetime) -> list[DomainEvent]:
        """
        Retrieve events for an aggregate since a specific timestamp.

        Args:
            aggregate_id: ID of the aggregate
            timestamp: Retrieve events after this time (exclusive)

        Returns:
            List of events after timestamp, in order

        Raises:
            AggregateNotFoundError: If aggregate has no events
        """
        all_events = self.get_events(aggregate_id)
        return [e for e in all_events if e.timestamp > timestamp]

    def get_all_aggregates(self) -> list[str]:
        """Get all aggregate IDs in the store."""
        return list(self._events.keys())

    def clear(self) -> None:
        """Clear all events (for testing)."""
        self._events.clear()

    def __len__(self) -> int:
        """Total event count across all aggregates."""
        return sum(len(events) for events in self._events.values())

    def __repr__(self) -> str:
        total_events = sum(len(events) for events in self._events.values())
        aggregate_count = len(self._events)
        return (
            f"InMemoryEventStore("
            f"aggregates={aggregate_count}, "
            f"total_events={total_events}"
            f")"
        )


class EventStoreFactory:
    """
    Factory for creating event store instances.

    Allows easy switching between implementations (in-memory, database, etc.)
    """

    _store: EventStore | None = None

    @classmethod
    def create_in_memory(cls) -> EventStore:
        """Create a new in-memory event store."""
        return InMemoryEventStore()

    @classmethod
    def get_singleton(cls) -> EventStore:
        """Get or create a singleton event store instance."""
        if cls._store is None:
            cls._store = cls.create_in_memory()
        return cls._store

    @classmethod
    def set_store(cls, store: EventStore) -> None:
        """Set a specific event store instance (for testing/DI)."""
        cls._store = store

    @classmethod
    def reset(cls) -> None:
        """Reset singleton (for testing)."""
        cls._store = None
