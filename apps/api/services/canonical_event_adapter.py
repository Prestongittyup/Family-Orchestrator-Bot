from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from apps.api.schemas.canonical_event import CanonicalEventEnvelope
from apps.api.schemas.event import SystemEvent
from apps.api.realtime.event_bus import RealtimeEvent
from household_os.runtime.domain_event import DomainEvent


class CanonicalEventAdapter:
    @staticmethod
    def to_envelope(event: object) -> CanonicalEventEnvelope:
        if isinstance(event, CanonicalEventEnvelope):
            return event
        if isinstance(event, DomainEvent):
            return CanonicalEventAdapter.from_domain_event(event)
        if isinstance(event, SystemEvent):
            return CanonicalEventAdapter.from_system_event(event)
        if isinstance(event, RealtimeEvent):
            return CanonicalEventAdapter.from_realtime_event(event)
        raise TypeError(f"Unsupported event type for canonical conversion: {type(event)!r}")

    @staticmethod
    def from_domain_event(event: DomainEvent) -> CanonicalEventEnvelope:
        return CanonicalEventEnvelope(
            event_id=event.event_id,
            event_type=event.event_type,
            actor_type=str(event.metadata.get("actor_type")) if event.metadata.get("actor_type") else None,
            household_id=str(event.metadata.get("household_id") or event.aggregate_id),
            timestamp=event.timestamp,
            watermark=None,
            idempotency_key=str(event.metadata.get("request_id")) if event.metadata.get("request_id") else None,
            source=str(event.metadata.get("source") or "domain_event"),
            severity=str(event.metadata.get("severity")) if event.metadata.get("severity") else None,
            payload=dict(event.payload),
            signature=event.signature,
        )

    @staticmethod
    def from_system_event(event: SystemEvent) -> CanonicalEventEnvelope:
        return CanonicalEventEnvelope(
            event_id=event.event_id,
            event_type=event.type,
            actor_type=event.actor_type,
            household_id=event.household_id,
            timestamp=event.timestamp or datetime.now(UTC),
            watermark=event.watermark,
            idempotency_key=event.idempotency_key,
            source=event.source,
            severity=event.severity,
            payload=dict(event.payload or {}),
            signature=event.signature,
        )

    @staticmethod
    def from_realtime_event(event: RealtimeEvent) -> CanonicalEventEnvelope:
        return CanonicalEventEnvelope(
            event_id=event.event_id,
            event_type=event.event_type,
            actor_type=event.actor_type,
            household_id=event.household_id,
            timestamp=event.timestamp,
            watermark=event.watermark,
            idempotency_key=event.idempotency_key,
            source=event.source,
            severity=event.severity,
            payload=dict(event.payload or {}),
            signature=event.signature,
        )

    @staticmethod
    def to_system_event(envelope: CanonicalEventEnvelope) -> SystemEvent:
        return SystemEvent(
            event_id=envelope.event_id,
            household_id=envelope.household_id,
            type=envelope.event_type,
            source=envelope.source,
            payload=dict(envelope.payload),
            severity=envelope.severity or "info",
            timestamp=envelope.timestamp,
            idempotency_key=envelope.idempotency_key,
            actor_type=envelope.actor_type,
            watermark=envelope.watermark,
            signature=envelope.signature,
        )
