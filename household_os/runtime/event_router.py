from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4


@dataclass(frozen=True)
class CanonicalEventEnvelope:
    event_type: str
    user_id: str | None
    household_id: str
    source: str
    payload: dict[str, Any]
    event_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    version: int = 1
    severity: str | None = None
    idempotency_key: str | None = None
    actor_type: str | None = None
    watermark: int | None = None
    signature: str | None = None


class EventLogWriter(Protocol):
    def idempotency_key_exists(self, key: str) -> bool:
        ...

    def append_envelope(self, envelope: CanonicalEventEnvelope):
        ...


class EventDispatcher(Protocol):
    def publish(self, envelope: CanonicalEventEnvelope) -> object | None:
        ...


class CanonicalEventRouter:
    def __init__(
        self,
        *,
        event_log_writer: EventLogWriter,
        dispatcher: EventDispatcher | None = None,
    ) -> None:
        self._event_log_writer = event_log_writer
        self._dispatcher = dispatcher

    def route(
        self,
        envelope: CanonicalEventEnvelope,
        *,
        persist: bool = True,
        dispatch: bool = True,
    ) -> object | None:
        self._validate_envelope(envelope)

        if persist and envelope.idempotency_key:
            if self._event_log_writer.idempotency_key_exists(envelope.idempotency_key):
                return {
                    "status": "duplicate",
                    "event_id": envelope.event_id,
                    "idempotency_key": envelope.idempotency_key,
                }

        if persist:
            self._event_log_writer.append_envelope(envelope)

        if dispatch and self._dispatcher is not None:
            return self._dispatcher.publish(envelope)

        return None

    def _validate_envelope(self, envelope: CanonicalEventEnvelope) -> None:
        if not envelope.event_id.strip():
            raise ValueError("event_id is required")
        if not envelope.event_type.strip():
            raise ValueError("event_type is required")
        if not envelope.household_id.strip():
            raise ValueError("household_id is required")
        if not envelope.source.strip():
            raise ValueError("source is required")
        if not isinstance(envelope.payload, dict):
            raise ValueError("payload must be a dict")
        if envelope.version < 1:
            raise ValueError("version must be >= 1")


__all__ = ["CanonicalEventEnvelope", "CanonicalEventRouter", "EventDispatcher", "EventLogWriter"]
