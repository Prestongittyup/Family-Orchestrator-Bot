"""Pure persistence and audit layer for canonical event logging."""

from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping
from uuid import uuid4

from app.adapters.db.event_log_repository import EventLogQuery, EventLogRepository
from app.adapters.db.models.event_log import EventLog


def internal_only(func):
    return func


@dataclass(frozen=True)
class SystemEventRecord:
    household_id: str
    type: str
    source: str
    payload: dict[str, Any]
    event_id: str | None = None
    user_id: str | None = None
    timestamp: datetime | None = None
    severity: str = "info"
    version: int = 1
    idempotency_key: str | None = None


class EventLogService:
    def __init__(self, *, repository: EventLogRepository | None = None) -> None:
        self._repository = repository or EventLogRepository()

    @internal_only
    def log_system_event(self, event: SystemEventRecord | Mapping[str, Any]) -> EventLog:
        payload = _event_to_payload(event)
        persisted_payload = dict(payload.get("payload") or {})
        persisted_payload.setdefault("event_id", payload["event_id"])
        persisted_payload.setdefault("source", payload["source"])
        persisted_payload.setdefault("severity", payload["severity"])
        if payload.get("idempotency_key") is not None:
            persisted_payload.setdefault("idempotency_key", payload["idempotency_key"])

        return self._repository.append_event(
            event_id=payload["event_id"],
            user_id=payload["user_id"],
            household_id=payload["household_id"],
            event_type=payload["type"],
            timestamp=payload.get("timestamp"),
            source=payload["source"],
            payload=persisted_payload,
            version=int(payload.get("version") or 1),
            severity=payload.get("severity") or "info",
            idempotency_key=payload.get("idempotency_key"),
        )

    def get_event_logs(
        self,
        *,
        household_id: str,
        user_id: str | None = None,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[EventLog]:
        return self._repository.list_events(
            query=EventLogQuery(
                household_id=household_id,
                user_id=user_id,
                event_type=event_type,
                limit=limit,
            )
        )

    def idempotency_key_exists(self, key: str) -> bool:
        return self._repository.idempotency_key_exists(key)


_service = EventLogService()


@internal_only
def log_system_event(event: SystemEventRecord | Mapping[str, Any]) -> EventLog:
    return _service.log_system_event(event)


def get_event_logs(
    household_id: str,
    event_type: str | None = None,
    *,
    user_id: str | None = None,
    limit: int = 100,
) -> list[EventLog]:
    return _service.get_event_logs(
        household_id=household_id,
        user_id=user_id,
        event_type=event_type,
        limit=limit,
    )


def idempotency_key_exists(key: str) -> bool:
    return _service.idempotency_key_exists(key)


def _event_to_payload(event: SystemEventRecord | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(event, SystemEventRecord):
        payload = {
            "event_id": event.event_id,
            "user_id": event.user_id,
            "household_id": event.household_id,
            "type": event.type,
            "timestamp": event.timestamp,
            "source": event.source,
            "payload": event.payload,
            "version": event.version,
            "severity": event.severity,
            "idempotency_key": event.idempotency_key,
        }
    else:
        payload = dict(event)

    payload["event_id"] = str(payload.get("event_id") or uuid4())
    payload["user_id"] = str(payload.get("user_id") or payload.get("actor_id") or "system")
    payload["household_id"] = str(payload.get("household_id") or "")
    payload["type"] = str(payload.get("type") or payload.get("event_type") or "")
    payload["source"] = str(payload.get("source") or "unknown")
    payload["severity"] = str(payload.get("severity") or "info")
    payload["version"] = int(payload.get("version") or 1)
    payload["payload"] = dict(payload.get("payload") or {})
    return payload


__all__ = [
    "EventLogService",
    "SystemEventRecord",
    "get_event_logs",
    "idempotency_key_exists",
    "internal_only",
    "log_system_event",
]
