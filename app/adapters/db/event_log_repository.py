from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from app.adapters.db.models.event_log import EventLog
from app.adapters.db.session_factory import Base, SessionLocal, engine


class CanonicalEnvelopeLike(Protocol):
    event_id: str
    event_type: str
    user_id: str | None
    household_id: str
    timestamp: datetime
    source: str
    payload: dict[str, Any]
    version: int
    severity: str | None
    idempotency_key: str | None


@dataclass(frozen=True)
class EventLogQuery:
    household_id: str
    user_id: str | None = None
    event_type: str | None = None
    limit: int = 100


class EventLogRepository:
    _schema_ready = False

    def _ensure_schema(self) -> None:
        if self.__class__._schema_ready:
            return
        Base.metadata.create_all(bind=engine)
        self.__class__._schema_ready = True

    def append_event(
        self,
        *,
        event_id: str,
        user_id: str,
        household_id: str,
        event_type: str,
        timestamp: datetime | None,
        source: str,
        payload: Mapping[str, Any],
        version: int = 1,
        severity: str = "info",
        idempotency_key: str | None = None,
    ) -> EventLog:
        self._ensure_schema()
        session = SessionLocal()
        try:
            row = EventLog(
                event_id=event_id,
                user_id=user_id,
                household_id=household_id,
                type=event_type,
                timestamp=timestamp or datetime.now(UTC),
                source=source,
                payload=dict(payload),
                version=max(1, int(version)),
                severity=severity or "info",
                idempotency_key=idempotency_key,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            session.expunge(row)
            return row
        finally:
            session.close()

    def append_envelope(self, envelope: CanonicalEnvelopeLike) -> EventLog:
        resolved_user_id = envelope.user_id or str(envelope.payload.get("user_id") or "system")
        return self.append_event(
            event_id=envelope.event_id,
            user_id=resolved_user_id,
            household_id=envelope.household_id,
            event_type=envelope.event_type,
            timestamp=envelope.timestamp,
            source=envelope.source,
            payload=envelope.payload,
            version=envelope.version,
            severity=envelope.severity or "info",
            idempotency_key=envelope.idempotency_key,
        )

    def idempotency_key_exists(self, key: str) -> bool:
        self._ensure_schema()
        session = SessionLocal()
        try:
            return session.query(EventLog.event_id).filter(EventLog.idempotency_key == key).first() is not None
        finally:
            session.close()

    def list_events(self, *, query: EventLogQuery) -> list[EventLog]:
        self._ensure_schema()
        session = SessionLocal()
        try:
            statement = session.query(EventLog).filter(EventLog.household_id == query.household_id)
            if query.user_id:
                statement = statement.filter(EventLog.user_id == query.user_id)
            if query.event_type:
                statement = statement.filter(EventLog.type == query.event_type)

            rows = statement.order_by(EventLog.timestamp.desc()).limit(max(1, query.limit)).all()
            for row in rows:
                session.expunge(row)
            return rows
        finally:
            session.close()


__all__ = ["EventLogRepository", "EventLogQuery"]
