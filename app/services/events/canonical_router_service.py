from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Iterable

from app.adapters.db.event_log_repository import EventLogRepository
from household_os.runtime.event_router import CanonicalEventEnvelope, CanonicalEventRouter


class NoopEventDispatcher:
    def publish(self, envelope: CanonicalEventEnvelope) -> dict[str, Any]:
        return {
            "status": "dispatched",
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
        }


@dataclass(frozen=True)
class CanonicalRouterResult:
    status: str
    event_id: str
    dispatched: bool


class CanonicalRouterService:
    def __init__(
        self,
        *,
        event_log_repository: EventLogRepository | None = None,
        dispatcher: NoopEventDispatcher | None = None,
        registered_event_types: Iterable[str] | None = None,
    ) -> None:
        self._registered_event_types = set(registered_event_types or [])
        self._router = CanonicalEventRouter(
            event_log_writer=event_log_repository or EventLogRepository(),
            dispatcher=dispatcher or NoopEventDispatcher(),
        )

    def route(
        self,
        envelope: CanonicalEventEnvelope | dict[str, Any],
        *,
        persist: bool = True,
        dispatch: bool = True,
    ) -> object | None:
        normalized = self._normalize_envelope(envelope)
        if self._registered_event_types and normalized.event_type not in self._registered_event_types:
            raise ValueError(f"Unregistered event_type: {normalized.event_type}")
        return self._router.route(normalized, persist=persist, dispatch=dispatch)

    def _normalize_envelope(self, envelope: CanonicalEventEnvelope | dict[str, Any]) -> CanonicalEventEnvelope:
        if isinstance(envelope, CanonicalEventEnvelope):
            return envelope

        payload = dict(envelope)
        return CanonicalEventEnvelope(
            event_id=str(payload.get("event_id") or payload.get("id") or ""),
            event_type=str(payload.get("event_type") or payload.get("type") or ""),
            user_id=(str(payload.get("user_id")) if payload.get("user_id") is not None else None),
            household_id=str(payload.get("household_id") or ""),
            timestamp=payload.get("timestamp") or datetime.now(UTC),
            source=str(payload.get("source") or "unknown"),
            payload=dict(payload.get("payload") or {}),
            version=int(payload.get("version") or 1),
            severity=(str(payload.get("severity")) if payload.get("severity") else None),
            idempotency_key=(str(payload.get("idempotency_key")) if payload.get("idempotency_key") else None),
            actor_type=(str(payload.get("actor_type")) if payload.get("actor_type") else None),
            watermark=(int(payload.get("watermark")) if payload.get("watermark") is not None else None),
            signature=(str(payload.get("signature")) if payload.get("signature") else None),
        )


canonical_router_service = CanonicalRouterService()


__all__ = [
    "CanonicalRouterResult",
    "CanonicalRouterService",
    "NoopEventDispatcher",
    "canonical_router_service",
]
