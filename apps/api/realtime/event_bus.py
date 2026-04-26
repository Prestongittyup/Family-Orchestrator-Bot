from __future__ import annotations

import json
import threading
from uuid import uuid4
from typing import Any, Callable

from apps.api.realtime.transport_event import RealtimeEvent

class RealtimeEventBus:
    def publish(self, event: RealtimeEvent) -> None:
        raise NotImplementedError

    def subscribe_all(self, handler: Callable[[RealtimeEvent], None]) -> None:
        raise NotImplementedError


class InMemoryRealtimeEventBus(RealtimeEventBus):
    def __init__(self) -> None:
        self._handlers: list[Callable[[RealtimeEvent], None]] = []

    def publish(self, event: RealtimeEvent) -> None:
        for handler in list(self._handlers):
            handler(event)

    def subscribe_all(self, handler: Callable[[RealtimeEvent], None]) -> None:
        self._handlers.append(handler)


class RedisRealtimeEventBus(RealtimeEventBus):
    CHANNEL_PREFIX = "hpal:realtime:household:"

    def __init__(self, url: str) -> None:
        self._handlers: list[Callable[[RealtimeEvent], None]] = []
        self._enabled = False
        self._client = None
        self._pubsub = None
        try:
            import redis  # type: ignore

            self._client = redis.from_url(url, decode_responses=True)
            self._pubsub = self._client.pubsub(ignore_subscribe_messages=True)
            self._pubsub.psubscribe(f"{self.CHANNEL_PREFIX}*")
            self._enabled = True
            thread = threading.Thread(target=self._listen_loop, daemon=True)
            thread.start()
        except Exception:
            self._enabled = False

    @property
    def enabled(self) -> bool:
        return self._enabled

    def publish(self, event: RealtimeEvent) -> None:
        if not self._enabled or self._client is None:
            return
        # Loop prevention lives in transport ingress/egress, not router.
        if event.source == "redis_distributed_transport":
            return
        channel = f"{self.CHANNEL_PREFIX}{event.household_id}"
        body = json.dumps(
            {
                "event_id": event.event_id,
                "actor_type": event.actor_type,
                "household_id": event.household_id,
                "event_type": event.event_type,
                "timestamp": event.timestamp.isoformat(),
                "watermark": event.watermark,
                "idempotency_key": event.idempotency_key,
                "source": event.source,
                "severity": event.severity,
                "payload": event.payload,
                "signature": event.signature,
            },
            sort_keys=True,
        )
        self._client.publish(channel, body)

    def subscribe_all(self, handler: Callable[[RealtimeEvent], None]) -> None:
        self._handlers.append(handler)

    def _listen_loop(self) -> None:
        """STRICT INVARIANCE: All events must re-enter canonical pipeline.
        
        Redis distributed events are reconstructed as SystemEvent,
        validated through CanonicalEventAdapter, and routed through
        CanonicalEventRouter with loop prevention handled in this transport.
        """
        if not self._enabled or self._pubsub is None:
            return
        
        # Deferred imports to avoid circular dependencies
        from apps.api.schemas.event import SystemEvent
        from apps.api.services.canonical_event_adapter import CanonicalEventAdapter
        from apps.api.services.canonical_event_router import canonical_event_router
        
        for msg in self._pubsub.listen():
            if not isinstance(msg, dict):
                continue
            raw = msg.get("data")
            if not isinstance(raw, str):
                continue
            try:
                parsed = json.loads(raw)
                
                # STRICT INVARIANCE: Reconstruct SystemEvent from Redis JSON
                system_event = SystemEvent(
                    household_id=str(parsed.get("household_id", "")),
                    event_id=str(parsed.get("event_id") or str(uuid4())),
                    type=str(parsed.get("event_type", "update")),
                    source="redis_distributed_transport",
                    payload=dict(parsed.get("payload", {})),
                    severity=(str(parsed.get("severity")) if parsed.get("severity") is not None else None),
                    idempotency_key=(
                        str(parsed.get("idempotency_key")) if parsed.get("idempotency_key") is not None else None
                    ),
                    signature=(str(parsed.get("signature")) if parsed.get("signature") is not None else None),
                    actor_type=(str(parsed.get("actor_type")) if parsed.get("actor_type") is not None else None),
                )
                
                # STRICT INVARIANCE: Re-enter canonical pipeline (Adapter → Router)
                envelope = CanonicalEventAdapter.to_envelope(system_event)
                
                # Re-enter canonical pipeline (persist disabled to avoid log duplication).
                canonical_event_router.route(
                    envelope,
                    persist=False,
                    dispatch=True,
                )
            except Exception:
                continue
