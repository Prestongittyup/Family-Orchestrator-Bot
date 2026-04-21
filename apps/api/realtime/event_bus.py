from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class RealtimeEvent:
    household_id: str
    event_type: str
    watermark: str
    payload: dict[str, Any]


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
        channel = f"{self.CHANNEL_PREFIX}{event.household_id}"
        body = json.dumps(
            {
                "household_id": event.household_id,
                "event_type": event.event_type,
                "watermark": event.watermark,
                "payload": event.payload,
            },
            sort_keys=True,
        )
        self._client.publish(channel, body)

    def subscribe_all(self, handler: Callable[[RealtimeEvent], None]) -> None:
        self._handlers.append(handler)

    def _listen_loop(self) -> None:
        if not self._enabled or self._pubsub is None:
            return
        for msg in self._pubsub.listen():
            if not isinstance(msg, dict):
                continue
            raw = msg.get("data")
            if not isinstance(raw, str):
                continue
            try:
                parsed = json.loads(raw)
                event = RealtimeEvent(
                    household_id=str(parsed.get("household_id", "")),
                    event_type=str(parsed.get("event_type", "update")),
                    watermark=str(parsed.get("watermark", "")),
                    payload=dict(parsed.get("payload", {})),
                )
                for handler in list(self._handlers):
                    handler(event)
            except Exception:
                continue
