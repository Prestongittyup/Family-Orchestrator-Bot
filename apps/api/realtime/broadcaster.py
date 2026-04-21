"""
Distributed household-scoped realtime broadcaster.

SSE API remains unchanged, but transport is pluggable:
    - Redis Pub/Sub (multi-instance safe)
    - In-memory fallback (single-instance development)

Includes:
    - Atomic watermark generation (no race condition)
    - Per-household event ring buffer for replay
    - last_watermark support for resumable streams
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from threading import Lock
from collections import defaultdict, deque
from typing import Any, AsyncIterator

from apps.api.observability.metrics import metrics, timer
from apps.api.observability.logging import log_event, log_error
from apps.api.observability.alerts import (
    check_resync_spike,
    signal_watermark_collision,
    signal_replay_gap,
    signal_duplicate_emission,
)
from apps.api.realtime.event_bus import (
    InMemoryRealtimeEventBus,
    RedisRealtimeEventBus,
    RealtimeEvent,
    RealtimeEventBus,
)


class AtomicCounter:
    """Thread-safe atomic counter for watermark generation."""
    
    def __init__(self) -> None:
        self._value = 0
        self._lock = Lock()  # Single lock covers all access
    
    def increment_and_get(self) -> int:
        """Atomically increment and return the new value."""
        with self._lock:
            self._value += 1
            return self._value


class HouseholdBroadcaster:
    # Ring buffer size per household (max events to replay on reconnect)
    RING_BUFFER_SIZE = 1000
    
    def __init__(self) -> None:
        self._subscribers: dict[str, set[asyncio.Queue[RealtimeEvent]]] = defaultdict(set)
        self._counter = AtomicCounter()  # Single atomic counter (no race condition)
        self._ring_buffers: dict[str, deque[RealtimeEvent]] = defaultdict(
            lambda: deque(maxlen=self.RING_BUFFER_SIZE)
        )
        # Sliding window set for watermark collision detection
        self._emitted_watermarks: set[str] = set()

        redis_url = os.getenv("REDIS_URL", "").strip()
        transport: RealtimeEventBus
        if redis_url:
            redis_transport = RedisRealtimeEventBus(redis_url)
            transport = redis_transport if redis_transport.enabled else InMemoryRealtimeEventBus()
        else:
            transport = InMemoryRealtimeEventBus()
        self._transport = transport
        self._transport.subscribe_all(self._fanout_local)

    async def publish(self, household_id: str, event_type: str, payload: dict[str, Any]) -> None:
        from apps.api.observability.safety import safety
        if safety.pause_writes:
            log_error("publish_blocked_pause_writes", "pause_writes safety control active",
                      household_id=household_id, event_type=event_type)
            return

        with timer("broadcast_latency_ms"):
            # Collision detection: record watermark before and after to detect counter race
            counter_value = self._counter.increment_and_get()
            watermark = f"{int(time.time() * 1000)}-{counter_value}"

            # Watermark collision check (should never fire with AtomicCounter)
            if watermark in self._emitted_watermarks:
                signal_watermark_collision(watermark, household_id)
            self._emitted_watermarks.add(watermark)

            event = RealtimeEvent(
                household_id=household_id,
                event_type=event_type,
                watermark=watermark,
                payload=payload,
            )

            self._ring_buffers[household_id].append(event)
            self._transport.publish(event)

        metrics.increment("events_broadcast_total", household_id=household_id)
        log_event("event_broadcast", household_id=household_id,
                  watermark=watermark, event_type=event_type)

    def publish_sync(self, household_id: str, event_type: str, payload: dict[str, Any]) -> None:
        """Thread-safe sync publish for service-layer code paths."""
        from apps.api.observability.safety import safety
        if safety.pause_writes:
            log_error("publish_sync_blocked_pause_writes", "pause_writes safety control active",
                      household_id=household_id, event_type=event_type)
            return

        start = time.perf_counter()
        counter_value = self._counter.increment_and_get()
        watermark = f"{int(time.time() * 1000)}-{counter_value}"

        if watermark in self._emitted_watermarks:
            signal_watermark_collision(watermark, household_id)
        self._emitted_watermarks.add(watermark)

        event = RealtimeEvent(
            household_id=household_id,
            event_type=event_type,
            watermark=watermark,
            payload=payload,
        )

        self._ring_buffers[household_id].append(event)
        self._transport.publish(event)

        elapsed_ms = (time.perf_counter() - start) * 1000
        metrics.histogram_observe("broadcast_latency_ms", elapsed_ms)
        metrics.increment("events_broadcast_total", household_id=household_id)
        log_event("event_broadcast_sync", household_id=household_id,
                  watermark=watermark, event_type=event_type)

    async def subscribe(
        self, household_id: str, last_watermark: str | None = None
    ) -> AsyncIterator[str]:
        from apps.api.observability.safety import safety
        queue: asyncio.Queue[RealtimeEvent] = asyncio.Queue(maxsize=100)
        self._subscribers[household_id].add(queue)
        metrics.gauge_inc("active_sse_connections")
        log_event("sse_connection_opened", household_id=household_id,
                  last_watermark=last_watermark)

        # Emit initial heartbeat immediately so clients know stream is live.
        yield self._format_sse(
            event_type="connected",
            data={
                "household_id": household_id,
                "watermark": f"{int(time.time() * 1000)}-0",
                "payload": {"status": "connected"},
            },
        )

        # REPLAY: If client provides last_watermark, replay buffered events
        # Safety kill switch: disable_replay suppresses replay and forces resync
        if last_watermark:
            if safety.disable_replay or safety.force_resync_mode:
                log_event("replay_suppressed_by_safety", household_id=household_id,
                          disable_replay=safety.disable_replay,
                          force_resync_mode=safety.force_resync_mode)
                yield self._format_sse(
                    event_type="resync_required",
                    data={"reason": "safety_control",
                          "message": "Replay disabled by safety control. Client must re-bootstrap."},
                )
                metrics.increment("resync_required_total", household_id=household_id)
                check_resync_spike()
            else:
                for chunk in self._replay_buffered_events(household_id, last_watermark):
                    yield chunk

        try:
            while True:
                event = await queue.get()
                # Duplicate live emission detection
                if event.watermark in self._emitted_watermarks and event.watermark not in ("0",):
                    signal_duplicate_emission(event.watermark, household_id)
                payload = {
                    "household_id": event.household_id,
                    "event_type": event.event_type,
                    "watermark": event.watermark,
                    "payload": event.payload,
                }
                yield self._format_sse(event_type="update", data=payload)
        finally:
            self._subscribers[household_id].discard(queue)
            if not self._subscribers[household_id]:
                self._subscribers.pop(household_id, None)
            metrics.gauge_dec("active_sse_connections")
            log_event("sse_connection_closed", household_id=household_id)

    def _replay_buffered_events(self, household_id: str, last_watermark: str) -> AsyncIterator[str]:
        """
        Replay all buffered events with watermark > last_watermark.
        
        If last_watermark is too old (not in ring buffer), emit RESYNC_REQUIRED signal.
        """
        start = time.perf_counter()
        replayed_count = 0
        ring_buffer = self._ring_buffers.get(household_id, deque())
        zero_sequence = self._is_zero_sequence_watermark(last_watermark)

        if not ring_buffer:
            if last_watermark and not zero_sequence:
                yield self._format_sse(
                    event_type="resync_required",
                    data={
                        "reason": "watermark_too_old",
                        "message": "Requested watermark is older than available replay buffer. Client must call full bootstrap.",
                    },
                )
                metrics.increment("resync_required_total", household_id=household_id)
                check_resync_spike()
                log_event("resync_required", household_id=household_id,
                          reason="empty_buffer", last_watermark=last_watermark)
            return

        try:
            _, seq_str = last_watermark.rsplit("-", 1)
            last_seq = int(seq_str)
        except (ValueError, AttributeError):
            return

        found = False
        prev_seq: int | None = None
        for event in ring_buffer:
            try:
                _, event_seq_str = event.watermark.rsplit("-", 1)
                event_seq = int(event_seq_str)
                if event_seq > last_seq:
                    # Gap detection: sequences must be strictly increasing
                    if prev_seq is not None and event_seq != prev_seq + 1:
                        signal_replay_gap(household_id, prev_seq + 1, event_seq)
                    prev_seq = event_seq
                    found = True
                    replayed_count += 1
                    payload = {
                        "household_id": event.household_id,
                        "event_type": event.event_type,
                        "watermark": event.watermark,
                        "payload": event.payload,
                    }
                    yield self._format_sse(event_type="update", data=payload)
            except (ValueError, AttributeError):
                continue

        if not found and not zero_sequence:
            yield self._format_sse(
                event_type="resync_required",
                data={
                    "reason": "watermark_too_old",
                    "message": "Requested watermark is older than available replay buffer. Client must call full bootstrap.",
                },
            )
            metrics.increment("resync_required_total", household_id=household_id)
            check_resync_spike()
            log_event("resync_required", household_id=household_id,
                      reason="watermark_not_in_buffer", last_watermark=last_watermark)
        else:
            elapsed_ms = (time.perf_counter() - start) * 1000
            metrics.histogram_observe("replay_latency_ms", elapsed_ms)
            metrics.increment("events_replayed_total", amount=replayed_count,
                              household_id=household_id)
            metrics.gauge_set("replay_queue_depth", replayed_count)
            log_event("events_replayed", household_id=household_id,
                      replayed_count=replayed_count, last_watermark=last_watermark)

    @staticmethod
    def _is_zero_sequence_watermark(watermark: str | None) -> bool:
        if not watermark:
            return True
        if watermark == "0":
            return True
        try:
            _prefix, seq_str = watermark.rsplit("-", 1)
            return int(seq_str) == 0
        except (ValueError, AttributeError):
            return False

    def _fanout_local(self, event: RealtimeEvent) -> None:
        queues = list(self._subscribers.get(event.household_id, set()))
        for queue in queues:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # Drop on backpressure; next reconcile keeps clients consistent.
                metrics.increment("errors_total")
                log_error("sse_queue_full", "SSE subscriber queue full — event dropped",
                          household_id=event.household_id, watermark=event.watermark)

    @staticmethod
    def _format_sse(event_type: str, data: dict[str, Any]) -> str:
        return f"event: {event_type}\ndata: {json.dumps(data, sort_keys=True)}\n\n"


broadcaster = HouseholdBroadcaster()
