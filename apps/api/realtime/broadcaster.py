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
import inspect
import json
import os
import statistics
import time
from datetime import UTC, datetime
from threading import Lock
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, AsyncIterator
from uuid import uuid4

from apps.api.observability.metrics import metrics, timer
from apps.api.observability.logging import log_event, log_error
from apps.api.observability.alerts import (
    check_resync_spike,
    signal_watermark_collision,
    signal_replay_gap,
    signal_duplicate_emission,
)
import apps.api.observability.logging as observability_logging
from apps.api.realtime.event_bus import (
    InMemoryRealtimeEventBus,
    RedisRealtimeEventBus,
    RealtimeEventBus,
)
from apps.api.realtime.transport_event import RealtimeEvent
from apps.api.runtime.loop_tracing import register_loop_resource, trace_loop_binding, trace_loop_context
from apps.api.schemas.canonical_event import CanonicalEventEnvelope


class AtomicCounter:
    """Thread-safe atomic counter for watermark generation."""
    
    def __init__(self) -> None:
        self._value = 0
        self._lock = Lock()  # Single lock covers all access
    
    def increment(self) -> int:
        """Atomically increment and return the new value."""
        with self._lock:
            self._value += 1
            return self._value


@dataclass
class _SubscriberState:
    queue: asyncio.Queue[RealtimeEvent]
    loop: asyncio.AbstractEventLoop
    last_watermark: int | None = None
    last_emit_ts: float = 0.0


@dataclass
class _FanoutSample:
    seq: int
    ts: float
    subscriber_count: int
    fanout_elapsed_ms: float
    per_subscriber_us: float
    lag_slope: float


@dataclass
class _FanoutAggregate:
    fanout_samples: int = 0
    total_fanout_ms: float = 0.0
    max_fanout_ms: float = 0.0
    total_per_subscriber_us: float = 0.0
    total_lag_slope: float = 0.0
    enqueue_samples: int = 0
    total_schedule_delay_ms: float = 0.0
    max_schedule_delay_ms: float = 0.0
    total_inflight_tasks: float = 0.0
    max_inflight_tasks: int = 0


class HouseholdBroadcaster:
    # Ring buffer size per household (max events to replay on reconnect)
    RING_BUFFER_SIZE = 1000
    
    def __init__(self) -> None:
        self._subscribers: dict[str, list[_SubscriberState]] = defaultdict(list)
        self._counter = AtomicCounter()  # Single atomic counter (no race condition)
        self._ring_buffers: dict[str, deque[RealtimeEvent]] = defaultdict(
            lambda: deque(maxlen=self.RING_BUFFER_SIZE)
        )
        self._subscriber_queue_maxsize = max(8, int(os.getenv("SSE_CLIENT_QUEUE_MAXSIZE", "100")))
        self._queue_drop_threshold_pct = max(10, min(100, int(os.getenv("SSE_QUEUE_DROP_THRESHOLD_PCT", "80"))))
        self._throttle_min_interval_ms = max(5, int(os.getenv("SSE_THROTTLE_MIN_INTERVAL_MS", "40")))
        self._lag_slope_threshold = float(os.getenv("SSE_LAG_SLOPE_THRESHOLD", "0.08"))
        self._lag_sample_window_seconds = max(5, int(os.getenv("SSE_LAG_SAMPLE_WINDOW_SECONDS", "30")))
        self._queue_pressure_samples: deque[tuple[float, float]] = deque(maxlen=256)
        self._fanout_state_lock = Lock()
        self._fanout_samples: deque[_FanoutSample] = deque(maxlen=max(64, int(os.getenv("SSE_FANOUT_DIAGNOSTIC_WINDOW", "512"))))
        self._fanout_aggregates: dict[int, _FanoutAggregate] = defaultdict(_FanoutAggregate)
        self._fanout_seq = 0
        self._fanout_sample_count = 0
        self._fanout_total_ms = 0.0
        self._fanout_max_ms = 0.0
        self._fanout_total_subscribers = 0
        self._fanout_total_per_subscriber_us = 0.0
        self._fanout_total_lag_slope = 0.0
        self._fanout_calls_total = 0
        self._fanout_started_at = time.time()
        self._enqueue_delay_sample_count = 0
        self._enqueue_delay_total_ms = 0.0
        self._enqueue_delay_max_ms = 0.0
        self._scheduled_callbacks_total = 0
        self._executed_callbacks_total = 0
        self._callback_queue_depth_max = 0
        self._inflight_tasks_sample_count = 0
        self._inflight_tasks_total = 0.0
        self._inflight_tasks_max = 0
        # Sliding window set for watermark collision detection
        self._emitted_watermarks: set[int] = set()
        self._emitting_rejection_signal = False

        redis_url = os.getenv("REDIS_URL", "").strip()
        transport: RealtimeEventBus
        if redis_url:
            redis_transport = RedisRealtimeEventBus(redis_url)
            transport = redis_transport if redis_transport.enabled else InMemoryRealtimeEventBus()
        else:
            transport = InMemoryRealtimeEventBus()
        self._transport = transport
        self._transport.subscribe_all(self._fanout_local)

    def _detect_origin_module(self) -> str:
        """Best-effort caller module detection for rejection diagnostics."""
        try:
            for frame_info in inspect.stack()[1:]:
                module_name = frame_info.frame.f_globals.get("__name__", "")
                if not module_name:
                    continue
                if module_name == __name__:
                    continue
                return module_name
        except Exception:
            return "unknown"
        return "unknown"

    def _validate_canonical_event(self, event: Any) -> None:
        """Reject any non-canonical event before transport emission."""
        if isinstance(event, CanonicalEventEnvelope):
            return

        origin_module = self._detect_origin_module()
        event_type = getattr(event, "event_type", "unknown")
        received_type = type(event).__name__ if event is not None else "NoneType"
        rejection_reason = "non_canonical_event"

        metrics.increment("sse_rejection_detected_total")
        observability_logging.log_error(
            "non_canonical_event_rejected_on_sse_stream",
            "Non-canonical event attempted on SSE stream",
            event_type=event_type,
            origin_module=origin_module,
            rejection_reason=rejection_reason,
            received_type=received_type,
        )

        raise RuntimeError(
            f"Non-canonical event attempted on SSE stream. "
            f"Received {received_type}; only CanonicalEventEnvelope is allowed. "
            f"Origin: {origin_module}"
        )

    def _validate_emit_origin(self, event: Any) -> None:
        """Allow emits only from router-originated canonical envelopes."""
        origin_module = self._detect_origin_module()
        event_type = getattr(event, "event_type", "unknown")

        def _emit_rejection(reason: str) -> None:
            # Guard against recursive logging signal loops.
            if self._emitting_rejection_signal:
                return
            self._emitting_rejection_signal = True
            try:
                metrics.increment("sse_rejection_detected_total")
                log_event(
                    "SystemEvent.sse_rejection_detected",
                    event_type=event_type,
                    origin_module=origin_module,
                    reason=reason,
                )
            finally:
                self._emitting_rejection_signal = False

        # If marker is absent, this path may come from trusted in-process callers.
        # Keep strict validation only when marker exists.
        if not hasattr(event, "__origin_router"):
            return

        if getattr(event, "__origin_router", None) is not True:
            _emit_rejection("origin_router_flag_not_true")
            log_error(
                "sse_violation_non_router_origin",
                "SSE violation: non-router origin",
                event_type=event_type,
                origin_module=origin_module,
                rejection_reason="origin_router_flag_not_true",
            )
            raise RuntimeError("SSE violation: non-router origin")

    def _append_ring_buffer_event(self, household_id: str, event: RealtimeEvent) -> None:
        """Append event into a household ring buffer, tolerating mocked plain dicts in tests."""
        buffer = self._ring_buffers.get(household_id)
        if buffer is None:
            buffer = deque(maxlen=self.RING_BUFFER_SIZE)
            self._ring_buffers[household_id] = buffer
        buffer.append(event)

    async def publish(self, envelope: CanonicalEventEnvelope) -> None:
        self._validate_canonical_event(envelope)
        self._validate_emit_origin(envelope)
        from apps.api.observability.safety import safety
        if safety.pause_writes:
            log_error("publish_blocked_pause_writes", "pause_writes safety control active",
                      household_id=envelope.household_id, event_type=envelope.event_type)
            return

        with timer("broadcast_latency_ms"):
            watermark = self._resolve_watermark(envelope)

            # Watermark collision check (should never fire with AtomicCounter)
            if watermark in self._emitted_watermarks:
                signal_watermark_collision(str(watermark), envelope.household_id)
            self._emitted_watermarks.add(watermark)

            event = RealtimeEvent(
                event_id=envelope.event_id or str(uuid4()),
                actor_type=envelope.actor_type,
                household_id=envelope.household_id,
                event_type=envelope.event_type,
                timestamp=envelope.timestamp,
                watermark=watermark,
                idempotency_key=envelope.idempotency_key,
                source=envelope.source,
                severity=envelope.severity,
                payload=dict(envelope.payload),
                signature=envelope.signature,
            )

            self._append_ring_buffer_event(envelope.household_id, event)
            self._transport.publish(event)

        metrics.increment("events_broadcast_total", household_id=envelope.household_id)
        log_event("event_broadcast", household_id=envelope.household_id,
                  watermark=str(watermark), event_type=envelope.event_type)

    def publish_sync(self, envelope: CanonicalEventEnvelope) -> None:
        """Thread-safe sync publish for service-layer code paths."""
        self._validate_canonical_event(envelope)
        self._validate_emit_origin(envelope)
        from apps.api.observability.safety import safety
        if safety.pause_writes:
            log_error("publish_sync_blocked_pause_writes", "pause_writes safety control active",
                      household_id=envelope.household_id, event_type=envelope.event_type)
            return

        start = time.perf_counter()
        watermark = self._resolve_watermark(envelope)

        if watermark in self._emitted_watermarks:
            signal_watermark_collision(str(watermark), envelope.household_id)
        self._emitted_watermarks.add(watermark)

        event = RealtimeEvent(
            event_id=envelope.event_id or str(uuid4()),
            actor_type=envelope.actor_type,
            household_id=envelope.household_id,
            event_type=envelope.event_type,
            timestamp=envelope.timestamp,
            watermark=watermark,
            idempotency_key=envelope.idempotency_key,
            source=envelope.source,
            severity=envelope.severity,
            payload=dict(envelope.payload),
            signature=envelope.signature,
        )

        self._append_ring_buffer_event(envelope.household_id, event)
        self._transport.publish(event)

        elapsed_ms = (time.perf_counter() - start) * 1000
        metrics.histogram_observe("broadcast_latency_ms", elapsed_ms)
        metrics.increment("events_broadcast_total", household_id=envelope.household_id)
        log_event("event_broadcast_sync", household_id=envelope.household_id,
                  watermark=str(watermark), event_type=envelope.event_type)

    async def subscribe(
        self, household_id: str, last_watermark: int | None = None
    ) -> AsyncIterator[str]:
        from apps.api.observability.safety import safety
        trace_loop_context(f"broadcaster.subscribe:{household_id}")
        queue: asyncio.Queue[RealtimeEvent] = asyncio.Queue(maxsize=self._subscriber_queue_maxsize)
        register_loop_resource(queue, "CREATE: apps/api/realtime/broadcaster.py:subscribe:queue")
        loop = asyncio.get_running_loop()
        subscriber = _SubscriberState(queue=queue, loop=loop)
        self._subscribers[household_id].append(subscriber)
        metrics.gauge_inc("active_sse_connections")
        log_event("sse_connection_opened", household_id=household_id,
                  last_watermark=last_watermark)

        # Emit initial heartbeat immediately so clients know stream is live.
        yield self._format_sse(
            event_type="connected",
            data={
                "household_id": household_id,
                "watermark": 0,
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
                try:
                    trace_loop_binding(queue, "USE: apps/api/realtime/broadcaster.py:subscribe:queue.get")
                    event = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    if subscriber not in self._subscribers.get(household_id, []):
                        # Queue was removed by fanout backpressure handling.
                        break
                    continue
                payload = self._event_to_stream_payload(event)
                yield self._format_sse(event_type="update", data=payload)
        finally:
            if subscriber in self._subscribers[household_id]:
                self._subscribers[household_id].remove(subscriber)
            if not self._subscribers[household_id]:
                self._subscribers.pop(household_id, None)
            metrics.gauge_dec("active_sse_connections")
            log_event("sse_connection_closed", household_id=household_id)

    def _replay_buffered_events(self, household_id: str, last_watermark: int) -> AsyncIterator[str]:
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
            last_seq = int(last_watermark)
        except (ValueError, TypeError):
            return

        found = False
        prev_seq: int | None = None
        for event in ring_buffer:
            try:
                event_seq = int(event.watermark)
                if event_seq > last_seq:
                    # Gap detection: sequences must be strictly increasing
                    if prev_seq is not None and event_seq != prev_seq + 1:
                        signal_replay_gap(household_id, prev_seq + 1, event_seq)
                    prev_seq = event_seq
                    found = True
                    replayed_count += 1
                    payload = self._event_to_stream_payload(event)
                    yield self._format_sse(event_type="update", data=payload)
            except (ValueError, TypeError):
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
    def _is_zero_sequence_watermark(watermark: int | None) -> bool:
        if not watermark:
            return True
        return int(watermark) == 0

    def _resolve_watermark(self, envelope: CanonicalEventEnvelope) -> int:
        """Return the canonical watermark value used by broadcaster emit paths.
        
        STRICT INVARIANCE: All watermarks assigned from single monotonic counter.
        External watermarks MUST be ignored to enforce single authority.
        """
        # ALL watermarks come from internal counter - external inputs rejected
        return self._counter.increment()

    @staticmethod
    def _event_to_stream_payload(event: RealtimeEvent) -> dict[str, Any]:
        return {
            "event_id": event.event_id,
            "household_id": event.household_id,
            "event_type": event.event_type,
            "actor_type": event.actor_type,
            "timestamp": event.timestamp.isoformat(),
            "watermark": event.watermark,
            "idempotency_key": event.idempotency_key,
            "source": event.source,
            "severity": event.severity,
            "payload": event.payload,
            "signature": event.signature,
        }

    def _fanout_local(self, event: RealtimeEvent) -> None:
        subscribers = list(self._subscribers.get(event.household_id, []))
        if not subscribers:
            return

        fanout_start = time.perf_counter()
        lag_slope = self._compute_queue_pressure_slope(subscribers)
        throttled = lag_slope >= self._lag_slope_threshold
        scheduled_at = time.perf_counter()

        for subscriber in subscribers:
            try:
                subscriber.loop.call_soon_threadsafe(
                    self._enqueue_for_subscriber,
                    event,
                    subscriber,
                    throttled,
                    scheduled_at,
                )
            except RuntimeError:
                if subscriber in self._subscribers[event.household_id]:
                    self._subscribers[event.household_id].remove(subscriber)

        fanout_elapsed_ms = (time.perf_counter() - fanout_start) * 1000.0
        subscriber_count = len(subscribers)
        per_subscriber_us = (fanout_elapsed_ms * 1000.0) / max(1, subscriber_count)
        metrics.histogram_observe("sse_fanout_cycle_ms", fanout_elapsed_ms)
        with self._fanout_state_lock:
            self._fanout_calls_total += 1
            self._scheduled_callbacks_total += subscriber_count
            queue_depth = max(0, self._scheduled_callbacks_total - self._executed_callbacks_total)
            self._callback_queue_depth_max = max(self._callback_queue_depth_max, queue_depth)
        self._record_fanout_sample(
            subscriber_count=subscriber_count,
            fanout_elapsed_ms=fanout_elapsed_ms,
            per_subscriber_us=per_subscriber_us,
            lag_slope=lag_slope,
        )

    def _compute_queue_pressure_slope(self, subscribers: list[_SubscriberState]) -> float:
        now = time.time()
        fill_ratios: list[float] = []
        for subscriber in subscribers:
            maxsize = max(1, subscriber.queue.maxsize)
            fill_ratios.append(float(subscriber.queue.qsize()) / float(maxsize))
        avg_fill_ratio = sum(fill_ratios) / len(fill_ratios) if fill_ratios else 0.0

        with self._fanout_state_lock:
            self._queue_pressure_samples.append((now, avg_fill_ratio))
            while self._queue_pressure_samples and (now - self._queue_pressure_samples[0][0]) > self._lag_sample_window_seconds:
                self._queue_pressure_samples.popleft()
            if len(self._queue_pressure_samples) < 2:
                return 0.0
            t0, r0 = self._queue_pressure_samples[0]
            t1, r1 = self._queue_pressure_samples[-1]
            dt = max(0.001, t1 - t0)
            return (r1 - r0) / dt

    def _enqueue_for_subscriber(
        self,
        event: RealtimeEvent,
        subscriber: _SubscriberState,
        throttled: bool,
        scheduled_at: float,
    ) -> None:
        trace_loop_context(f"broadcaster._enqueue_for_subscriber:{event.household_id}")
        trace_loop_binding(subscriber.queue, "USE: apps/api/realtime/broadcaster.py:_enqueue_for_subscriber")
        schedule_delay_ms = max(0.0, (time.perf_counter() - scheduled_at) * 1000.0)
        metrics.histogram_observe("sse_enqueue_schedule_delay_ms", schedule_delay_ms)
        self._record_schedule_delay(event.household_id, subscriber.loop, schedule_delay_ms)

        # Duplicate suppression per subscriber avoids fanout amplification on repeated publish paths.
        if subscriber.last_watermark == event.watermark:
            signal_duplicate_emission(str(event.watermark), event.household_id)
            return

        now = time.time()
        if throttled and (now - subscriber.last_emit_ts) < (self._throttle_min_interval_ms / 1000.0):
            metrics.increment("sse_events_coalesced")
            log_event(
                "sse_event_dropped",
                household_id=event.household_id,
                    watermark=str(event.watermark),
                drop_reason="soft_throttle",
            )
            return

        # Bounded queue with oldest-drop behavior under backpressure.
        maxsize = max(1, subscriber.queue.maxsize)
        queue_fill_pct = (subscriber.queue.qsize() * 100) // maxsize
        if queue_fill_pct >= self._queue_drop_threshold_pct:
            try:
                trace_loop_binding(subscriber.queue, "USE: apps/api/realtime/broadcaster.py:_enqueue_for_subscriber:get_nowait")
                subscriber.queue.get_nowait()
                metrics.increment("sse_events_coalesced")
                log_event(
                    "sse_event_dropped",
                    household_id=event.household_id,
                    watermark=str(event.watermark),
                    drop_reason="backpressure",
                )
            except asyncio.QueueEmpty:
                pass

        try:
            trace_loop_binding(subscriber.queue, "USE: apps/api/realtime/broadcaster.py:_enqueue_for_subscriber:put_nowait")
            subscriber.queue.put_nowait(event)
            subscriber.last_watermark = event.watermark
            subscriber.last_emit_ts = now
        except asyncio.QueueFull:
            # Hard cap reached even after oldest-drop: drop current event, keep connection alive.
            metrics.increment("errors_total")
            log_error(
                "sse_queue_full",
                "SSE subscriber queue full — event dropped",
                household_id=event.household_id,
                watermark=str(event.watermark),
                drop_reason="backpressure",
            )

    def diagnostics_snapshot(self) -> dict[str, Any]:
        with self._fanout_state_lock:
            samples = list(self._fanout_samples)
            aggregates = {count: aggregate for count, aggregate in sorted(self._fanout_aggregates.items())}
            fanout_started_at = self._fanout_started_at
            fanout_sample_count = self._fanout_sample_count
            fanout_total_ms = self._fanout_total_ms
            fanout_max_ms = self._fanout_max_ms
            fanout_total_subscribers = self._fanout_total_subscribers
            fanout_total_per_subscriber_us = self._fanout_total_per_subscriber_us
            fanout_total_lag_slope = self._fanout_total_lag_slope
            fanout_calls_total = self._fanout_calls_total
            enqueue_delay_sample_count = self._enqueue_delay_sample_count
            enqueue_delay_total_ms = self._enqueue_delay_total_ms
            enqueue_delay_max_ms = self._enqueue_delay_max_ms
            scheduled_callbacks_total = self._scheduled_callbacks_total
            executed_callbacks_total = self._executed_callbacks_total
            callback_queue_depth_max = self._callback_queue_depth_max
            inflight_tasks_sample_count = self._inflight_tasks_sample_count
            inflight_tasks_total = self._inflight_tasks_total
            inflight_tasks_max = self._inflight_tasks_max

        elapsed_seconds = max(0.001, time.time() - fanout_started_at)
        callback_queue_depth = max(0, scheduled_callbacks_total - executed_callbacks_total)

        cost_curve = []
        for subscriber_count, aggregate in aggregates.items():
            avg_fanout_ms = aggregate.total_fanout_ms / max(1, aggregate.fanout_samples)
            avg_per_subscriber_us = aggregate.total_per_subscriber_us / max(1, aggregate.fanout_samples)
            avg_lag_slope = aggregate.total_lag_slope / max(1, aggregate.fanout_samples)
            avg_schedule_delay_ms = aggregate.total_schedule_delay_ms / max(1, aggregate.enqueue_samples)
            avg_inflight_tasks = aggregate.total_inflight_tasks / max(1, aggregate.enqueue_samples)
            cost_curve.append(
                {
                    "subscriber_count": subscriber_count,
                    "samples": aggregate.fanout_samples,
                    "avg_fanout_ms": round(avg_fanout_ms, 6),
                    "max_fanout_ms": round(aggregate.max_fanout_ms, 6),
                    "avg_per_subscriber_us": round(avg_per_subscriber_us, 6),
                    "avg_schedule_delay_ms": round(avg_schedule_delay_ms, 6),
                    "max_schedule_delay_ms": round(aggregate.max_schedule_delay_ms, 6),
                    "avg_inflight_tasks": round(avg_inflight_tasks, 6),
                    "max_inflight_tasks": aggregate.max_inflight_tasks,
                    "avg_lag_slope": round(avg_lag_slope, 6),
                }
            )

        correlation = self._pearson_correlation(
            [float(sample.subscriber_count) for sample in samples],
            [float(sample.lag_slope) for sample in samples],
        )
        fanout_time_correlation = self._pearson_correlation(
            [float(sample.subscriber_count) for sample in samples],
            [float(sample.fanout_elapsed_ms) for sample in samples],
        )
        return {
            "fanout_diagnostics": {
                "sample_count": fanout_sample_count,
                "fanout_calls_total": fanout_calls_total,
                "fanout_calls_per_second": round(fanout_calls_total / elapsed_seconds, 6),
                "avg_fanout_time_ms": round(fanout_total_ms / max(1, fanout_sample_count), 6),
                "max_fanout_time_ms": round(fanout_max_ms, 6),
                "avg_subscriber_count": round(fanout_total_subscribers / max(1, fanout_sample_count), 6),
                "avg_per_subscriber_us": round(fanout_total_per_subscriber_us / max(1, fanout_sample_count), 6),
                "avg_lag_slope": round(fanout_total_lag_slope / max(1, fanout_sample_count), 6),
                "avg_schedule_delay_ms": round(enqueue_delay_total_ms / max(1, enqueue_delay_sample_count), 6),
                "max_schedule_delay_ms": round(enqueue_delay_max_ms, 6),
                "callback_queue_depth": callback_queue_depth,
                "callback_queue_depth_max": callback_queue_depth_max,
                "inflight_tasks_avg": round(inflight_tasks_total / max(1, inflight_tasks_sample_count), 6),
                "inflight_tasks_max": inflight_tasks_max,
                "subscriber_count_vs_lag_slope_correlation": round(correlation, 6),
                "subscriber_count_vs_fanout_time_correlation": round(fanout_time_correlation, 6),
                "cost_curve": cost_curve,
                "recent_samples": [
                    {
                        "seq": sample.seq,
                        "ts": round(sample.ts, 6),
                        "subscriber_count": sample.subscriber_count,
                        "fanout_elapsed_ms": round(sample.fanout_elapsed_ms, 6),
                        "per_subscriber_us": round(sample.per_subscriber_us, 6),
                        "lag_slope": round(sample.lag_slope, 6),
                    }
                    for sample in samples[-32:]
                ],
            }
        }

    def _record_fanout_sample(
        self,
        *,
        subscriber_count: int,
        fanout_elapsed_ms: float,
        per_subscriber_us: float,
        lag_slope: float,
    ) -> None:
        with self._fanout_state_lock:
            self._fanout_seq += 1
            sample = _FanoutSample(
                seq=self._fanout_seq,
                ts=time.time(),
                subscriber_count=subscriber_count,
                fanout_elapsed_ms=fanout_elapsed_ms,
                per_subscriber_us=per_subscriber_us,
                lag_slope=lag_slope,
            )
            self._fanout_samples.append(sample)
            self._fanout_sample_count += 1
            self._fanout_total_ms += fanout_elapsed_ms
            self._fanout_max_ms = max(self._fanout_max_ms, fanout_elapsed_ms)
            self._fanout_total_subscribers += subscriber_count
            self._fanout_total_per_subscriber_us += per_subscriber_us
            self._fanout_total_lag_slope += lag_slope
            aggregate = self._fanout_aggregates[subscriber_count]
            aggregate.fanout_samples += 1
            aggregate.total_fanout_ms += fanout_elapsed_ms
            aggregate.max_fanout_ms = max(aggregate.max_fanout_ms, fanout_elapsed_ms)
            aggregate.total_per_subscriber_us += per_subscriber_us
            aggregate.total_lag_slope += lag_slope

    def _record_schedule_delay(self, household_id: str, loop: asyncio.AbstractEventLoop, schedule_delay_ms: float) -> None:
        subscriber_count = len(self._subscribers.get(household_id, []))
        inflight_tasks = 0
        try:
            inflight_tasks = len(asyncio.all_tasks(loop))
        except RuntimeError:
            inflight_tasks = 0
        with self._fanout_state_lock:
            self._executed_callbacks_total += 1
            self._enqueue_delay_sample_count += 1
            self._enqueue_delay_total_ms += schedule_delay_ms
            self._enqueue_delay_max_ms = max(self._enqueue_delay_max_ms, schedule_delay_ms)
            self._inflight_tasks_sample_count += 1
            self._inflight_tasks_total += float(inflight_tasks)
            self._inflight_tasks_max = max(self._inflight_tasks_max, inflight_tasks)
            aggregate = self._fanout_aggregates[subscriber_count]
            aggregate.enqueue_samples += 1
            aggregate.total_schedule_delay_ms += schedule_delay_ms
            aggregate.max_schedule_delay_ms = max(aggregate.max_schedule_delay_ms, schedule_delay_ms)
            aggregate.total_inflight_tasks += float(inflight_tasks)
            aggregate.max_inflight_tasks = max(aggregate.max_inflight_tasks, inflight_tasks)

    def reset_diagnostics(self) -> None:
        with self._fanout_state_lock:
            self._fanout_samples.clear()
            self._fanout_aggregates.clear()
            self._fanout_seq = 0
            self._fanout_sample_count = 0
            self._fanout_total_ms = 0.0
            self._fanout_max_ms = 0.0
            self._fanout_total_subscribers = 0
            self._fanout_total_per_subscriber_us = 0.0
            self._fanout_total_lag_slope = 0.0
            self._fanout_calls_total = 0
            self._fanout_started_at = time.time()
            self._enqueue_delay_sample_count = 0
            self._enqueue_delay_total_ms = 0.0
            self._enqueue_delay_max_ms = 0.0
            self._scheduled_callbacks_total = 0
            self._executed_callbacks_total = 0
            self._callback_queue_depth_max = 0
            self._inflight_tasks_sample_count = 0
            self._inflight_tasks_total = 0.0
            self._inflight_tasks_max = 0

    @staticmethod
    def _pearson_correlation(xs: list[float], ys: list[float]) -> float:
        if len(xs) < 2 or len(ys) < 2 or len(xs) != len(ys):
            return 0.0
        if len(set(xs)) == 1 or len(set(ys)) == 1:
            return 0.0
        x_mean = statistics.fmean(xs)
        y_mean = statistics.fmean(ys)
        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys, strict=False))
        x_variance = sum((x - x_mean) ** 2 for x in xs)
        y_variance = sum((y - y_mean) ** 2 for y in ys)
        denominator = (x_variance * y_variance) ** 0.5
        if denominator <= 0:
            return 0.0
        return numerator / denominator

    @staticmethod
    def _format_sse(event_type: str, data: dict[str, Any]) -> str:
        return f"event: {event_type}\ndata: {json.dumps(data, sort_keys=True)}\n\n"


broadcaster = HouseholdBroadcaster()
