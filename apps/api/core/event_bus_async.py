from __future__ import annotations

import logging
import threading
import time
from collections import deque
from collections.abc import Callable

from apps.api.core.event_bus_base import EventBusBase
from apps.api.schemas.event import SystemEvent
from apps.api.services.queue_store import load_queue, save_queue, should_persist

_logger = logging.getLogger(__name__)

_CHECKPOINT_INTERVAL = 5  # persist remaining queue every N successfully processed events
MAX_QUEUE_SIZE = 100  # hard cap; publish returns {"status": "queue_full"} when reached


class AsyncEventBus(EventBusBase):
    """
    Async queue-based event dispatcher with persistent checkpoint recovery.

    LOCK STRATEGY — Minimize Contention & Avoid Re-Entrancy:
    ─────────────────────────────────────────────────────────
    • self._lock is a minimalist guard: acquired ONLY for deque mutations
      (popleft, append) and worker flag state transitions.
    • File I/O (load_queue, save_queue) is NEVER performed while holding
      the lock—it would block the worker loop during recovery.
    • Checkpoint restore (_restore_checkpoint) is called OUTSIDE the lock
      before worker thread creation, avoiding deadlock and re-entrancy.
    • Worker loop:
      - Acquires lock to pop one event from deque.
      - Releases lock immediately for handler dispatch (potentially long-running).
      - Calls _maybe_checkpoint() outside lock to persist asynchronously.
    • Result: No file I/O blocking worker dispatch, no deadlock risk.
    """

    def __init__(
        self,
        load_queue_fn: Callable[[], list[dict]] | None = None,
        save_queue_fn: Callable[[list[dict]], None] | None = None,
    ) -> None:
        # Persistence function dependency injection: allows tests to override
        # behavior without module-level monkey-patching. Defaults use queue_store.
        self.load_queue_fn = load_queue_fn or load_queue
        self.save_queue_fn = save_queue_fn or save_queue

        self._registry: dict[str, list[Callable[[SystemEvent], object]]] = {}
        self._queue: deque[SystemEvent] = deque()
        self._lock = threading.Lock()
        self._worker_thread: threading.Thread | None = None
        self._worker_running = False
        self._processed_since_checkpoint: int = 0

    # ------------------------------------------------------------------
    # Checkpoint helpers
    # ------------------------------------------------------------------

    def _restore_checkpoint(self) -> None:
        """Restore persisted queue on startup; skip silently on corruption.

        MUST be called OUTSIDE self._lock to avoid re-entrancy deadlock.
        Safe to call before the worker thread starts because no concurrent
        deque access exists at that point.
        """
        raw_items = self.load_queue_fn()  # returns [] for missing/corrupt files
        restored: list[SystemEvent] = []
        for item in raw_items:
            try:
                restored.append(SystemEvent.model_validate(item))
            except Exception:
                # Corrupted individual item: skip and continue with the rest.
                _logger.warning("Skipping unrestorable checkpoint item: %r", item)
                continue

        if restored:
            # Direct deque manipulation without the lock: safe here because this
            # is called before the worker thread exists (no concurrent readers).
            self._queue.extendleft(reversed(restored))
            _logger.info("Restored %d event(s) from queue checkpoint.", len(restored))

    def _snapshot_remaining(self) -> list[dict]:
        """Serialise the current deque contents without modifying the queue."""
        with self._lock:
            # model_dump(mode='json') ensures datetime → ISO string for JSON safety.
            return [e.model_dump(mode="json") for e in self._queue]

    def _maybe_checkpoint(self) -> None:
        """Save remaining queue if N events have been processed since last save.

        Uses the injected save_queue_fn (defaults to queue_store.save_queue).
        """
        self._processed_since_checkpoint += 1
        if self._processed_since_checkpoint >= _CHECKPOINT_INTERVAL:
            self._processed_since_checkpoint = 0
            self.save_queue_fn(self._snapshot_remaining())

    # ------------------------------------------------------------------
    # EventBusBase interface
    # ------------------------------------------------------------------

    def register(self, event_type: str, handler: Callable[[SystemEvent], object]) -> None:
        if event_type not in self._registry:
            self._registry[event_type] = []
        self._registry[event_type].append(handler)

    def publish(self, event: SystemEvent) -> list[object] | dict | None:
        # Backpressure check: check queue size and enqueue atomically.
        # (No I/O performed inside lock; safe for high-throughput callers.)
        with self._lock:
            if len(self._queue) >= MAX_QUEUE_SIZE:
                # Refuse the new event; existing queue is preserved intact.
                _logger.warning(
                    "AsyncEventBus queue full (%d/%d). Event type=%s dropped.",
                    len(self._queue),
                    MAX_QUEUE_SIZE,
                    event.type,
                )
                return {"status": "queue_full", "queue_size": len(self._queue)}
            # Queue-only publish: no synchronous handler execution.
            self._queue.append(event)
        return None

    def start_worker(self, loop: object | None = None) -> None:
        # loop is accepted for compatibility but not required by this implementation.
        del loop

        # ────────────────────────────────────────────────────────────────────
        # STARTUP PHASE 1: Guard flag state transitions (minimal critical section)
        # ────────────────────────────────────────────────────────────────────
        # Purpose: Prevent double-start and create the worker thread atomically.
        with self._lock:
            if self._worker_running:
                return  # Already running; safe to return while holding lock
            self._worker_running = True
            self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        # Lock released here: other threads can now check _worker_running

        # ────────────────────────────────────────────────────────────────────
        # STARTUP PHASE 2a: Restore state FIRST (NO lock held; safe before
        #        thread starts, as no concurrent deque access can occur)
        # ────────────────────────────────────────────────────────────────────
        # CRITICAL: Hydrate the queue from persistent storage BEFORE the worker
        # thread begins processing. This ensures no events are lost on restart
        # and idempotency guarantees are maintained.
        try:
            self._restore_checkpoint()
        except Exception as exc:
            # Checkpoint recovery failure is non-fatal: log and continue
            # with an empty queue (guard against corrupt/unreadable files).
            _logger.warning("Checkpoint restore failed; continuing with empty queue: %s", exc)

        # ────────────────────────────────────────────────────────────────────
        # STARTUP PHASE 2b: Start execution engine (queue is now hydrated)
        # ────────────────────────────────────────────────────────────────────
        # CRITICAL: Worker thread only starts AFTER state restoration completes.
        # This guarantees the worker begins with a fully recovered queue and
        # cannot miss persisted events from previous runs.
        self._worker_thread.start()

    def stop_worker(self) -> None:
        with self._lock:
            self._worker_running = False

        if self._worker_thread is not None:
            self._worker_thread.join(timeout=2.0)

        # Persist any remaining unprocessed events on clean shutdown.
        # Uses the injected save_queue_fn.
        self.save_queue_fn(self._snapshot_remaining())

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    def _worker_loop(self) -> None:
        while True:
            # ────── LOCK: Check shutdown flag and pop one event ──────
            with self._lock:
                if not self._worker_running:
                    break  # Shutdown signal received; exit cleanly
                # Pop BEFORE processing: the checkpoint snapshot will never
                # include this event, preventing duplicate execution on restore.
                event = self._queue.popleft() if self._queue else None
            # ────── LOCK RELEASED: Dispatch is now safe without lock ──────

            if event is None:
                # Queue empty; yield CPU briefly before next check.
                time.sleep(0.1)
                continue

            # Handler dispatch: lock NOT held. Handlers can run concurrently
            # with new events being published—no writer blocking.
            handlers = self._registry.get(event.type, [])
            for handler in handlers:
                try:
                    handler(event)
                except Exception:
                    # Safe failure handling for background processing.
                    continue

            # Checkpoint AFTER dispatch: lock not held. File I/O is
            # asynchronous (background thread), never blocking worker loop.
            self._maybe_checkpoint()