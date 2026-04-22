"""
Execution Fairness Layer — per-class semaphore pool that prevents one request
class from monopolising the event loop.

Classes
-------
SHORT   – auth checks, small read-only lookups (<50 ms expected)
LONG    – chat / compute-heavy endpoints (may take >1 s)
STREAM  – SSE stream connections (long-lived)

Each class has a dedicated *asyncio.Semaphore*.  A request acquires its
class semaphore before proceeding.  If unavailable it falls back to a
shared overflow pool before returning HTTP 429.

Usage
-----
    from apps.api.runtime.execution_fairness import fairness_gate

    async with fairness_gate.acquire("LONG"):
        ...  # handle request

    # Or protect a FastAPI endpoint via the dependency helper:
    async def my_endpoint(
        _: None = Depends(fairness_gate.dependency("SHORT")),
    ) -> ...:
        ...
"""
from __future__ import annotations

import asyncio
import os
import weakref
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Literal

from fastapi import HTTPException
from apps.api.observability.logging import log_event
from apps.api.runtime.loop_tracing import register_loop_resource, trace_loop_binding, trace_loop_context

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #
RequestClass = Literal["SHORT", "LONG", "STREAM"]

_SLOTS: dict[RequestClass, int] = {
    "SHORT": max(1, int(os.getenv("FAIRNESS_SHORT_SLOTS", "50"))),
    "LONG": max(1, int(os.getenv("FAIRNESS_LONG_SLOTS", "30"))),
    "STREAM": max(1, int(os.getenv("FAIRNESS_STREAM_SLOTS", "20"))),
}
_OVERFLOW_SLOTS = max(1, int(os.getenv("FAIRNESS_OVERFLOW_SLOTS", "10")))
_ACQUIRE_TIMEOUT_S: float = float(os.getenv("FAIRNESS_ACQUIRE_TIMEOUT_S", "0.08"))

# --------------------------------------------------------------------------- #
# Loop-Local Resource Registry (SAFE: WeakKeyDictionary)                      #
# --------------------------------------------------------------------------- #
# Keys are actual event loop objects (not IDs), ensuring no cross-loop
# resource sharing even if Python reuses numeric IDs. WeakKeyDictionary
# ensures cleanup when loops are destroyed.
_loop_local_resources: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[str, object]] = weakref.WeakKeyDictionary()


def assert_loop_owner(resource: object, context: str) -> None:
    """Verify that a resource is owned by the current running loop.

    Raises RuntimeError if resource is bound to a different loop.
    """
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        return  # No running loop — skip check

    owner_loop = getattr(resource, "_loop_owner", None)
    if owner_loop is not None and owner_loop is not current_loop:
        raise RuntimeError(
            f"[LOOP VIOLATION] {context} "
            f"resource_id={id(resource)} "
            f"owner_loop={id(owner_loop)} "
            f"current_loop={id(current_loop)}"
        )


def get_loop_local_resource(key: str, factory) -> object:
    """Return a per-event-loop resource instance using WeakKeyDictionary.

    Resources are keyed by actual loop object (not id()), ensuring no
    cross-loop contamination even if Python reuses numeric loop IDs.

    Args:
        key: Resource key within the loop's bucket
        factory: Callable that creates a new resource instance

    Returns:
        Resource instance, created on first access or cached thereafter
    """
    loop = asyncio.get_running_loop()
    trace_loop_context(f"execution_fairness.get_loop_local_resource:{key}")

    # Retrieve or create the bucket for this specific loop object
    bucket = _loop_local_resources.get(loop)
    if bucket is None:
        bucket = {}
        _loop_local_resources[loop] = bucket

    # Retrieve or create the resource
    if key not in bucket:
        resource = factory()
        # Attach loop ownership for runtime checks
        if not hasattr(resource, "_loop_owner"):
            setattr(resource, "_loop_owner", loop)
        bucket[key] = resource
        register_loop_resource(resource, f"CREATE: apps/api/runtime/execution_fairness.py:get_loop_local_resource:{key}")
    else:
        resource = bucket[key]
        # Runtime integrity check: ensure cached resource matches current loop
        assert_loop_owner(resource, f"USE: apps/api/runtime/execution_fairness.py:get_loop_local_resource:{key}")

    return resource


@dataclass
class _LoopFairnessState:
    semaphores: dict[RequestClass, asyncio.Semaphore]
    overflow: asyncio.Semaphore
    accepted: dict[str, int]
    rejected: dict[str, int]


class _FairnessGate:
    """Per-loop fairness gate state (semaphores must be loop-local)."""

    def __init__(self, slots: dict[RequestClass, int], overflow: int) -> None:
        self._slots = slots
        self._overflow_count = overflow

    def _state(self) -> _LoopFairnessState:
        key = f"fairness_gate_state:{id(self)}"

        def _factory() -> _LoopFairnessState:
            semaphores = {cls: asyncio.Semaphore(count) for cls, count in self._slots.items()}
            for cls, semaphore in semaphores.items():
                register_loop_resource(semaphore, f"CREATE: apps/api/runtime/execution_fairness.py:_state:{cls}")
            overflow = asyncio.Semaphore(self._overflow_count)
            register_loop_resource(overflow, "CREATE: apps/api/runtime/execution_fairness.py:_state:OVERFLOW")
            return _LoopFairnessState(
                semaphores=semaphores,
                overflow=overflow,
                accepted={k: 0 for k in ("SHORT", "LONG", "STREAM", "OVERFLOW")},
                rejected={k: 0 for k in ("SHORT", "LONG", "STREAM")},
            )

        state = get_loop_local_resource(key, _factory)
        assert isinstance(state, _LoopFairnessState)  # noqa: S101
        return state

    @asynccontextmanager
    async def acquire(self, cls: RequestClass) -> AsyncIterator[None]:
        """Context-manager that acquires a fairness slot or raises HTTP 429."""
        state = self._state()

        sem = state.semaphores[cls]
        trace_loop_context(f"execution_fairness.acquire:{cls}")
        # Strict loop ownership check
        assert_loop_owner(sem, f"USE: apps/api/runtime/execution_fairness.py:acquire:{cls}:class")
        trace_loop_binding(sem, f"USE: apps/api/runtime/execution_fairness.py:acquire:{cls}:class")
        # Non-blocking attempt on class semaphore.
        acquired_class = sem._value > 0  # optimistic peek (no lock, best-effort)
        if acquired_class:
            try:
                await asyncio.wait_for(asyncio.shield(sem.acquire()), timeout=_ACQUIRE_TIMEOUT_S)
                state.accepted[cls] = state.accepted.get(cls, 0) + 1
                try:
                    yield
                finally:
                    sem.release()
                return
            except asyncio.TimeoutError:
                pass  # fall through to overflow

        # Try overflow pool.
        try:
            assert_loop_owner(state.overflow, f"USE: apps/api/runtime/execution_fairness.py:acquire:{cls}:overflow")
            trace_loop_binding(state.overflow, f"USE: apps/api/runtime/execution_fairness.py:acquire:{cls}:overflow")
            await asyncio.wait_for(asyncio.shield(state.overflow.acquire()), timeout=_ACQUIRE_TIMEOUT_S)
            state.accepted["OVERFLOW"] = state.accepted.get("OVERFLOW", 0) + 1
            try:
                yield
            finally:
                state.overflow.release()
            return
        except asyncio.TimeoutError:
            pass

        # All pools full — reject.
        state.rejected[cls] = state.rejected.get(cls, 0) + 1
        raise HTTPException(
            status_code=429,
            detail=f"class_capacity_exceeded:{cls}",
            headers={"Retry-After": "1"},
        )

    async def _acquire_raw(self, cls: RequestClass) -> str:
        """Acquire a slot and return which pool was used ("cls" or "OVERFLOW").
        Raises HTTPException(429) if all pools are full.
        Caller must call _release_raw() with the returned pool name.
        """
        state = self._state()
        sem = state.semaphores[cls]
        trace_loop_context(f"execution_fairness._acquire_raw:{cls}")
        # Strict loop ownership check
        assert_loop_owner(sem, f"USE: apps/api/runtime/execution_fairness.py:_acquire_raw:{cls}:class")
        trace_loop_binding(sem, f"USE: apps/api/runtime/execution_fairness.py:_acquire_raw:{cls}:class")
        try:
            await asyncio.wait_for(asyncio.shield(sem.acquire()), timeout=_ACQUIRE_TIMEOUT_S)
            state.accepted[cls] = state.accepted.get(cls, 0) + 1
            return cls
        except asyncio.TimeoutError:
            pass
        try:
            assert_loop_owner(state.overflow, f"USE: apps/api/runtime/execution_fairness.py:_acquire_raw:{cls}:overflow")
            trace_loop_binding(state.overflow, f"USE: apps/api/runtime/execution_fairness.py:_acquire_raw:{cls}:overflow")
            await asyncio.wait_for(asyncio.shield(state.overflow.acquire()), timeout=_ACQUIRE_TIMEOUT_S)
            state.accepted["OVERFLOW"] = state.accepted.get("OVERFLOW", 0) + 1
            return "OVERFLOW"
        except asyncio.TimeoutError:
            pass
        state.rejected[cls] = state.rejected.get(cls, 0) + 1
        raise HTTPException(
            status_code=429,
            detail=f"class_capacity_exceeded:{cls}",
            headers={"Retry-After": "1"},
        )

    def _release_raw(self, pool: str) -> None:
        """Release a slot previously obtained via _acquire_raw."""
        state = self._state()
        if pool == "OVERFLOW":
            assert_loop_owner(state.overflow, "USE: apps/api/runtime/execution_fairness.py:_release_raw:OVERFLOW")
            trace_loop_binding(state.overflow, "USE: apps/api/runtime/execution_fairness.py:_release_raw:OVERFLOW")
            state.overflow.release()
        elif pool in state.semaphores:
            assert_loop_owner(state.semaphores[pool], f"USE: apps/api/runtime/execution_fairness.py:_release_raw:{pool}")
            trace_loop_binding(state.semaphores[pool], f"USE: apps/api/runtime/execution_fairness.py:_release_raw:{pool}")
            state.semaphores[pool].release()  # type: ignore[index]

    def dependency(self, cls: RequestClass):
        """Return a FastAPI dependency that guards this class slot."""

        async def _dep() -> None:
            async with self.acquire(cls):
                yield

        return _dep

    def snapshot(self) -> dict[str, object]:
        """Return depth and stats for the metrics endpoint."""
        state = self._state()
        depths: dict[str, int] = {}
        for cls, sem in state.semaphores.items():
            used = self._slots[cls] - sem._value
            depths[cls] = max(0, used)
        depths["OVERFLOW"] = max(0, self._overflow_count - state.overflow._value)
        return {
            "fairness_queue_depth": depths,
            "fairness_accepted": dict(state.accepted),
            "fairness_rejected": dict(state.rejected),
            "fairness_slots": dict(self._slots),
        }


# Module-level singleton (semaphores are lazy-initialised on first use).
fairness_gate = _FairnessGate(slots=_SLOTS, overflow=_OVERFLOW_SLOTS)
