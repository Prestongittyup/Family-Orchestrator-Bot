from __future__ import annotations

import os
import asyncio
from threading import Lock

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, StreamingResponse

from apps.api.realtime.broadcaster import broadcaster
from apps.api.core.backpressure_middleware import _global_limiter
from apps.api.observability.metrics import metrics
from apps.api.runtime.sse_pressure_guard import sse_guard
from apps.api.runtime.execution_fairness import fairness_gate
from apps.api.runtime.loop_tracing import trace_loop_context

router = APIRouter(prefix="/v1/realtime", tags=["realtime"])

# Legacy limit kept as fallback; sse_guard enforces its own limits.
_MAX_SSE_CONNECTIONS = max(1, int(os.getenv("MAX_SSE_CONNECTIONS", "25")))
_sse_connections_lock = Lock()
_sse_connections_in_use = 0


@router.get("/stream")
async def stream_updates(
    household_id: str = Query(..., description="Household scope for real-time events"),
    last_watermark: str | None = Query(None, description="Last received event watermark for resumable streams. Triggers replay of missed events."),
) -> StreamingResponse:
    """SSE stream for household-scoped live updates with optional replay.
    
    Args:
        household_id: Household scope for events
        last_watermark: Last watermark received by client (on reconnect). If provided, replays buffered events > this watermark.
    """

    trace_loop_context(f"realtime_router.stream_updates:{household_id}")
    # --- Pressure guard: per-household + global limits --------------------
    ok, reason = sse_guard.acquire(household_id)
    if not ok:
        metrics.note_request_rejection(reason or "sse_pressure_guard")
        return JSONResponse(
            {"detail": reason or "sse_limit_exceeded"},
            status_code=429,
            headers={"Retry-After": "2"},
        )

    # --- Execution fairness: STREAM class semaphore -----------------------
    try:
        fairness_pool = await fairness_gate._acquire_raw("STREAM")
    except Exception:
        sse_guard.release(household_id)
        return JSONResponse(
            {"detail": "class_capacity_exceeded:STREAM"},
            status_code=429,
            headers={"Retry-After": "2"},
        )

    async def event_stream():
        trace_loop_context(f"realtime_router.event_stream:{household_id}")
        try:
            async for chunk in broadcaster.subscribe(household_id, last_watermark=last_watermark):
                yield chunk
        finally:
            sse_guard.release(household_id)
            fairness_gate._release_raw(fairness_pool)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
