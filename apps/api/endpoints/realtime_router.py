from __future__ import annotations

import os
from threading import Lock

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, StreamingResponse

from apps.api.realtime.broadcaster import broadcaster
from apps.api.observability.metrics import metrics

router = APIRouter(prefix="/v1/realtime", tags=["realtime"])

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

    global _sse_connections_in_use
    with _sse_connections_lock:
        if _sse_connections_in_use >= _MAX_SSE_CONNECTIONS:
            metrics.note_request_rejection("sse_limit")
            metrics.note_sse_connection_rejection()
            return JSONResponse({"detail": "too_many_sse_connections"}, status_code=429)
        _sse_connections_in_use += 1

    async def event_stream():
        global _sse_connections_in_use
        try:
            async for chunk in broadcaster.subscribe(household_id, last_watermark=last_watermark):
                yield chunk
        finally:
            with _sse_connections_lock:
                _sse_connections_in_use = max(0, _sse_connections_in_use - 1)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
