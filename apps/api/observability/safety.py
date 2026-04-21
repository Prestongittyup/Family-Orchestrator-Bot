"""
Runtime safety controls (kill switches).

All state is held in-memory and takes effect immediately — no restart needed.

Controls:
    disable_replay     — suppress event replay on reconnect (clients get RESYNC_REQUIRED)
    force_resync_mode  — force all SSE subscribers to receive RESYNC_REQUIRED on next event
    pause_writes       — reject all incoming event writes with 503

Admin endpoint exposed at:
    POST /admin/safety
    GET  /admin/safety
"""
from __future__ import annotations

from threading import Lock
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Thread-safe global switch state
# ---------------------------------------------------------------------------

class _SafetyState:
    def __init__(self) -> None:
        self._lock = Lock()
        self._disable_replay: bool = False
        self._force_resync_mode: bool = False
        self._pause_writes: bool = False

    # --- Readers (hot path — lock contention is minimal) ---

    @property
    def disable_replay(self) -> bool:
        with self._lock:
            return self._disable_replay

    @property
    def force_resync_mode(self) -> bool:
        with self._lock:
            return self._force_resync_mode

    @property
    def pause_writes(self) -> bool:
        with self._lock:
            return self._pause_writes

    # --- Bulk update ---

    def update(self, **kwargs: Any) -> None:
        with self._lock:
            for key, value in kwargs.items():
                if not hasattr(self, f"_{key}"):
                    raise ValueError(f"Unknown safety control: {key!r}")
                setattr(self, f"_{key}", bool(value))

    def snapshot(self) -> dict[str, bool]:
        with self._lock:
            return {
                "disable_replay": self._disable_replay,
                "force_resync_mode": self._force_resync_mode,
                "pause_writes": self._pause_writes,
            }


# Module-level singleton — imported everywhere that needs a safety check
safety = _SafetyState()


# ---------------------------------------------------------------------------
# Admin router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/admin", tags=["admin"])


class SafetyPayload(BaseModel):
    disable_replay: bool | None = None
    force_resync_mode: bool | None = None
    pause_writes: bool | None = None


@router.get("/safety")
async def get_safety() -> JSONResponse:
    """Return current safety control state."""
    return JSONResponse(safety.snapshot())


@router.post("/safety")
async def set_safety(payload: SafetyPayload) -> JSONResponse:
    """Dynamically adjust safety controls without restarting the server.

    Only provided fields are updated; omitted fields retain their current value.

    Example::

        POST /admin/safety
        {"disable_replay": true}
    """
    updates: dict[str, bool] = {}
    if payload.disable_replay is not None:
        updates["disable_replay"] = payload.disable_replay
    if payload.force_resync_mode is not None:
        updates["force_resync_mode"] = payload.force_resync_mode
    if payload.pause_writes is not None:
        updates["pause_writes"] = payload.pause_writes

    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided to update.")

    safety.update(**updates)

    from apps.api.observability.logging import log_event
    log_event("safety_controls_updated", changes=updates)

    return JSONResponse({"status": "ok", "current": safety.snapshot()})
