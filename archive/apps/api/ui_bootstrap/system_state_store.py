"""
System state read projection (GSCL + GRE view abstraction).

This store is read-only for UI bootstrap and can be populated by any projection
builder without exposing control-plane internals.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from threading import RLock
from typing import Literal

ModeType = Literal["NORMAL", "DEGRADED", "RECONCILIATION_HEAVY", "QUARANTINE_FOCUSED", "HALTED"]


@dataclass(frozen=True)
class SystemStateProjection:
    family_id: str
    mode: ModeType
    health_score: float
    active_repair_count: int
    last_reconciliation_at: datetime
    projection_epoch: str
    version: int


class SystemStateProjectionStore:
    """Family-scoped projection store for system state summaries."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._rows: dict[str, SystemStateProjection] = {}

    def get(self, family_id: str) -> SystemStateProjection | None:
        with self._lock:
            return self._rows.get(family_id)

    def upsert(self, projection: SystemStateProjection) -> None:
        with self._lock:
            self._rows[projection.family_id] = projection


DEFAULT_SYSTEM_STATE_STORE = SystemStateProjectionStore()
