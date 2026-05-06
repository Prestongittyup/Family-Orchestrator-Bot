from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


@dataclass
class RealtimeEvent:
    household_id: str
    event_type: str
    watermark: int | str
    payload: dict[str, Any]
    event_id: str = field(default_factory=lambda: str(uuid4()))
    actor_type: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    idempotency_key: str | None = None
    source: str = "realtime_transport"
    severity: str | None = None
    signature: str | None = None
