from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


def _derive_system_event_registry() -> frozenset[str]:
    """Derive allowed event types from the SystemEvent handler registry."""
    bootstrap_path = Path(__file__).resolve().parents[1] / "core" / "bootstrap.py"
    if not bootstrap_path.exists():
        return frozenset()

    source = bootstrap_path.read_text(encoding="utf-8")
    matches = re.findall(r"register\(\s*['\"]([^'\"]+)['\"]\s*,", source)
    return frozenset(matches)


SYSTEM_EVENT_REGISTRY: frozenset[str] = _derive_system_event_registry()


def is_registered_event_type(event_type: str) -> bool:
    return event_type in SYSTEM_EVENT_REGISTRY


class CanonicalEventEnvelope(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str
    actor_type: str | None = None
    household_id: str
    timestamp: datetime
    watermark: int | None = None
    idempotency_key: str | None = None
    source: str
    severity: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    signature: str | None = None

    @model_validator(mode="after")
    def _validate_event_type(self) -> CanonicalEventEnvelope:
        if self.event_type not in SYSTEM_EVENT_REGISTRY:
            raise ValueError("Invalid canonical event type")
        return self

    @model_validator(mode="after")
    def _normalize_timestamp(self) -> CanonicalEventEnvelope:
        if self.timestamp.tzinfo is None:
            self.timestamp = self.timestamp.replace(tzinfo=timezone.utc)
        return self
