from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class SystemEvent(BaseModel):
    household_id: str
    type: str
    source: str
    payload: dict
    severity: str = "info"
    timestamp: datetime | None = None
