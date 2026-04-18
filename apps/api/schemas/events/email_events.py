from __future__ import annotations

from pydantic import BaseModel


class EmailReceivedEvent(BaseModel):
    subject: str
    sender: str | None = None
    body: str | None = None
    priority: str | None = None
    category: str | None = None
    force_fail: bool | None = None
    max_retries: int | None = None
