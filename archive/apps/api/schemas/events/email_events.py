from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class EmailReceivedEvent(BaseModel):
    subject: str
    sender: str | None = None
    recipient: str | None = None
    received_at: str | None = None
    provider: str | None = None
    body: str | None = None
    priority: str | None = None
    category: str | None = None
    summary: str | None = None
    importance_score: float | None = None
    importance_bucket: str | None = None
    junk_score: float | None = None
    is_junk: bool | None = None
    triage_decision: str | None = None
    action_items: list[dict[str, Any]] | None = None
    calendar_candidates: list[dict[str, Any]] | None = None
    informational_items: list[dict[str, Any]] | None = None
    force_fail: bool | None = None
    max_retries: int | None = None
