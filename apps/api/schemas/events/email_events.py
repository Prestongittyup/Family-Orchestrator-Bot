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
    state_summary: str | None = None
    reason: str | None = None
    importance_score: float | None = None
    importance_bucket: str | None = None
    rule_score: int | None = None
    base_score: int | None = None
    junk_score: float | None = None
    is_junk: bool | None = None
    needs_attention: bool | None = None
    triage_decision: str | None = None
    actions: list[dict[str, Any]] | None = None
    action_items: list[dict[str, Any]] | None = None
    calendar_candidates: list[dict[str, Any]] | None = None
    informational_items: list[dict[str, Any]] | None = None
    called_llm: bool | None = None
    thread_id: str | None = None
    latest_message_id: str | None = None
    thread_length: int | None = None
    force_fail: bool | None = None
    max_retries: int | None = None
