from __future__ import annotations

from apps.api.schemas.events.email_events import EmailReceivedEvent


def evaluate_email_rules(data: EmailReceivedEvent) -> dict:
    priority = data.priority or "medium"
    tags: list[str] = []

    if data.priority == "high":
        priority = "urgent"

    if data.category == "finance":
        tags.append("financial")

    if "urgent" in data.subject.lower():
        priority = "urgent"

    return {
        "priority": priority,
        "tags": tags,
    }
