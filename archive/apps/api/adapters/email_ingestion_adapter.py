from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class EmailInput(BaseModel):
    email_id: str
    household_id: str
    sender: str
    subject: str
    body: str
    received_at: str


def _ensure_iso_datetime(value: str) -> str:
    normalized = value.replace("Z", "+00:00")
    datetime.fromisoformat(normalized)
    return value


def convert_email_to_event(email: EmailInput) -> dict[str, Any]:
    """
    Convert deterministic mock email input into a valid /event payload.

    Mapping:
    - email_id -> idempotency_key
    - household_id -> household_id
    - subject -> event_title (carried in payload.event_title and payload.subject)
    - body -> event_payload (carried in payload.event_payload and payload.body)
    - sender -> metadata.sender (carried in payload.metadata.sender and payload.sender)
    - received_at -> timestamp
    """
    timestamp = _ensure_iso_datetime(email.received_at)

    return {
        "household_id": email.household_id,
        "type": "email_received",
        "source": "email_ingestion_adapter",
        "timestamp": timestamp,
        "severity": "info",
        "idempotency_key": email.email_id,
        "payload": {
            "subject": email.subject,
            "body": email.body,
            "sender": email.sender,
            "event_title": email.subject,
            "event_payload": email.body,
            "metadata": {
                "sender": email.sender,
            },
        },
    }


def send_email_as_event(client: Any, email: EmailInput):
    payload = convert_email_to_event(email)
    return client.post("/event", json=payload)
