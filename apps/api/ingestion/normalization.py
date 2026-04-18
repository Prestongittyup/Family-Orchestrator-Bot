"""
Webhook to OS-1 event format normalization.

Converts external webhook payloads into the standardized OS-1 /event format.
All conversions are deterministic and produce bitwise-identical results for
identical inputs (excluding timestamps).
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from apps.api.ingestion.models import WebhookPayload


HOUSEHOLD_MAPPING = {
    # Map external source identifiers to household_ids
    "calendar_api": "hh-001",
    "reminder_service": "hh-001",
    "email_ingestion": "hh-001",
    "webhook_generic": "hh-001",
}


def get_household_for_source(source: str) -> str:
    """
    Deterministic household mapping for external source.
    
    Args:
        source: External source identifier
        
    Returns:
        household_id string
        
    Raises:
        ValueError: If source has no mapping (must be explicitly defined)
    """
    if source not in HOUSEHOLD_MAPPING:
        raise ValueError(
            f"Source '{source}' has no household mapping. "
            f"Add mapping to HOUSEHOLD_MAPPING dict in apps/api/ingestion/normalization.py"
        )
    return HOUSEHOLD_MAPPING[source]


def compute_idempotency_key(source: str, timestamp: str, event_type: str) -> str:
    """
    Deterministic hash-based idempotency key.
    
    Ensures the same webhook payload processed twice produces identical idempotency_key
    (and thus is deduplicated by OS-1).
    
    Args:
        source: Webhook source identifier
        timestamp: ISO-8601 timestamp string
        event_type: Event type string
        
    Returns:
        Hex hash string (32 chars)
    """
    key_material = f"{source}|{timestamp}|{event_type}"
    return hashlib.sha256(key_material.encode()).hexdigest()


def convert_webhook_to_os1_event(webhook: WebhookPayload) -> dict[str, Any]:
    """
    Convert webhook payload to OS-1 /event format.
    
    Deterministically transforms external webhook into the standardized event format
    accepted by POST /event endpoint.
    
    Args:
        webhook: Validated WebhookPayload
        
    Returns:
        Dict with OS-1 event schema:
        {
            "household_id": str,
            "type": str,
            "timestamp": str,  # ISO-8601
            "idempotency_key": str,
            "source": str,
            "data": dict
        }
    """
    household_id = get_household_for_source(webhook.source)
    idempotency_key = compute_idempotency_key(
        webhook.source,
        webhook.timestamp,
        webhook.type
    )
    
    return {
        "household_id": household_id,
        "type": webhook.type,
        "timestamp": webhook.timestamp,
        "idempotency_key": idempotency_key,
        "source": webhook.source,
        "payload": webhook.data,
        "severity": "info",
    }


def convert_email_to_os1_event(
    email_id: str,
    sender: str,
    subject: str,
    body: str,
    received_at: datetime,
    provider: str = "generic"
) -> dict[str, Any]:
    """
    Convert email to OS-1 /event format.
    
    Transforms email metadata and content into a schedulable task or reminder.
    Email subject becomes task title; body becomes description.
    
    Args:
        email_id: Unique email identifier from provider
        sender: Email sender address
        subject: Email subject line (becomes task title)
        body: Email body (becomes task description)
        received_at: When email was received
        provider: Email provider ('gmail', 'outlook', 'imap', 'generic')
        
    Returns:
        Dict with OS-1 event schema for email-derived task
    """
    household_id = get_household_for_source("email_ingestion")
    
    # Normalize timestamp to UTC ISO-8601 with trailing Z for deterministic parsing.
    if received_at.tzinfo is None:
        normalized_dt = received_at.replace(tzinfo=timezone.utc)
    else:
        normalized_dt = received_at.astimezone(timezone.utc)
    timestamp_str = normalized_dt.isoformat().replace("+00:00", "Z")
    
    idempotency_key = compute_idempotency_key(
        "email_ingestion",
        timestamp_str,
        f"email_from_{sender}"
    )
    
    return {
        "household_id": household_id,
        "type": "email_received",
        "timestamp": timestamp_str,
        "idempotency_key": idempotency_key,
        "source": "email_ingestion",
        "payload": {
            "email_id": email_id,
            "sender": sender,
            "provider": provider,
            "subject": subject,
            "body": body,
        },
        "severity": "info",
    }
