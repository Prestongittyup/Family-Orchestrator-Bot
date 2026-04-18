"""
External ingestion adapters for Family Orchestration Bot.

This package provides adapters for external systems (webhooks, emails) to
feed data into the OS-1 pipeline without modifying core logic.

Modules:
- models: Webhook and email input schemas
- normalization: Conversion functions (external format → OS-1 format)
- service: Central ingestion router (validate → normalize → route)
"""
from apps.api.ingestion.models import WebhookPayload, EmailInput, IngestionError
from apps.api.ingestion.service import ingest_webhook, ingest_email

__all__ = [
    "WebhookPayload",
    "EmailInput",
    "ingest_webhook",
    "ingest_email",
    "IngestionError",
]
