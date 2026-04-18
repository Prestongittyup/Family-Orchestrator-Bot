"""
External ingestion data models for webhook and email sources.

These models define the contracts for external systems to feed data into OS-1.
All models are deterministic, stateless, and side-effect free.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class WebhookPayload(BaseModel):
    """
    Standard webhook ingestion contract.
    
    External systems POST to /ingest/webhook with this schema.
    """
    source: str = Field(..., description="Webhook source identifier (e.g., 'calendar', 'reminder_service')")
    type: str = Field(..., description="Event type (e.g., 'scheduled_event', 'reminder', 'task_created')")
    timestamp: str = Field(..., description="ISO-8601 timestamp of event")
    data: dict[str, Any] = Field(default_factory=dict, description="Event payload data")
    
    class Config:
        schema_extra = {
            "example": {
                "source": "calendar_api",
                "type": "event_created",
                "timestamp": "2026-04-15T10:30:00Z",
                "data": {
                    "event_id": "evt-123",
                    "title": "Team meeting",
                    "start": "2026-04-15T14:00:00Z",
                    "end": "2026-04-15T15:00:00Z",
                }
            }
        }


class EmailInput(BaseModel):
    """
    Email ingestion model supporting multiple provider formats.
    
    This is a provider-agnostic abstraction for:
    - Gmail-style emails
    - Outlook-style emails
    - IMAP-like payloads
    - Generic email objects
    """
    email_id: str = Field(..., description="Unique email identifier from provider")
    sender: str = Field(..., description="Email sender address")
    recipient: str = Field(..., description="Email recipient address")
    subject: str = Field(..., description="Email subject line")
    body: str = Field(..., description="Email body content")
    received_at: datetime = Field(..., description="Timestamp when email was received")
    provider: str = Field(default="generic", description="Email provider ('gmail', 'outlook', 'imap', 'generic')")
    
    class Config:
        schema_extra = {
            "example": {
                "email_id": "msg-abc123@gmail.com",
                "sender": "team@company.com",
                "recipient": "household@example.com",
                "subject": "Project update",
                "body": "Here's the latest on the project...",
                "received_at": "2026-04-15T10:30:00Z",
                "provider": "gmail"
            }
        }


class WebhookValidationError(Exception):
    """Raised when webhook payload fails validation."""
    pass


class EmailValidationError(Exception):
    """Raised when email payload fails validation."""
    pass


class IngestionError(Exception):
    """
    Structured ingestion error with trace/quarantine metadata.

    This is returned by adapter endpoints and does not change OS-1/OS-2 behavior.
    """

    def __init__(
        self,
        *,
        message: str,
        detail: dict[str, Any],
        status_code: int = 400,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail
        self.status_code = status_code


def validate_webhook_payload(payload: dict[str, Any]) -> WebhookPayload:
    """
    Validate and parse webhook payload into strongly-typed model.
    
    Args:
        payload: Raw webhook payload dict
        
    Returns:
        Validated WebhookPayload
        
    Raises:
        WebhookValidationError: If payload is invalid
    """
    try:
        return WebhookPayload(**payload)
    except Exception as e:
        raise WebhookValidationError(f"Invalid webhook payload: {str(e)}")


def validate_email_payload(payload: dict[str, Any]) -> EmailInput:
    """Validate and parse email payload into strongly-typed model."""
    try:
        return EmailInput(**payload)
    except Exception as e:
        raise EmailValidationError(f"Invalid email payload: {str(e)}")
