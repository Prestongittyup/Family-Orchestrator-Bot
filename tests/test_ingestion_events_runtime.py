from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from apps.api.ingestion.models import EmailValidationError, IngestionError
from apps.api.ingestion import service as ingestion_service


@dataclass
class _Flags:
    ingestion_enabled: bool = True
    tracing_enabled: bool = False
    debug_mode: bool = False


@dataclass
class _Email:
    email_id: str
    sender: str
    recipient: str
    subject: str
    body: str
    received_at: datetime
    provider: str


class _CaptureRouter:
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(event)


def test_valid_email_emits_email_parsed(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _CaptureRouter()

    monkeypatch.setattr(ingestion_service, "router", capture)
    monkeypatch.setattr(ingestion_service, "resolve_feature_flags", lambda household_id: _Flags())
    monkeypatch.setattr(
        ingestion_service,
        "validate_email_payload",
        lambda payload: _Email(
            email_id=str(payload["email_id"]),
            sender=str(payload["sender"]),
            recipient=str(payload["recipient"]),
            subject=str(payload["subject"]),
            body=str(payload["body"]),
            received_at=datetime.now(timezone.utc),
            provider=str(payload["provider"]),
        ),
    )
    monkeypatch.setattr(
        ingestion_service,
        "convert_email_to_os1_event",
        lambda **kwargs: {
            "household_id": "hh-001",
            "type": "email_received",
            "timestamp": "2026-04-23T00:00:00Z",
            "idempotency_key": "idem-1",
            "source": "email_ingestion",
            "payload": {
                "email_id": kwargs["email_id"],
                "sender": kwargs["sender"],
                "subject": kwargs["subject"],
                "body": kwargs["body"],
                "provider": kwargs["provider"],
            },
            "severity": "info",
        },
    )
    monkeypatch.setattr(ingestion_service.canonical_event_router, "route", lambda *args, **kwargs: None)

    result = ingestion_service.ingest_email(
        email_id="mail-1",
        sender="a@b.com",
        recipient="home@x.com",
        subject="Dinner",
        body="Please plan dinner",
        received_at="2026-04-23T00:00:00Z",
        provider="generic",
    )

    assert result["status"] == "success"
    assert capture.events
    assert capture.events[-1].type == "email_parsed"


def test_invalid_email_emits_email_parse_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _CaptureRouter()

    monkeypatch.setattr(ingestion_service, "router", capture)
    monkeypatch.setattr(
        ingestion_service,
        "validate_email_payload",
        lambda payload: (_ for _ in ()).throw(EmailValidationError("bad email")),
    )

    with pytest.raises(IngestionError):
        ingestion_service.ingest_email(
            email_id="mail-2",
            sender="bad",
            recipient="home@x.com",
            subject="",
            body="",
            received_at="2026-04-23T00:00:00Z",
            provider="generic",
        )

    assert capture.events
    assert capture.events[-1].type == "email_parse_failed"
