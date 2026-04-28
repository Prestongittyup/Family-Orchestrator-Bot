from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from archive.apps.api.ingestion.models import EmailValidationError, IngestionError
from archive.apps.api.ingestion import service as ingestion_service


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
    monkeypatch.setattr(
        ingestion_service,
        "summarize_email_to_actions",
        lambda **kwargs: {
            "summary": "High priority email from team.",
            "importance_score": 0.82,
            "importance_bucket": "high",
            "priority_label": "high",
            "rule_score": 42,
            "action_items": [
                {
                    "title": "Reply to the dinner request",
                    "details": "email_subject",
                    "importance_score": 0.82,
                    "importance_bucket": "high",
                    "due_hint": "2026-04-24",
                    "source_line": 0,
                }
            ],
            "calendar_candidates": [
                {
                    "title": "Dinner planning check-in",
                    "time_hint": "2026-04-24",
                    "confidence": 0.71,
                    "source_line": 1,
                }
            ],
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
    assert result["analysis"]["importance_bucket"] == "high"
    assert result["analysis"]["action_items"][0]["title"] == "Reply to the dinner request"
    assert capture.events
    assert capture.events[-1].type == "email_parsed"
    parsed_fields = capture.events[-1].payload["parsed_fields"]
    assert parsed_fields["importance_bucket"] == "high"
    assert parsed_fields["priority_label"] == "high"
    assert parsed_fields["rule_score"] == 42
    assert parsed_fields["action_items"][0]["title"] == "Reply to the dinner request"


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


def test_junk_email_is_ignored_before_routing(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _CaptureRouter()
    routed_calls: list[object] = []

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
            "idempotency_key": "idem-junk-1",
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
    monkeypatch.setattr(
        ingestion_service,
        "summarize_email_to_actions",
        lambda **kwargs: {
            "summary": "Likely promotional or low-signal email.",
            "importance_score": 0.2,
            "importance_bucket": "low",
            "action_items": [],
            "calendar_candidates": [],
            "informational_items": [],
            "junk_score": 0.91,
            "is_junk": True,
            "triage_decision": "junk",
        },
    )
    monkeypatch.setattr(
        ingestion_service.canonical_event_router,
        "route",
        lambda *args, **kwargs: routed_calls.append((args, kwargs)),
    )

    result = ingestion_service.ingest_email(
        email_id="mail-junk-1",
        sender="noreply@offers.test",
        recipient="home@x.com",
        subject="Limited time sale",
        body="Unsubscribe for more offers",
        received_at="2026-04-23T00:00:00Z",
        provider="generic",
    )

    assert result["status"] == "ignored_junk"
    assert result["analysis"]["is_junk"] is True
    assert routed_calls == []
    assert capture.events
    assert capture.events[-1].type == "email_parse_failed"
    assert capture.events[-1].payload["reason"] == "junk_filtered"


def test_resolve_household_id_for_email_prefers_explicit_household_id() -> None:
    resolved = ingestion_service._resolve_household_id_for_email(
        recipient="member@home.test",
        explicit_household_id="hh-explicit-123",
    )

    assert resolved == "hh-explicit-123"


def test_resolve_household_id_for_email_uses_recipient_identity_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Repo:
        def get_user_by_email(self, email: str):
            assert email == "member@home.test"
            return type("U", (), {"household_id": "hh-user-555"})()

    monkeypatch.setattr(ingestion_service, "IdentityRepository", lambda: _Repo())
    monkeypatch.setattr(ingestion_service, "get_household_for_source", lambda source: "hh-fallback")

    resolved = ingestion_service._resolve_household_id_for_email(
        recipient="member@home.test",
        explicit_household_id=None,
    )

    assert resolved == "hh-user-555"


def test_resolve_household_id_for_email_falls_back_to_source_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Repo:
        def get_user_by_email(self, email: str):
            assert email == "unknown@home.test"
            return None

    monkeypatch.setattr(ingestion_service, "IdentityRepository", lambda: _Repo())
    monkeypatch.setattr(ingestion_service, "get_household_for_source", lambda source: "hh-fallback")

    resolved = ingestion_service._resolve_household_id_for_email(
        recipient="unknown@home.test",
        explicit_household_id=None,
    )

    assert resolved == "hh-fallback"
