from __future__ import annotations
import pytest

from fastapi.testclient import TestClient

from archive.apps.api.ingestion.models import IngestionError
from app.main import app
import archive.apps.api.endpoints.ingestion_router as ingestion_router

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



@pytest.mark.integration
@pytest.mark.legacy
def test_ingest_email_route_delegates_to_service(monkeypatch) -> None:
    captured: dict[str, str | None] = {}

    def _fake_ingest_email(
        email_id: str,
        sender: str,
        recipient: str,
        subject: str,
        body: str,
        received_at: str,
        provider: str = "generic",
        household_id: str | None = None,
    ) -> dict[str, str]:
        captured["email_id"] = email_id
        captured["sender"] = sender
        captured["recipient"] = recipient
        captured["subject"] = subject
        captured["body"] = body
        captured["received_at"] = received_at
        captured["provider"] = provider
        captured["household_id"] = household_id
        return {"status": "success", "event_id": "evt-mail-1"}

    monkeypatch.setattr(ingestion_router, "ingest_email", _fake_ingest_email)

    payload = {
        "email_id": "mail-123",
        "sender": "school@example.test",
        "recipient": "family@example.test",
        "subject": "Field trip reminder",
        "body": "Please sign the permission slip.",
        "received_at": "2026-04-26T12:30:00Z",
        "provider": "generic",
        "household_id": "hh-123",
    }

    with TestClient(app) as client:
        response = client.post("/ingest/email", json=payload)

    assert response.status_code == 200
    assert response.json() == {"status": "success", "event_id": "evt-mail-1"}
    assert captured["email_id"] == payload["email_id"]
    assert captured["sender"] == payload["sender"]
    assert captured["recipient"] == payload["recipient"]
    assert captured["subject"] == payload["subject"]
    assert captured["body"] == payload["body"]
    assert captured["provider"] == payload["provider"]
    assert captured["household_id"] == payload["household_id"]
    assert "2026-04-26T12:30:00" in str(captured["received_at"])


@pytest.mark.integration
@pytest.mark.legacy
def test_ingest_email_route_maps_ingestion_error(monkeypatch) -> None:
    def _raise_ingestion_error(**_kwargs):
        raise IngestionError(
            message="Email ingestion failed",
            detail={"status": "quarantined", "failure_trace": {"stage": "validation"}},
            status_code=422,
        )

    monkeypatch.setattr(ingestion_router, "ingest_email", _raise_ingestion_error)

    payload = {
        "email_id": "mail-err-1",
        "sender": "sender@example.test",
        "recipient": "recipient@example.test",
        "subject": "Bad payload",
        "body": "n/a",
        "received_at": "2026-04-26T12:30:00Z",
    }

    with TestClient(app) as client:
        response = client.post("/ingest/email", json=payload)

    assert response.status_code == 422
    assert response.json() == {
        "detail": {
            "message": "Email ingestion failed",
            "detail": {
                "status": "quarantined",
                "failure_trace": {"stage": "validation"},
            },
        }
    }


@pytest.mark.integration
@pytest.mark.legacy
def test_ingest_webhook_route_delegates_to_service(monkeypatch) -> None:
    def _fake_ingest_webhook(payload):
        assert payload["source"] == "calendar_api"
        assert payload["type"] == "event_created"
        return {"status": "success", "event_id": "evt-webhook-1"}

    monkeypatch.setattr(ingestion_router, "ingest_webhook", _fake_ingest_webhook)

    payload = {
        "source": "calendar_api",
        "type": "event_created",
        "timestamp": "2026-04-26T12:30:00Z",
        "data": {"title": "Doctor visit"},
    }

    with TestClient(app) as client:
        response = client.post("/ingest/webhook", json=payload)

    assert response.status_code == 200
    assert response.json() == {"status": "success", "event_id": "evt-webhook-1"}


@pytest.mark.integration
@pytest.mark.legacy
def test_ingest_provider_email_route_parses_and_delegates(monkeypatch) -> None:
    captured: dict[str, str | None] = {}

    def _fake_ingest_email(
        email_id: str,
        sender: str,
        recipient: str,
        subject: str,
        body: str,
        received_at: str,
        provider: str = "generic",
        household_id: str | None = None,
    ) -> dict[str, str]:
        captured["email_id"] = email_id
        captured["sender"] = sender
        captured["recipient"] = recipient
        captured["subject"] = subject
        captured["body"] = body
        captured["received_at"] = received_at
        captured["provider"] = provider
        captured["household_id"] = household_id
        return {"status": "success", "event_id": "evt-provider-1"}

    monkeypatch.setattr(ingestion_router, "ingest_email", _fake_ingest_email)

    payload = {
        "payload": {
            "id": "outlook-42",
            "receivedDateTime": "2026-04-26T12:30:00Z",
            "subject": "Meeting reminder",
            "from": {"emailAddress": {"address": "alerts@contoso.test"}},
            "toRecipients": [{"emailAddress": {"address": "family@example.test"}}],
            "body": {"content": "Don't forget tomorrow's meeting."},
        },
        "household_id": "hh-777",
    }

    with TestClient(app) as client:
        response = client.post("/ingest/email/provider/outlook", json=payload)

    assert response.status_code == 200
    assert response.json() == {"status": "success", "event_id": "evt-provider-1"}
    assert captured["provider"] == "outlook"
    assert captured["email_id"] == "outlook-42"
    assert captured["sender"] == "alerts@contoso.test"
    assert captured["recipient"] == "family@example.test"
    assert captured["household_id"] == "hh-777"


@pytest.mark.integration
@pytest.mark.legacy
def test_ingest_provider_email_batch_route_returns_row_statuses(monkeypatch) -> None:
    def _fake_ingest_email(
        email_id: str,
        sender: str,
        recipient: str,
        subject: str,
        body: str,
        received_at: str,
        provider: str = "generic",
        household_id: str | None = None,
    ) -> dict[str, str]:
        if email_id == "":
            raise IngestionError(
                message="Email id missing",
                detail={"status": "quarantined"},
                status_code=422,
            )
        return {"status": "success", "event_id": f"evt-{email_id}"}

    monkeypatch.setattr(ingestion_router, "ingest_email", _fake_ingest_email)

    payload = {
        "payloads": [
            {
                "id": "mail-1",
                "from": "sender@example.test",
                "to": "recipient@example.test",
                "subject": "Valid",
                "body": "ok",
                "received_at": "2026-04-26T12:30:00Z",
            },
            {
                "from": "sender@example.test",
                "to": "recipient@example.test",
                "subject": "Invalid",
                "body": "missing id",
                "received_at": "2026-04-26T12:30:00Z",
            },
        ],
        "household_id": "hh-777",
    }

    with TestClient(app) as client:
        response = client.post("/ingest/email/provider/custommail/batch", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["provider"] == "custommail"
    assert body["count"] == 2
    assert body["processed_count"] == 1
    assert body["failed_count"] == 1
    assert body["results"][0]["status"] == "processed"
    assert body["results"][1]["status"] == "failed"