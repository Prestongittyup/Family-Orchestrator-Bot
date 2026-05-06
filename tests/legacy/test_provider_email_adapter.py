from __future__ import annotations
import pytest

import base64

from archive.apps.api.ingestion.adapters.provider_email_adapter import (

    ProviderEmailAdapter,
    normalize_provider_name,
)

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



def _urlsafe(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode("utf-8").rstrip("=")


@pytest.mark.integration
@pytest.mark.legacy
def test_google_payload_parser_supports_gmail_message_shape() -> None:
    adapter = ProviderEmailAdapter(provider_name="gmail")
    raw = {
        "id": "gmail-1",
        "internalDate": "1714521600000",
        "payload": {
            "headers": [
                {"name": "From", "value": "alerts@school.test"},
                {"name": "To", "value": "family@test.local"},
                {"name": "Subject", "value": "School update"},
            ],
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {"data": _urlsafe("Please review tomorrow's reminder")},
                }
            ],
        },
    }

    parsed = adapter.parse_message(raw)

    assert parsed.provider == "google"
    assert parsed.email_id == "gmail-1"
    assert parsed.sender == "alerts@school.test"
    assert parsed.recipient == "family@test.local"
    assert parsed.subject == "School update"
    assert "tomorrow" in parsed.body
    assert parsed.received_at.endswith("Z")


@pytest.mark.integration
@pytest.mark.legacy
def test_google_payload_parser_converts_html_body_to_plain_text() -> None:
    adapter = ProviderEmailAdapter(provider_name="gmail")
    raw = {
        "id": "gmail-html-1",
        "internalDate": "1714521600000",
        "payload": {
            "headers": [
                {"name": "From", "value": "alerts@school.test"},
                {"name": "To", "value": "family@test.local"},
                {"name": "Subject", "value": "Field trip reminder"},
            ],
            "parts": [
                {
                    "mimeType": "text/html",
                    "body": {"data": _urlsafe("<div><p>Please sign the permission slip.</p><br/><p>Due tomorrow.</p></div>")},
                }
            ],
        },
    }

    parsed = adapter.parse_message(raw)

    assert "<p>" not in parsed.body
    assert "permission slip" in parsed.body
    assert "Due tomorrow." in parsed.body


@pytest.mark.integration
@pytest.mark.legacy
def test_outlook_payload_parser_supports_graph_message_shape() -> None:
    adapter = ProviderEmailAdapter(provider_name="microsoft")
    raw = {
        "id": "outlook-1",
        "receivedDateTime": "2026-05-01T13:10:00Z",
        "subject": "Calendar invite",
        "from": {"emailAddress": {"address": "planner@work.test"}},
        "toRecipients": [
            {"emailAddress": {"address": "home@test.local"}},
        ],
        "body": {"content": "Please confirm attendance."},
    }

    parsed = adapter.parse_message(raw)

    assert parsed.provider == "outlook"
    assert parsed.email_id == "outlook-1"
    assert parsed.sender == "planner@work.test"
    assert parsed.recipient == "home@test.local"
    assert parsed.subject == "Calendar invite"
    assert parsed.body == "Please confirm attendance."
    assert parsed.received_at == "2026-05-01T13:10:00Z"


@pytest.mark.integration
@pytest.mark.legacy
def test_yahoo_payload_parser_supports_header_style_shape() -> None:
    adapter = ProviderEmailAdapter(provider_name="yahoo")
    raw = {
        "mid": "yahoo-1",
        "headers": {
            "from": "alerts@yahoo.test",
            "to": "home@test.local",
            "subject": "Delivery update",
        },
        "content": "Package arrives tomorrow",
        "receivedDate": "2026-05-01T08:15:00Z",
    }

    parsed = adapter.parse_message(raw)

    assert parsed.provider == "yahoo"
    assert parsed.email_id == "yahoo-1"
    assert parsed.sender == "alerts@yahoo.test"
    assert parsed.recipient == "home@test.local"
    assert parsed.subject == "Delivery update"
    assert parsed.body == "Package arrives tomorrow"
    assert parsed.received_at == "2026-05-01T08:15:00Z"


@pytest.mark.integration
@pytest.mark.legacy
def test_unknown_provider_uses_generic_parser() -> None:
    adapter = ProviderEmailAdapter(provider_name="AcmeMail")
    parsed = adapter.parse_message(
        {
            "id": "acme-1",
            "from": "sender@acme.test",
            "to": "household@test.local",
            "subject": "Hello",
            "body": "World",
            "received_at": "2026-05-02T09:00:00Z",
        }
    )

    assert parsed.provider == "acmemail"
    assert parsed.email_id == "acme-1"
    assert parsed.subject == "Hello"


@pytest.mark.integration
@pytest.mark.legacy
def test_provider_name_normalization_aliases() -> None:
    assert normalize_provider_name("GMAIL") == "google"
    assert normalize_provider_name("office365") == "outlook"
    assert normalize_provider_name("ymail") == "yahoo"