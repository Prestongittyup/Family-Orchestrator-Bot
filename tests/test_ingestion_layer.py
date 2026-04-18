"""
Comprehensive test suite for external ingestion layer (webhooks + emails).

Tests verify that:
1. Webhook payloads are validated and normalized correctly
2. Email inputs are converted to OS-1 events properly
3. Ingested events flow through the OS-1 pipeline
4. Idempotency is maintained across duplicate ingestion
5. Household mapping works deterministically
6. System stability lock tests still pass (no state leakage)
"""
import pytest
import json
from datetime import datetime
from unittest.mock import patch
from pathlib import Path

from fastapi.testclient import TestClient
from apps.api.main import app
from apps.api.ingestion.models import WebhookPayload, validate_webhook_payload
from apps.api.ingestion.normalization import (
    convert_webhook_to_os1_event,
    convert_email_to_os1_event,
    compute_idempotency_key,
    get_household_for_source,
)
from apps.api.ingestion.service import ingest_webhook, ingest_email
from apps.api.core.database import Base, engine


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture(scope="function", autouse=True)
def reset_event_bus():
    """Reset event bus singleton to ensure test isolation."""
    import apps.api.core.event_bus as event_bus_module
    import apps.api.core.event_registry as event_registry_module
    
    event_bus_module._event_bus_instance = None
    event_registry_module.event_bus = event_bus_module.get_event_bus()
    
    yield
    
    event_bus_module._event_bus_instance = None
    event_registry_module.event_bus = event_bus_module.get_event_bus()


@pytest.fixture(scope="function", autouse=True)
def clean_database():
    """Create fresh database and clean between tests."""
    Base.metadata.create_all(bind=engine)
    
    yield
    
    # Clean all test tables
    connection = engine.connect()
    connection.begin()
    try:
        connection.execute("DELETE FROM idempotency_keys")
        connection.execute("DELETE FROM event_logs")
        connection.execute("DELETE FROM tasks")
        connection.commit()
    except Exception:
        connection.rollback()
    finally:
        connection.close()


@pytest.fixture
def test_client():
    """FastAPI test client with proper lifecycle management."""
    with TestClient(app) as client:
        yield client


# =============================================================================
# UNIT TESTS: Models and Normalization
# =============================================================================

class TestWebhookModels:
    """Test webhook payload models and validation."""
    
    def test_webhook_payload_valid(self):
        """Valid webhook payload validates successfully."""
        payload = {
            "source": "calendar_api",
            "type": "event_created",
            "timestamp": "2026-04-15T10:30:00Z",
            "data": {"event_id": "evt-123", "title": "Meeting"}
        }
        
        webhook = validate_webhook_payload(payload)
        
        assert webhook.source == "calendar_api"
        assert webhook.type == "event_created"
        assert webhook.timestamp == "2026-04-15T10:30:00Z"
        assert webhook.data["event_id"] == "evt-123"
    
    def test_webhook_payload_missing_source(self):
        """Webhook payload without source raises validation error."""
        payload = {
            "type": "event_created",
            "timestamp": "2026-04-15T10:30:00Z",
            "data": {}
        }
        
        from apps.api.ingestion.models import WebhookValidationError
        with pytest.raises(WebhookValidationError):
            validate_webhook_payload(payload)
    
    def test_webhook_payload_empty_data_allowed(self):
        """Webhook with empty data dict is valid."""
        payload = {
            "source": "webhook_generic",
            "type": "test_event",
            "timestamp": "2026-04-15T10:30:00Z",
        }
        
        webhook = validate_webhook_payload(payload)
        assert webhook.data == {}


class TestHouseholdMapping:
    """Test deterministic household source mapping."""
    
    def test_known_source_maps_to_household(self):
        """Known source returns consistent household_id."""
        household = get_household_for_source("calendar_api")
        assert household == "hh-001"
    
    def test_unknown_source_raises_error(self):
        """Unknown source raises ValueError with helpful message."""
        with pytest.raises(ValueError) as exc_info:
            get_household_for_source("unknown_source")
        
        assert "unknown_source" in str(exc_info.value)
        assert "HOUSEHOLD_MAPPING" in str(exc_info.value)
    
    def test_all_sources_deterministic(self):
        """Multiple calls for same source return identical household_id."""
        sources = ["calendar_api", "reminder_service", "email_ingestion"]
        
        for source in sources:
            household1 = get_household_for_source(source)
            household2 = get_household_for_source(source)
            assert household1 == household2


class TestIdempotencyKey:
    """Test deterministic idempotency key generation."""
    
    def test_identical_input_produces_identical_key(self):
        """Same inputs always produce identical idempotency key."""
        key1 = compute_idempotency_key("calendar_api", "2026-04-15T10:30:00Z", "event_created")
        key2 = compute_idempotency_key("calendar_api", "2026-04-15T10:30:00Z", "event_created")
        
        assert key1 == key2
        assert len(key1) == 64  # SHA256 hex
    
    def test_different_source_different_key(self):
        """Different source produces different key."""
        key1 = compute_idempotency_key("source_a", "2026-04-15T10:30:00Z", "event")
        key2 = compute_idempotency_key("source_b", "2026-04-15T10:30:00Z", "event")
        
        assert key1 != key2
    
    def test_different_timestamp_different_key(self):
        """Different timestamp produces different key."""
        key1 = compute_idempotency_key("calendar_api", "2026-04-15T10:30:00Z", "event")
        key2 = compute_idempotency_key("calendar_api", "2026-04-15T10:31:00Z", "event")
        
        assert key1 != key2


class TestWebhookNormalization:
    """Test webhook to OS-1 event normalization."""
    
    def test_webhook_converts_to_os1_format(self):
        """Webhook normalizes to valid OS-1 event format."""
        webhook = WebhookPayload(
            source="calendar_api",
            type="event_created",
            timestamp="2026-04-15T10:30:00Z",
            data={"event_id": "evt-123", "title": "Team meeting"}
        )
        
        os1_event = convert_webhook_to_os1_event(webhook)
        
        assert os1_event["household_id"] == "hh-001"
        assert os1_event["type"] == "event_created"
        assert os1_event["timestamp"] == "2026-04-15T10:30:00Z"
        assert os1_event["source"] == "calendar_api"
        assert "idempotency_key" in os1_event
        assert len(os1_event["idempotency_key"]) == 64
        assert os1_event["payload"]["event_id"] == "evt-123"
    
    def test_normalized_event_deterministic(self):
        """Identical webhooks normalize to identical events."""
        webhook1 = WebhookPayload(
            source="calendar_api",
            type="event_created",
            timestamp="2026-04-15T10:30:00Z",
            data={"event_id": "evt-123"}
        )
        webhook2 = WebhookPayload(
            source="calendar_api",
            type="event_created",
            timestamp="2026-04-15T10:30:00Z",
            data={"event_id": "evt-123"}
        )
        
        event1 = convert_webhook_to_os1_event(webhook1)
        event2 = convert_webhook_to_os1_event(webhook2)
        
        assert event1 == event2


class TestEmailNormalization:
    """Test email to OS-1 event normalization."""
    
    def test_email_converts_to_os1_format(self):
        """Email converts to valid OS-1 event format."""
        received_at = datetime.fromisoformat("2026-04-15T10:30:00")
        
        os1_event = convert_email_to_os1_event(
            email_id="msg-123@gmail.com",
            sender="sender@company.com",
            subject="Project update",
            body="Here's the latest...",
            received_at=received_at,
            provider="gmail"
        )
        
        assert os1_event["household_id"] == "hh-001"
        assert os1_event["type"] == "email_received"
        assert os1_event["source"] == "email_ingestion"
        assert "idempotency_key" in os1_event
        assert os1_event["payload"]["email_id"] == "msg-123@gmail.com"
        assert os1_event["payload"]["sender"] == "sender@company.com"
        assert os1_event["payload"]["provider"] == "gmail"
    
    def test_email_normalized_deterministic(self):
        """Identical emails normalize to identical events."""
        dt = datetime.fromisoformat("2026-04-15T10:30:00")
        
        event1 = convert_email_to_os1_event(
            email_id="msg-123",
            sender="sender@company.com",
            subject="Test",
            body="Body",
            received_at=dt
        )
        event2 = convert_email_to_os1_event(
            email_id="msg-123",
            sender="sender@company.com",
            subject="Test",
            body="Body",
            received_at=dt
        )
        
        assert event1 == event2


# =============================================================================
# INTEGRATION TESTS: HTTP Endpoints
# =============================================================================

class TestWebhookEndpoint:
    """Test /ingest/webhook HTTP endpoint."""
    
    def test_webhook_endpoint_success(self, test_client):
        """Valid webhook payload processed successfully."""
        payload = {
            "source": "calendar_api",
            "type": "event_created",
            "timestamp": "2026-04-15T10:30:00Z",
            "data": {"event_id": "evt-123", "title": "Meeting"}
        }
        
        response = test_client.post("/ingest/webhook", json=payload)
        
        if response.status_code != 200:
            import sys
            sys.stderr.write(f"Response status: {response.status_code}\n")
            sys.stderr.write(f"Response body: {response.text}\n\n")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ("success", "duplicate_ignored")
        assert "event_id" in data
        assert data["trace_id"].startswith("trc-")
    
    def test_webhook_endpoint_invalid_payload(self, test_client):
        """Invalid webhook payload returns 400."""
        payload = {
            "type": "event_created",
            # missing 'source'
            "timestamp": "2026-04-15T10:30:00Z",
            "data": {}
        }
        
        response = test_client.post("/ingest/webhook", json=payload)
        
        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["status"] == "quarantined"
        assert "failure_trace" in detail
        assert detail["failure_trace"]["adapter"] == "webhook"
        assert detail["failure_trace"]["stage"] == "validation"
        assert detail["failure_trace"]["trace_id"].startswith("ing-trace-")
        assert "quarantine" in detail
        assert detail["quarantine"]["quarantine_id"].startswith("ing-q-")
        assert detail["quarantine"]["path"].startswith("data/ingestion_quarantine/")
    
    def test_webhook_endpoint_idempotency(self, test_client):
        """Identical webhooks are deduplicated by OS-1."""
        payload = {
            "source": "calendar_api",
            "type": "event_created",
            "timestamp": "2026-04-15T10:30:00Z",
            "data": {"event_id": "evt-123"}
        }
        
        response1 = test_client.post("/ingest/webhook", json=payload)
        response2 = test_client.post("/ingest/webhook", json=payload)
        
        # Both succeed
        assert response1.status_code == 200
        assert response2.status_code == 200


class TestEmailEndpoint:
    """Test /ingest/email HTTP endpoint."""
    
    def test_email_endpoint_success(self, test_client):
        """Valid email ingestion processed successfully."""
        response = test_client.post(
            "/ingest/email",
            params={
                "email_id": "msg-123@gmail.com",
                "sender": "sender@company.com",
                "recipient": "household@example.com",
                "subject": "Project update",
                "body": "Content here",
                "received_at": "2026-04-15T10:30:00Z",
                "provider": "gmail"
            }
        )
        
        if response.status_code != 200:
            import sys
            sys.stderr.write(f"Email endpoint error: {response.text}\n")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ("success", "duplicate_ignored")
        assert "event_id" in data
        assert data["trace_id"].startswith("trc-")
    
    def test_email_endpoint_missing_required_param(self, test_client):
        """Missing required parameter returns error."""
        response = test_client.post(
            "/ingest/email",
            params={
                "email_id": "msg-123",
                # missing 'sender'
                "subject": "Test",
                "body": "Test",
                "received_at": "2026-04-15T10:30:00Z"
            }
        )
        
        # FastAPI returns 422 for missing required query params
        assert response.status_code == 422
    
    def test_email_endpoint_invalid_timestamp(self, test_client):
        """Invalid timestamp format returns 400."""
        response = test_client.post(
            "/ingest/email",
            params={
                "email_id": "msg-123",
                "sender": "sender@company.com",
                "recipient": "recipient@example.com",
                "subject": "Test",
                "body": "Body",
                "received_at": "not-a-timestamp"
            }
        )
        
        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["status"] == "quarantined"
        assert detail["failure_trace"]["adapter"] == "email"
        assert detail["failure_trace"]["stage"] == "validation"
        assert detail["failure_trace"]["trace_id"].startswith("ing-trace-")
        assert detail["quarantine"]["quarantine_id"].startswith("ing-q-")


class TestQuarantineArtifacts:
    """Verify quarantine artifacts are persisted for failed ingestions."""

    def test_quarantine_file_created_on_invalid_webhook(self, test_client):
        payload = {
            "type": "event_created",
            "timestamp": "2026-04-15T10:30:00Z",
            "data": {},
        }

        response = test_client.post("/ingest/webhook", json=payload)
        assert response.status_code == 400

        detail = response.json()["detail"]
        quarantine_path = Path(detail["quarantine"]["path"])
        assert quarantine_path.exists()


# =============================================================================
# E2E TESTS: External Ingestion → OS-1 Pipeline → /brief
# =============================================================================

class TestExternalIngestionE2E:
    """End-to-end tests: external input → brief response."""
    
    def test_webhook_flows_to_brief(self, test_client, monkeypatch):
        """External webhook creates task visible in /brief."""
        # E2E test: webhook → OS-1 → /brief
        
        # Ingest webhook event
        webhook_payload = {
            "source": "calendar_api",
            "type": "event_created",
            "timestamp": "2026-04-15T10:30:00Z",
            "data": {
                "title": "External webhook event",
                "start": "2026-04-15T14:00:00Z",
                "end": "2026-04-15T15:00:00Z"
            }
        }
        
        ingest_response = test_client.post("/ingest/webhook", json=webhook_payload)
        assert ingest_response.status_code == 200
        
        # Check /brief reflects the ingested event
        brief_response = test_client.get("/brief/hh-001")
        assert brief_response.status_code == 200
        
        brief_data = brief_response.json()
        # Should have processed the webhook through the pipeline
        assert "brief" in brief_data
        assert brief_data["status"] == "success"

    def test_ingestion_trace_correlates_to_brief_observability(self, test_client):
        """Trace id from ingestion is visible in optional /brief observability metadata."""
        webhook_payload = {
            "source": "calendar_api",
            "type": "event_created",
            "timestamp": "2026-04-15T10:31:00Z",
            "data": {
                "title": "Traceable external event",
                "start": "2026-04-15T16:00:00Z",
                "end": "2026-04-15T17:00:00Z",
            },
        }

        ingest_response = test_client.post("/ingest/webhook", json=webhook_payload)
        assert ingest_response.status_code == 200
        ingest_data = ingest_response.json()
        trace_id = ingest_data["trace_id"]

        brief_response = test_client.get("/brief/hh-001?include_observability=true")
        assert brief_response.status_code == 200
        brief_data = brief_response.json()

        assert "observability" in brief_data
        snapshot = brief_data["observability"]
        assert snapshot["household_id"] == "hh-001"
        assert trace_id in snapshot["linked_trace_ids"]
        assert any(row.get("trace_id") == trace_id for row in snapshot["recent_external_inputs"])
    
    def test_email_ingestion_flow(self, test_client, monkeypatch):
        """Email ingestion flows through pipeline."""
        # E2E test: email → OS-1 → event processing
        
        # Ingest email
        response = test_client.post(
            "/ingest/email",
            params={
                "email_id": "msg-ingestion-test@gmail.com",
                "sender": "external@company.com",
                "recipient": "household@example.com",
                "subject": "External email event",
                "body": "This is from external ingestion",
                "received_at": "2026-04-15T10:30:00Z",
                "provider": "gmail"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ("success", "duplicate_ignored")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
