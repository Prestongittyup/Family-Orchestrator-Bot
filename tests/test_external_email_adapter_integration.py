from __future__ import annotations

from pathlib import Path

from apps.api.core.feature_flags import set_household_feature_flags
from apps.api.ingestion.adapters.email_integration_service import (
    ingest_polled_email_messages,
    ingest_push_email_messages,
)
from apps.api.ingestion.adapters.mock_email_provider import MockEmailProviderAdapter


def test_external_poll_email_flow_to_brief(test_client):
    adapter = MockEmailProviderAdapter(
        provider_name="imap",
        fixed_poll_dataset=[
            {
                "uid": "imap-001",
                "envelope": {
                    "from": "alerts@example.com",
                    "to": "home@example.com",
                    "subject": "Pay electric bill",
                },
                "body": "Please pay by Friday.",
                "internaldate": "2026-04-15T08:00:00Z",
            }
        ],
    )

    ingest_result = ingest_polled_email_messages(adapter)
    assert ingest_result["status"] == "ok"
    assert ingest_result["count"] == 1
    first_outcome = ingest_result["results"][0]["outcome"]
    assert first_outcome["status"] == "processed"
    assert first_outcome["result"]["status"] in {"success", "duplicate_ignored"}

    brief_response = test_client.get("/brief/hh-001?include_observability=true")
    assert brief_response.status_code == 200
    payload = brief_response.json()
    assert payload["status"] == "success"
    assert "brief" in payload
    assert "observability" in payload
    assert any(
        row.get("source") == "email_ingestion" for row in payload["observability"]["recent_external_inputs"]
    )


def test_quarantine_triggers_for_malformed_external_payload():
    adapter = MockEmailProviderAdapter(
        provider_name="imap",
        fixed_poll_dataset=[
            {
                "uid": "imap-bad-001",
                "envelope": {
                    "from": "alerts@example.com",
                    "to": "home@example.com",
                    "subject": "Malformed timestamp",
                },
                "body": "Broken message",
                "internaldate": "not-a-timestamp",
            }
        ],
    )

    ingest_result = ingest_polled_email_messages(adapter)
    row = ingest_result["results"][0]
    outcome = row["outcome"]

    assert outcome["status"] == "failed"
    detail = outcome["error"]["detail"]
    assert detail["status"] == "quarantined"
    quarantine = detail["quarantine"]
    assert quarantine["quarantine_id"].startswith("ing-q-")
    assert Path(quarantine["path"]).exists()


def test_feature_flags_gate_external_adapter_ingestion():
    set_household_feature_flags("hh-001", {"ingestion_enabled": False})

    adapter = MockEmailProviderAdapter(
        provider_name="api",
        fixed_poll_dataset=[
            {
                "id": "api-001",
                "from": "service@example.com",
                "to": "home@example.com",
                "subject": "Flag gate test",
                "body": "Should be disabled",
                "received_at": "2026-04-15T09:00:00Z",
            }
        ],
    )

    ingest_result = ingest_polled_email_messages(adapter)
    row = ingest_result["results"][0]
    outcome = row["outcome"]

    assert outcome["status"] == "processed"
    assert outcome["result"]["status"] == "disabled"


def test_external_adapter_deterministic_repeated_runs():
    fixed_dataset = [
        {
            "id": "api-det-001",
            "from": "service@example.com",
            "to": "home@example.com",
            "subject": "Determinism",
            "body": "Stable input",
            "received_at": "2026-04-15T09:30:00Z",
        }
    ]

    first = ingest_polled_email_messages(
        MockEmailProviderAdapter(provider_name="api", fixed_poll_dataset=fixed_dataset)
    )
    second = ingest_polled_email_messages(
        MockEmailProviderAdapter(provider_name="api", fixed_poll_dataset=fixed_dataset)
    )

    assert first["count"] == 1
    assert second["count"] == 1

    first_status = first["results"][0]["outcome"]["result"]["status"]
    second_status = second["results"][0]["outcome"]["result"]["status"]

    assert first_status == "success"
    assert second_status == "duplicate_ignored"


def test_push_simulation_uses_same_pipeline():
    adapter = MockEmailProviderAdapter(provider_name="imap")
    adapter.queue_push_message(
        {
            "uid": "push-001",
            "envelope": {
                "from": "alerts@example.com",
                "to": "home@example.com",
                "subject": "Push ingest",
            },
            "body": "Push content",
            "internaldate": "2026-04-15T10:00:00Z",
        }
    )

    ingest_result = ingest_push_email_messages(adapter)
    assert ingest_result["status"] == "ok"
    assert ingest_result["mode"] == "push"
    assert ingest_result["count"] == 1
    assert ingest_result["results"][0]["outcome"]["status"] == "processed"
