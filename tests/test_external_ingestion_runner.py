from __future__ import annotations

from pathlib import Path

from apps.api.core.feature_flags import set_household_feature_flags
from apps.api.ingestion.adapters.execution_runner import run_email_ingestion_cycle
from apps.api.ingestion.adapters.mock_email_provider import MockEmailProviderAdapter


def _valid_api_dataset() -> list[dict[str, str]]:
    return [
        {
            "id": "runner-api-001",
            "from": "service@example.com",
            "to": "home@example.com",
            "subject": "Runner poll cycle",
            "body": "Please process this message",
            "received_at": "2026-04-15T09:00:00Z",
        }
    ]


def test_runner_repeated_execution_deterministic_os1_os2_outcomes(test_client):
    adapter = MockEmailProviderAdapter(
        provider_name="api",
        fixed_poll_dataset=_valid_api_dataset(),
    )

    first = run_email_ingestion_cycle(adapter, mode="poll")
    assert first["status"] == "ok"
    assert first["summary"]["processed"] == 1
    first_result = first["cycle_result"]["results"][0]["outcome"]["result"]
    assert first_result["status"] == "success"

    brief_after_first = test_client.get("/brief/hh-001?include_observability=true")
    assert brief_after_first.status_code == 200
    first_payload = brief_after_first.json()

    second = run_email_ingestion_cycle(adapter, mode="poll")
    assert second["status"] == "ok"
    assert second["summary"]["processed"] == 1
    second_result = second["cycle_result"]["results"][0]["outcome"]["result"]
    assert second_result["status"] == "duplicate_ignored"

    brief_after_second = test_client.get("/brief/hh-001?include_observability=true")
    assert brief_after_second.status_code == 200
    second_payload = brief_after_second.json()

    # OS-2 brief output should remain stable because second run is idempotent duplicate.
    assert first_payload["brief"] == second_payload["brief"]



def test_runner_push_mode_uses_validation_and_quarantine():
    adapter = MockEmailProviderAdapter(provider_name="imap")
    adapter.queue_push_message(
        {
            "uid": "runner-push-bad-001",
            "envelope": {
                "from": "alerts@example.com",
                "to": "home@example.com",
                "subject": "Bad timestamp",
            },
            "body": "Malformed payload",
            "internaldate": "not-a-timestamp",
        }
    )

    run = run_email_ingestion_cycle(adapter, mode="push")
    assert run["status"] == "ok"
    assert run["summary"]["failed"] == 1

    outcome = run["cycle_result"]["results"][0]["outcome"]
    assert outcome["status"] == "failed"
    detail = outcome["error"]["detail"]
    assert detail["status"] == "quarantined"
    quarantine = detail["quarantine"]
    assert quarantine["quarantine_id"].startswith("ing-q-")
    assert Path(quarantine["path"]).exists()



def test_runner_respects_feature_flags_ingestion_gate():
    set_household_feature_flags("hh-001", {"ingestion_enabled": False})

    adapter = MockEmailProviderAdapter(
        provider_name="api",
        fixed_poll_dataset=_valid_api_dataset(),
    )

    run = run_email_ingestion_cycle(adapter, mode="poll")
    assert run["status"] == "ok"
    assert run["summary"]["processed"] == 1

    outcome = run["cycle_result"]["results"][0]["outcome"]
    assert outcome["status"] == "processed"
    assert outcome["result"]["status"] == "disabled"



def test_runner_invalid_mode_fails_fast():
    adapter = MockEmailProviderAdapter(provider_name="api", fixed_poll_dataset=[])

    try:
        run_email_ingestion_cycle(adapter, mode="stream")
    except ValueError as exc:
        assert "Unsupported ingestion mode" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unsupported mode")
