from __future__ import annotations

from pathlib import Path

import pytest

from apps.api.ingestion.adapters.execution_runner import (
    _reset_execution_runner_state_for_tests,
    run_email_ingestion_cycle,
)
from apps.api.ingestion.adapters.imap_email_adapter import ImapEmailAdapter
from apps.api.ingestion.adapters.mock_email_provider import MockEmailProviderAdapter


@pytest.fixture(autouse=True)
def reset_runner_state():
    _reset_execution_runner_state_for_tests()
    yield
    _reset_execution_runner_state_for_tests()


def _dataset() -> list[dict[str, str]]:
    return [
        {
            "id": "resilience-001",
            "from": "alerts@example.com",
            "to": "home@example.com",
            "subject": "Resilience test",
            "body": "Stable message",
            "received_at": "2026-04-15T10:00:00Z",
        }
    ]


def test_retry_does_not_duplicate_events_and_preserves_idempotency(test_client):
    class FlakyProvider(MockEmailProviderAdapter):
        def __init__(self):
            super().__init__(provider_name="api", fixed_poll_dataset=_dataset())
            self.calls = 0

        def poll_messages(self):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient adapter error")
            return super().poll_messages()

    adapter = FlakyProvider()

    run = run_email_ingestion_cycle(
        adapter,
        mode="poll",
        retry_attempts=2,
        backoff_seconds=0.0,
        max_backoff_seconds=0.0,
        sleep_fn=lambda _: None,
    )

    assert run["status"] == "ok"
    assert run["attempts_used"] == 2
    outcome = run["cycle_result"]["results"][0]["outcome"]
    assert outcome["status"] == "processed"
    assert outcome["result"]["status"] == "success"

    # Replay same input through runner and ensure idempotency still wins.
    second = run_email_ingestion_cycle(
        adapter,
        mode="poll",
        retry_attempts=1,
        backoff_seconds=0.0,
        max_backoff_seconds=0.0,
        sleep_fn=lambda _: None,
    )
    second_outcome = second["cycle_result"]["results"][0]["outcome"]
    assert second_outcome["status"] == "processed"
    assert second_outcome["result"]["status"] == "duplicate_ignored"


def test_rate_limiting_is_deterministic_for_replay():
    adapter = MockEmailProviderAdapter(provider_name="api", fixed_poll_dataset=_dataset())

    now_values = iter([1000.0, 1000.0, 1000.0])
    now_fn = lambda: next(now_values)

    first = run_email_ingestion_cycle(
        adapter,
        mode="poll",
        rate_limit_max_cycles=1,
        rate_limit_window_seconds=60.0,
        now_fn=now_fn,
    )
    assert first["status"] == "ok"

    second = run_email_ingestion_cycle(
        adapter,
        mode="poll",
        rate_limit_max_cycles=1,
        rate_limit_window_seconds=60.0,
        now_fn=now_fn,
    )
    third = run_email_ingestion_cycle(
        adapter,
        mode="poll",
        rate_limit_max_cycles=1,
        rate_limit_window_seconds=60.0,
        now_fn=now_fn,
    )

    assert second["status"] == "rate_limited"
    assert third["status"] == "rate_limited"
    assert second["retry_after_seconds"] == third["retry_after_seconds"]


def test_failures_still_route_to_quarantine_via_runner():
    adapter = ImapEmailAdapter(
        sandbox_mode=True,
        sandbox_messages=[
            {
                "uid": "bad-resilience-1",
                "envelope": {
                    "from": "alerts@example.com",
                    "to": "home@example.com",
                    "subject": "Malformed",
                },
                "body": "bad",
                "internaldate": "not-a-timestamp",
            }
        ],
    )

    run = run_email_ingestion_cycle(adapter, mode="poll")
    assert run["status"] == "ok"
    outcome = run["cycle_result"]["results"][0]["outcome"]
    assert outcome["status"] == "failed"
    detail = outcome["error"]["detail"]
    assert detail["status"] == "quarantined"
    assert Path(detail["quarantine"]["path"]).exists()


def test_os1_os2_outputs_remain_identical_across_retries(test_client):
    class FlakyProvider(MockEmailProviderAdapter):
        def __init__(self):
            super().__init__(provider_name="api", fixed_poll_dataset=_dataset())
            self.calls = 0

        def poll_messages(self):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient adapter error")
            return super().poll_messages()

    adapter = FlakyProvider()

    first = run_email_ingestion_cycle(
        adapter,
        mode="poll",
        retry_attempts=2,
        backoff_seconds=0.0,
        max_backoff_seconds=0.0,
        sleep_fn=lambda _: None,
    )
    assert first["status"] == "ok"

    brief_after_first = test_client.get("/brief/hh-001?include_observability=true")
    assert brief_after_first.status_code == 200
    first_payload = brief_after_first.json()

    second = run_email_ingestion_cycle(
        adapter,
        mode="poll",
        retry_attempts=2,
        backoff_seconds=0.0,
        max_backoff_seconds=0.0,
        sleep_fn=lambda _: None,
    )
    assert second["status"] == "ok"

    brief_after_second = test_client.get("/brief/hh-001?include_observability=true")
    assert brief_after_second.status_code == 200
    second_payload = brief_after_second.json()

    assert first_payload["brief"] == second_payload["brief"]
