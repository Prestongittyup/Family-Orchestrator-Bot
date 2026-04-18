from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import text

from apps.api.core.database import SessionLocal
from apps.api.endpoints.brief_endpoint import _clear_brief_cache
from apps.api.ingestion.adapters.execution_runner import (
    _reset_execution_runner_state_for_tests,
    get_ingestion_runtime_status,
    run_email_ingestion_cycle,
)
from apps.api.ingestion.adapters.ingestion_defaults import (
    get_ingestion_execution_config,
    list_ingestion_profiles,
)
from apps.api.ingestion.adapters.mock_email_provider import MockEmailProviderAdapter
from apps.api.services.worker import start_worker_loop, stop_worker_loop


@pytest.fixture(autouse=True)
def reset_runner_state() -> Iterator[None]:
    _reset_execution_runner_state_for_tests()
    _clear_brief_cache()
    yield
    _reset_execution_runner_state_for_tests()
    _clear_brief_cache()



def _clean_os_state() -> None:
    session = SessionLocal()
    try:
        session.execute(text("DELETE FROM tasks"))
        session.execute(text("DELETE FROM event_logs"))
        session.execute(text("DELETE FROM idempotency_keys"))
        try:
            session.execute(text("DELETE FROM calendar_events"))
        except Exception:
            pass
        session.commit()
    finally:
        session.close()



def _dataset(msg_id: str) -> list[dict[str, str]]:
    return [
        {
            "id": msg_id,
            "from": "ops@example.com",
            "to": "home@example.com",
            "subject": "Profile check",
            "body": "Stable profile payload",
            "received_at": "2026-04-15T09:00:00Z",
        }
    ]


def _normalize_brief_for_comparison(value):
    if isinstance(value, dict):
        return {
            key: _normalize_brief_for_comparison(item)
            for key, item in value.items()
            if key != "proposal_id"
        }
    if isinstance(value, list):
        return [_normalize_brief_for_comparison(item) for item in value]
    return value



def test_profiles_are_explicit_and_config_only(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("INGESTION_RETRY_ATTEMPTS", raising=False)
    monkeypatch.delenv("INGESTION_BACKOFF_SECONDS", raising=False)
    monkeypatch.delenv("INGESTION_MAX_BACKOFF_SECONDS", raising=False)
    monkeypatch.delenv("INGESTION_RATE_LIMIT_WINDOW_SECONDS", raising=False)
    monkeypatch.delenv("INGESTION_RATE_LIMIT_MAX_CYCLES", raising=False)

    profiles = list_ingestion_profiles()
    assert profiles == ["dev", "production", "staging"]

    dev = get_ingestion_execution_config(profile="dev")
    staging = get_ingestion_execution_config(profile="staging")
    production = get_ingestion_execution_config(profile="production")

    # Same config shape across profiles.
    keys = set(dev.__dict__.keys())
    assert keys == set(staging.__dict__.keys()) == set(production.__dict__.keys())

    # Profiles differ by configured values only.
    assert (dev.retry_attempts, dev.rate_limit_max_cycles) != (
        staging.retry_attempts,
        staging.rate_limit_max_cycles,
    )
    assert (staging.retry_attempts, staging.rate_limit_max_cycles) != (
        production.retry_attempts,
        production.rate_limit_max_cycles,
    )



def test_env_overrides_profile_deterministically(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("INGESTION_RETRY_ATTEMPTS", "4")
    monkeypatch.setenv("INGESTION_BACKOFF_SECONDS", "0.2")
    monkeypatch.setenv("INGESTION_MAX_BACKOFF_SECONDS", "0.8")
    monkeypatch.setenv("INGESTION_RATE_LIMIT_WINDOW_SECONDS", "30")
    monkeypatch.setenv("INGESTION_RATE_LIMIT_MAX_CYCLES", "7")

    cfg1 = get_ingestion_execution_config(profile="staging")
    cfg2 = get_ingestion_execution_config(profile="staging")

    assert cfg1 == cfg2
    assert cfg1.retry_attempts == 4
    assert cfg1.backoff_seconds == 0.2
    assert cfg1.max_backoff_seconds == 0.8
    assert cfg1.rate_limit_window_seconds == 30.0
    assert cfg1.rate_limit_max_cycles == 7



def test_runtime_status_reflects_active_config(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("INGESTION_EXECUTION_PROFILE", "production")

    adapter = MockEmailProviderAdapter(provider_name="api", fixed_poll_dataset=[])
    status = get_ingestion_runtime_status(adapter=adapter)

    assert status["active_profile"] == "production"
    assert "retry_configuration" in status
    assert "rate_limit_configuration" in status
    assert status["adapter_status"]["provider"] == "api"
    assert "password" not in str(status).lower()



def test_retry_and_rate_limit_remain_deterministic_with_fixed_injection(monkeypatch: pytest.MonkeyPatch):
    class FlakyProvider(MockEmailProviderAdapter):
        def __init__(self):
            super().__init__(provider_name="api", fixed_poll_dataset=_dataset("profile-det-1"))
            self.calls = 0

        def poll_messages(self):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient")
            return super().poll_messages()

    monkeypatch.setenv("INGESTION_EXECUTION_PROFILE", "production")
    monkeypatch.setenv("INGESTION_RATE_LIMIT_MAX_CYCLES", "1")
    monkeypatch.setenv("INGESTION_RATE_LIMIT_WINDOW_SECONDS", "60")

    adapter = FlakyProvider()

    now_values = iter([2000.0, 2000.0])
    now_fn = lambda: next(now_values)

    first = run_email_ingestion_cycle(
        adapter,
        mode="poll",
        sleep_fn=lambda _: None,
        now_fn=now_fn,
    )
    assert first["status"] == "ok"
    assert first["attempts_used"] == 2

    second = run_email_ingestion_cycle(
        adapter,
        mode="poll",
        sleep_fn=lambda _: None,
        now_fn=now_fn,
    )
    assert second["status"] == "rate_limited"
    assert second["retry_after_seconds"] == 60.0



def test_os1_os2_outputs_unchanged_across_profiles(test_client):
    baseline_brief = None
    stop_worker_loop()
    try:
        for profile in ("dev", "staging", "production"):
            _clean_os_state()
            _clear_brief_cache()
            _reset_execution_runner_state_for_tests()

            adapter = MockEmailProviderAdapter(
                provider_name="api",
                fixed_poll_dataset=_dataset("profile-os-stable-001"),
            )

            run = run_email_ingestion_cycle(
                adapter,
                mode="poll",
                profile=profile,
                sleep_fn=lambda _: None,
                now_fn=lambda: 3000.0,
            )
            assert run["status"] == "ok"

            # Duplicate replay should still be deduped regardless of profile.
            second = run_email_ingestion_cycle(
                adapter,
                mode="poll",
                profile=profile,
                sleep_fn=lambda _: None,
                now_fn=lambda: 3001.0,
            )
            assert second["status"] == "ok"
            second_outcome = second["cycle_result"]["results"][0]["outcome"]["result"]
            assert second_outcome["status"] == "duplicate_ignored"

            brief_response = test_client.get("/brief/hh-001")
            assert brief_response.status_code == 200
            payload = brief_response.json()
            assert payload["status"] == "success"

            if baseline_brief is None:
                baseline_brief = _normalize_brief_for_comparison(payload["brief"])
            else:
                assert _normalize_brief_for_comparison(payload["brief"]) == baseline_brief
    finally:
        start_worker_loop()
