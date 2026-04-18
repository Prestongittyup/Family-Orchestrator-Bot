"""
Tests for the feature flag system (feature_flags.py) and its integration
with the OS-1 bridge (os1_bridge.py).

Covers:
  - flag defaults to disabled
  - accepted truthy env var values enable the flag
  - all non-truthy values keep the flag disabled
  - disabled ingestion: skips OS-1 entirely, returns "disabled" status
  - enabled ingestion: proceeds normally through OS-1 bridge
  - flag evaluated at call time (not import time), so monkeypatching env works
"""
from __future__ import annotations

import os

import pytest

from apps.api.integration_core.feature_flags import (
    INTEGRATION_CORE_INGESTION_ENABLED,
    flag_default,
    is_enabled,
)
from apps.api.integration_core.normalization import ExternalEvent
from apps.api.integration_core.os1_bridge import _IdempotencyStore, ingest_external_events


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_event(event_id: str = "ext-001") -> ExternalEvent:
    return ExternalEvent(
        event_id=event_id,
        user_id="user-flag-test",
        provider_name="gmail",
        event_type="email.received",
        timestamp="2026-04-16T09:00:00Z",
        payload={"subject": "Hello"},
    )


def _mock_ingest(call_log: list):
    def _inner(payload):
        call_log.append(payload["data"]["external_event_id"])
        return {"status": "success", "event_id": payload["data"]["external_event_id"]}
    return _inner


# ---------------------------------------------------------------------------
# Feature flag unit tests
# ---------------------------------------------------------------------------


class TestFeatureFlagDefaults:
    def test_ingestion_flag_defaults_to_false(self):
        # Ensure env var is absent for this test
        os.environ.pop(INTEGRATION_CORE_INGESTION_ENABLED, None)
        assert is_enabled(INTEGRATION_CORE_INGESTION_ENABLED) is False

    def test_flag_default_registered_as_false(self):
        assert flag_default(INTEGRATION_CORE_INGESTION_ENABLED) is False

    def test_unknown_flag_defaults_to_false(self):
        assert is_enabled("NONEXISTENT_FLAG_XYZ") is False

    def test_flag_default_unknown_returns_false(self):
        assert flag_default("NONEXISTENT_FLAG_XYZ") is False


class TestFeatureFlagTruthyValues:
    @pytest.mark.parametrize("truthy", ["1", "true", "True", "TRUE", "yes", "YES", "on", "ON"])
    def test_truthy_env_enables_flag(self, monkeypatch, truthy):
        monkeypatch.setenv(INTEGRATION_CORE_INGESTION_ENABLED, truthy)
        assert is_enabled(INTEGRATION_CORE_INGESTION_ENABLED) is True

    @pytest.mark.parametrize("falsy", ["0", "false", "no", "off", "", "disabled", "  "])
    def test_falsy_env_keeps_flag_disabled(self, monkeypatch, falsy):
        monkeypatch.setenv(INTEGRATION_CORE_INGESTION_ENABLED, falsy)
        assert is_enabled(INTEGRATION_CORE_INGESTION_ENABLED) is False

    def test_missing_env_var_is_safe(self, monkeypatch):
        monkeypatch.delenv(INTEGRATION_CORE_INGESTION_ENABLED, raising=False)
        assert is_enabled(INTEGRATION_CORE_INGESTION_ENABLED) is False


# ---------------------------------------------------------------------------
# Bridge: disabled flag skips OS-1
# ---------------------------------------------------------------------------


class TestIngestionDisabled:
    def _run_disabled(self, monkeypatch, call_log):
        monkeypatch.delenv(INTEGRATION_CORE_INGESTION_ENABLED, raising=False)
        monkeypatch.setattr(
            "apps.api.integration_core.os1_bridge.ingest_webhook", _mock_ingest(call_log)
        )
        return ingest_external_events(
            "u1", [_make_event("ext-A"), _make_event("ext-B")],
            idempotency_store=_IdempotencyStore(),
        )

    def test_os1_not_called_when_disabled(self, monkeypatch):
        call_log: list = []
        self._run_disabled(monkeypatch, call_log)
        assert call_log == []

    def test_top_level_status_is_disabled(self, monkeypatch):
        result = self._run_disabled(monkeypatch, [])
        assert result["status"] == "disabled"

    def test_ingested_count_is_zero_when_disabled(self, monkeypatch):
        result = self._run_disabled(monkeypatch, [])
        assert result["ingested_count"] == 0

    def test_total_events_still_reported_when_disabled(self, monkeypatch):
        result = self._run_disabled(monkeypatch, [])
        assert result["total_events"] == 2

    def test_each_result_has_disabled_status(self, monkeypatch):
        result = self._run_disabled(monkeypatch, [])
        for row in result["results"]:
            assert row["status"] == "disabled"
            assert row["result"] is None

    def test_event_ids_present_in_disabled_results(self, monkeypatch):
        result = self._run_disabled(monkeypatch, [])
        ids = {r["external_event_id"] for r in result["results"]}
        assert ids == {"ext-A", "ext-B"}

    def test_empty_batch_disabled_is_safe(self, monkeypatch):
        monkeypatch.delenv(INTEGRATION_CORE_INGESTION_ENABLED, raising=False)
        result = ingest_external_events("u1", [], idempotency_store=_IdempotencyStore())
        assert result["status"] == "disabled"
        assert result["results"] == []


# ---------------------------------------------------------------------------
# Bridge: enabled flag triggers OS-1
# ---------------------------------------------------------------------------


class TestIngestionEnabled:
    def _run_enabled(self, monkeypatch, call_log, events=None):
        monkeypatch.setenv(INTEGRATION_CORE_INGESTION_ENABLED, "true")
        monkeypatch.setattr(
            "apps.api.integration_core.os1_bridge.ingest_webhook", _mock_ingest(call_log)
        )
        evts = events or [_make_event("ext-C"), _make_event("ext-D")]
        return ingest_external_events("u1", evts, idempotency_store=_IdempotencyStore())

    def test_os1_called_for_each_event_when_enabled(self, monkeypatch):
        call_log: list = []
        self._run_enabled(monkeypatch, call_log)
        assert sorted(call_log) == ["ext-C", "ext-D"]

    def test_no_disabled_status_in_results_when_enabled(self, monkeypatch):
        result = self._run_enabled(monkeypatch, [])
        for row in result["results"]:
            assert row["status"] != "disabled"

    def test_ingested_count_matches_events_when_enabled(self, monkeypatch):
        result = self._run_enabled(monkeypatch, [])
        assert result["ingested_count"] == 2

    def test_top_level_status_key_absent_when_enabled(self, monkeypatch):
        """The 'disabled' shortcircuit status should not appear in normal results."""
        result = self._run_enabled(monkeypatch, [])
        assert result.get("status") != "disabled"

    def test_flag_evaluated_at_call_time(self, monkeypatch):
        """Toggling the env var between calls changes behaviour without restart."""
        call_log: list = []
        monkeypatch.setattr(
            "apps.api.integration_core.os1_bridge.ingest_webhook", _mock_ingest(call_log)
        )
        event = _make_event("ext-toggle")
        store = _IdempotencyStore()

        # First call: disabled
        monkeypatch.delenv(INTEGRATION_CORE_INGESTION_ENABLED, raising=False)
        r1 = ingest_external_events("u1", [event], idempotency_store=store)
        assert r1["status"] == "disabled"
        assert call_log == []

        # Second call: enabled
        monkeypatch.setenv(INTEGRATION_CORE_INGESTION_ENABLED, "1")
        r2 = ingest_external_events("u1", [event], idempotency_store=store)
        assert r2.get("status") != "disabled"
        assert call_log == ["ext-toggle"]
