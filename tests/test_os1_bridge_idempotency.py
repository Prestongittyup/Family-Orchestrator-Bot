"""
Tests for the idempotency layer in apps.api.integration_core.os1_bridge.

Covers:
  - Duplicate event IDs do not re-trigger OS-1 (ingest_webhook not called again)
  - Duplicate entries receive status "duplicate_ignored"
  - Repeated batch ingestion (same batch twice) is stable and deterministic
  - Thread-safety: concurrent calls with overlapping event IDs deduplicate correctly
  - _IdempotencyStore.clear() resets state between test runs
"""
from __future__ import annotations

import threading
from typing import Any

import pytest

from apps.api.integration_core.normalization import ExternalEvent
from apps.api.integration_core.os1_bridge import _IdempotencyStore, ingest_external_events


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_event(event_id: str, provider: str = "gmail") -> ExternalEvent:
    return ExternalEvent(
        event_id=event_id,
        user_id="user-test",
        provider_name=provider,
        event_type="test.event",
        timestamp="2026-04-16T10:00:00Z",
        payload={"key": "value"},
    )


def _mock_ingest(call_log: list[str]):
    """Return a fake ingest_webhook that records which event_id it was called with."""
    def _inner(payload: dict[str, Any]) -> dict[str, Any]:
        call_log.append(payload["data"]["external_event_id"])
        return {"status": "success", "event_id": payload["data"]["external_event_id"]}
    return _inner


# ---------------------------------------------------------------------------
# Idempotency store unit tests
# ---------------------------------------------------------------------------


class TestIdempotencyStore:
    def test_new_store_is_empty(self):
        store = _IdempotencyStore()
        assert len(store) == 0

    def test_mark_and_check(self):
        store = _IdempotencyStore()
        assert not store.is_seen("evt-1")
        store.mark_seen("evt-1")
        assert store.is_seen("evt-1")

    def test_clear_resets_state(self):
        store = _IdempotencyStore()
        store.mark_seen("evt-1")
        store.clear()
        assert not store.is_seen("evt-1")
        assert len(store) == 0

    def test_multiple_ids_tracked_independently(self):
        store = _IdempotencyStore()
        store.mark_seen("a")
        store.mark_seen("b")
        assert store.is_seen("a")
        assert store.is_seen("b")
        assert not store.is_seen("c")


# ---------------------------------------------------------------------------
# Duplicate ingestion does not re-trigger OS-1
# ---------------------------------------------------------------------------


class TestDuplicateSuppression:
    def test_second_call_with_same_event_id_skips_os1(self, monkeypatch):
        call_log: list[str] = []
        store = _IdempotencyStore()
        monkeypatch.setattr(
            "apps.api.integration_core.os1_bridge.ingest_webhook", _mock_ingest(call_log)
        )
        event = _make_event("ext-aaa")

        ingest_external_events("u1", [event], idempotency_store=store)
        ingest_external_events("u1", [event], idempotency_store=store)

        # OS-1 must only have been called once
        assert call_log.count("ext-aaa") == 1

    def test_duplicate_status_is_duplicate_ignored(self, monkeypatch):
        store = _IdempotencyStore()
        monkeypatch.setattr(
            "apps.api.integration_core.os1_bridge.ingest_webhook",
            _mock_ingest([]),
        )
        event = _make_event("ext-bbb")

        ingest_external_events("u1", [event], idempotency_store=store)
        second = ingest_external_events("u1", [event], idempotency_store=store)

        assert second["results"][0]["status"] == "duplicate_ignored"
        assert second["results"][0]["result"] is None

    def test_mixed_batch_new_and_duplicate(self, monkeypatch):
        call_log: list[str] = []
        store = _IdempotencyStore()
        monkeypatch.setattr(
            "apps.api.integration_core.os1_bridge.ingest_webhook", _mock_ingest(call_log)
        )
        evt_old = _make_event("ext-old")
        evt_new = _make_event("ext-new")

        # First pass: ingest both
        ingest_external_events("u1", [evt_old, evt_new], idempotency_store=store)
        call_log.clear()

        # Second pass: evt_old is duplicate, evt_new is still a new event id
        evt_extra = _make_event("ext-extra")
        result = ingest_external_events("u1", [evt_old, evt_extra], idempotency_store=store)

        # Only ext-extra should reach OS-1
        assert call_log == ["ext-extra"]
        statuses = {r["external_event_id"]: r["status"] for r in result["results"]}
        assert statuses["ext-old"] == "duplicate_ignored"
        assert statuses["ext-extra"] != "duplicate_ignored"

    def test_ingested_count_excludes_duplicates(self, monkeypatch):
        store = _IdempotencyStore()
        monkeypatch.setattr(
            "apps.api.integration_core.os1_bridge.ingest_webhook",
            _mock_ingest([]),
        )
        event = _make_event("ext-ccc")
        ingest_external_events("u1", [event], idempotency_store=store)
        second = ingest_external_events("u1", [event], idempotency_store=store)

        assert second["ingested_count"] == 0
        assert second["total_events"] == 1


# ---------------------------------------------------------------------------
# Repeated batch ingestion is stable and deterministic
# ---------------------------------------------------------------------------


class TestRepeatedBatchStability:
    def test_same_batch_twice_produces_consistent_structure(self, monkeypatch):
        store = _IdempotencyStore()
        monkeypatch.setattr(
            "apps.api.integration_core.os1_bridge.ingest_webhook",
            _mock_ingest([]),
        )
        events = [_make_event(f"ext-{i:03d}") for i in range(5)]

        first = ingest_external_events("u1", events, idempotency_store=store)
        second = ingest_external_events("u1", events, idempotency_store=store)

        assert first["total_events"] == second["total_events"] == 5
        assert first["ingested_count"] == 5
        assert second["ingested_count"] == 0

        second_statuses = [r["status"] for r in second["results"]]
        assert all(s == "duplicate_ignored" for s in second_statuses)

    def test_third_run_still_all_duplicates(self, monkeypatch):
        store = _IdempotencyStore()
        monkeypatch.setattr(
            "apps.api.integration_core.os1_bridge.ingest_webhook",
            _mock_ingest([]),
        )
        events = [_make_event("ext-stable")]

        for _ in range(3):
            ingest_external_events("u1", events, idempotency_store=store)

        result = ingest_external_events("u1", events, idempotency_store=store)
        assert result["results"][0]["status"] == "duplicate_ignored"

    def test_different_users_same_event_id_both_deduplicated(self, monkeypatch):
        """Idempotency store is keyed on event_id only — same ID for different users
        is still considered a duplicate. This documents the current contract."""
        call_log: list[str] = []
        store = _IdempotencyStore()
        monkeypatch.setattr(
            "apps.api.integration_core.os1_bridge.ingest_webhook", _mock_ingest(call_log)
        )
        evt = _make_event("ext-shared")

        ingest_external_events("user-A", [evt], idempotency_store=store)
        result = ingest_external_events("user-B", [evt], idempotency_store=store)

        assert len(call_log) == 1
        assert result["results"][0]["status"] == "duplicate_ignored"


# ---------------------------------------------------------------------------
# Thread-safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_ingestion_deduplicates(self, monkeypatch):
        call_log: list[str] = []
        lock = threading.Lock()
        store = _IdempotencyStore()

        def _safe_mock(payload: dict[str, Any]) -> dict[str, Any]:
            eid = payload["data"]["external_event_id"]
            with lock:
                call_log.append(eid)
            return {"status": "success", "event_id": eid}

        monkeypatch.setattr("apps.api.integration_core.os1_bridge.ingest_webhook", _safe_mock)

        event = _make_event("ext-concurrent")
        errors: list[Exception] = []

        def _worker():
            try:
                ingest_external_events("u1", [event], idempotency_store=store)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=_worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        # Only one thread should have reached OS-1
        assert call_log.count("ext-concurrent") == 1
