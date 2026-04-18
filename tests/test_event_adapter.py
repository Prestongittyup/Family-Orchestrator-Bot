"""
Tests for apps.api.integration_core.event_adapter
---------------------------------------------------
Covers:
  - deterministic mapping (same input → identical output)
  - multi-provider input consistency
  - field preservation (event_id, timestamp, source_provider → source metadata)
  - payload canonical stability (key-ordering normalised)
"""
from __future__ import annotations

import copy

import pytest

from apps.api.integration_core.event_adapter import adapt_external_events, external_event_to_os1_payload
from apps.api.integration_core.normalization import ExternalEvent


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

GMAIL_EVENT = ExternalEvent(
    event_id="ext-aabbccdd001122334455aabb",
    user_id="user-001",
    provider_name="gmail",
    event_type="email.received",
    timestamp="2026-04-16T10:00:00Z",
    payload={"subject": "Hello", "from": "alice@example.com"},
)

CALENDAR_EVENT = ExternalEvent(
    event_id="ext-ffeeddcc998877665544ffee",
    user_id="user-001",
    provider_name="google_calendar",
    event_type="calendar.event_created",
    timestamp="2026-04-16T11:30:00Z",
    payload={"title": "Team standup", "location": "Zoom"},
)


# ---------------------------------------------------------------------------
# Determinism tests
# ---------------------------------------------------------------------------


class TestDeterministicMapping:
    def test_same_event_produces_identical_output(self):
        out1 = external_event_to_os1_payload(GMAIL_EVENT)
        out2 = external_event_to_os1_payload(GMAIL_EVENT)
        assert out1 == out2

    def test_deep_copy_input_produces_identical_output(self):
        """Frozen dataclass + copy should not affect result."""
        cloned = copy.deepcopy(GMAIL_EVENT)
        assert external_event_to_os1_payload(GMAIL_EVENT) == external_event_to_os1_payload(cloned)

    def test_batch_adapter_is_deterministic(self):
        batch = [GMAIL_EVENT, CALENDAR_EVENT]
        assert adapt_external_events(batch) == adapt_external_events(batch)

    def test_payload_with_unsorted_keys_normalised(self):
        """Payload key order in input must not affect the output payload dict."""
        event_z_first = ExternalEvent(
            event_id="ext-000000000000000000000000",
            user_id="u1",
            provider_name="test",
            event_type="test.event",
            timestamp="2026-01-01T00:00:00Z",
            payload={"z_key": 99, "a_key": 1},
        )
        event_a_first = ExternalEvent(
            event_id="ext-000000000000000000000000",
            user_id="u1",
            provider_name="test",
            event_type="test.event",
            timestamp="2026-01-01T00:00:00Z",
            payload={"a_key": 1, "z_key": 99},
        )
        assert (
            external_event_to_os1_payload(event_z_first)["data"]["payload"]
            == external_event_to_os1_payload(event_a_first)["data"]["payload"]
        )


# ---------------------------------------------------------------------------
# Field preservation tests
# ---------------------------------------------------------------------------


class TestFieldPreservation:
    def test_event_id_preserved_in_data(self):
        result = external_event_to_os1_payload(GMAIL_EVENT)
        assert result["data"]["external_event_id"] == GMAIL_EVENT.event_id

    def test_timestamp_preserved_at_top_level(self):
        result = external_event_to_os1_payload(GMAIL_EVENT)
        assert result["timestamp"] == GMAIL_EVENT.timestamp

    def test_source_metadata_contains_provider_name(self):
        result = external_event_to_os1_payload(GMAIL_EVENT)
        assert result["source"] == f"integration_core:{GMAIL_EVENT.provider_name}"

    def test_user_id_preserved_in_data(self):
        result = external_event_to_os1_payload(GMAIL_EVENT)
        assert result["data"]["user_id"] == GMAIL_EVENT.user_id

    def test_event_type_mapped_to_type_field(self):
        result = external_event_to_os1_payload(GMAIL_EVENT)
        assert result["type"] == GMAIL_EVENT.event_type

    def test_payload_content_preserved(self):
        result = external_event_to_os1_payload(GMAIL_EVENT)
        assert result["data"]["payload"]["subject"] == "Hello"
        assert result["data"]["payload"]["from"] == "alice@example.com"


# ---------------------------------------------------------------------------
# Multi-provider consistency tests
# ---------------------------------------------------------------------------


class TestMultiProviderConsistency:
    def test_batch_preserves_order(self):
        batch = [GMAIL_EVENT, CALENDAR_EVENT]
        results = adapt_external_events(batch)
        assert results[0]["source"] == "integration_core:gmail"
        assert results[1]["source"] == "integration_core:google_calendar"

    def test_each_provider_event_independent(self):
        results = adapt_external_events([GMAIL_EVENT, CALENDAR_EVENT])
        assert results[0]["data"]["external_event_id"] == GMAIL_EVENT.event_id
        assert results[1]["data"]["external_event_id"] == CALENDAR_EVENT.event_id

    def test_different_providers_produce_distinct_sources(self):
        results = adapt_external_events([GMAIL_EVENT, CALENDAR_EVENT])
        sources = {r["source"] for r in results}
        assert sources == {"integration_core:gmail", "integration_core:google_calendar"}

    def test_same_provider_multiple_events_all_deterministic(self):
        events = [
            ExternalEvent(
                event_id=f"ext-{'0' * 22}{i:02d}",
                user_id="u1",
                provider_name="gmail",
                event_type="email.received",
                timestamp=f"2026-04-16T0{i}:00:00Z",
                payload={"index": i},
            )
            for i in range(3)
        ]
        first_run = adapt_external_events(events)
        second_run = adapt_external_events(events)
        assert first_run == second_run
