from __future__ import annotations

from apps.api.integration_core.normalization import ExternalEvent
from apps.api.integration_core.os1_bridge import ingest_external_events


def test_ingest_external_events_batch_calls_os1_entrypoint(monkeypatch) -> None:
    captured_payloads: list[dict] = []

    def _fake_ingest_webhook(payload: dict):
        captured_payloads.append(payload)
        return {"status": "success", "event_id": payload["data"]["external_event_id"]}

    monkeypatch.setattr("apps.api.integration_core.os1_bridge.ingest_webhook", _fake_ingest_webhook)

    events = [
        ExternalEvent(
            event_id="ext-1",
            user_id="u-1",
            provider_name="gmail",
            event_type="external_event",
            timestamp="2026-02-01T09:00:00",
            payload={"subject": "A"},
        ),
        ExternalEvent(
            event_id="ext-2",
            user_id="u-1",
            provider_name="google_calendar",
            event_type="external_event",
            timestamp="2026-02-01T10:00:00",
            payload={"title": "B"},
        ),
    ]

    result = ingest_external_events("u-1", events)

    assert result["total_events"] == 2
    assert result["ingested_count"] == 2
    assert len(captured_payloads) == 2


def test_external_event_conversion_matches_os1_webhook_shape(monkeypatch) -> None:
    captured_payloads: list[dict] = []

    def _fake_ingest_webhook(payload: dict):
        captured_payloads.append(payload)
        return {"status": "success", "event_id": "ok"}

    monkeypatch.setattr("apps.api.integration_core.os1_bridge.ingest_webhook", _fake_ingest_webhook)

    event = ExternalEvent(
        event_id="ext-abc",
        user_id="u-src",
        provider_name="gmail",
        event_type="calendar_event",
        timestamp="2026-02-01T11:00:00",
        payload={"foo": "bar"},
    )

    ingest_external_events("u-target", [event])
    payload = captured_payloads[0]

    assert payload["source"] == "integration_core:gmail"
    assert payload["type"] == "calendar_event"
    assert payload["timestamp"] == "2026-02-01T11:00:00"
    assert payload["data"]["user_id"] == "u-target"
    assert payload["data"]["external_event_id"] == "ext-abc"
    assert payload["data"]["provider_name"] == "gmail"
    assert payload["data"]["payload"] == {"foo": "bar"}


def test_ingest_external_events_is_deterministic_for_same_inputs(monkeypatch) -> None:
    def _fake_ingest_webhook(payload: dict):
        return {"status": "success", "event_id": payload["data"]["external_event_id"]}

    monkeypatch.setattr("apps.api.integration_core.os1_bridge.ingest_webhook", _fake_ingest_webhook)

    events = [
        ExternalEvent(
            event_id="ext-1",
            user_id="u-1",
            provider_name="gmail",
            event_type="external_event",
            timestamp="2026-02-01T09:00:00",
            payload={"subject": "A"},
        )
    ]

    first = ingest_external_events("u-1", events)
    second = ingest_external_events("u-1", events)

    assert first == second
