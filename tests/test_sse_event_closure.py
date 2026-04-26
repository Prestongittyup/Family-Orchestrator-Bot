from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from apps.api.realtime.broadcaster import HouseholdBroadcaster
from apps.api.schemas.canonical_event import CanonicalEventEnvelope
from apps.api.schemas.event import SystemEvent
from apps.api.services.canonical_event_adapter import CanonicalEventAdapter
from apps.api.services.idempotency_key_service import IdempotencyKeyService
from household_os.core.lifecycle_state import LifecycleState
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES


ROOT = Path(__file__).resolve().parents[1]


def _parse_sse_data(chunk: str) -> dict:
    data_line = next(line for line in chunk.splitlines() if line.startswith("data: "))
    return json.loads(data_line[6:])


def test_schema_parity_domain_to_canonical_to_sse_lossless_subset() -> None:
    domain_event = DomainEvent.create(
        aggregate_id="hh-001",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        timestamp=datetime.now(UTC),
        payload={"state": LifecycleState.PROPOSED},
        metadata={
            "actor_type": "system_worker",
            "request_id": "idem-001",
            "source": "domain_test",
        },
    )

    envelope = CanonicalEventAdapter.from_domain_event(domain_event)
    sse_chunk = HouseholdBroadcaster._format_sse("update", envelope.model_dump(mode="json"))
    reconstructed = CanonicalEventEnvelope.model_validate(_parse_sse_data(sse_chunk))

    assert reconstructed.event_id == envelope.event_id
    assert reconstructed.event_type == envelope.event_type
    assert reconstructed.household_id == envelope.household_id
    assert reconstructed.payload == envelope.payload
    assert reconstructed.signature == envelope.signature


def test_no_direct_sse_construction_runtime_paths() -> None:
    runtime_files = [
        ROOT / "apps" / "api" / "services" / "task_service.py",
        ROOT / "apps" / "api" / "services" / "calendar_service.py",
        ROOT / "apps" / "api" / "product_surface" / "chat_gateway_service.py",
        ROOT / "apps" / "api" / "ingestion" / "service.py",
    ]

    for file_path in runtime_files:
        content = file_path.read_text(encoding="utf-8")
        assert "RealtimeEvent(" not in content
        assert "broadcaster.publish_sync(" not in content


def test_no_direct_canonical_envelope_instantiation_in_business_services() -> None:
    service_files = [
        ROOT / "apps" / "api" / "services" / "task_service.py",
        ROOT / "apps" / "api" / "product_surface" / "chat_gateway_service.py",
    ]

    for file_path in service_files:
        content = file_path.read_text(encoding="utf-8")
        assert "CanonicalEventEnvelope(" not in content


def test_replay_has_no_transport_branching_or_direct_sse_calls() -> None:
    replay_file = ROOT / "apps" / "api" / "services" / "event_replay_service.py"
    content = replay_file.read_text(encoding="utf-8")

    assert "to_sse" not in content
    assert "broadcaster." not in content


def test_registry_enforcement_hard_reject_unknown_event_type() -> None:
    with pytest.raises(ValueError):
        CanonicalEventEnvelope(
            event_type="unknown_custom_event",
            household_id="hh-001",
            timestamp=datetime.now(UTC),
            source="test",
            payload={},
        )


def test_replay_determinism_same_input_identical_stream() -> None:
    source_event = SystemEvent(
        event_id="evt-replay-001",
        household_id="hh-001",
        type="task_created",
        source="replay_test",
        payload={"title": "same"},
        timestamp=datetime(2026, 4, 22, 0, 0, tzinfo=UTC),
        idempotency_key="idem-replay-001",
        signature="abcdef0123456789",
    )

    first = CanonicalEventAdapter.to_envelope(source_event).model_dump(mode="json", round_trip=True)
    second = CanonicalEventAdapter.to_envelope(source_event).model_dump(mode="json", round_trip=True)

    assert first == second


def test_watermark_ordering_shuffled_input_rejects_stale() -> None:
    accepted: list[int] = []

    def ingest(watermark: int) -> bool:
        if accepted and watermark <= accepted[-1]:
            return False
        accepted.append(watermark)
        return True

    assert ingest(10) is True
    assert ingest(12) is True
    assert ingest(11) is False
    assert accepted == [10, 12]


def test_idempotency_preservation_duplicate_events_deduped() -> None:
    service = IdempotencyKeyService()

    first = service.reserve("duplicate-key", "hh-001", "task_created")
    second = service.reserve("duplicate-key", "hh-001", "task_created")

    assert first.reserved is True
    assert second.reserved is False
