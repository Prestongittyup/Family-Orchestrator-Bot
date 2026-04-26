from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from apps.api.schemas.canonical_event import CanonicalEventEnvelope
from apps.api.schemas.event import SystemEvent
from apps.api.services.canonical_event_adapter import CanonicalEventAdapter
from apps.api.realtime.broadcaster import HouseholdBroadcaster

ROOT = Path(__file__).resolve().parents[1]
APPS_API = ROOT / "apps" / "api"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_no_to_sse_anywhere_in_runtime_paths() -> None:
    runtime_targets = [
        APPS_API / "services" / "canonical_event_router.py",
        APPS_API / "services" / "event_replay_service.py",
        APPS_API / "realtime" / "event_bus.py",
    ]
    for file_path in runtime_targets:
        assert "to_sse" not in _read(file_path)


def test_router_has_no_conditional_sse_logic() -> None:
    router_file = APPS_API / "services" / "canonical_event_router.py"
    content = _read(router_file)

    assert "if to_sse:" not in content
    assert "from_transport" not in content
    assert "broadcaster.publish_sync(envelope)" in content


def test_no_direct_realtimeevent_outside_broadcaster() -> None:
    violations: list[str] = []
    for file_path in APPS_API.rglob("*.py"):
        rel = file_path.relative_to(ROOT).as_posix()
        if rel == "apps/api/realtime/broadcaster.py":
            continue
        if "RealtimeEvent(" in _read(file_path):
            violations.append(rel)

    assert not violations, f"RealtimeEvent constructor found outside broadcaster: {violations}"


def test_no_direct_sse_formatting_outside_broadcaster() -> None:
    violations: list[str] = []
    for file_path in APPS_API.rglob("*.py"):
        rel = file_path.relative_to(ROOT).as_posix()
        if rel == "apps/api/realtime/broadcaster.py":
            continue

        content = _read(file_path)
        if "_format_sse(" in content:
            violations.append(rel)

    assert not violations, f"_format_sse usage outside broadcaster: {violations}"


def test_replay_routes_via_router_without_transport_branching(monkeypatch) -> None:
    import apps.api.services.event_replay_service as replay_service

    calls: list[dict[str, object]] = []

    def _fake_route(envelope, **kwargs):
        calls.append({"envelope": envelope, "kwargs": kwargs})
        return []

    monkeypatch.setattr(replay_service.canonical_event_router, "route", _fake_route)

    class _Log:
        household_id = "hh-1"
        id = "event-1"
        type = "task_created"
        source = "replay_test"
        payload = {"x": 1}
        severity = "info"
        idempotency_key = "k-1"

    replay_service._route_replay_log_entry(_Log())

    assert len(calls) == 1
    kwargs = calls[0]["kwargs"]
    assert kwargs == {"persist": False, "dispatch": True}


def test_watermark_ignores_external_and_is_monotonic() -> None:
    b = HouseholdBroadcaster()

    e1 = CanonicalEventEnvelope(
        event_id=str(uuid4()),
        household_id="hh-watermark",
        event_type="task_created",
        actor_type="user",
        timestamp="2026-01-01T00:00:00+00:00",
        payload={"n": 1},
        source="test",
        watermark=999999,
    )
    e2 = CanonicalEventEnvelope(
        event_id=str(uuid4()),
        household_id="hh-watermark",
        event_type="task_created",
        actor_type="user",
        timestamp="2026-01-01T00:00:01+00:00",
        payload={"n": 2},
        source="test",
        watermark=1,
    )

    w1 = b._resolve_watermark(e1)
    w2 = b._resolve_watermark(e2)

    assert w1 != 999999
    assert w2 != 1
    assert w2 == w1 + 1


def test_redis_ingress_reenters_canonical_pipeline(monkeypatch) -> None:
    from apps.api.realtime.event_bus import RedisRealtimeEventBus

    class _FakePubSub:
        def listen(self):
            payload = {
                "event_id": "redis-1",
                "actor_type": "system_worker",
                "household_id": "hh-redis",
                "event_type": "task_created",
                "timestamp": "2026-01-01T00:00:00+00:00",
                "watermark": 500,
                "idempotency_key": "idemp-1",
                "source": "redis_test",
                "severity": "info",
                "payload": {"task": "demo"},
                "signature": None,
            }
            yield {"data": json.dumps(payload)}

    bus = RedisRealtimeEventBus("redis://localhost:6379/0")
    bus._enabled = True
    bus._pubsub = _FakePubSub()

    calls: list[dict[str, object]] = []

    def _fake_route(envelope, **kwargs):
        calls.append({"envelope": envelope, "kwargs": kwargs})
        return []

    import apps.api.services.canonical_event_router as router_module

    monkeypatch.setattr(router_module.canonical_event_router, "route", _fake_route)

    bus._listen_loop()

    assert len(calls) == 1
    kwargs = calls[0]["kwargs"]
    assert kwargs == {"persist": False, "dispatch": True}
    assert hasattr(calls[0]["envelope"], "event_type")


def test_single_emission_path_runtime_contract() -> None:
    event = SystemEvent(
        household_id="hh-path",
        event_id="evt-path",
        type="task_created",
        source="path_test",
        payload={"a": 1},
    )

    envelope = CanonicalEventAdapter.to_envelope(event)

    assert envelope.household_id == "hh-path"
    assert envelope.event_type == "task_created"
    assert envelope.payload == {"a": 1}


def test_no_transport_flags_in_repository() -> None:
    import os

    forbidden = ["to_sse", "from_transport"]

    for root, _, files in os.walk("apps"):
        for file_name in files:
            if file_name.endswith(".py"):
                path = os.path.join(root, file_name)
                with open(path, "r", encoding="utf-8") as fh:
                    content = fh.read()
                    for token in forbidden:
                        assert token not in content, f"{token} found in {path}"
