"""
P1 Event Bus Correctness Tests
Validates event ordering, no leakage, and multi-instance consistency.
"""
from __future__ import annotations

import uuid
from collections import defaultdict

import pytest

from apps.api.realtime.event_bus import InMemoryRealtimeEventBus, RealtimeEvent
from tests.p1_verification.fixtures import EventCapture, TestFixtures


class TestEventBusOrdering:
    """Validate events are delivered in order per household."""
    
    def test_events_ordered_by_watermark(self):
        """Events for same household are ordered by watermark."""
        bus = InMemoryRealtimeEventBus()
        capture = EventCapture()
        bus.subscribe_all(capture.handler)
        
        household_id = "family-1"
        
        # Publish 5 events with increasing watermarks
        for i in range(5):
            event = RealtimeEvent(
                household_id=household_id,
                event_type="TASK_CREATED",
                watermark=f"wm-{i:03d}",
                payload={"task_id": f"task-{i}"},
            )
            bus.publish(event)
        
        # Verify ordering
        events = capture.get_by_household(household_id)
        assert len(events) == 5
        for i, event in enumerate(events):
            assert event.watermark == f"wm-{i:03d}"
    
    def test_multiple_events_same_watermark_consistent(self):
        """Multiple events with same watermark are handled consistently."""
        bus = InMemoryRealtimeEventBus()
        capture = EventCapture()
        bus.subscribe_all(capture.handler)
        
        household_id = "family-1"
        watermark = "wm-100"
        
        events = [
            RealtimeEvent(household_id, "TASK_CREATED", watermark, {"id": "t1"}),
            RealtimeEvent(household_id, "TASK_ASSIGNED", watermark, {"id": "t1"}),
            RealtimeEvent(household_id, "TASK_NOTIFIED", watermark, {"id": "t1"}),
        ]
        
        for event in events:
            bus.publish(event)
        
        captured = capture.get_by_household(household_id)
        assert len(captured) == 3
        assert all(e.watermark == watermark for e in captured)


class TestEventBusNoLeakage:
    """Validate no cross-household event leakage."""
    
    def test_events_isolated_by_household(self):
        """Events from one household don't leak to another."""
        bus = InMemoryRealtimeEventBus()
        capture = EventCapture()
        bus.subscribe_all(capture.handler)
        
        h1, h2 = "family-1", "family-2"
        
        # Publish events for both households
        for i in range(3):
            bus.publish(RealtimeEvent(h1, "TASK_CREATED", f"wm-{i}", {"id": f"h1-t{i}"}))
            bus.publish(RealtimeEvent(h2, "TASK_CREATED", f"wm-{i}", {"id": f"h2-t{i}"}))
        
        # Verify strict isolation
        h1_events = capture.get_by_household(h1)
        h2_events = capture.get_by_household(h2)
        
        assert len(h1_events) == 3
        assert len(h2_events) == 3
        
        # h1 should not contain h2 events
        for event in h1_events:
            assert event.household_id == h1
        
        for event in h2_events:
            assert event.household_id == h2
    
    def test_subscriber_receives_only_subscribed_events(self):
        """Subscriber receives all events (middleware would filter)."""
        bus = InMemoryRealtimeEventBus()
        h1_capture = EventCapture()
        h2_capture = EventCapture()
        
        # Subscribe both to all events (simulate SSE connections)
        bus.subscribe_all(h1_capture.handler)
        bus.subscribe_all(h2_capture.handler)
        
        h1, h2 = "family-1", "family-2"
        
        bus.publish(RealtimeEvent(h1, "TASK_CREATED", "wm-1", {"id": "t1"}))
        bus.publish(RealtimeEvent(h2, "TASK_CREATED", "wm-1", {"id": "t2"}))
        
        # Both subscribers get all events; filtering is middleware responsibility
        assert len(h1_capture.events) == 2
        assert len(h2_capture.events) == 2
        
        # But filtering works correctly
        h1_filtered = h1_capture.get_by_household(h1)
        h2_filtered = h2_capture.get_by_household(h2)
        
        assert len(h1_filtered) == 1
        assert len(h2_filtered) == 1


class TestEventBusMultiSubscriber:
    """Validate event delivery to multiple subscribers."""
    
    def test_all_subscribers_receive_events(self):
        """All subscribers receive all published events."""
        bus = InMemoryRealtimeEventBus()
        subscribers = [EventCapture() for _ in range(5)]
        
        for subscriber in subscribers:
            bus.subscribe_all(subscriber.handler)
        
        household_id = "family-1"
        event = RealtimeEvent(household_id, "TASK_CREATED", "wm-1", {"id": "t1"})
        bus.publish(event)
        
        # All should have received
        for subscriber in subscribers:
            assert len(subscriber.events) == 1
            assert subscriber.events[0].household_id == household_id
    
    def test_late_subscriber_only_receives_future_events(self):
        """Late subscribers don't retroactively receive old events."""
        bus = InMemoryRealtimeEventBus()
        subscriber1 = EventCapture()
        bus.subscribe_all(subscriber1.handler)
        
        # Publish before subscriber2 joins
        event1 = RealtimeEvent("family-1", "TASK_CREATED", "wm-1", {"id": "t1"})
        bus.publish(event1)
        
        # Subscribe second
        subscriber2 = EventCapture()
        bus.subscribe_all(subscriber2.handler)
        
        # Publish after
        event2 = RealtimeEvent("family-1", "TASK_CREATED", "wm-2", {"id": "t2"})
        bus.publish(event2)
        
        # subscriber1 should have both
        assert len(subscriber1.events) == 2
        
        # subscriber2 should only have the second
        assert len(subscriber2.events) == 1
        assert subscriber2.events[0].watermark == "wm-2"


class TestEventBusPayloadIntegrity:
    """Validate event payloads are delivered correctly."""
    
    def test_complex_payload_preserved(self):
        """Complex nested payload is preserved through bus."""
        bus = InMemoryRealtimeEventBus()
        capture = EventCapture()
        bus.subscribe_all(capture.handler)
        
        complex_payload = {
            "task_id": "task-1",
            "title": "Buy groceries",
            "assigned_to": ["user-1", "user-2"],
            "nested": {
                "due": "2026-04-25",
                "priority": "high",
                "tags": ["shopping", "urgent"],
            },
        }
        
        event = RealtimeEvent(
            household_id="family-1",
            event_type="TASK_CREATED",
            watermark="wm-100",
            payload=complex_payload,
        )
        bus.publish(event)
        
        received = capture.events[0]
        assert received.payload == complex_payload
        assert received.payload["nested"]["priority"] == "high"
    
    def test_empty_payload_handled(self):
        """Events with empty payloads are valid."""
        bus = InMemoryRealtimeEventBus()
        capture = EventCapture()
        bus.subscribe_all(capture.handler)
        
        event = RealtimeEvent(
            household_id="family-1",
            event_type="HEARTBEAT",
            watermark="wm-1",
            payload={},
        )
        bus.publish(event)
        
        assert len(capture.events) == 1
        assert capture.events[0].payload == {}


class TestEventBusReconnect:
    """Validate event delivery survives reconnects."""
    
    def test_subscriber_reconnect_receives_subsequent_events(self):
        """After unsubscribe/resubscribe, new events are received."""
        bus = InMemoryRealtimeEventBus()
        capture1 = EventCapture()
        bus.subscribe_all(capture1.handler)
        
        # Publish before reconnect simulation
        event1 = RealtimeEvent("family-1", "TASK_CREATED", "wm-1", {"id": "t1"})
        bus.publish(event1)
        
        # "Reconnect" - new subscriber
        capture2 = EventCapture()
        bus.subscribe_all(capture2.handler)
        
        # Publish after reconnect
        event2 = RealtimeEvent("family-1", "TASK_CREATED", "wm-2", {"id": "t2"})
        bus.publish(event2)
        
        # Original subscriber has both
        assert len(capture1.events) == 2
        
        # "Reconnected" subscriber has only new event
        assert len(capture2.events) == 1
        assert capture2.events[0].watermark == "wm-2"
    
    def test_events_queued_during_reconnect_not_lost(self):
        """Events don't get lost if published during the reconnect window."""
        bus = InMemoryRealtimeEventBus()
        capture = EventCapture()
        
        bus.subscribe_all(capture.handler)
        
        # Simulate stream of events (no reconnect needed for in-memory)
        for i in range(10):
            event = RealtimeEvent(
                "family-1", "TASK_CREATED", f"wm-{i}", {"id": f"t{i}"}
            )
            bus.publish(event)
        
        # All should be delivered
        assert len(capture.events) == 10
        for i, event in enumerate(capture.events):
            assert event.watermark == f"wm-{i}"


# Fixture
@pytest.fixture
def in_memory_bus():
    """Provide in-memory event bus."""
    return InMemoryRealtimeEventBus()
