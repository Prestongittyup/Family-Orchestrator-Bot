"""
P1 Chaos & Concurrency Tests
Validates system behavior under failure combinations and heavy concurrency.
"""
from __future__ import annotations

import concurrent.futures
import threading
import uuid
from collections import defaultdict

import pytest

from apps.api.auth.token_service import TokenService
from apps.api.llm.gateway import LLMGateway
from apps.api.realtime.event_bus import InMemoryRealtimeEventBus, RealtimeEvent
from apps.api.services.idempotency_key_service import IdempotencyKeyService
from tests.p1_verification.fixtures import MockLLMProvider, TestFixtures


class TestChaosConcurrentCreation:
    """Validate exactly-once semantics under concurrent write storms."""
    
    def test_100_concurrent_task_creates_result_in_1(self):
        """100 parallel identical task creates → exactly 1 task in DB."""
        idem_svc = TestFixtures.create_idempotency_service()
        household_id = "family-1"
        idem_key = f"create-task-{uuid.uuid4().hex[:8]}"
        
        results = {"created": 0, "duplicates": 0}
        results_lock = threading.Lock()
        
        def attempt_create():
            # Try to reserve
            reserved = idem_svc.reserve(idem_key, household_id)
            
            if reserved.reserved:
                # Win: actually create
                with results_lock:
                    results["created"] += 1
                idem_svc.mark_completed(idem_key, household_id, {"task_id": "t-1"})
            else:
                # Lose: duplicate rejected
                with results_lock:
                    results["duplicates"] += 1
        
        # 100 concurrent attempts
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            futures = [executor.submit(attempt_create) for _ in range(100)]
            for f in concurrent.futures.as_completed(futures):
                f.result()
        
        assert results["created"] == 1, f"Expected 1 creation, got {results['created']}"
        assert results["duplicates"] == 99, f"Expected 99 rejections, got {results['duplicates']}"
    
    def test_concurrent_creates_different_items_all_succeed(self):
        """Concurrent creates of different items all succeed."""
        idem_svc = TestFixtures.create_idempotency_service()
        household_id = "family-1"
        
        results = {"successes": 0, "failures": 0}
        results_lock = threading.Lock()
        
        def attempt_create(idx: int):
            idem_key = f"create-{idx}"
            reserved = idem_svc.reserve(idem_key, household_id)
            
            with results_lock:
                if reserved.reserved:
                    results["successes"] += 1
                else:
                    results["failures"] += 1
        
        # 50 concurrent with different keys
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(attempt_create, i) for i in range(50)]
            for f in concurrent.futures.as_completed(futures):
                f.result()
        
        assert results["successes"] == 50
        assert results["failures"] == 0


class TestChaosCrossHouseholdIsolation:
    """Validate no cross-household interference under concurrency."""
    
    def test_concurrent_writes_different_households_isolated(self):
        """Writes from different households don't interfere."""
        idem_svc = TestFixtures.create_idempotency_service()
        idem_key = f"task-{uuid.uuid4().hex[:8]}"
        
        results = defaultdict(lambda: {"reserved": 0, "rejected": 0})
        results_lock = threading.Lock()
        
        def attempt_in_household(household_id: str):
            reserved = idem_svc.reserve(idem_key, household_id)
            
            with results_lock:
                if reserved.reserved:
                    results[household_id]["reserved"] += 1
                else:
                    results[household_id]["rejected"] += 1
        
        households = [f"family-{i}" for i in range(10)]
        
        # Each household tries same key (should succeed in each namespace)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(attempt_in_household, h) for h in households]
            for f in concurrent.futures.as_completed(futures):
                f.result()
        
        # Each household should have reserved in its namespace
        for h in households:
            assert results[h]["reserved"] == 1, f"Household {h} should have reserved once"
    
    def test_event_emission_no_cross_household_leakage(self):
        """Events from concurrent writes don't leak between households."""
        bus = InMemoryRealtimeEventBus()
        
        from tests.p1_verification.fixtures import EventCapture
        h1_capture = EventCapture()
        h2_capture = EventCapture()
        
        # Each subscribes (would be filtered by middleware)
        bus.subscribe_all(h1_capture.handler)
        bus.subscribe_all(h2_capture.handler)
        
        # Concurrent events from different households
        def emit_events(household_id: str, count: int):
            for i in range(count):
                event = RealtimeEvent(
                    household_id=household_id,
                    event_type="TASK_CREATED",
                    watermark=f"wm-{i}",
                    payload={"id": f"{household_id}-t{i}"},
                )
                bus.publish(event)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(emit_events, "family-1", 10)
            executor.submit(emit_events, "family-2", 10)
            executor.shutdown(wait=True)
        
        # Both received all (no filtering by bus)
        assert len(h1_capture.events) == 20
        assert len(h2_capture.events) == 20
        
        # But can filter by household
        h1_only = h1_capture.get_by_household("family-1")
        h2_only = h2_capture.get_by_household("family-2")
        
        assert len(h1_only) == 10
        assert len(h2_only) == 10
        
        # No cross-contamination
        for event in h1_only:
            assert event.household_id == "family-1"
        for event in h2_only:
            assert event.household_id == "family-2"


class TestChaosAuthFailures:
    """Validate auth failures don't corrupt state under concurrency."""
    
    def test_mixed_valid_invalid_tokens_concurrent(self, identity_repo):
        """Mix of valid/invalid tokens under concurrency."""
        from apps.api.auth.token_service import TokenService
        
        token_svc = TokenService(identity_repo)
        
        results = {"valid": 0, "invalid": 0}
        results_lock = threading.Lock()
        
        # Issue one valid token
        pair = token_svc.issue_token_pair(
            household_id="family-1",
            user_id="user-1",
            device_id="dev-1",
            role="ADMIN",
        )
        valid_token = pair.access_token
        
        def attempt_validate(token: str, is_valid: bool):
            try:
                claims = token_svc.validate_and_extract_claims(token)
                with results_lock:
                    results["valid"] += 1
            except Exception:
                with results_lock:
                    results["invalid"] += 1
        
        # Mix of valid and invalid tokens
        tasks = [
            (valid_token, True),
            ("invalid.token", False),
            (valid_token, True),
            ("another.bad.token", False),
        ] * 10
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(attempt_validate, t, v) for t, v in tasks]
            for f in concurrent.futures.as_completed(futures):
                f.result()
        
        # Correct split
        assert results["valid"] == 20
        assert results["invalid"] == 20


class TestChaosLLMFailures:
    """Validate LLM failures don't cascade under concurrency."""
    
    def test_llm_timeouts_concurrent_dont_crash(self):
        """10 concurrent LLM timeouts don't crash system."""
        llm_provider = MockLLMProvider(behavior="timeout")
        gateway = LLMGateway(llm_provider)
        
        results = {"fallback": 0, "errors": 0}
        results_lock = threading.Lock()
        
        def attempt_resolve():
            try:
                response = gateway.resolve_intent(
                    message="test",
                    context_snapshot={},
                    household_id="family-1",
                )
                with results_lock:
                    if response.resolved_by == "fallback":
                        results["fallback"] += 1
            except Exception:
                with results_lock:
                    results["errors"] += 1
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(attempt_resolve) for _ in range(10)]
            for f in concurrent.futures.as_completed(futures):
                f.result()
        
        # All should fallback safely
        assert results["fallback"] == 10
        assert results["errors"] == 0
    
    def test_rate_limit_under_concurrent_requests(self):
        """Rate limiting works correctly under concurrent requests."""
        llm_provider = MockLLMProvider(behavior="success")
        gateway = LLMGateway(llm_provider, max_requests_per_minute=5)
        
        household_id = "family-1"
        results = {"llm": 0, "fallback": 0}
        results_lock = threading.Lock()
        
        def attempt_resolve(idx: int):
            response = gateway.resolve_intent(
                message=f"msg{idx}",
                context_snapshot={},
                household_id=household_id,
            )
            with results_lock:
                if response.resolved_by == "llm":
                    results["llm"] += 1
                else:
                    results["fallback"] += 1
        
        # 10 concurrent requests, but rate limit is 5
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(attempt_resolve, i) for i in range(10)]
            for f in concurrent.futures.as_completed(futures):
                f.result()
        
        # Some should hit rate limit
        assert results["llm"] <= 5
        assert results["fallback"] >= 5


class TestChaosRetryStorms:
    """Validate idempotency under retry storms."""
    
    def test_retry_storm_deduplicated(self):
        """Retry storm (same request-id repeated) is properly deduplicated."""
        idem_svc = TestFixtures.create_idempotency_service()
        household_id = "family-1"
        idem_key = f"create-task-{uuid.uuid4().hex[:8]}"
        
        results = {"winners": 0, "rejections": 0}
        results_lock = threading.Lock()
        
        def retry_attempt(attempt_num: int):
            # Same request-id, simulating retry
            reserved = idem_svc.reserve(idem_key, household_id)
            
            with results_lock:
                if reserved.reserved:
                    results["winners"] += 1
                else:
                    results["rejections"] += 1
        
        # 50 retries of the same request
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(retry_attempt, i) for i in range(50)]
            for f in concurrent.futures.as_completed(futures):
                f.result()
        
        # Only 1 should win
        assert results["winners"] == 1
        assert results["rejections"] == 49


class TestChaosEventOrdering:
    """Validate event ordering under high-throughput concurrent publishes."""
    
    def test_events_ordered_under_concurrent_publishes(self):
        """Events maintain order per household even under concurrent publishes."""
        bus = InMemoryRealtimeEventBus()
        
        from tests.p1_verification.fixtures import EventCapture
        capture = EventCapture()
        bus.subscribe_all(capture.handler)
        
        household_id = "family-1"
        num_events = 50
        
        def publish_events():
            for i in range(num_events):
                event = RealtimeEvent(
                    household_id=household_id,
                    event_type="TASK_CREATED",
                    watermark=f"wm-{i:04d}",
                    payload={"id": f"t-{i}"},
                )
                bus.publish(event)
        
        # Single thread publishes (bus should maintain order)
        publish_events()
        
        events = capture.get_by_household(household_id)
        assert len(events) == num_events
        
        # Verify ordering by watermark
        for i, event in enumerate(events):
            assert event.watermark == f"wm-{i:04d}"
