"""
P1 Idempotency Tests
Validates exactly-once semantics under replay and concurrency.
"""
from __future__ import annotations

import concurrent.futures
import threading
import uuid
from dataclasses import dataclass

import pytest

from apps.api.services.idempotency_key_service import IdempotencyKeyService
from tests.p1_verification.fixtures import TestFixtures


@dataclass
class IdempotencyTestResult:
    """Result of an idempotency check."""
    key: str
    reserved: bool
    error: str | None = None


class TestIdempotencyReservation:
    """Validate idempotency key reservation/release semantics."""
    
    def test_first_reservation_succeeds(self, idem_service: IdempotencyKeyService):
        """First request with key reserves it."""
        key = f"req-{uuid.uuid4().hex[:16]}"
        result = idem_service.reserve(key, "global")
        assert result.reserved is True
    
    def test_duplicate_reservation_rejected(self, idem_service: IdempotencyKeyService):
        """Second request with same key is rejected."""
        key = f"req-{uuid.uuid4().hex[:16]}"
        
        # First reservation
        r1 = idem_service.reserve(key, "global")
        assert r1.reserved is True
        
        # Second reservation on same key
        r2 = idem_service.reserve(key, "global")
        assert r2.reserved is False
        assert r2.status_code == 409  # Conflict
    
    def test_different_households_different_namespace(self, idem_service: IdempotencyKeyService):
        """Same idempotency key in different households is allowed."""
        key = f"req-{uuid.uuid4().hex[:16]}"
        h1 = "family-1"
        h2 = "family-2"
        
        # Reserve for household 1
        r1 = idem_service.reserve(key, h1)
        assert r1.reserved is True
        
        # Same key for household 2 should also succeed (different namespace)
        r2 = idem_service.reserve(key, h2)
        assert r2.reserved is True
    
    def test_release_allows_reuse(self, idem_service: IdempotencyKeyService):
        """Releasing a key allows it to be reused (e.g., on retry)."""
        key = f"req-{uuid.uuid4().hex[:16]}"
        scope = "global"
        
        # Reserve
        r1 = idem_service.reserve(key, scope)
        assert r1.reserved is True
        
        # Duplicate should fail
        r2 = idem_service.reserve(key, scope)
        assert r2.reserved is False
        
        # Release (e.g., after 5xx response)
        idem_service.release(key, scope)
        
        # Now reserve should work again for retry
        r3 = idem_service.reserve(key, scope)
        assert r3.reserved is True


class TestIdempotencyUnderConcurrency:
    """Validate exactly-once semantics under parallel requests."""
    
    def test_parallel_identical_requests_single_winner(
        self, idem_service: IdempotencyKeyService
    ):
        """With 10 parallel identical requests, only 1 succeeds."""
        key = f"req-{uuid.uuid4().hex[:16]}"
        scope = "global"
        results: list[IdempotencyTestResult] = []
        results_lock = threading.Lock()
        
        def attempt_reserve() -> None:
            try:
                result = idem_service.reserve(key, scope)
                idem_test = IdempotencyTestResult(
                    key=key,
                    reserved=result.reserved,
                )
            except Exception as e:
                idem_test = IdempotencyTestResult(key=key, reserved=False, error=str(e))
            
            with results_lock:
                results.append(idem_test)
        
        # 10 concurrent attempts
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(attempt_reserve) for _ in range(10)]
            for fut in concurrent.futures.as_completed(futures):
                fut.result()
        
        # Exactly one should have reserved
        reserved_count = sum(1 for r in results if r.reserved)
        assert reserved_count == 1, f"Expected 1 winner, got {reserved_count}"
    
    def test_concurrent_different_keys_all_succeed(
        self, idem_service: IdempotencyKeyService
    ):
        """Concurrent requests with different keys all succeed."""
        scope = "global"
        results: list[IdempotencyTestResult] = []
        results_lock = threading.Lock()
        
        def attempt_with_key(key: str) -> None:
            try:
                result = idem_service.reserve(key, scope)
                idem_test = IdempotencyTestResult(key=key, reserved=result.reserved)
            except Exception as e:
                idem_test = IdempotencyTestResult(key=key, reserved=False, error=str(e))
            
            with results_lock:
                results.append(idem_test)
        
        # 20 concurrent attempts with different keys
        keys = [f"req-{uuid.uuid4().hex[:16]}" for _ in range(20)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(attempt_with_key, k) for k in keys]
            for fut in concurrent.futures.as_completed(futures):
                fut.result()
        
        # All should succeed
        reserved_count = sum(1 for r in results if r.reserved)
        assert reserved_count == 20, f"Expected 20 winners, got {reserved_count}"


class TestIdempotencyReplayAfterFailure:
    """Validate safe replay patterns after server failures."""
    
    def test_5xx_response_releases_key_safely(self, idem_service: IdempotencyKeyService):
        """After 5xx, key is released for safe replay."""
        key = f"req-{uuid.uuid4().hex[:16]}"
        scope = "global"
        
        # Request 1: succeeds initially
        r1 = idem_service.reserve(key, scope)
        assert r1.reserved is True
        # Simulate processing and marking as complete with 5xx
        idem_service.release(key, scope)
        
        # Request 2: retry after release should succeed
        r2 = idem_service.reserve(key, scope)
        assert r2.reserved is True
    
    def test_2xx_response_records_completion(self, idem_service: IdempotencyKeyService):
        """After 2xx, result is stored and replayed."""
        key = f"req-{uuid.uuid4().hex[:16]}"
        scope = "household-1"
        
        # Reserve
        r1 = idem_service.reserve(key, scope)
        assert r1.reserved is True
        
        # Mark as completed (simulate successful response storage)
        idem_service.mark_completed(key, scope, response_data={"task_id": "task-123"})
        
        # Duplicate request should return cached result, not re-execute
        r2 = idem_service.get_cached_result(key, scope)
        assert r2 is not None
        assert r2["task_id"] == "task-123"


class TestIdempotencyNoDuplicateWrites:
    """Validate end-to-end that idempotency prevents duplicate writes."""
    
    def test_concurrent_task_creations_deduplicated(
        self, idem_service: IdempotencyKeyService
    ):
        """10 concurrent identical task creates result in 1 task."""
        key = f"create-task-{uuid.uuid4().hex[:8]}"
        household_id = "family-1"
        
        def create_task_with_idempotency() -> dict:
            reserved = idem_service.reserve(key, household_id)
            if not reserved.reserved:
                # Would be rejected at middleware
                return {"created": False, "reason": "duplicate"}
            
            # Would normally create task in DB
            return {"created": True, "task_id": "task-xyz"}
        
        # Simulate 10 concurrent requests
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(create_task_with_idempotency) for _ in range(10)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        # Exactly 1 should have created
        created_count = sum(1 for r in results if r.get("created"))
        assert created_count == 1, f"Expected 1 creation, got {created_count}"


# Fixtures
@pytest.fixture
def idem_service() -> IdempotencyKeyService:
    """Provide test idempotency service."""
    return TestFixtures.create_idempotency_service()
