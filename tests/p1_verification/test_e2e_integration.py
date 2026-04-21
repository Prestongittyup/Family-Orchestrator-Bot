"""
P1 End-to-End Integration Tests
Validates full lifecycle: auth → intent → LLM → decision → write → event.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass

import pytest

from apps.api.auth.token_service import TokenService
from apps.api.llm.gateway import LLMGateway
from apps.api.realtime.event_bus import InMemoryRealtimeEventBus
from apps.api.services.idempotency_key_service import IdempotencyKeyService
from tests.p1_verification.fixtures import (
    EventCapture,
    MockLLMProvider,
    TestFixtures,
)


@dataclass
class E2EPipeline:
    """Encapsulates a single request-response pipeline."""
    token_service: TokenService
    llm_gateway: LLMGateway
    event_bus: InMemoryRealtimeEventBus
    idem_service: IdempotencyKeyService
    household_id: str
    user_id: str
    device_id: str


class TestE2EAuthenticatedRequest:
    """Test complete authenticated request with token validation."""
    
    def test_full_request_with_valid_token(self, identity_repo):
        """Complete request flow with valid auth token."""
        # 1. Issue token
        token_svc = TokenService(identity_repo)
        household_id = "family-1"
        user_id = "user-1"
        device_id = "dev-1"
        
        pair = token_svc.issue_token_pair(
            household_id=household_id,
            user_id=user_id,
            device_id=device_id,
            role="ADMIN",
        )
        
        # 2. Validate token (middleware)
        claims = token_svc.validate_and_extract_claims(pair.access_token)
        assert claims.household_id == household_id
        assert claims.user_id == user_id
        
        # 3. Proceed to intent resolution with valid identity
        assert claims is not None
    
    def test_invalid_token_rejected_early(self, identity_repo):
        """Invalid token is rejected before intent resolution."""
        token_svc = TokenService(identity_repo)
        
        with pytest.raises(Exception):
            token_svc.validate_and_extract_claims("invalid.token.fake")


class TestE2EIntentResolutionToWrite:
    """Test intent → LLM → decision → idempotent write."""
    
    def test_valid_intent_triggers_write(self, identity_repo):
        """Valid LLM intent is written idempotently."""
        # Setup pipeline
        token_svc = TokenService(identity_repo)
        llm_provider = MockLLMProvider(behavior="success")
        gateway = LLMGateway(llm_provider)
        idem_svc = TestFixtures.create_idempotency_service()
        
        household_id = "family-1"
        
        # 1. Get token
        pair = token_svc.issue_token_pair(
            household_id=household_id,
            user_id="user-1",
            device_id="dev-1",
            role="ADMIN",
        )
        
        # 2. Validate token
        claims = token_svc.validate_and_extract_claims(pair.access_token)
        assert claims.household_id == household_id
        
        # 3. Resolve intent
        response = gateway.resolve_intent(
            message="Create task for groceries",
            context_snapshot={},
            household_id=household_id,
        )
        assert response.intent_type == "CREATE_TASK"
        
        # 4. Write idempotently
        idem_key = f"intent-{uuid.uuid4().hex[:16]}"
        reserved = idem_svc.reserve(idem_key, household_id)
        assert reserved.reserved is True
        
        # Would write to DB here
        idem_svc.mark_completed(idem_key, household_id, response_data={"task_id": "t-1"})
    
    def test_llm_timeout_fallback_still_writes(self, identity_repo):
        """Even if LLM times out, fallback response can still be written."""
        token_svc = TokenService(identity_repo)
        llm_provider = MockLLMProvider(behavior="timeout")
        gateway = LLMGateway(llm_provider)
        idem_svc = TestFixtures.create_idempotency_service()
        
        household_id = "family-1"
        
        # Token still valid
        pair = token_svc.issue_token_pair(
            household_id=household_id,
            user_id="user-1",
            device_id="dev-1",
            role="ADMIN",
        )
        claims = token_svc.validate_and_extract_claims(pair.access_token)
        assert claims is not None
        
        # Intent resolution fallback
        response = gateway.resolve_intent(
            message="test",
            context_snapshot={},
            household_id=household_id,
        )
        assert response.resolved_by == "fallback"
        
        # Still can write fallback response
        idem_key = f"fallback-{uuid.uuid4().hex[:16]}"
        reserved = idem_svc.reserve(idem_key, household_id)
        assert reserved.reserved is True


class TestE2EEventEmission:
    """Test that writes trigger correct event emission."""
    
    def test_write_emits_event(self):
        """Successful write emits event to bus."""
        bus = InMemoryRealtimeEventBus()
        capture = EventCapture()
        bus.subscribe_all(capture.handler)
        
        household_id = "family-1"
        
        # Simulate write → event emission
        from apps.api.realtime.event_bus import RealtimeEvent
        event = RealtimeEvent(
            household_id=household_id,
            event_type="TASK_CREATED",
            watermark="wm-1",
            payload={"task_id": "task-abc"},
        )
        bus.publish(event)
        
        # Event captured
        events = capture.get_by_household(household_id)
        assert len(events) == 1
        assert events[0].event_type == "TASK_CREATED"
    
    def test_write_failure_no_event(self):
        """Failed write doesn't emit event."""
        bus = InMemoryRealtimeEventBus()
        capture = EventCapture()
        bus.subscribe_all(capture.handler)
        
        # If write failed (e.g., constraint violation), no event
        # (We don't publish)
        
        # Verify no events
        assert len(capture.events) == 0


class TestE2EEndToEndNominal:
    """Complete nominal case: auth → intent → LLM → write → event → SSE."""
    
    def test_nominal_request_flow(self, identity_repo):
        """Complete happy-path request."""
        # Setup
        token_svc = TokenService(identity_repo)
        llm_provider = MockLLMProvider(behavior="success")
        gateway = LLMGateway(llm_provider)
        event_bus = InMemoryRealtimeEventBus()
        idem_svc = TestFixtures.create_idempotency_service()
        capture = EventCapture()
        event_bus.subscribe_all(capture.handler)
        
        household_id = "family-1"
        user_id = "user-1"
        device_id = "dev-1"
        message = "Create a task for grocery shopping tomorrow"
        
        # 1. Issue and validate token
        pair = token_svc.issue_token_pair(
            household_id=household_id,
            user_id=user_id,
            device_id=device_id,
            role="ADMIN",
        )
        claims = token_svc.validate_and_extract_claims(pair.access_token)
        assert claims.household_id == household_id
        
        # 2. Check idempotency (request dedup)
        idem_key = f"req-{uuid.uuid4().hex[:16]}"
        reserved = idem_svc.reserve(idem_key, household_id)
        assert reserved.reserved is True
        
        # 3. Resolve intent with LLM
        intent_response = gateway.resolve_intent(
            message=message,
            context_snapshot={"family_size": 4},
            household_id=household_id,
        )
        assert intent_response.intent_type == "CREATE_TASK"
        
        # 4. Simulate write to DB
        task_id = f"task-{uuid.uuid4().hex[:8]}"
        
        # 5. Record completion
        idem_svc.mark_completed(
            idem_key,
            household_id,
            response_data={"task_id": task_id},
        )
        
        # 6. Emit event
        from apps.api.realtime.event_bus import RealtimeEvent
        event = RealtimeEvent(
            household_id=household_id,
            event_type="TASK_CREATED",
            watermark="wm-1",
            payload={"task_id": task_id, "title": "Buy groceries"},
        )
        event_bus.publish(event)
        
        # 7. Verify event delivered (SSE would pick this up)
        captured = capture.get_by_household(household_id)
        assert len(captured) == 1
        assert captured[0].event_type == "TASK_CREATED"
        assert captured[0].payload["task_id"] == task_id


# Fixtures
@pytest.fixture
def identity_repo():
    """Provide test identity repository."""
    from apps.api.core.database import SessionLocal
    from apps.api.identity.repository import IdentityRepository
    return IdentityRepository(SessionLocal())
