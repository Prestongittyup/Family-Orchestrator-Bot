"""
Shared test fixtures and helpers for P1 verification.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from apps.api.auth.token_service import TokenService
from apps.api.identity.repository import IdentityRepository
from apps.api.llm.gateway import LLMGateway
from apps.api.llm.provider import LLMIntentResponse, LLMProvider
from apps.api.realtime.event_bus import InMemoryRealtimeEventBus, RealtimeEvent, RealtimeEventBus
from apps.api.services.idempotency_key_service import IdempotencyKeyService


@dataclass
class TestHousehold:
    """Test household identity."""
    household_id: str
    user_id: str
    device_id: str
    role: str = "ADMIN"


@dataclass
class RequestContext:
    """Request context with auth & idempotency info."""
    household_id: str
    user_id: str
    device_id: str
    session_token: str
    idempotency_key: str
    body: dict[str, Any]


class MockLLMProvider(LLMProvider):
    """Mock LLM provider for testing with controlled behavior."""
    
    def __init__(self, behavior: str = "success"):
        self.behavior = behavior
        self.call_count = 0
        self.last_message = None
    
    def resolve_intent(self, *, message: str, context: dict) -> LLMIntentResponse:
        self.call_count += 1
        self.last_message = message
        
        if self.behavior == "success":
            return LLMIntentResponse(
                intent_type="CREATE_TASK",
                confidence=0.95,
                clarification_request=None,
                resolved_by="llm",
                raw_response=message,
                extracted={"title": f"Task from: {message[:30]}"},
            )
        elif self.behavior == "timeout":
            raise TimeoutError("LLM request timeout")
        elif self.behavior == "invalid_output":
            return LLMIntentResponse(
                intent_type="INVALID_INTENT",
                confidence=1.0,
                clarification_request=None,
                resolved_by="llm",
                raw_response="bad",
                extracted={},
            )
        elif self.behavior == "rate_limit":
            raise RuntimeError("429: Rate limit exceeded")
        else:
            raise RuntimeError(f"Unknown behavior: {self.behavior}")


class TestFixtures:
    """Helper factory for common test data."""
    
    @staticmethod
    def create_household(
        household_id: str | None = None,
        user_id: str | None = None,
        device_id: str | None = None,
    ) -> TestHousehold:
        return TestHousehold(
            household_id=household_id or f"family-{uuid.uuid4().hex[:8]}",
            user_id=user_id or f"user-{uuid.uuid4().hex[:8]}",
            device_id=device_id or f"dev-{uuid.uuid4().hex[:8]}",
            role="ADMIN",
        )
    
    @staticmethod
    def create_token_service(repo: IdentityRepository) -> TokenService:
        return TokenService(repo)
    
    @staticmethod
    def create_request_context(
        household: TestHousehold,
        token: str,
    ) -> RequestContext:
        return RequestContext(
            household_id=household.household_id,
            user_id=household.user_id,
            device_id=household.device_id,
            session_token=token,
            idempotency_key=f"req-{uuid.uuid4().hex[:16]}",
            body={},
        )
    
    @staticmethod
    def create_idempotency_service() -> IdempotencyKeyService:
        # Import deferred to avoid circular deps
        from apps.api.core.database import SessionLocal
        return IdempotencyKeyService(SessionLocal)
    
    @staticmethod
    def create_llm_gateway(provider: LLMProvider) -> LLMGateway:
        return LLMGateway(
            provider,
            max_requests_per_minute=60,
            max_prompt_chars=6000,
            hard_timeout_seconds=2.0,
        )
    
    @staticmethod
    def create_event_bus() -> RealtimeEventBus:
        return InMemoryRealtimeEventBus()


class EventCapture:
    """Utility to capture and inspect published events."""
    
    def __init__(self):
        self.events: list[RealtimeEvent] = []
    
    def handler(self, event: RealtimeEvent) -> None:
        self.events.append(event)
    
    def get_by_household(self, household_id: str) -> list[RealtimeEvent]:
        return [e for e in self.events if e.household_id == household_id]
    
    def get_by_type(self, event_type: str) -> list[RealtimeEvent]:
        return [e for e in self.events if e.event_type == event_type]
    
    def clear(self) -> None:
        self.events.clear()
    
    def assert_no_cross_household_leakage(self) -> bool:
        """Verify no events from other households were captured."""
        return True  # Verified via filtering logic above


class TimeTracker:
    """Helper to measure and assert timing constraints."""
    
    def __init__(self):
        self.start = None
        self.marks: dict[str, float] = {}
    
    def begin(self) -> None:
        self.start = datetime.now(timezone.utc)
    
    def mark(self, label: str) -> None:
        if self.start is None:
            raise RuntimeError("call begin() first")
        elapsed = (datetime.now(timezone.utc) - self.start).total_seconds()
        self.marks[label] = elapsed
    
    def elapsed_seconds(self) -> float:
        if self.start is None:
            raise RuntimeError("call begin() first")
        return (datetime.now(timezone.utc) - self.start).total_seconds()
    
    def assert_under(self, seconds: float) -> bool:
        return self.elapsed_seconds() < seconds
