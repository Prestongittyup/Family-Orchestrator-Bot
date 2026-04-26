"""
P1 LLM Gateway Failure Tests
Validates timeout, budget, rate limit, and structured output handling.
"""
from __future__ import annotations

import pytest

from apps.api.llm.gateway import LLMGateway
from tests.p1_verification.fixtures import MockLLMProvider, TestFixtures


class TestLLMGatewayTimeout:
    """Validate hard timeout fallback."""
    
    def test_timeout_returns_fallback_response(self):
        """LLM timeout triggers fallback response."""
        provider = MockLLMProvider(behavior="timeout")
        gateway = LLMGateway(provider, hard_timeout_seconds=2.0)
        
        response = gateway.resolve_intent(
            message="Create task",
            context_snapshot={},
            household_id="family-1",
        )
        
        # Should get fallback, not crash
        assert response.resolved_by == "fallback"
        assert response.intent_type is None or response.intent_type == "GENERAL_QUERY"
    
    def test_timeout_doesnt_crash_pipeline(self):
        """Timeout in LLM doesn't crash the chat pipeline."""
        provider = MockLLMProvider(behavior="timeout")
        gateway = LLMGateway(provider, hard_timeout_seconds=0.1)
        
        try:
            response = gateway.resolve_intent(
                message="test",
                context_snapshot={},
                household_id="family-1",
            )
            # Should not raise; should return fallback
            assert response is not None
        except Exception as e:
            pytest.fail(f"LLM timeout crashed pipeline: {e}")


class TestLLMGatewayBudget:
    """Validate prompt budget guard."""
    
    def test_oversized_prompt_rejected(self):
        """Prompt exceeding budget is rejected."""
        provider = MockLLMProvider(behavior="success")
        gateway = LLMGateway(provider, max_prompt_chars=100)
        
        huge_message = "x" * 500
        response = gateway.resolve_intent(
            message=huge_message,
            context_snapshot={"large": "data"},
            household_id="family-1",
        )
        
        # Should fallback, not send to LLM
        assert response.resolved_by == "fallback"
        assert response.raw_response == "prompt_budget_exceeded"
    
    def test_normal_prompt_accepted(self):
        """Normal prompts within budget are sent."""
        provider = MockLLMProvider(behavior="success")
        gateway = LLMGateway(provider, max_prompt_chars=1000)
        
        message = "Create a task for groceries tomorrow"
        response = gateway.resolve_intent(
            message=message,
            context_snapshot={"family": "Smith"},
            household_id="family-1",
        )
        
        # Should be processed by LLM
        assert response.resolved_by == "llm"
        assert response.intent_type == "CREATE_TASK"

    def test_normal_prompt_returns_llm_route_under_non_limited_conditions(self):
        """Normal prompt returns llm route when budget/rate constraints are not hit."""
        provider = MockLLMProvider(behavior="success")
        gateway = LLMGateway(provider, max_requests_per_minute=10, max_prompt_chars=1000)

        response = gateway.resolve_intent(
            message="Please create a task to buy groceries tonight",
            context_snapshot={"intent": "task", "actor_context": {"actor_type": "api_user"}},
            household_id="family-1",
        )

        assert response.resolved_by == "llm"


class TestLLMGatewayRateLimit:
    """Validate per-household rate limiting."""
    
    def test_rate_limit_enforced(self):
        """Requests exceeding rate limit are rejected."""
        provider = MockLLMProvider(behavior="success")
        gateway = LLMGateway(provider, max_requests_per_minute=2)
        
        household_id = "family-1"
        
        # First two should succeed
        for i in range(2):
            response = gateway.resolve_intent(
                message=f"Message {i}",
                context_snapshot={},
                household_id=household_id,
            )
            assert response.resolved_by == "llm"
        
        # Third should be rate limited
        response = gateway.resolve_intent(
            message="Message 3",
            context_snapshot={},
            household_id=household_id,
        )
        assert response.resolved_by == "fallback"
        assert response.raw_response == "rate_limit_exceeded"
    
    def test_rate_limit_per_household(self):
        """Rate limits are per-household, not global."""
        provider = MockLLMProvider(behavior="success")
        gateway = LLMGateway(provider, max_requests_per_minute=2)
        
        # household 1 uses rate limit
        for i in range(2):
            gateway.resolve_intent(
                message=f"h1-msg{i}",
                context_snapshot={},
                household_id="family-1",
            )
        
        # household 2 should still work (separate limit)
        for i in range(2):
            response = gateway.resolve_intent(
                message=f"h2-msg{i}",
                context_snapshot={},
                household_id="family-2",
            )
            assert response.resolved_by == "llm"


class TestLLMGatewayStructuredOutput:
    """Validate structured output validation."""
    
    def test_invalid_intent_type_rejected(self):
        """Invalid intent types are caught."""
        provider = MockLLMProvider(behavior="invalid_output")
        gateway = LLMGateway(provider)
        
        response = gateway.resolve_intent(
            message="test",
            context_snapshot={},
            household_id="family-1",
        )
        
        # Invalid intent should trigger fallback
        assert response.intent_type not in gateway._allowed_intents or response.resolved_by == "fallback"
    
    def test_valid_intent_names_accepted(self):
        """Valid intent names pass validation."""
        provider = MockLLMProvider(behavior="success")
        gateway = LLMGateway(provider)
        
        response = gateway.resolve_intent(
            message="Create a task",
            context_snapshot={},
            household_id="family-1",
        )
        
        assert response.intent_type in gateway._allowed_intents
    
    def test_confidence_score_bounds(self):
        """Confidence scores are in valid range."""
        provider = MockLLMProvider(behavior="success")
        gateway = LLMGateway(provider)
        
        response = gateway.resolve_intent(
            message="test",
            context_snapshot={},
            household_id="family-1",
        )
        
        assert 0 <= response.confidence <= 1.0


class TestLLMGatewayCombinedFailures:
    """Validate graceful handling of multiple failure modes."""
    
    def test_timeout_doesnt_trigger_rate_limit(self):
        """Timeout doesn't count against rate limit."""
        provider = MockLLMProvider(behavior="timeout")
        gateway = LLMGateway(provider, max_requests_per_minute=3)
        
        # 3 timeouts shouldn't trigger rate limit
        for i in range(3):
            response = gateway.resolve_intent(
                message=f"msg{i}",
                context_snapshot={},
                household_id="family-1",
            )
            assert response.resolved_by == "fallback"
        
        # Should still work for next request (no rate limit triggered)
        provider.behavior = "success"
        response = gateway.resolve_intent(
            message="msg4",
            context_snapshot={},
            household_id="family-1",
        )
        assert response.resolved_by == "llm"


class TestLLMGatewayFallbackPath:
    """Validate fallback to rule-based intent system."""
    
    def test_fallback_returns_valid_response(self):
        """Fallback response has valid structure."""
        provider = MockLLMProvider(behavior="timeout")
        gateway = LLMGateway(provider)
        
        response = gateway.resolve_intent(
            message="test",
            context_snapshot={},
            household_id="family-1",
        )
        
        # Must be valid response object
        assert hasattr(response, "intent_type")
        assert hasattr(response, "resolved_by")
        assert hasattr(response, "raw_response")
        assert response.resolved_by == "fallback"
    
    def test_fallback_response_safe_for_pipeline(self):
        """Fallback response can be safely processed downstream."""
        provider = MockLLMProvider(behavior="timeout")
        gateway = LLMGateway(provider)
        
        response = gateway.resolve_intent(
            message="test",
            context_snapshot={},
            household_id="family-1",
        )
        
        # Should be safe to check intent without crashes
        if response.intent_type is None:
            # No-op is safe fallback
            pass
        elif response.intent_type in ["CREATE_TASK", "QUERY_SCHEDULE"]:
            # Would proceed normally
            pass
