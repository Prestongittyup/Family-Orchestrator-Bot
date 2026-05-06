from __future__ import annotations

import pytest

from app.services.routing.intelligence_routing import IntelligenceRoutingService
from app.services.usage.limiter import LimitCheck, UsageSnapshot

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate]



def _empty_snapshot() -> UsageSnapshot:
    return UsageSnapshot(calls=0, tokens_in=0, tokens_out=0, estimated_cost=0.0)


class _StubUsageLimiter:
    def __init__(
        self,
        *,
        tier: str = "free",
        resolve_error: Exception | None = None,
        check_error: Exception | None = None,
        allowed: bool = True,
    ) -> None:
        self._tier = tier
        self._resolve_error = resolve_error
        self._check_error = check_error
        self._allowed = allowed

    async def resolve_user_tier(self, user_id: str) -> str:
        _ = user_id
        if self._resolve_error is not None:
            raise self._resolve_error
        return self._tier

    async def check_before_llm(self, user_id: str, *, tier: str, projected_tokens_in: int = 0) -> LimitCheck:
        _ = (user_id, tier, projected_tokens_in)
        if self._check_error is not None:
            raise self._check_error
        reason = None if self._allowed else "tier_limit_reached"
        return LimitCheck(allowed=self._allowed, reason=reason, daily=_empty_snapshot(), monthly=_empty_snapshot())

    def build_metadata(self, *, current_tier: str) -> dict[str, object]:
        return {
            "upgrade_available": current_tier == "free",
            "message": "ok",
            "benefit": [],
        }

    def build_upgrade_metadata(self, *, current_tier: str) -> dict[str, object]:
        _ = current_tier
        return {
            "upgrade_available": True,
            "message": "upgrade",
            "benefit": ["more_ai"],
            "recommended_tier": "pro",
            "recommended_price": 4.99,
        }


@pytest.mark.asyncio
@pytest.mark.integration
async def test_decide_returns_rule_only_when_tier_resolution_fails_for_ai_path() -> None:
    routing = IntelligenceRoutingService(
        _StubUsageLimiter(resolve_error=RuntimeError("redis unavailable")),
    )

    outcome = await routing.decide(
        user_id="user-1",
        is_ambiguous=True,
        prefer_ai=True,
        projected_tokens_in=400,
    )

    assert outcome.decision == "RULE_ONLY"
    assert outcome.reason == "usage_backend_unavailable"
    assert outcome.tier == "free"
    assert outcome.metadata["routing_degraded"] is True
    assert outcome.metadata["degradation_stage"] == "resolve_user_tier"


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.flaky
async def test_decide_returns_rule_only_when_limit_check_fails() -> None:
    routing = IntelligenceRoutingService(
        _StubUsageLimiter(tier="pro", check_error=RuntimeError("usage check timeout")),
    )

    outcome = await routing.decide(
        user_id="user-2",
        is_ambiguous=True,
        prefer_ai=True,
        projected_tokens_in=220,
    )

    assert outcome.decision == "RULE_ONLY"
    assert outcome.reason == "usage_backend_unavailable"
    assert outcome.tier == "pro"
    assert outcome.metadata["routing_degraded"] is True
    assert outcome.metadata["degradation_stage"] == "check_before_llm"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_decide_keeps_rules_sufficient_when_ai_not_needed_even_if_tier_lookup_fails() -> None:
    routing = IntelligenceRoutingService(
        _StubUsageLimiter(resolve_error=RuntimeError("redis unavailable")),
    )

    outcome = await routing.decide(
        user_id="user-3",
        is_ambiguous=False,
        prefer_ai=False,
        projected_tokens_in=100,
    )

    assert outcome.decision == "RULE_ONLY"
    assert outcome.reason == "rules_sufficient"
    assert outcome.metadata["routing_degraded"] is True
    assert outcome.metadata["degradation_stage"] == "resolve_user_tier"