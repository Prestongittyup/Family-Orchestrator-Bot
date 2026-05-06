from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.adapters.cache.redis_client import RedisCacheClient

from app.services.usage.tiers import UPGRADE_BENEFITS, SubscriptionTier, next_tier, normalize_tier, policy_for


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


@dataclass(frozen=True)
class UsageSnapshot:
    calls: int
    tokens_in: int
    tokens_out: int
    estimated_cost: float


@dataclass(frozen=True)
class LimitCheck:
    allowed: bool
    reason: str | None
    daily: UsageSnapshot
    monthly: UsageSnapshot


class UsageLimiter:
    def __init__(
        self,
        cache_client: RedisCacheClient,
        *,
        input_cost_per_1k: float = 0.00035,
        output_cost_per_1k: float = 0.00105,
    ) -> None:
        self._cache = cache_client
        self._input_cost_per_1k = input_cost_per_1k
        self._output_cost_per_1k = output_cost_per_1k

    async def resolve_user_tier(self, user_id: str) -> SubscriptionTier:
        key = f"subscription:{user_id}"
        raw = await self._cache.hash_get(key, "tier")
        if raw is None:
            return "free"
        return normalize_tier(str(raw))

    async def check_before_llm(self, user_id: str, *, tier: str, projected_tokens_in: int = 0) -> LimitCheck:
        policy = policy_for(tier)
        daily_key = self._daily_usage_key(user_id)
        monthly_key = self._monthly_usage_key(user_id)

        daily_usage, monthly_usage = await self._cache.hash_get_pair(daily_key, monthly_key)

        daily = _snapshot_from_hash(daily_usage)
        monthly = _snapshot_from_hash(monthly_usage)

        if not policy["llm_enabled"]:
            return LimitCheck(allowed=False, reason="tier_limit_reached", daily=daily, monthly=monthly)

        if daily.calls >= policy["daily_ai_calls"]:
            return LimitCheck(allowed=False, reason="tier_limit_reached", daily=daily, monthly=monthly)

        if monthly.calls >= policy["monthly_ai_calls"]:
            return LimitCheck(allowed=False, reason="tier_limit_reached", daily=daily, monthly=monthly)

        projected_total_tokens = daily.tokens_in + daily.tokens_out + max(0, projected_tokens_in)
        if projected_total_tokens > policy["daily_token_budget"]:
            return LimitCheck(allowed=False, reason="tier_limit_reached", daily=daily, monthly=monthly)

        return LimitCheck(allowed=True, reason=None, daily=daily, monthly=monthly)

    async def check_limits(self, user_id: str, tier: str) -> tuple[bool, str | None]:
        decision = await self.check_before_llm(user_id, tier=tier)
        return decision.allowed, decision.reason

    async def would_exceed_daily_tokens(self, user_id: str, *, tier: str, additional_tokens: int) -> bool:
        decision = await self.check_before_llm(
            user_id,
            tier=tier,
            projected_tokens_in=max(0, additional_tokens),
        )
        return not decision.allowed and decision.reason == "tier_limit_reached"

    async def increment_usage(self, user_id: str, tokens_in: int, tokens_out: int) -> None:
        normalized_tokens_in = max(0, tokens_in)
        normalized_tokens_out = max(0, tokens_out)
        estimated_cost = self._estimate_cost(tokens_in=tokens_in, tokens_out=tokens_out)

        now = datetime.now(UTC)
        daily_key = self._daily_usage_key(user_id)
        monthly_key = self._monthly_usage_key(user_id)
        ledger_key = self._ledger_key(user_id)

        await self._cache.increment_usage_counters(
            daily_key=daily_key,
            monthly_key=monthly_key,
            ledger_key=ledger_key,
            tokens_in=normalized_tokens_in,
            tokens_out=normalized_tokens_out,
            estimated_cost=estimated_cost,
            now_utc=now,
        )

    def build_upgrade_metadata(self, *, current_tier: str) -> dict[str, Any]:
        current = normalize_tier(current_tier)
        upgrade_tier = next_tier(current)
        price = policy_for(upgrade_tier)["price"]
        display_tier = {
            "pro": "Pro",
            "premium": "Premium",
            "free": "Free",
        }.get(upgrade_tier, "Pro")

        return {
            "upgrade_available": True,
            "message": f"Upgrade to {display_tier} for AI-powered summaries and task extraction",
            "benefit": list(UPGRADE_BENEFITS[current]),
            "recommended_tier": upgrade_tier,
            "recommended_price": price,
        }

    def build_blocked_payload(self, *, current_tier: str) -> dict[str, Any]:
        metadata = self.build_upgrade_metadata(current_tier=current_tier)
        return {
            "priority": "medium",
            "needs_attention": False,
            "actions": [],
            "state_summary": "Upgrade required for AI features",
            "reason": "tier_limit_reached",
            "upgrade_available": True,
            "metadata": metadata,
        }

    def build_metadata(self, *, current_tier: str) -> dict[str, Any]:
        normalized = normalize_tier(current_tier)
        if normalized == "free":
            return self.build_upgrade_metadata(current_tier=current_tier)
        return {
            "upgrade_available": False,
            "message": "AI processing enabled",
            "benefit": [],
        }

    def estimate_cost(self, *, tokens_in: int, tokens_out: int) -> float:
        return self._estimate_cost(tokens_in=tokens_in, tokens_out=tokens_out)

    def _estimate_cost(self, *, tokens_in: int, tokens_out: int) -> float:
        input_cost = (max(0, tokens_in) / 1000.0) * self._input_cost_per_1k
        output_cost = (max(0, tokens_out) / 1000.0) * self._output_cost_per_1k
        return round(input_cost + output_cost, 8)

    @staticmethod
    def _daily_usage_key(user_id: str) -> str:
        return f"usage:{user_id}:daily"

    @staticmethod
    def _monthly_usage_key(user_id: str) -> str:
        return f"usage:{user_id}:monthly"

    @staticmethod
    def _ledger_key(user_id: str) -> str:
        return f"usage:{user_id}:ledger"


def _to_int(value: object, *, default: int) -> int:
    if value is None:
        return default
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def _to_float(value: object, *, default: float) -> float:
    if value is None:
        return default
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return default


def _snapshot_from_hash(data: object) -> UsageSnapshot:
    payload = data if isinstance(data, dict) else {}
    return UsageSnapshot(
        calls=_to_int(payload.get("calls"), default=0),
        tokens_in=_to_int(payload.get("tokens_in"), default=0),
        tokens_out=_to_int(payload.get("tokens_out"), default=0),
        estimated_cost=_to_float(payload.get("estimated_cost"), default=0.0),
    )
