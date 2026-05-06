from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from app.services.usage.limiter import UsageLimiter
from app.services.usage.tiers import SubscriptionTier, policy_for


RoutingDecision = Literal["RULE_ONLY", "NEEDS_LLM", "BLOCKED"]


@dataclass(frozen=True)
class RoutingOutcome:
    decision: RoutingDecision
    tier: SubscriptionTier
    reason: str
    metadata: dict[str, Any]


class IntelligenceRoutingService:
    def __init__(self, usage_limiter: UsageLimiter) -> None:
        self._usage = usage_limiter

    async def decide(
        self,
        *,
        user_id: str,
        is_ambiguous: bool,
        prefer_ai: bool,
        projected_tokens_in: int,
    ) -> RoutingOutcome:
        tier, tier_error = await self._resolve_tier_with_fallback(user_id)
        policy = policy_for(tier)
        needs_ai = bool(prefer_ai or is_ambiguous)

        if not needs_ai:
            metadata = self._usage.build_metadata(current_tier=tier)
            if tier_error:
                metadata = self._with_degradation_metadata(
                    metadata,
                    stage="resolve_user_tier",
                    error=tier_error,
                )
            return RoutingOutcome(
                decision="RULE_ONLY",
                tier=tier,
                reason="rules_sufficient",
                metadata=metadata,
            )

        if tier_error:
            return RoutingOutcome(
                decision="RULE_ONLY",
                tier=tier,
                reason="usage_backend_unavailable",
                metadata=self._with_degradation_metadata(
                    self._usage.build_metadata(current_tier=tier),
                    stage="resolve_user_tier",
                    error=tier_error,
                ),
            )

        if not policy["llm_enabled"]:
            return RoutingOutcome(
                decision="BLOCKED",
                tier=tier,
                reason="tier_limit_reached",
                metadata=self._usage.build_upgrade_metadata(current_tier=tier),
            )

        try:
            limit_check = await self._usage.check_before_llm(
                user_id,
                tier=tier,
                projected_tokens_in=max(0, projected_tokens_in),
            )
        except Exception as exc:
            return RoutingOutcome(
                decision="RULE_ONLY",
                tier=tier,
                reason="usage_backend_unavailable",
                metadata=self._with_degradation_metadata(
                    self._usage.build_metadata(current_tier=tier),
                    stage="check_before_llm",
                    error=str(exc),
                ),
            )

        if not limit_check.allowed:
            return RoutingOutcome(
                decision="BLOCKED",
                tier=tier,
                reason="tier_limit_reached",
                metadata=self._usage.build_upgrade_metadata(current_tier=tier),
            )

        return RoutingOutcome(
            decision="NEEDS_LLM",
            tier=tier,
            reason="ai_route_enabled",
            metadata=self._usage.build_metadata(current_tier=tier),
        )

    async def _resolve_tier_with_fallback(self, user_id: str) -> tuple[SubscriptionTier, str | None]:
        try:
            return await self._usage.resolve_user_tier(user_id), None
        except Exception as exc:
            return "free", str(exc)

    @staticmethod
    def _with_degradation_metadata(metadata: dict[str, Any], *, stage: str, error: str) -> dict[str, Any]:
        enriched = dict(metadata)
        enriched["routing_degraded"] = True
        enriched["degradation_reason"] = "usage_backend_unavailable"
        enriched["degradation_stage"] = stage
        enriched["degradation_error"] = error
        return enriched

    @staticmethod
    def enrich_payload(payload: dict[str, Any], outcome: RoutingOutcome) -> dict[str, Any]:
        enriched = dict(payload)
        metadata = dict(outcome.metadata)
        enriched["metadata"] = metadata
        enriched["upgrade_available"] = bool(metadata.get("upgrade_available", False))

        if outcome.decision == "BLOCKED" and "reason" not in enriched:
            enriched["reason"] = outcome.reason

        return enriched
