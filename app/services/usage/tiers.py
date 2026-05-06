from __future__ import annotations

from typing import Final, Literal, TypedDict


SubscriptionTier = Literal["free", "pro", "premium"]


class TierPolicy(TypedDict):
    llm_enabled: bool
    daily_ai_calls: int
    monthly_ai_calls: int
    daily_token_budget: int
    price: float
    features: list[str]


FREE: Final[TierPolicy] = {
    "llm_enabled": False,
    "daily_ai_calls": 0,
    "monthly_ai_calls": 0,
    "daily_token_budget": 0,
    "price": 0.0,
    "features": ["rules_only_email", "basic_pantry", "static_recipes"],
}

PRO: Final[TierPolicy] = {
    "llm_enabled": True,
    "daily_ai_calls": 50,
    "monthly_ai_calls": 1500,
    "daily_token_budget": 250000,
    "price": 9.99,
    "features": ["email_ai", "pantry_ai", "task_extraction"],
}

PREMIUM: Final[TierPolicy] = {
    "llm_enabled": True,
    "daily_ai_calls": 150,
    "monthly_ai_calls": 4500,
    "daily_token_budget": 900000,
    "price": 19.99,
    "features": ["pro_features", "proactive_ai", "family_mode"],
}

TIERS: Final[dict[SubscriptionTier, TierPolicy]] = {
    "free": FREE,
    "pro": PRO,
    "premium": PREMIUM,
}


UPGRADE_BENEFITS: Final[dict[SubscriptionTier, list[str]]] = {
    "free": [
        "Smart prioritization",
        "Automatic task creation",
        "Context-aware summaries",
    ],
    "pro": [
        "Proactive household suggestions",
        "Higher AI call limits",
        "Multi-domain family coordination",
    ],
    "premium": [
        "Highest intelligence limits",
        "Best latency under peak load",
        "Advanced household orchestration",
    ],
}


def normalize_tier(value: str | None) -> SubscriptionTier:
    normalized = (value or "").strip().lower()
    if normalized in TIERS:
        return normalized  # type: ignore[return-value]
    return "free"


def next_tier(current_tier: SubscriptionTier) -> SubscriptionTier:
    if current_tier == "free":
        return "pro"
    if current_tier == "pro":
        return "premium"
    return "premium"


def policy_for(tier: str | None) -> TierPolicy:
    normalized = normalize_tier(tier)
    return TIERS[normalized]
