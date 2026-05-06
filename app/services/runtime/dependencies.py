"""Runtime composition helpers.

Layer responsibility:
- build service dependencies for runtime wiring only

Allowed internal imports:
- app.adapters.*
- app.services.* (local service contracts)

Forbidden internal imports:
- app.api.*
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

from app.adapters.cache.redis_client import RedisCacheClient
from app.services.llm_gateway.gateway import LLMGateway
from app.services.usage.limiter import UsageLimiter


async def get_redis_cache_client() -> AsyncIterator[RedisCacheClient]:
    client = RedisCacheClient.from_url(resolve_redis_url())
    try:
        yield client
    finally:
        await client.aclose()


def build_llm_gateway(*, cache_client: RedisCacheClient, usage_limiter: UsageLimiter) -> LLMGateway:
    return LLMGateway.from_default_providers(
        cache_client=cache_client,
        usage_hook=_UsageHook(usage_limiter),
    )


def resolve_redis_url() -> str:
    explicit = (os.getenv("REDIS_URL") or "").strip()
    if explicit:
        return explicit

    host = (os.getenv("REDIS_HOST") or "redis").strip() or "redis"
    port = (os.getenv("REDIS_PORT") or "6379").strip() or "6379"
    db = (os.getenv("REDIS_DB") or "0").strip() or "0"
    return f"redis://{host}:{port}/{db}"


class _UsageHook:
    def __init__(self, usage_limiter: UsageLimiter) -> None:
        self._usage_limiter = usage_limiter

    async def record_usage(self, user_id: str, *, tokens_in: int, tokens_out: int) -> None:
        await self._usage_limiter.increment_usage(user_id, tokens_in=tokens_in, tokens_out=tokens_out)

    def estimate_cost(self, *, tokens_in: int, tokens_out: int) -> float:
        return self._usage_limiter.estimate_cost(tokens_in=tokens_in, tokens_out=tokens_out)
