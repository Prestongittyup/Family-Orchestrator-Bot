from __future__ import annotations

import hashlib
from typing import Any

from app.adapters.cache.redis_client import RedisCacheClient


CACHE_TTL_SECONDS = 3600


def build_thread_cache_key(*, thread_id: str, latest_message_id: str, messages: list[str]) -> str:
    last_three = messages[-3:]
    digest_source = f"{thread_id}{latest_message_id}{'||'.join(last_three)}"
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()
    return f"email-triage:{digest}"


class RedisThreadCache:
    def __init__(self, cache_client: RedisCacheClient, *, ttl_seconds: int = CACHE_TTL_SECONDS) -> None:
        self._cache = cache_client
        self._ttl_seconds = ttl_seconds

    async def get(self, key: str) -> dict[str, Any] | None:
        return await self._cache.get_json_dict(key)

    async def set(self, key: str, payload: dict[str, Any]) -> None:
        await self._cache.set_json(key, payload, ttl_seconds=self._ttl_seconds)
