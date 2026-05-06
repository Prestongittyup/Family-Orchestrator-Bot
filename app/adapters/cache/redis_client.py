from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

try:
    from redis.asyncio import Redis as AsyncRedis
except Exception:  # pragma: no cover - fallback path for environments without redis package
    AsyncRedis = None


class _InMemoryPipeline:
    def __init__(self, redis_client: "_InMemoryRedis") -> None:
        self._redis = redis_client
        self._ops: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def _add(self, op: str, *args: Any, **kwargs: Any) -> "_InMemoryPipeline":
        self._ops.append((op, args, kwargs))
        return self

    def hgetall(self, key: str) -> "_InMemoryPipeline":
        return self._add("hgetall", key)

    def set(self, key: str, value: str, ex: int | None = None) -> "_InMemoryPipeline":
        return self._add("set", key, value, ex=ex)

    def lpush(self, key: str, value: str) -> "_InMemoryPipeline":
        return self._add("lpush", key, value)

    def ltrim(self, key: str, start: int, stop: int) -> "_InMemoryPipeline":
        return self._add("ltrim", key, start, stop)

    def hincrby(self, key: str, field: str, amount: int) -> "_InMemoryPipeline":
        return self._add("hincrby", key, field, amount)

    def hincrbyfloat(self, key: str, field: str, amount: float) -> "_InMemoryPipeline":
        return self._add("hincrbyfloat", key, field, amount)

    def expire(self, key: str, ttl_seconds: int) -> "_InMemoryPipeline":
        return self._add("expire", key, ttl_seconds)

    def hset(self, key: str, mapping: dict[str, Any]) -> "_InMemoryPipeline":
        return self._add("hset", key, mapping=mapping)

    async def execute(self) -> list[Any]:
        results: list[Any] = []
        for op, args, kwargs in self._ops:
            fn = getattr(self._redis, op)
            results.append(await fn(*args, **kwargs))
        self._ops.clear()
        return results


class _InMemoryRedis:
    def __init__(self) -> None:
        self._kv: dict[str, str] = {}
        self._hashes: dict[str, dict[str, str]] = {}
        self._lists: dict[str, list[str]] = {}

    async def aclose(self) -> None:
        return None

    async def get(self, key: str) -> str | None:
        return self._kv.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        _ = ex
        self._kv[key] = value
        return True

    async def delete(self, key: str) -> int:
        existed = int(key in self._kv)
        self._kv.pop(key, None)
        self._hashes.pop(key, None)
        self._lists.pop(key, None)
        return existed

    async def hget(self, key: str, field: str) -> str | None:
        return self._hashes.get(key, {}).get(field)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._hashes.get(key, {}))

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        values = list(self._lists.get(key, []))
        if stop == -1:
            return values[start:]
        return values[start : stop + 1]

    async def mget(self, keys: list[str]) -> list[str | None]:
        return [self._kv.get(key) for key in keys]

    def pipeline(self, transaction: bool = True) -> _InMemoryPipeline:
        _ = transaction
        return _InMemoryPipeline(self)

    async def lpush(self, key: str, value: str) -> int:
        values = self._lists.setdefault(key, [])
        values.insert(0, value)
        return len(values)

    async def ltrim(self, key: str, start: int, stop: int) -> bool:
        values = self._lists.setdefault(key, [])
        if stop == -1:
            self._lists[key] = values[start:]
        else:
            self._lists[key] = values[start : stop + 1]
        return True

    async def hincrby(self, key: str, field: str, amount: int) -> int:
        hash_map = self._hashes.setdefault(key, {})
        current = int(hash_map.get(field, "0"))
        next_value = current + amount
        hash_map[field] = str(next_value)
        return next_value

    async def hincrbyfloat(self, key: str, field: str, amount: float) -> float:
        hash_map = self._hashes.setdefault(key, {})
        current = float(hash_map.get(field, "0"))
        next_value = current + amount
        hash_map[field] = str(next_value)
        return next_value

    async def expire(self, key: str, ttl_seconds: int) -> bool:
        _ = (key, ttl_seconds)
        return True

    async def hset(self, key: str, mapping: dict[str, Any]) -> int:
        hash_map = self._hashes.setdefault(key, {})
        for field, value in mapping.items():
            hash_map[str(field)] = str(value)
        return len(mapping)


class RedisCacheClient:
    """Centralized Redis adapter for cache, usage, and lightweight persistence flows."""

    def __init__(self, redis_client: Any) -> None:
        self._redis = redis_client

    @classmethod
    def from_url(cls, redis_url: str, *, decode_responses: bool = True) -> "RedisCacheClient":
        if AsyncRedis is None:
            _ = (redis_url, decode_responses)
            return cls(_InMemoryRedis())
        return cls(AsyncRedis.from_url(redis_url, decode_responses=decode_responses))

    async def aclose(self) -> None:
        await self._redis.aclose()

    async def get(self, key: str) -> str | None:
        raw = await self._redis.get(key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="ignore")
        return str(raw)

    async def set(self, key: str, value: str, *, ttl_seconds: int | None = None) -> None:
        await self._redis.set(key, value, ex=ttl_seconds)

    async def delete(self, key: str) -> int:
        deleted = await self._redis.delete(key)
        return int(deleted)

    async def get_json_dict(self, key: str) -> dict[str, Any] | None:
        raw = await self.get(key)
        if raw is None:
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    async def set_json(self, key: str, payload: dict[str, Any], *, ttl_seconds: int | None = None) -> None:
        serialized = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        await self.set(key, serialized, ttl_seconds=ttl_seconds)

    async def hash_get(self, key: str, field: str) -> str | None:
        raw = await self._redis.hget(key, field)
        if raw is None:
            return None
        return raw.decode("utf-8", errors="ignore") if isinstance(raw, bytes) else str(raw)

    async def hash_getall(self, key: str) -> dict[str, Any]:
        raw = await self._redis.hgetall(key)
        return raw if isinstance(raw, dict) else {}

    async def hash_get_pair(self, first_key: str, second_key: str) -> tuple[dict[str, Any], dict[str, Any]]:
        first, second = await (
            self._redis.pipeline(transaction=False)
            .hgetall(first_key)
            .hgetall(second_key)
            .execute()
        )
        first_map = first if isinstance(first, dict) else {}
        second_map = second if isinstance(second, dict) else {}
        return first_map, second_map

    async def list_range(self, key: str, start: int, stop: int) -> list[str]:
        values = await self._redis.lrange(key, start, stop)
        output: list[str] = []
        for value in values:
            if isinstance(value, bytes):
                output.append(value.decode("utf-8", errors="ignore"))
            else:
                output.append(str(value))
        return output

    async def multi_get(self, keys: list[str]) -> list[str | None]:
        values = await self._redis.mget(keys)
        output: list[str | None] = []
        for value in values:
            if value is None:
                output.append(None)
                continue
            if isinstance(value, bytes):
                output.append(value.decode("utf-8", errors="ignore"))
            else:
                output.append(str(value))
        return output

    async def save_evaluation_record(
        self,
        *,
        key: str,
        serialized_payload: str,
        ttl_seconds: int,
        global_index_key: str,
        max_index_size: int,
        user_index_key: str | None = None,
    ) -> None:
        pipeline = self._redis.pipeline(transaction=True)
        pipeline.set(key, serialized_payload, ex=ttl_seconds)
        pipeline.lpush(global_index_key, key)
        pipeline.ltrim(global_index_key, 0, max_index_size - 1)

        if user_index_key:
            pipeline.lpush(user_index_key, key)
            pipeline.ltrim(user_index_key, 0, max_index_size - 1)

        await pipeline.execute()

    async def increment_usage_counters(
        self,
        *,
        daily_key: str,
        monthly_key: str,
        ledger_key: str,
        tokens_in: int,
        tokens_out: int,
        estimated_cost: float,
        now_utc: datetime | None = None,
    ) -> None:
        now = now_utc or datetime.now(UTC)
        daily_ttl_seconds = _seconds_until_next_day(now)
        monthly_ttl_seconds = _seconds_until_next_month(now)

        pipeline = self._redis.pipeline(transaction=True)

        pipeline.hincrby(daily_key, "calls", 1)
        pipeline.hincrby(daily_key, "tokens_in", max(0, tokens_in))
        pipeline.hincrby(daily_key, "tokens_out", max(0, tokens_out))
        pipeline.hincrbyfloat(daily_key, "estimated_cost", estimated_cost)
        pipeline.expire(daily_key, daily_ttl_seconds)

        pipeline.hincrby(monthly_key, "calls", 1)
        pipeline.hincrby(monthly_key, "tokens_in", max(0, tokens_in))
        pipeline.hincrby(monthly_key, "tokens_out", max(0, tokens_out))
        pipeline.hincrbyfloat(monthly_key, "estimated_cost", estimated_cost)
        pipeline.expire(monthly_key, monthly_ttl_seconds)

        pipeline.hincrby(ledger_key, "total_llm_calls", 1)
        pipeline.hincrby(ledger_key, "tokens_in", max(0, tokens_in))
        pipeline.hincrby(ledger_key, "tokens_out", max(0, tokens_out))
        pipeline.hincrbyfloat(ledger_key, "estimated_cost", estimated_cost)
        pipeline.hset(ledger_key, mapping={"last_updated_utc": now.isoformat()})

        await pipeline.execute()

    async def increment_daily_usage(
        self,
        *,
        usage_key: str,
        tokens_in: int,
        tokens_out: int,
        estimated_cost: float,
        ttl_seconds: int,
    ) -> None:
        pipeline = self._redis.pipeline(transaction=True)
        pipeline.hincrby(usage_key, "calls", 1)
        pipeline.hincrby(usage_key, "tokens_in", max(0, tokens_in))
        pipeline.hincrby(usage_key, "tokens_out", max(0, tokens_out))
        pipeline.hincrbyfloat(usage_key, "estimated_cost", estimated_cost)
        pipeline.expire(usage_key, max(60, ttl_seconds))
        await pipeline.execute()


def _seconds_until_next_day(now: datetime) -> int:
    next_day = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(60, int((next_day - now).total_seconds()))


def _seconds_until_next_month(now: datetime) -> int:
    if now.month == 12:
        next_month = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        next_month = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return max(60, int((next_month - now).total_seconds()))
