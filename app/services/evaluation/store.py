from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app.adapters.cache.redis_client import RedisCacheClient


class EvaluationStore:
    def __init__(
        self,
        cache_client: RedisCacheClient,
        *,
        ttl_seconds: int = 60 * 60 * 24 * 30,
        max_index_size: int = 5000,
    ) -> None:
        self._cache = cache_client
        self._ttl_seconds = ttl_seconds
        self._max_index_size = max_index_size

    async def save_email_comparison(self, record: dict[str, Any]) -> None:
        evaluation_id = str(record.get("evaluation_id") or "").strip()
        if not evaluation_id:
            return

        user_id = str(record.get("user_id") or "").strip()

        key = self._record_key(evaluation_id)
        serialized = json.dumps(record, separators=(",", ":"), sort_keys=True)
        user_index_key = self._index_key(user_id=user_id) if user_id else None

        await self._cache.save_evaluation_record(
            key=key,
            serialized_payload=serialized,
            ttl_seconds=self._ttl_seconds,
            global_index_key=self._index_key(),
            max_index_size=self._max_index_size,
            user_index_key=user_index_key,
        )

    async def list_email_comparisons(self, *, limit: int = 500, user_id: str | None = None) -> list[dict[str, Any]]:
        normalized_limit = max(1, min(int(limit), self._max_index_size))
        normalized_user = (user_id or "").strip() or None

        keys = await self._cache.list_range(self._index_key(user_id=normalized_user), 0, normalized_limit - 1)
        if not keys:
            return []

        values = await self._cache.multi_get(keys)
        records: list[dict[str, Any]] = []
        for raw in values:
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                records.append(payload)
        return records

    async def save_batch_summary(self, payload: dict[str, Any]) -> None:
        run_id = str(payload.get("batch_run_id") or "").strip()
        if not run_id:
            return

        key = f"evaluation:batch:{run_id}"
        serialized = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        await self._cache.set(key, serialized, ttl_seconds=self._ttl_seconds)

    @staticmethod
    def timestamp_utc() -> str:
        return datetime.now(UTC).isoformat()

    @staticmethod
    def _record_key(evaluation_id: str) -> str:
        return f"evaluation:email:{evaluation_id}"

    @staticmethod
    def _index_key(*, user_id: str | None = None) -> str:
        if user_id:
            return f"evaluation:email:index:user:{user_id}"
        return "evaluation:email:index"
