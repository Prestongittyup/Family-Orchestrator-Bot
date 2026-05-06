from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

import app.adapters.llm.gateway as gateway_module
from app.adapters.llm.gateway import LLMGateway, LLMGatewayRequest

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.reliability]



class _ScriptedProvider:
    def __init__(self, *, name: str, outcomes: list[str | Exception], has_api_key: bool = True) -> None:
        self.name = name
        self._outcomes = list(outcomes)
        self._has_api_key = has_api_key
        self.calls = 0

    @property
    def has_api_key(self) -> bool:
        return self._has_api_key

    async def stream_json_text(
        self,
        *,
        prompt: str,
        max_output_tokens: int = 768,
        response_mime_type: str = "application/json",
    ) -> AsyncIterator[str]:
        _ = (prompt, max_output_tokens, response_mime_type)
        self.calls += 1

        if not self._outcomes:
            raise RuntimeError("no scripted outcomes")

        index = min(self.calls - 1, len(self._outcomes) - 1)
        outcome = self._outcomes[index]
        if isinstance(outcome, Exception):
            raise outcome

        yield outcome


class _BlockingProvider:
    def __init__(self) -> None:
        self.name = "gemini"
        self.calls = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    @property
    def has_api_key(self) -> bool:
        return True

    async def stream_json_text(
        self,
        *,
        prompt: str,
        max_output_tokens: int = 768,
        response_mime_type: str = "application/json",
    ) -> AsyncIterator[str]:
        _ = (prompt, max_output_tokens, response_mime_type)
        self.calls += 1
        if self.calls > 1:
            raise AssertionError("provider executed more than once")

        self.started.set()
        await self.release.wait()
        yield '{"status":"ok"}'


class _SlowProvider:
    def __init__(self, *, delay_seconds: float = 0.03) -> None:
        self.name = "gemini"
        self.calls = 0
        self._delay_seconds = delay_seconds

    @property
    def has_api_key(self) -> bool:
        return True

    async def stream_json_text(
        self,
        *,
        prompt: str,
        max_output_tokens: int = 768,
        response_mime_type: str = "application/json",
    ) -> AsyncIterator[str]:
        _ = (prompt, max_output_tokens, response_mime_type)
        self.calls += 1
        await asyncio.sleep(self._delay_seconds)
        yield '{"status":"ok"}'


class _MemoryCache:
    def __init__(self, *, fail_get: bool = False, fail_set: bool = False) -> None:
        self._store: dict[str, str] = {}
        self._fail_get = fail_get
        self._fail_set = fail_set
        self.get_calls = 0
        self.set_calls = 0

    async def get(self, key: str) -> str | None:
        self.get_calls += 1
        if self._fail_get:
            raise RuntimeError("cache read unavailable")
        return self._store.get(key)

    async def set(self, key: str, value: str, *, ttl_seconds: int | None = None) -> None:
        _ = ttl_seconds
        self.set_calls += 1
        if self._fail_set:
            raise RuntimeError("cache write unavailable")
        self._store[key] = value


class _RecordingUsageHook:
    def __init__(self) -> None:
        self.calls: list[dict[str, int | str]] = []

    async def record_usage(self, user_id: str, *, tokens_in: int, tokens_out: int) -> None:
        self.calls.append(
            {
                "user_id": user_id,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
            }
        )

    def estimate_cost(self, *, tokens_in: int, tokens_out: int) -> float:
        return round((tokens_in + tokens_out) / 1_000_000.0, 8)


@pytest.mark.asyncio
@pytest.mark.chaos
async def test_generate_text_retries_transient_failures_with_bounded_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _ScriptedProvider(
        name="gemini",
        outcomes=[TimeoutError("timed out"), TimeoutError("timed out"), '{"ok":true}'],
    )
    gateway = LLMGateway(
        providers={"gemini": provider},
        primary_provider="gemini",
        fallback_provider=None,
        max_retries=2,
        retry_base_delay_seconds=0.05,
        retry_max_delay_seconds=0.06,
    )

    sleep_calls: list[float] = []

    async def _fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(gateway_module.asyncio, "sleep", _fake_sleep)

    result = await gateway.generate_text(
        LLMGatewayRequest(
            prompt="Return JSON",
            response_mime_type="application/json",
        )
    )

    assert result.error is None
    assert provider.calls == 3
    assert result.usage["retry_count"] == 2
    assert sleep_calls == [0.05, 0.06]


@pytest.mark.asyncio
@pytest.mark.chaos
async def test_generate_text_does_not_retry_invalid_json_responses() -> None:
    provider = _ScriptedProvider(name="gemini", outcomes=["not-json"])
    gateway = LLMGateway(
        providers={"gemini": provider},
        primary_provider="gemini",
        fallback_provider=None,
        max_retries=3,
    )

    result = await gateway.generate_text(
        LLMGatewayRequest(
            prompt="Return JSON",
            response_mime_type="application/json",
        )
    )

    assert provider.calls == 1
    assert result.error is not None
    assert "provider_invalid_response" in result.error
    assert result.usage["retry_count"] == 0


@pytest.mark.asyncio
@pytest.mark.chaos
async def test_generate_text_uses_fallback_once_and_records_usage_once() -> None:
    primary = _ScriptedProvider(name="gemini", outcomes=[RuntimeError("503 unavailable")])
    fallback = _ScriptedProvider(name="openai", outcomes=['{"priority":"high"}'])
    usage_hook = _RecordingUsageHook()

    gateway = LLMGateway(
        providers={"gemini": primary, "openai": fallback},
        primary_provider="gemini",
        fallback_provider="openai",
        usage_hook=usage_hook,
        max_retries=0,
    )

    result = await gateway.generate_text(LLMGatewayRequest(prompt="Return JSON", user_id="user-1"))

    assert result.error is None
    assert result.provider == "openai"
    assert result.usage["fallback_used"] is True
    assert primary.calls == 1
    assert fallback.calls == 1
    assert len(usage_hook.calls) == 1


@pytest.mark.asyncio
@pytest.mark.chaos
async def test_generate_text_cache_hit_skips_provider_and_usage() -> None:
    provider = _ScriptedProvider(name="gemini", outcomes=['{"should_not":"run"}'])
    cache = _MemoryCache()
    usage_hook = _RecordingUsageHook()
    cache._store["llm:response:cache-key"] = '{"cached":true}'

    gateway = LLMGateway(
        providers={"gemini": provider},
        primary_provider="gemini",
        fallback_provider=None,
        cache_client=cache,
        usage_hook=usage_hook,
    )

    result = await gateway.generate_text(
        LLMGatewayRequest(
            prompt="Return JSON",
            cache_key="cache-key",
        )
    )

    assert result.cache_hit is True
    assert result.provider == "cache"
    assert result.error is None
    assert provider.calls == 0
    assert usage_hook.calls == []


@pytest.mark.asyncio
@pytest.mark.chaos
async def test_generate_text_inflight_deduplicates_parallel_calls() -> None:
    provider = _BlockingProvider()
    cache = _MemoryCache()
    usage_hook = _RecordingUsageHook()
    gateway = LLMGateway(
        providers={"gemini": provider},
        primary_provider="gemini",
        fallback_provider=None,
        cache_client=cache,
        usage_hook=usage_hook,
        request_timeout_seconds=2.0,
        provider_timeout_seconds=2.0,
    )

    request = LLMGatewayRequest(prompt="Return JSON", cache_key="shared", user_id="user-1")

    owner_task = asyncio.create_task(gateway.generate_text(request))
    await provider.started.wait()

    waiter_task = asyncio.create_task(gateway.generate_text(request))
    provider.release.set()

    owner_result, waiter_result = await asyncio.gather(owner_task, waiter_task)

    assert owner_result.error is None
    assert waiter_result.error is None
    assert provider.calls == 1
    assert len(usage_hook.calls) == 1
    assert {owner_result.provider, waiter_result.provider} == {"gemini", "inflight"}


@pytest.mark.asyncio
@pytest.mark.chaos
async def test_generate_text_survives_cache_read_write_failures() -> None:
    provider = _ScriptedProvider(name="gemini", outcomes=['{"ok":true}'])
    cache = _MemoryCache(fail_get=True, fail_set=True)
    gateway = LLMGateway(
        providers={"gemini": provider},
        primary_provider="gemini",
        fallback_provider=None,
        cache_client=cache,
    )

    result = await gateway.generate_text(
        LLMGatewayRequest(prompt="Return JSON", cache_key="degraded-cache")
    )

    assert result.error is None
    assert provider.calls == 1
    assert result.usage["cache_status"] == "miss_persist_failed"


@pytest.mark.asyncio
@pytest.mark.chaos
async def test_generate_text_inflight_burst_keeps_single_provider_execution() -> None:
    provider = _SlowProvider()
    cache = _MemoryCache()
    usage_hook = _RecordingUsageHook()
    gateway = LLMGateway(
        providers={"gemini": provider},
        primary_provider="gemini",
        fallback_provider=None,
        cache_client=cache,
        usage_hook=usage_hook,
        request_timeout_seconds=3.0,
        provider_timeout_seconds=3.0,
    )

    request = LLMGatewayRequest(prompt="Return JSON", cache_key="burst", user_id="user-9")
    tasks = [asyncio.create_task(gateway.generate_text(request)) for _ in range(12)]
    results = await asyncio.gather(*tasks)

    providers = {result.provider for result in results}

    assert provider.calls == 1
    assert len(usage_hook.calls) == 1
    assert all(result.error is None for result in results)
    assert providers.issubset({"gemini", "inflight"})
    assert "inflight" in providers