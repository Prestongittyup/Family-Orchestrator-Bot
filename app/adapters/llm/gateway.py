"""LLM gateway contract.

Layer responsibility:
- single LLM orchestration boundary (normalize -> cache -> provider execute -> normalize -> usage hook)

Allowed internal imports:
- app.adapters.llm.providers.*

Forbidden internal imports:
- app.services.*
- app.api.*
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from hashlib import sha256
from collections.abc import AsyncIterator
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Protocol

from app.adapters.external.http_client import ExternalHTTPClient
from app.adapters.llm.providers.gemini import GeminiProvider
from app.adapters.llm.providers.openai import OpenAIProvider


logger = logging.getLogger(__name__)


class LLMGatewayError(RuntimeError):
    pass


class ProviderResponseError(RuntimeError):
    pass


class LLMProvider(Protocol):
    name: str

    @property
    def has_api_key(self) -> bool: ...

    async def stream_json_text(
        self,
        *,
        prompt: str,
        max_output_tokens: int = 768,
        response_mime_type: str = "application/json",
    ) -> AsyncIterator[str]: ...


class LLMUsageHook(Protocol):
    async def record_usage(self, user_id: str, *, tokens_in: int, tokens_out: int) -> None: ...

    def estimate_cost(self, *, tokens_in: int, tokens_out: int) -> float: ...


class GatewayCacheClient(Protocol):
    async def get(self, key: str) -> str | None: ...

    async def set(self, key: str, value: str, *, ttl_seconds: int | None = None) -> None: ...


@dataclass(frozen=True)
class LLMGatewayRequest:
    prompt: str
    user_id: str | None = None
    cache_key: str | None = None
    context_hash: str | None = None
    metadata: dict[str, Any] | None = None
    max_output_tokens: int = 768
    response_mime_type: str = "application/json"


@dataclass(frozen=True)
class LLMGatewayResult:
    response: dict[str, Any] | str | None
    cache_hit: bool
    usage: dict[str, Any]
    raw_text: str | None
    normalized_text: str | None
    parsed_json: dict[str, Any] | None
    provider: str | None
    tokens_in: int
    tokens_out: int
    estimated_cost: float
    latency_ms: float
    error: str | None


class LLMGateway:
    def __init__(
        self,
        *,
        providers: dict[str, LLMProvider],
        primary_provider: str = "gemini",
        fallback_provider: str | None = "openai",
        max_retries: int = 2,
        cache_client: GatewayCacheClient | None = None,
        cache_ttl_seconds: int = 900,
        usage_hook: LLMUsageHook | None = None,
        provider_timeout_seconds: float = 45.0,
        request_timeout_seconds: float = 60.0,
        retry_base_delay_seconds: float = 0.2,
        retry_max_delay_seconds: float = 2.0,
    ) -> None:
        self._providers = {name.strip().lower(): provider for name, provider in providers.items() if name.strip()}
        self._primary_provider = primary_provider.strip().lower() or "gemini"
        self._fallback_provider = (fallback_provider or "").strip().lower() or None
        self._max_retries = max(0, max_retries)
        self._cache = cache_client
        self._cache_ttl_seconds = max(60, cache_ttl_seconds)
        self._usage_hook = usage_hook
        self._provider_timeout_seconds = max(1.0, provider_timeout_seconds)
        self._request_timeout_seconds = max(self._provider_timeout_seconds, request_timeout_seconds)
        self._retry_base_delay_seconds = max(0.01, retry_base_delay_seconds)
        self._retry_max_delay_seconds = max(self._retry_base_delay_seconds, retry_max_delay_seconds)
        self._inflight_responses: dict[str, asyncio.Future[str]] = {}
        self._inflight_lock = asyncio.Lock()

    @classmethod
    def from_default_providers(
        cls,
        *,
        cache_client: GatewayCacheClient | None = None,
        usage_hook: LLMUsageHook | None = None,
    ) -> "LLMGateway":
        providers = {
            "gemini": GeminiProvider(
                api_key=(os.getenv("GEMINI_API_KEY") or "").strip(),
                model=(os.getenv("GEMINI_MODEL") or "gemini-1.5-flash").strip() or "gemini-1.5-flash",
                temperature=_safe_float_env("GEMINI_TEMPERATURE", default=0.2),
                timeout_seconds=_safe_float_env("LLM_PROVIDER_TIMEOUT_SECONDS", default=45.0),
                stream_transport=_stream_gemini_via_gateway,
            ),
            "openai": OpenAIProvider(api_key=(os.getenv("OPENAI_API_KEY") or "").strip()),
        }

        return cls(
            providers=providers,
            primary_provider=(os.getenv("LLM_PRIMARY_PROVIDER") or "gemini").strip().lower() or "gemini",
            fallback_provider=(os.getenv("LLM_FALLBACK_PROVIDER") or "openai").strip().lower() or "openai",
            max_retries=_safe_int_env("LLM_PROVIDER_RETRIES", default=2),
            cache_client=cache_client,
            cache_ttl_seconds=_safe_int_env("LLM_CACHE_TTL_SECONDS", default=900),
            usage_hook=usage_hook,
            provider_timeout_seconds=_safe_float_env("LLM_PROVIDER_TIMEOUT_SECONDS", default=45.0),
            request_timeout_seconds=_safe_float_env("LLM_REQUEST_TIMEOUT_SECONDS", default=60.0),
            retry_base_delay_seconds=_safe_float_env("LLM_RETRY_BASE_DELAY_SECONDS", default=0.2),
            retry_max_delay_seconds=_safe_float_env("LLM_RETRY_MAX_DELAY_SECONDS", default=2.0),
        )

    async def stream_text(self, request: LLMGatewayRequest) -> AsyncIterator[str]:
        started = perf_counter()
        shaped_prompt = _shape_prompt(request.prompt)
        if not shaped_prompt:
            raise LLMGatewayError("empty_prompt")

        provider_order = self._provider_order()
        effective_cache_key = _effective_cache_key(request, prompt=shaped_prompt, provider_order=provider_order)
        cached_text, cache_error = await self._load_cached_response(effective_cache_key)
        cache_status = "error" if cache_error else "miss"
        if cached_text is not None:
            self._emit_observation(
                operation="stream_text",
                provider="cache",
                latency_ms=_elapsed_ms(started),
                cache_hit=True,
                cache_status="hit",
                retry_count=0,
                fallback_used=False,
                tokens_in=estimate_tokens(shaped_prompt),
                tokens_out=estimate_tokens(cached_text),
                estimated_cost=0.0,
                usage_recorded=False,
                usage_record_error=None,
                error=None,
            )
            yield cached_text
            return

        inflight_owner = False
        inflight_future: asyncio.Future[str] | None = None
        if effective_cache_key:
            inflight_future, inflight_owner = await self._claim_inflight(effective_cache_key)
            if not inflight_owner:
                waited = await self._await_inflight_response(inflight_future)
                if waited is not None:
                    self._emit_observation(
                        operation="stream_text",
                        provider="inflight",
                        latency_ms=_elapsed_ms(started),
                        cache_hit=True,
                        cache_status="inflight",
                        retry_count=0,
                        fallback_used=False,
                        tokens_in=estimate_tokens(shaped_prompt),
                        tokens_out=estimate_tokens(waited),
                        estimated_cost=0.0,
                        usage_recorded=False,
                        usage_record_error=None,
                        error=None,
                    )
                    yield waited
                    return

                inflight_future, inflight_owner = await self._claim_inflight(effective_cache_key)

        provider_names = provider_order
        if not provider_names:
            completion_exc = LLMGatewayError("no_configured_provider")
            await self._finalize_inflight(effective_cache_key, inflight_future, None, completion_exc)
            raise completion_exc

        errors: list[str] = []
        retry_count = 0
        usage_recorded = False
        usage_record_error: str | None = None
        fallback_used = False
        completion_text: str | None = None
        completion_exc: Exception | None = None
        failure_error: str | None = None

        try:
            async with asyncio.timeout(self._request_timeout_seconds):
                for provider_index, provider_name in enumerate(provider_names):
                    provider = self._providers.get(provider_name)
                    if provider is None or not provider.has_api_key:
                        continue

                    fallback_used = provider_index > 0

                    for attempt in range(self._max_retries + 1):
                        emitted = False
                        chunks: list[str] = []
                        try:
                            async with asyncio.timeout(self._provider_timeout_seconds):
                                async for chunk in provider.stream_json_text(
                                    prompt=shaped_prompt,
                                    max_output_tokens=request.max_output_tokens,
                                    response_mime_type=request.response_mime_type,
                                ):
                                    emitted = True
                                    chunks.append(chunk)
                                    yield chunk

                            raw_text = "".join(chunks).strip()
                            if not raw_text:
                                raise ProviderResponseError("empty_provider_response")

                            expects_json = request.response_mime_type.strip().lower() == "application/json"
                            if expects_json and _parse_json_object(_normalize_response(raw_text)) is None:
                                raise ProviderResponseError("invalid_json_response")

                            cache_persisted = await self._persist_cached_response(effective_cache_key, raw_text)
                            usage_recorded, usage_record_error = await self._record_usage(
                                user_id=request.user_id,
                                prompt=shaped_prompt,
                                raw_text=raw_text,
                            )
                            completion_text = raw_text
                            cache_status = "miss_persisted" if cache_persisted else "miss_persist_failed"
                            self._emit_observation(
                                operation="stream_text",
                                provider=provider_name,
                                latency_ms=_elapsed_ms(started),
                                cache_hit=False,
                                cache_status=cache_status,
                                retry_count=retry_count,
                                fallback_used=fallback_used,
                                tokens_in=estimate_tokens(shaped_prompt),
                                tokens_out=estimate_tokens(raw_text),
                                estimated_cost=self._estimate_cost(
                                    tokens_in=estimate_tokens(shaped_prompt),
                                    tokens_out=estimate_tokens(raw_text),
                                ),
                                usage_recorded=usage_recorded,
                                usage_record_error=usage_record_error,
                                error=None,
                            )
                            return
                        except Exception as exc:
                            category = _error_category(exc)
                            errors.append(f"{provider_name}#{attempt + 1}:{category}:{exc}")

                            if emitted:
                                completion_exc = LLMGatewayError("stream_interrupted")
                                raise completion_exc from exc

                            if _should_retry(exc) and attempt < self._max_retries:
                                retry_count += 1
                                await asyncio.sleep(self._retry_delay_seconds(attempt))
                                continue

                            if not _is_fallback_eligible(exc):
                                completion_exc = LLMGatewayError(f"{category}:{exc}")
                                raise completion_exc from exc
                            break

                failure_error = "; ".join(errors) if errors else "provider_failed"
                completion_exc = LLMGatewayError(failure_error)
        except TimeoutError as exc:
            errors.append("request_timeout")
            failure_error = "request_timeout"
            completion_exc = LLMGatewayError("request_timeout")
            raise completion_exc from exc
        finally:
            await self._finalize_inflight(effective_cache_key, inflight_future, completion_text, completion_exc)

        final_error = failure_error or ("; ".join(errors) if errors else "provider_failed")
        completion_exc = completion_exc or LLMGatewayError(final_error)
        self._emit_observation(
            operation="stream_text",
            provider=None,
            latency_ms=_elapsed_ms(started),
            cache_hit=False,
            cache_status=cache_status,
            retry_count=retry_count,
            fallback_used=fallback_used,
            tokens_in=estimate_tokens(shaped_prompt),
            tokens_out=0,
            estimated_cost=0.0,
            usage_recorded=usage_recorded,
            usage_record_error=usage_record_error,
            error=final_error,
        )
        raise completion_exc

    async def generate_text(self, request: LLMGatewayRequest) -> LLMGatewayResult:
        started = perf_counter()
        shaped_prompt = _shape_prompt(request.prompt)
        provider_order = self._provider_order()
        effective_cache_key = _effective_cache_key(request, prompt=shaped_prompt, provider_order=provider_order)
        tokens_in = estimate_tokens(shaped_prompt) if shaped_prompt else 0

        if not shaped_prompt:
            usage = _usage_payload(
                tokens_in=0,
                tokens_out=0,
                estimated_cost=0.0,
                retry_count=0,
                fallback_used=False,
                cache_status="miss",
                usage_recorded=False,
                usage_record_error=None,
            )
            failure_response = _failure_response("empty_prompt")
            self._emit_observation(
                operation="generate_text",
                provider=None,
                latency_ms=_elapsed_ms(started),
                cache_hit=False,
                cache_status="miss",
                retry_count=0,
                fallback_used=False,
                tokens_in=0,
                tokens_out=0,
                estimated_cost=0.0,
                usage_recorded=False,
                usage_record_error=None,
                error="empty_prompt",
            )
            return LLMGatewayResult(
                response=failure_response,
                cache_hit=False,
                usage=usage,
                raw_text=None,
                normalized_text=None,
                parsed_json=None,
                provider=None,
                tokens_in=0,
                tokens_out=0,
                estimated_cost=0.0,
                latency_ms=_elapsed_ms(started),
                error="empty_prompt",
            )

        cached_text, cache_error = await self._load_cached_response(effective_cache_key)
        cache_status = "error" if cache_error else "miss"
        if cached_text is not None:
            normalized = _normalize_response(cached_text)
            parsed = _parse_json_object(normalized)
            response_payload = _response_payload(parsed, normalized)
            tokens_out = estimate_tokens(cached_text)
            usage = _usage_payload(
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                estimated_cost=0.0,
                retry_count=0,
                fallback_used=False,
                cache_status="hit",
                usage_recorded=False,
                usage_record_error=None,
            )
            self._emit_observation(
                operation="generate_text",
                provider="cache",
                latency_ms=_elapsed_ms(started),
                cache_hit=True,
                cache_status="hit",
                retry_count=0,
                fallback_used=False,
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                estimated_cost=0.0,
                usage_recorded=False,
                usage_record_error=None,
                error=None,
            )
            return LLMGatewayResult(
                response=response_payload,
                cache_hit=True,
                usage=usage,
                raw_text=cached_text,
                normalized_text=normalized,
                parsed_json=parsed,
                provider="cache",
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                estimated_cost=0.0,
                latency_ms=_elapsed_ms(started),
                error=None,
            )

        inflight_owner = False
        inflight_future: asyncio.Future[str] | None = None
        if effective_cache_key:
            inflight_future, inflight_owner = await self._claim_inflight(effective_cache_key)
            if not inflight_owner:
                waited = await self._await_inflight_response(inflight_future)
                if waited is not None:
                    normalized = _normalize_response(waited)
                    parsed = _parse_json_object(normalized)
                    tokens_out = estimate_tokens(waited)
                    usage = _usage_payload(
                        tokens_in=tokens_in,
                        tokens_out=tokens_out,
                        estimated_cost=0.0,
                        retry_count=0,
                        fallback_used=False,
                        cache_status="inflight",
                        usage_recorded=False,
                        usage_record_error=None,
                    )
                    self._emit_observation(
                        operation="generate_text",
                        provider="inflight",
                        latency_ms=_elapsed_ms(started),
                        cache_hit=True,
                        cache_status="inflight",
                        retry_count=0,
                        fallback_used=False,
                        tokens_in=tokens_in,
                        tokens_out=tokens_out,
                        estimated_cost=0.0,
                        usage_recorded=False,
                        usage_record_error=None,
                        error=None,
                    )
                    return LLMGatewayResult(
                        response=_response_payload(parsed, normalized),
                        cache_hit=True,
                        usage=usage,
                        raw_text=waited,
                        normalized_text=normalized,
                        parsed_json=parsed,
                        provider="inflight",
                        tokens_in=tokens_in,
                        tokens_out=tokens_out,
                        estimated_cost=0.0,
                        latency_ms=_elapsed_ms(started),
                        error=None,
                    )

                inflight_future, inflight_owner = await self._claim_inflight(effective_cache_key)

        provider_names = provider_order
        if not provider_names:
            usage = _usage_payload(
                tokens_in=tokens_in,
                tokens_out=0,
                estimated_cost=0.0,
                retry_count=0,
                fallback_used=False,
                cache_status=cache_status,
                usage_recorded=False,
                usage_record_error=None,
            )
            failure_response = _failure_response("no_configured_provider")
            failure_result = LLMGatewayResult(
                response=failure_response,
                cache_hit=False,
                usage=usage,
                raw_text=None,
                normalized_text=None,
                parsed_json=None,
                provider=None,
                tokens_in=tokens_in,
                tokens_out=0,
                estimated_cost=0.0,
                latency_ms=_elapsed_ms(started),
                error="no_configured_provider",
            )
            self._emit_observation(
                operation="generate_text",
                provider=None,
                latency_ms=failure_result.latency_ms,
                cache_hit=False,
                cache_status=cache_status,
                retry_count=0,
                fallback_used=False,
                tokens_in=tokens_in,
                tokens_out=0,
                estimated_cost=0.0,
                usage_recorded=False,
                usage_record_error=None,
                error="no_configured_provider",
            )
            await self._finalize_inflight(
                effective_cache_key,
                inflight_future,
                None,
                LLMGatewayError("no_configured_provider"),
            )
            return failure_result

        errors: list[str] = []
        retry_count = 0
        fallback_used = False
        usage_recorded = False
        usage_record_error: str | None = None
        completion_text: str | None = None
        completion_exc: Exception | None = None
        failure_error: str | None = None

        try:
            async with asyncio.timeout(self._request_timeout_seconds):
                for provider_index, provider_name in enumerate(provider_names):
                    provider = self._providers.get(provider_name)
                    if provider is None or not provider.has_api_key:
                        continue

                    fallback_used = provider_index > 0

                    for attempt in range(self._max_retries + 1):
                        chunks: list[str] = []
                        try:
                            async with asyncio.timeout(self._provider_timeout_seconds):
                                async for chunk in provider.stream_json_text(
                                    prompt=shaped_prompt,
                                    max_output_tokens=request.max_output_tokens,
                                    response_mime_type=request.response_mime_type,
                                ):
                                    chunks.append(chunk)

                            raw_text = "".join(chunks).strip()
                            if not raw_text:
                                raise ProviderResponseError("empty_provider_response")

                            normalized = _normalize_response(raw_text)
                            parsed = _parse_json_object(normalized)

                            expects_json = request.response_mime_type.strip().lower() == "application/json"
                            if expects_json and parsed is None:
                                raise ProviderResponseError("invalid_json_response")

                            tokens_out = estimate_tokens(raw_text)
                            response_payload = _response_payload(parsed, normalized)

                            cache_persisted = await self._persist_cached_response(effective_cache_key, raw_text)
                            usage_recorded, usage_record_error = await self._record_usage(
                                user_id=request.user_id,
                                prompt=shaped_prompt,
                                raw_text=raw_text,
                            )

                            estimated_cost = self._estimate_cost(tokens_in=tokens_in, tokens_out=tokens_out)
                            completion_text = raw_text
                            cache_status = "miss_persisted" if cache_persisted else "miss_persist_failed"
                            usage = _usage_payload(
                                tokens_in=tokens_in,
                                tokens_out=tokens_out,
                                estimated_cost=estimated_cost,
                                retry_count=retry_count,
                                fallback_used=fallback_used,
                                cache_status=cache_status,
                                usage_recorded=usage_recorded,
                                usage_record_error=usage_record_error,
                            )

                            result = LLMGatewayResult(
                                response=response_payload,
                                cache_hit=False,
                                usage=usage,
                                raw_text=raw_text,
                                normalized_text=normalized,
                                parsed_json=parsed,
                                provider=provider_name,
                                tokens_in=tokens_in,
                                tokens_out=tokens_out,
                                estimated_cost=estimated_cost,
                                latency_ms=_elapsed_ms(started),
                                error=None,
                            )
                            self._emit_observation(
                                operation="generate_text",
                                provider=provider_name,
                                latency_ms=result.latency_ms,
                                cache_hit=False,
                                cache_status=cache_status,
                                retry_count=retry_count,
                                fallback_used=fallback_used,
                                tokens_in=tokens_in,
                                tokens_out=tokens_out,
                                estimated_cost=estimated_cost,
                                usage_recorded=usage_recorded,
                                usage_record_error=usage_record_error,
                                error=None,
                            )
                            return result
                        except Exception as exc:
                            category = _error_category(exc)
                            errors.append(f"{provider_name}#{attempt + 1}:{category}:{exc}")

                            if _should_retry(exc) and attempt < self._max_retries:
                                retry_count += 1
                                await asyncio.sleep(self._retry_delay_seconds(attempt))
                                continue

                            if not _is_fallback_eligible(exc):
                                completion_exc = LLMGatewayError(f"{category}:{exc}")
                                raise completion_exc from exc
                            break

                failure_error = "; ".join(errors) if errors else "provider_failed"
                completion_exc = LLMGatewayError(failure_error)
        except TimeoutError as exc:
            errors.append("request_timeout")
            completion_exc = LLMGatewayError("request_timeout")
            completion_text = None
            failure_error = "request_timeout"
            usage = _usage_payload(
                tokens_in=tokens_in,
                tokens_out=0,
                estimated_cost=0.0,
                retry_count=retry_count,
                fallback_used=fallback_used,
                cache_status=cache_status,
                usage_recorded=False,
                usage_record_error=None,
            )
            failure = LLMGatewayResult(
                response=_failure_response(failure_error),
                cache_hit=False,
                usage=usage,
                raw_text=None,
                normalized_text=None,
                parsed_json=None,
                provider=None,
                tokens_in=tokens_in,
                tokens_out=0,
                estimated_cost=0.0,
                latency_ms=_elapsed_ms(started),
                error=failure_error,
            )
            self._emit_observation(
                operation="generate_text",
                provider=None,
                latency_ms=failure.latency_ms,
                cache_hit=False,
                cache_status=cache_status,
                retry_count=retry_count,
                fallback_used=fallback_used,
                tokens_in=tokens_in,
                tokens_out=0,
                estimated_cost=0.0,
                usage_recorded=False,
                usage_record_error=None,
                error=failure_error,
            )
            return failure
        finally:
            await self._finalize_inflight(effective_cache_key, inflight_future, completion_text, completion_exc)

        failure_error = failure_error or ("; ".join(errors) if errors else "provider_failed")
        usage = _usage_payload(
            tokens_in=tokens_in,
            tokens_out=0,
            estimated_cost=0.0,
            retry_count=retry_count,
            fallback_used=fallback_used,
            cache_status=cache_status,
            usage_recorded=False,
            usage_record_error=None,
        )
        failure_response = _failure_response(failure_error)
        failure = LLMGatewayResult(
            response=failure_response,
            cache_hit=False,
            usage=usage,
            raw_text=None,
            normalized_text=None,
            parsed_json=None,
            provider=None,
            tokens_in=tokens_in,
            tokens_out=0,
            estimated_cost=0.0,
            latency_ms=_elapsed_ms(started),
            error=failure_error,
        )
        self._emit_observation(
            operation="generate_text",
            provider=None,
            latency_ms=failure.latency_ms,
            cache_hit=False,
            cache_status=cache_status,
            retry_count=retry_count,
            fallback_used=fallback_used,
            tokens_in=tokens_in,
            tokens_out=0,
            estimated_cost=0.0,
            usage_recorded=False,
            usage_record_error=None,
            error=failure_error,
        )
        return failure

    def _provider_order(self) -> list[str]:
        order: list[str] = []
        if self._primary_provider:
            order.append(self._primary_provider)
        if self._fallback_provider and self._fallback_provider != self._primary_provider:
            order.append(self._fallback_provider)

        for name in self._providers:
            if name not in order:
                order.append(name)

        return order

    async def _load_cached_response(self, cache_key: str | None) -> tuple[str | None, bool]:
        if self._cache is None or not cache_key:
            return None, False
        try:
            return await self._cache.get(_cache_key(cache_key)), False
        except Exception as exc:
            logger.warning("llm_gateway_cache_read_failed: %s", exc)
            return None, True

    async def _persist_cached_response(self, cache_key: str | None, raw_text: str) -> bool:
        if self._cache is None or not cache_key or not raw_text:
            return False
        try:
            await self._cache.set(_cache_key(cache_key), raw_text, ttl_seconds=self._cache_ttl_seconds)
            return True
        except Exception as exc:
            logger.warning("llm_gateway_cache_write_failed: %s", exc)
            return False

    async def _record_usage(self, *, user_id: str | None, prompt: str, raw_text: str) -> tuple[bool, str | None]:
        if self._usage_hook is None:
            return False, None

        normalized_user = (user_id or "").strip()
        if not normalized_user:
            return False, None

        try:
            await self._usage_hook.record_usage(
                normalized_user,
                tokens_in=estimate_tokens(prompt),
                tokens_out=estimate_tokens(raw_text),
            )
            return True, None
        except Exception as exc:
            message = str(exc)
            logger.warning("llm_gateway_usage_record_failed: %s", message)
            return False, message

    def _estimate_cost(self, *, tokens_in: int, tokens_out: int) -> float:
        if self._usage_hook is None:
            return 0.0
        try:
            return float(self._usage_hook.estimate_cost(tokens_in=tokens_in, tokens_out=tokens_out))
        except Exception as exc:
            logger.warning("llm_gateway_cost_estimate_failed: %s", exc)
            return 0.0

    async def _claim_inflight(self, cache_key: str) -> tuple[asyncio.Future[str], bool]:
        async with self._inflight_lock:
            existing = self._inflight_responses.get(cache_key)
            if existing is not None and not existing.done():
                return existing, False

            future = asyncio.get_running_loop().create_future()
            future.add_done_callback(_consume_future_exception)
            self._inflight_responses[cache_key] = future
            return future, True

    async def _await_inflight_response(self, inflight: asyncio.Future[str]) -> str | None:
        try:
            return await asyncio.wait_for(
                asyncio.shield(inflight),
                timeout=self._request_timeout_seconds,
            )
        except Exception:
            return None

    async def _finalize_inflight(
        self,
        cache_key: str | None,
        inflight: asyncio.Future[str] | None,
        response_text: str | None,
        failure_exc: Exception | None,
    ) -> None:
        if not cache_key or inflight is None:
            return

        if not inflight.done():
            if response_text is not None:
                inflight.set_result(response_text)
            elif failure_exc is not None:
                inflight.set_exception(failure_exc)
            else:
                inflight.set_exception(LLMGatewayError("inflight_unresolved"))

        async with self._inflight_lock:
            current = self._inflight_responses.get(cache_key)
            if current is inflight:
                self._inflight_responses.pop(cache_key, None)

    def _retry_delay_seconds(self, attempt: int) -> float:
        computed = self._retry_base_delay_seconds * (2 ** max(0, attempt))
        return min(self._retry_max_delay_seconds, computed)

    def _emit_observation(
        self,
        *,
        operation: str,
        provider: str | None,
        latency_ms: float,
        cache_hit: bool,
        cache_status: str,
        retry_count: int,
        fallback_used: bool,
        tokens_in: int,
        tokens_out: int,
        estimated_cost: float,
        usage_recorded: bool,
        usage_record_error: str | None,
        error: str | None,
    ) -> None:
        logger.info(
            "llm_gateway_runtime operation=%s provider=%s latency_ms=%.3f cache_hit=%s cache_status=%s retries=%d fallback=%s tokens_in=%d tokens_out=%d estimated_cost=%.8f usage_recorded=%s usage_record_error=%s error=%s",
            operation,
            provider or "none",
            latency_ms,
            cache_hit,
            cache_status,
            retry_count,
            fallback_used,
            tokens_in,
            tokens_out,
            estimated_cost,
            usage_recorded,
            usage_record_error or "",
            error or "",
        )


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _shape_prompt(prompt: str) -> str:
    return str(prompt or "").strip()


def _normalize_response(raw_text: str) -> str:
    return _unwrap_markdown_json(raw_text)


def _response_payload(parsed: dict[str, Any] | None, normalized: str) -> dict[str, Any] | str:
    return parsed if isinstance(parsed, dict) else normalized


def _usage_payload(
    *,
    tokens_in: int,
    tokens_out: int,
    estimated_cost: float,
    retry_count: int,
    fallback_used: bool,
    cache_status: str,
    usage_recorded: bool,
    usage_record_error: str | None,
) -> dict[str, Any]:
    return {
        "tokens_in": int(tokens_in),
        "tokens_out": int(tokens_out),
        "estimated_cost": float(estimated_cost),
        "retry_count": int(retry_count),
        "fallback_used": bool(fallback_used),
        "cache_status": cache_status,
        "usage_recorded": bool(usage_recorded),
        "usage_record_error": usage_record_error,
    }


def _failure_response(error_message: str) -> dict[str, Any]:
    return {
        "error": "llm_gateway_failure",
        "message": error_message,
    }


def _effective_cache_key(request: LLMGatewayRequest, *, prompt: str, provider_order: list[str]) -> str | None:
    if request.cache_key:
        return request.cache_key

    strategy = ">".join(provider_order)
    context_hash = (request.context_hash or "").strip()
    metadata = request.metadata or {}
    metadata_part = json.dumps(metadata, sort_keys=True, separators=(",", ":"))
    digest_source = "|".join(
        [
            prompt,
            strategy,
            context_hash,
            str(request.max_output_tokens),
            request.response_mime_type,
            metadata_part,
        ]
    )
    digest = sha256(digest_source.encode("utf-8")).hexdigest()
    return f"llm:request:{digest}"


async def _stream_gemini_via_gateway(
    *,
    api_key: str,
    model: str,
    temperature: float,
    prompt: str,
    max_output_tokens: int,
    response_mime_type: str,
    timeout_seconds: float,
) -> AsyncIterator[str]:
    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/{model}"
        f":streamGenerateContent?alt=sse&key={api_key}"
    )

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": temperature,
            "topK": 1,
            "topP": 0.8,
            "maxOutputTokens": max_output_tokens,
            "responseMimeType": response_mime_type,
        },
    }

    http_client = ExternalHTTPClient(timeout_seconds=timeout_seconds)
    async for line in http_client.stream_lines(
        method="POST",
        url=endpoint,
        json_body=payload,
        headers={"Content-Type": "application/json"},
    ):
        chunk = _extract_gemini_chunk(line)
        if chunk:
            yield chunk


def _extract_gemini_chunk(line: str) -> str:
    if not line or not line.startswith("data:"):
        return ""

    payload_text = line[len("data:") :].strip()
    if payload_text in {"", "[DONE]"}:
        return ""

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return ""

    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        return ""

    chunks: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        content = candidate.get("content")
        if not isinstance(content, dict):
            continue
        parts = content.get("parts")
        if not isinstance(parts, list):
            continue
        for part in parts:
            if isinstance(part, dict):
                text = part.get("text")
                if isinstance(text, str) and text:
                    chunks.append(text)

    return "".join(chunks)


def _should_retry(exc: Exception) -> bool:
    return _error_category(exc) in {
        "transient_timeout",
        "transient_network",
        "transient_rate_limit",
    }


def _is_fallback_eligible(exc: Exception) -> bool:
    category = _error_category(exc)
    return category.startswith("provider_") or category.startswith("transient_")


def _error_category(exc: Exception) -> str:
    if isinstance(exc, ProviderResponseError):
        return "provider_invalid_response"

    if isinstance(exc, LLMGatewayError):
        return "gateway_error"

    class_name = exc.__class__.__name__.lower()
    message = str(exc).lower()

    if "timeout" in class_name or "timeout" in message or "timed out" in message:
        return "transient_timeout"

    if "transient" in class_name:
        if "429" in message or "rate" in message:
            return "transient_rate_limit"
        return "transient_network"

    if any(token in message for token in ["429", "rate limit", "too many requests"]):
        return "transient_rate_limit"

    if any(token in message for token in ["network", "connection", "reset by peer", "temporarily unavailable", "503", "502", "504"]):
        return "transient_network"

    if "permanent" in class_name:
        return "provider_permanent"

    if any(token in message for token in ["401", "403", "unauthorized", "forbidden", "auth", "authentication"]):
        return "provider_auth"

    if "not implemented" in message:
        return "provider_not_implemented"

    if any(token in message for token in ["invalid", "schema", "malformed", "json"]):
        return "provider_invalid_response"

    return "provider_error"


def _safe_int_env(name: str, *, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def _safe_float_env(name: str, *, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        return default


def _cache_key(cache_key: str) -> str:
    return f"llm:response:{cache_key}"


def _unwrap_markdown_json(text: str) -> str:
    candidate = text.strip()
    if not candidate.startswith("```"):
        return candidate

    lines = [line for line in candidate.splitlines() if not line.strip().startswith("```")]
    return "\n".join(lines).strip()


def _parse_json_object(text: str) -> dict[str, Any] | None:
    if not text:
        return None

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            parsed = json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            return None

    return parsed if isinstance(parsed, dict) else None


def _elapsed_ms(started: float) -> float:
    return round((perf_counter() - started) * 1000.0, 3)


def _consume_future_exception(future: asyncio.Future[Any]) -> None:
    if future.cancelled():
        return

    try:
        future.exception()
    except Exception:
        return
