"""Service facade for LLM orchestration.

Layer responsibility:
- prepare service payloads and delegate all LLM execution to app.services.llm_gateway.gateway

Allowed internal imports:
- app.services.llm_gateway.*
- app.schemas.*

Forbidden internal imports:
- app.adapters.llm.providers.*
- direct external SDK/library calls
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from time import perf_counter
from typing import Any, AsyncIterator

from app.services.llm_gateway.gateway import LLMGateway, LLMGatewayRequest
from app.schemas.email_schema import fallback_email_response, validate_email_payload
from app.services.usage.limiter import estimate_tokens


_JSON_RE = re.compile(r"\{.*\}", flags=re.DOTALL)


@dataclass(frozen=True)
class EmailGatewayInput:
    user_id: str
    sender: str
    subject: str
    to_me: bool
    cc_me: bool
    messages: list[str]
    rule_payload: dict[str, Any]


@dataclass(frozen=True)
class LLMEmailAnalysisResult:
    payload: dict[str, Any]
    tokens_in: int
    tokens_out: int
    estimated_cost: float
    latency_ms: float
    llm_used: bool


class LLMGatewayService:
    def __init__(self, *, llm_gateway: LLMGateway) -> None:
        self._gateway = llm_gateway

    def estimate_email_prompt_tokens(self, *, sender: str, subject: str, to_me: bool, cc_me: bool, messages: list[str]) -> int:
        prompt = _build_email_prompt(
            sender=sender,
            subject=subject,
            to_me=to_me,
            cc_me=cc_me,
            thread_context=_build_thread_context(messages),
        )
        return estimate_tokens(prompt)

    def estimate_pantry_prompt_tokens(self, *, items: list[str]) -> int:
        prompt = _build_pantry_prompt(items=items)
        return estimate_tokens(prompt)

    def estimate_schedule_prompt_tokens(self, *, title: str, details: str) -> int:
        prompt = _build_schedule_prompt(title=title, details=details)
        return estimate_tokens(prompt)

    async def stream_email_analysis(self, request: EmailGatewayInput) -> AsyncIterator[dict[str, Any]]:
        fallback = _normalize_email_payload(request.rule_payload)

        prompt = _build_email_prompt(
            sender=request.sender,
            subject=request.subject,
            to_me=request.to_me,
            cc_me=request.cc_me,
            thread_context=_build_thread_context(request.messages),
        )

        chunks: list[str] = []

        try:
            async for chunk in self._gateway.stream_text(
                LLMGatewayRequest(
                    prompt=prompt,
                    user_id=request.user_id,
                    cache_key=None,
                )
            ):
                chunks.append(chunk)
                yield {"type": "chunk", "content": chunk}
        except Exception as exc:
            yield {"type": "error"}
            fallback["reason"] = _gateway_error_reason(str(exc))
            yield {"type": "final", "data": fallback}
            return

        raw_response = "".join(chunks).strip()

        parsed = _parse_json_object(raw_response)
        if parsed is None:
            fallback["reason"] = "invalid_llm_json"
            yield {"type": "final", "data": fallback}
            return

        valid, _errors = validate_email_payload(parsed)
        if not valid:
            fallback["reason"] = "invalid_llm_schema"
            yield {"type": "final", "data": fallback}
            return

        parsed.setdefault("reason", "")
        yield {"type": "final", "data": _normalize_email_payload(parsed)}

    async def analyze_email(
        self,
        *,
        user_id: str,
        sender: str,
        subject: str,
        to_me: bool,
        cc_me: bool,
        messages: list[str],
        fallback_payload: dict[str, Any],
    ) -> dict[str, Any]:
        result = await self.analyze_email_with_metrics(
            user_id=user_id,
            sender=sender,
            subject=subject,
            to_me=to_me,
            cc_me=cc_me,
            messages=messages,
            fallback_payload=fallback_payload,
        )
        return result.payload

    async def analyze_email_with_metrics(
        self,
        *,
        user_id: str,
        sender: str,
        subject: str,
        to_me: bool,
        cc_me: bool,
        messages: list[str],
        fallback_payload: dict[str, Any],
    ) -> LLMEmailAnalysisResult:
        started = perf_counter()
        fallback = _normalize_email_payload(dict(fallback_payload))

        prompt = _build_email_prompt(
            sender=sender,
            subject=subject,
            to_me=to_me,
            cc_me=cc_me,
            thread_context=_build_thread_context(messages),
        )

        result = await self._gateway.generate_text(
            LLMGatewayRequest(
                prompt=prompt,
                user_id=user_id,
                cache_key=None,
            )
        )

        if not result.raw_text:
            reason = _gateway_error_reason(result.error or "")
            fallback["reason"] = reason
            return LLMEmailAnalysisResult(
                payload=fallback,
                tokens_in=result.tokens_in,
                tokens_out=0,
                estimated_cost=result.estimated_cost,
                latency_ms=_elapsed_ms(started),
                llm_used=(reason != "gemini_unavailable"),
            )

        raw_response = result.normalized_text or result.raw_text
        token_in = result.tokens_in
        token_out = result.tokens_out
        estimated_cost = result.estimated_cost

        parsed = result.parsed_json or _parse_json_object(raw_response)
        if parsed is None:
            fallback["reason"] = "invalid_llm_json"
            return LLMEmailAnalysisResult(
                payload=fallback,
                tokens_in=token_in,
                tokens_out=token_out,
                estimated_cost=estimated_cost,
                latency_ms=_elapsed_ms(started),
                llm_used=True,
            )

        valid, _errors = validate_email_payload(parsed)
        if not valid:
            fallback["reason"] = "invalid_llm_schema"
            return LLMEmailAnalysisResult(
                payload=fallback,
                tokens_in=token_in,
                tokens_out=token_out,
                estimated_cost=estimated_cost,
                latency_ms=_elapsed_ms(started),
                llm_used=True,
            )

        parsed.setdefault("reason", "")
        return LLMEmailAnalysisResult(
            payload=_normalize_email_payload(parsed),
            tokens_in=token_in,
            tokens_out=token_out,
            estimated_cost=estimated_cost,
            latency_ms=_elapsed_ms(started),
            llm_used=(result.provider != "cache"),
        )

    async def enhance_pantry(self, *, user_id: str, items: list[str], fallback_payload: dict[str, Any]) -> dict[str, Any]:
        prompt = _build_pantry_prompt(items=items)
        result = await self._gateway.generate_text(LLMGatewayRequest(prompt=prompt, user_id=user_id, cache_key=None))
        if not result.raw_text:
            return dict(fallback_payload)

        parsed = result.parsed_json or _parse_json_object(result.normalized_text or result.raw_text)
        if not isinstance(parsed, dict):
            return dict(fallback_payload)

        state_summary = str(parsed.get("state_summary") or "").strip()
        reason = str(parsed.get("reason") or "").strip()
        meals_raw = parsed.get("suggested_meals")
        meals = [str(item).strip() for item in meals_raw if str(item).strip()] if isinstance(meals_raw, list) else []

        if not state_summary or not reason or not meals:
            return dict(fallback_payload)

        return {
            "state_summary": state_summary,
            "suggested_meals": meals[:4],
            "reason": reason,
        }

    async def enhance_schedule(
        self,
        *,
        user_id: str,
        title: str,
        details: str,
        fallback_payload: dict[str, Any],
    ) -> dict[str, Any]:
        prompt = _build_schedule_prompt(title=title, details=details)
        result = await self._gateway.generate_text(LLMGatewayRequest(prompt=prompt, user_id=user_id, cache_key=None))
        if not result.raw_text:
            return dict(fallback_payload)

        parsed = result.parsed_json or _parse_json_object(result.normalized_text or result.raw_text)
        if not isinstance(parsed, dict):
            return dict(fallback_payload)

        state_summary = str(parsed.get("state_summary") or "").strip()
        reason = str(parsed.get("reason") or "").strip()
        suggestions_raw = parsed.get("suggestions")
        suggestions = (
            [str(item).strip() for item in suggestions_raw if str(item).strip()]
            if isinstance(suggestions_raw, list)
            else []
        )

        if not state_summary or not reason or not suggestions:
            return dict(fallback_payload)

        return {
            "state_summary": state_summary,
            "suggestions": suggestions[:4],
            "reason": reason,
        }


def _build_thread_context(messages: list[str]) -> str:
    rows = [value.strip() for value in messages if value and value.strip()]
    return "\n\n---\n\n".join(rows[-3:])


def _build_email_prompt(*, sender: str, subject: str, to_me: bool, cc_me: bool, thread_context: str) -> str:
    return (
        "You are an email triage engine. Return JSON only.\n"
        "Schema:\n"
        "{\"priority\":\"high|medium|low\",\"needs_attention\":true|false,"
        "\"actions\":[{\"type\":\"reply|task\",\"title\":\"string\",\"due\":\"YYYY-MM-DD|null\"}],"
        "\"state_summary\":\"string\",\"reason\":\"string\"}\n"
        "Keep responses concise and deterministic.\n"
        f"sender={sender}\n"
        f"subject={subject}\n"
        f"to_me={to_me}\n"
        f"cc_me={cc_me}\n"
        "thread_context:\n"
        f"{thread_context}\n"
    )


def _build_pantry_prompt(*, items: list[str]) -> str:
    normalized = [item.strip() for item in items if item.strip()]
    return (
        "You are a pantry planning assistant. Return JSON only.\n"
        "Schema:{\"state_summary\":\"string\",\"suggested_meals\":[\"string\"],\"reason\":\"string\"}\n"
        f"items={normalized}\n"
    )


def _build_schedule_prompt(*, title: str, details: str) -> str:
    return (
        "You are a household scheduling assistant. Return JSON only.\n"
        "Schema:{\"state_summary\":\"string\",\"suggestions\":[\"string\"],\"reason\":\"string\"}\n"
        f"title={title.strip()}\n"
        f"details={details.strip()}\n"
    )


def _normalize_email_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "priority": str(payload.get("priority") or "medium").strip().lower(),
        "needs_attention": bool(payload.get("needs_attention", False)),
        "actions": [],
        "state_summary": str(payload.get("state_summary") or "").strip(),
        "reason": str(payload.get("reason") or "").strip(),
    }

    if normalized["priority"] not in {"high", "medium", "low"}:
        normalized["priority"] = "medium"

    actions = payload.get("actions")
    if isinstance(actions, list):
        for row in actions:
            if not isinstance(row, dict):
                continue
            action_type = str(row.get("type") or "").strip().lower()
            if action_type not in {"reply", "task"}:
                continue
            title = str(row.get("title") or "").strip()
            if not title:
                continue
            due_raw = row.get("due")
            due = str(due_raw).strip() if isinstance(due_raw, str) else None
            normalized["actions"].append({
                "type": action_type,
                "title": title,
                "due": due or None,
            })

    if not normalized["state_summary"]:
        fallback = fallback_email_response(reason=str(normalized["reason"] or "tier_limit_reached"))
        return fallback

    return normalized


def _parse_json_object(text: str) -> dict[str, Any] | None:
    candidate = _unwrap_markdown_json(text)
    if not candidate:
        return None

    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        match = _JSON_RE.search(candidate)
        if match is None:
            return None
        try:
            parsed = json.loads(match.group(0))
        except json.JSONDecodeError:
            return None

    if isinstance(parsed, dict):
        return parsed
    return None


def _unwrap_markdown_json(text: str) -> str:
    value = text.strip()
    if not value.startswith("```"):
        return value

    lines = [line for line in value.splitlines() if not line.strip().startswith("```")]
    return "\n".join(lines).strip()


def _elapsed_ms(started: float) -> float:
    return round((perf_counter() - started) * 1000.0, 3)


def _gateway_error_reason(error: str) -> str:
    lowered = error.lower()
    if "no_configured_provider" in lowered:
        return "gemini_unavailable"
    return "gemini_error"
