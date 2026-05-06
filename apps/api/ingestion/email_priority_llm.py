from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import date
from typing import Any

from pydantic import BaseModel, Field, ValidationError
from app.services.llm_gateway.gateway import LLMGateway, LLMGatewayRequest


_AMBIGUITY_SIGNALS = (
    "asap",
    "urgent",
    "deadline",
    "approve",
    "confirmation",
    "confirm",
    "review",
    "payment",
    "invoice",
    "reminder",
    "help",
    "?",
)


@dataclass(frozen=True)
class EmailPriorityLLMConfig:
    enabled: bool
    model: str
    api_key: str | None
    access_token: str | None
    timeout_seconds: float
    max_prompt_chars: int
    min_confidence: float
    max_requests_per_minute: int
    max_requests_per_day: int
    cache_ttl_seconds: int

    @classmethod
    def from_env(cls) -> "EmailPriorityLLMConfig":
        return cls(
            enabled=_env_bool("EMAIL_PRIORITY_LLM_ENABLED", default=False),
            model=os.getenv("EMAIL_PRIORITY_LLM_MODEL", "gemini-2.0-flash"),
            api_key=_clean_env("GOOGLE_API_KEY") or _clean_env("EMAIL_PRIORITY_LLM_API_KEY"),
            access_token=_clean_env("GOOGLE_ACCESS_TOKEN") or _clean_env("EMAIL_PRIORITY_LLM_ACCESS_TOKEN"),
            timeout_seconds=_env_float("EMAIL_PRIORITY_LLM_TIMEOUT_SECONDS", default=3.5),
            max_prompt_chars=_env_int("EMAIL_PRIORITY_LLM_MAX_PROMPT_CHARS", default=2200),
            min_confidence=_env_float("EMAIL_PRIORITY_LLM_MIN_CONFIDENCE", default=0.6),
            max_requests_per_minute=_env_int("EMAIL_PRIORITY_LLM_MAX_REQUESTS_PER_MIN", default=40),
            max_requests_per_day=_env_int("EMAIL_PRIORITY_LLM_MAX_REQUESTS_PER_DAY", default=1500),
            cache_ttl_seconds=_env_int("EMAIL_PRIORITY_LLM_CACHE_TTL_SECONDS", default=1800),
        )

    def has_credentials(self) -> bool:
        return bool(self.api_key or self.access_token)


class _ActionPayload(BaseModel):
    type: str = Field(min_length=1)
    title: str | None = None
    urgency: str | None = None
    due: str | None = None


class _TriagePayload(BaseModel):
    priority: str = Field(min_length=1)
    needs_attention: bool
    actions: list[_ActionPayload] = Field(default_factory=list)
    state_summary: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    confidence: float = 0.0


_budget_lock = threading.Lock()
_requests_last_minute: deque[float] = deque()
_requests_last_day: deque[float] = deque()
_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_gateway_instance: LLMGateway | None = None


def maybe_refine_email_priority(
    *,
    sender: str,
    subject: str,
    body: str,
    score: int | None = None,
    rule_score: int | None = None,
    to_me: bool | None = None,
    cc_me: bool | None = None,
    thread_id: str | None = None,
    latest_message_id: str | None = None,
    thread_context: str | None = None,
) -> dict[str, Any] | None:
    config = EmailPriorityLLMConfig.from_env()
    if not config.enabled or not config.has_credentials():
        return None

    resolved_score = _resolve_score(score=score, rule_score=rule_score)
    content_for_gate = thread_context or body
    if not _should_call_llm(score=resolved_score, subject=subject, body=content_for_gate):
        return None

    cache_key = _build_cache_key(
        sender=sender,
        subject=subject,
        body=content_for_gate,
        thread_id=thread_id,
        latest_message_id=latest_message_id,
    )
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    if not _consume_budget(config):
        return None

    prompt = _build_prompt(
        sender=sender,
        subject=subject,
        body=content_for_gate,
        to_me=to_me,
        cc_me=cc_me,
        max_prompt_chars=config.max_prompt_chars,
    )

    response = _request_gemini_priority(config=config, prompt=prompt)
    if response is None:
        return None

    parsed = _parse_response(response)
    if parsed is None:
        return None

    if parsed.get("confidence", 0.0) < config.min_confidence:
        return None

    normalized = _normalize_triage(parsed, body=content_for_gate, subject=subject)
    _set_cached(cache_key, normalized, config.cache_ttl_seconds)
    return normalized


def _resolve_score(*, score: int | None, rule_score: int | None) -> int:
    source = score if score is not None else rule_score
    if source is None:
        return 0
    try:
        return int(source)
    except (TypeError, ValueError):
        return 0


def _should_call_llm(*, score: int, subject: str, body: str) -> bool:
    if score >= 12:
        return True
    if score < 6:
        return False
    return _is_ambiguous(subject=subject, body=body)


def _is_ambiguous(*, subject: str, body: str) -> bool:
    merged = f"{subject}\n{body}".lower()
    return any(signal in merged for signal in _AMBIGUITY_SIGNALS)


def _build_prompt(
    *,
    sender: str,
    subject: str,
    body: str,
    to_me: bool | None,
    cc_me: bool | None,
    max_prompt_chars: int,
) -> str:
    compact_body = _compact(body, limit=max_prompt_chars)
    return (
        "You are an email triage classifier. Return only valid JSON.\n"
        "Rules:\n"
        "1) priority must be one of high, medium, low.\n"
        "2) needs_attention must be boolean.\n"
        "3) actions is a JSON array of actions.\n"
        "4) state_summary is 1-2 short sentences.\n"
        "5) reason is concise and factual.\n"
        "6) Default to conservative outputs if uncertain.\n"
        "7) If promotional/newsletter/unsubscribe, set low priority, needs_attention=false, actions=[].\n"
        "8) due must be YYYY-MM-DD or null.\n"
        "Input:\n"
        f"sender={sender}\n"
        f"to_me={bool(to_me)}\n"
        f"cc_me={bool(cc_me)}\n"
        f"subject={subject}\n"
        f"content={compact_body}\n"
        "Output schema:\n"
        "{"
        '"priority":"high|medium|low",'
        '"needs_attention":true,'
        '"actions":[{"type":"reply|task","title":"string","urgency":"high|normal","due":"YYYY-MM-DD|null"}],'
        '"state_summary":"string",'
        '"reason":"string",'
        '"confidence":0.0'
        "}"
    )


def _request_gemini_priority(*, config: EmailPriorityLLMConfig, prompt: str) -> dict[str, Any] | None:
    del config
    request = LLMGatewayRequest(
        prompt=prompt,
        user_id="email_priority",
        cache_key=f"email_priority:{hashlib.sha256(prompt.encode('utf-8')).hexdigest()[:24]}",
        response_mime_type="application/json",
        max_output_tokens=300,
    )
    try:
        result = _run_async(_llm_gateway().generate_text(request))
    except Exception:
        return None

    if isinstance(result.parsed_json, dict):
        return result.parsed_json
    if not result.raw_text:
        return None

    parsed = _parse_json_text(result.raw_text)
    return parsed if isinstance(parsed, dict) else None


def _llm_gateway() -> LLMGateway:
    global _gateway_instance
    if _gateway_instance is None:
        _gateway_instance = LLMGateway.from_default_providers()
    return _gateway_instance


def _run_async(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: dict[str, object] = {}
    error: list[BaseException] = []

    def _runner() -> None:
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover - defensive bridge
            error.append(exc)

    worker = threading.Thread(target=_runner, daemon=True)
    worker.start()
    worker.join()
    if error:
        raise error[0]
    return result.get("value")


def _parse_response(response: dict[str, Any]) -> dict[str, Any] | None:
    text = _extract_text_response(response)
    if not text:
        return None

    decoded = _parse_json_text(text)
    if not isinstance(decoded, dict):
        return None

    try:
        model = _TriagePayload.model_validate(decoded)
    except ValidationError:
        return None
    return model.model_dump()


def _extract_text_response(response: dict[str, Any]) -> str | None:
    candidates = response.get("candidates")
    if not isinstance(candidates, list):
        return None

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
            if not isinstance(part, dict):
                continue
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()
    return None


def _parse_json_text(text: str) -> Any:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = _unwrap_markdown_json(stripped)

    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", stripped, flags=re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None


def _unwrap_markdown_json(text: str) -> str:
    lines = [line for line in text.splitlines() if not line.strip().startswith("```")]
    return "\n".join(lines).strip()


def _normalize_triage(raw: dict[str, Any], *, body: str, subject: str) -> dict[str, Any]:
    priority = _normalize_priority(raw.get("priority"))
    needs_attention = bool(raw.get("needs_attention", False))
    reason = _compact(str(raw.get("reason", "")), 180)
    state_summary = _normalize_state_summary(raw.get("state_summary"))

    hard_negative = _is_hard_negative(subject=subject, body=body)
    actions = _normalize_actions(raw.get("actions"), subject=subject)

    if hard_negative:
        priority = "low"
        needs_attention = False
        actions = []
        if not reason:
            reason = "Hard-negative promotional or unsubscribe signal."
        if not state_summary:
            state_summary = "Promotional or low-signal thread; no action required."

    if priority == "low" and not actions:
        needs_attention = False

    triage_decision = "task" if (needs_attention or actions) else "informational"
    is_junk = hard_negative and not actions
    if is_junk:
        triage_decision = "junk"

    confidence = _clamp_float(raw.get("confidence", 0.0), minimum=0.0, maximum=1.0)
    importance_bucket = _importance_bucket_for_priority(priority)

    return {
        "priority": priority,
        "priority_label": priority,
        "needs_attention": needs_attention,
        "actions": actions,
        "state_summary": state_summary,
        "reason": reason,
        "confidence": confidence,
        "score": _score_for_priority(priority),
        "importance_bucket": importance_bucket,
        "importance_score": _importance_score_for_priority(priority),
        "triage_decision": triage_decision,
        "is_junk": is_junk,
        "llm_refined": True,
    }


def _normalize_actions(value: Any, *, subject: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    actions: list[dict[str, Any]] = []
    for row in value:
        if not isinstance(row, dict):
            continue
        action_type = str(row.get("type", "")).strip().lower()
        if action_type not in {"reply", "task"}:
            continue

        title = str(row.get("title") or "").strip()
        if not title:
            title = f"Reply re: {subject}" if action_type == "reply" else subject.strip() or "Follow up"

        urgency_raw = str(row.get("urgency") or "normal").strip().lower()
        urgency = "high" if urgency_raw == "high" else "normal"

        due = _normalize_due_date(row.get("due"))
        actions.append(
            {
                "type": action_type,
                "title": title[:120],
                "urgency": urgency,
                "due": due,
            }
        )

    return actions[:4]


def _normalize_due_date(value: Any) -> str | None:
    if value in (None, "", "null"):
        return None
    candidate = str(value).strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
        return None
    try:
        date.fromisoformat(candidate)
    except ValueError:
        return None
    return candidate


def _normalize_priority(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"high", "medium", "low"}:
        return normalized
    return "medium"


def _normalize_state_summary(value: Any) -> str:
    text = _compact(str(value or ""), 280)
    if not text:
        return "Latest thread state captured."

    sentences = [segment.strip() for segment in re.split(r"(?<=[.!?])\s+", text) if segment.strip()]
    if not sentences:
        return "Latest thread state captured."
    return " ".join(sentences[:2])


def _is_hard_negative(*, subject: str, body: str) -> bool:
    merged = f"{subject}\n{body}".lower()
    return (
        "unsubscribe" in merged
        or "newsletter" in merged
        or "marketing" in merged
        or "promo" in merged
    )


def _score_for_priority(priority: str) -> int:
    if priority == "high":
        return 15
    if priority == "medium":
        return 8
    return 3


def _importance_bucket_for_priority(priority: str) -> str:
    if priority == "high":
        return "high"
    if priority == "medium":
        return "medium"
    return "low"


def _importance_score_for_priority(priority: str) -> float:
    if priority == "high":
        return 0.75
    if priority == "medium":
        return 0.55
    return 0.35


def _build_cache_key(
    *,
    sender: str,
    subject: str,
    body: str,
    thread_id: str | None,
    latest_message_id: str | None,
) -> str:
    normalized_thread = (thread_id or "").strip().lower()
    normalized_latest = (latest_message_id or "").strip().lower()
    if normalized_thread and normalized_latest:
        return f"thread:{normalized_thread}:{normalized_latest}"

    digest_source = "|".join(
        [
            sender.strip().lower(),
            subject.strip().lower(),
            _compact(body, 1600),
        ]
    )
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()
    return f"content:{digest}"


def _get_cached(cache_key: str) -> dict[str, Any] | None:
    now = time.time()
    with _cache_lock:
        existing = _cache.get(cache_key)
        if existing is None:
            return None
        expires_at, payload = existing
        if expires_at <= now:
            _cache.pop(cache_key, None)
            return None
        return dict(payload)


def _set_cached(cache_key: str, payload: dict[str, Any], ttl_seconds: int) -> None:
    expires_at = time.time() + max(ttl_seconds, 30)
    with _cache_lock:
        _cache[cache_key] = (expires_at, dict(payload))


def _consume_budget(config: EmailPriorityLLMConfig) -> bool:
    now = time.time()
    minute_cutoff = now - 60
    day_cutoff = now - 86400

    with _budget_lock:
        while _requests_last_minute and _requests_last_minute[0] <= minute_cutoff:
            _requests_last_minute.popleft()
        while _requests_last_day and _requests_last_day[0] <= day_cutoff:
            _requests_last_day.popleft()

        if len(_requests_last_minute) >= max(config.max_requests_per_minute, 1):
            return False
        if len(_requests_last_day) >= max(config.max_requests_per_day, 1):
            return False

        _requests_last_minute.append(now)
        _requests_last_day.append(now)
        return True


def _compact(text: str, limit: int) -> str:
    collapsed = " ".join((text or "").split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: max(limit - 3, 0)].rstrip() + "..."


def _clamp_float(value: Any, *, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = minimum
    return max(minimum, min(maximum, parsed))


def _clean_env(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    return normalized in {"1", "true", "yes", "on"}


def _env_int(name: str, *, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except (TypeError, ValueError):
        return default


def _env_float(name: str, *, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except (TypeError, ValueError):
        return default
