from __future__ import annotations

import json
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any


_ALLOWED_PROVIDER_INTENTS = {
    "CREATE_TASK",
    "COMPLETE_TASK",
    "RESCHEDULE_TASK",
    "CREATE_EVENT",
    "UPDATE_EVENT",
    "DELETE_EVENT",
    "CREATE_PLAN",
    "QUERY_SCHEDULE",
    "GENERAL_QUERY",
}

_ALLOWED_ROUTE_DECISIONS = {"llm", "fallback", "blocked"}

_DEFAULT_ALLOWED_CONTEXT_INTENTS = {
    "chat",
    "task",
    "calendar",
    "grocery",
    "analysis",
    "create_task",
    "complete_task",
    "reschedule_task",
    "create_event",
    "update_event",
    "delete_event",
    "create_plan",
    "query_schedule",
    "general_query",
}


@dataclass
class RateWindow:
    timestamps: deque[float]


@dataclass(frozen=True)
class GatewayIntentResult:
    intent_type: str | None
    confidence: float
    clarification_request: str | None
    resolved_by: str
    raw_response: str
    extracted: dict[str, Any]


class GatewayGuard:
    """LLM intent guardrails without direct provider calls."""

    def __init__(
        self,
        *,
        max_requests_per_minute: int = 60,
        max_prompt_chars: int = 6000,
        allowed_context_intents: set[str] | None = None,
    ) -> None:
        self._max_requests_per_minute = max(1, int(max_requests_per_minute))
        self._max_prompt_chars = max(512, int(max_prompt_chars))
        self._allowed_context_intents = allowed_context_intents or set(_DEFAULT_ALLOWED_CONTEXT_INTENTS)

        self._rate_lock = threading.Lock()
        self._rate_windows: dict[str, RateWindow] = defaultdict(lambda: RateWindow(deque()))

    def should_allow_call(self, *, message: str, context_snapshot: dict[str, Any], household_id: str) -> tuple[bool, str | None]:
        prompt_size = self.prompt_size(message=message, context_snapshot=context_snapshot)
        if prompt_size > self._max_prompt_chars:
            return False, "prompt_budget_exceeded"

        if not self.can_call_within_rate_limit(household_id=household_id):
            return False, "rate_limit_exceeded"

        if not self.is_valid_intent_context(context_snapshot=context_snapshot):
            return False, "invalid_intent"

        return True, None

    def can_call_within_rate_limit(self, *, household_id: str) -> bool:
        now = time.time()
        cutoff = now - 60.0
        with self._rate_lock:
            window = self._rate_windows[household_id].timestamps
            while window and window[0] < cutoff:
                window.popleft()
            return len(window) < self._max_requests_per_minute

    def record_call(self, *, household_id: str) -> None:
        now = time.time()
        cutoff = now - 60.0
        with self._rate_lock:
            window = self._rate_windows[household_id].timestamps
            while window and window[0] < cutoff:
                window.popleft()
            window.append(now)

    def prompt_size(self, *, message: str, context_snapshot: dict[str, Any]) -> int:
        serialized_context = json.dumps(context_snapshot, sort_keys=True, separators=(",", ":"), default=str)
        return len(message) + len(serialized_context)

    def is_valid_intent_context(self, *, context_snapshot: dict[str, Any]) -> bool:
        raw_intent = context_snapshot.get("intent") or context_snapshot.get("intent_type") or context_snapshot.get("intent_category")
        if raw_intent is None:
            return True
        normalized = str(raw_intent).strip().lower()
        return normalized in self._allowed_context_intents

    def validate_provider_response(self, result: GatewayIntentResult) -> GatewayIntentResult:
        intent = (result.intent_type or "").upper()
        if intent not in _ALLOWED_PROVIDER_INTENTS:
            return GatewayIntentResult(
                intent_type=None,
                confidence=0.0,
                clarification_request="I need a bit more detail to classify this safely.",
                resolved_by="fallback",
                raw_response="invalid_intent",
                extracted={},
            )

        confidence = max(0.0, min(1.0, result.confidence))
        extracted = result.extracted if isinstance(result.extracted, dict) else {}

        return GatewayIntentResult(
            intent_type=intent,
            confidence=confidence,
            clarification_request=result.clarification_request,
            resolved_by=self.normalize_route_decision(result.resolved_by),
            raw_response=result.raw_response,
            extracted=extracted,
        )

    def normalize_route_decision(self, decision: str | None) -> str:
        normalized = (decision or "").strip().lower()
        if normalized in _ALLOWED_ROUTE_DECISIONS:
            return normalized
        return "fallback"


__all__ = ["GatewayGuard", "GatewayIntentResult"]
