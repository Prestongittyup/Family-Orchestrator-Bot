from __future__ import annotations

import json
import inspect
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass

from apps.api.llm.provider import LLMIntentResponse, LLMProvider


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


@dataclass
class RateWindow:
    timestamps: deque[float]


class LLMGateway:
    """
    Production-safety wrapper around LLM provider.

    Guarantees:
      - household-scoped rate limiting
      - prompt budget guard
      - hard timeout fallback signal
      - strict structured intent validation
    """

    def __init__(
        self,
        provider: LLMProvider,
        *,
        max_requests_per_minute: int = 60,
        max_prompt_chars: int = 6000,
        hard_timeout_seconds: float = 8.0,
        timeout_fallback_enabled: bool = True,
    ) -> None:
        self._provider = provider
        self._max_requests_per_minute = max_requests_per_minute
        self._max_prompt_chars = max_prompt_chars
        self._hard_timeout_seconds = hard_timeout_seconds
        self._timeout_fallback_enabled = timeout_fallback_enabled
        self._allowed_intents = [
            "chat",
            "task",
            "calendar",
            "grocery",
            "analysis",
            "CREATE_TASK",
            "COMPLETE_TASK",
            "RESCHEDULE_TASK",
            "CREATE_EVENT",
            "UPDATE_EVENT",
            "DELETE_EVENT",
            "CREATE_PLAN",
            "QUERY_SCHEDULE",
            "GENERAL_QUERY",
        ]

        self._rate_lock = threading.Lock()
        self._rate_windows: dict[str, RateWindow] = defaultdict(lambda: RateWindow(deque()))

    def resolve_intent(
        self,
        *,
        message: str,
        context_snapshot: dict,
        household_id: str,
    ) -> LLMIntentResponse:
        actor_context = self._extract_actor_context(context_snapshot=context_snapshot)
        prompt_size = self._prompt_size(message=message, context_snapshot=context_snapshot)
        if prompt_size > self._max_prompt_chars:
            return LLMIntentResponse(
                intent_type=None,
                confidence=0.0,
                clarification_request=None,
                resolved_by="fallback",
                raw_response="prompt_budget_exceeded",
                extracted={},
            )

        if not self._can_call_within_rate_limit(household_id):
            return LLMIntentResponse(
                intent_type=None,
                confidence=0.0,
                clarification_request=None,
                resolved_by="fallback",
                raw_response="rate_limit_exceeded",
                extracted={},
            )

        if not self._is_valid_intent_context(actor_context=actor_context, context_snapshot=context_snapshot):
            return LLMIntentResponse(
                intent_type=None,
                confidence=0.0,
                clarification_request="I need a bit more detail to classify this safely.",
                resolved_by="fallback",
                raw_response="invalid_intent",
                extracted={},
            )

        result: LLMIntentResponse | None = None
        err: Exception | None = None

        def _run() -> None:
            nonlocal result, err
            try:
                result = self._invoke_provider(
                    message=message,
                    context_snapshot=context_snapshot,
                    household_id=household_id,
                )
            except Exception as exc:
                err = exc

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        thread.join(timeout=self._hard_timeout_seconds)

        if thread.is_alive():
            timeout_route = "fallback" if self._timeout_fallback_enabled else "blocked"
            return LLMIntentResponse(
                intent_type=None,
                confidence=0.0,
                clarification_request=None,
                resolved_by=timeout_route,
                raw_response="timeout",
                extracted={},
            )
        if err is not None or result is None:
            return LLMIntentResponse(
                intent_type=None,
                confidence=0.0,
                clarification_request=None,
                resolved_by="fallback",
                raw_response=f"provider_error:{err}",
                extracted={},
            )

        self._record_rate_limit_call(household_id)

        validated = self._validate_structured_response(result)
        return validated

    def _can_call_within_rate_limit(self, household_id: str) -> bool:
        now = time.time()
        cutoff = now - 60.0
        with self._rate_lock:
            window = self._rate_windows[household_id].timestamps
            while window and window[0] < cutoff:
                window.popleft()
            return len(window) < self._max_requests_per_minute

    def _record_rate_limit_call(self, household_id: str) -> None:
        now = time.time()
        cutoff = now - 60.0
        with self._rate_lock:
            window = self._rate_windows[household_id].timestamps
            while window and window[0] < cutoff:
                window.popleft()
            window.append(now)

    def _extract_actor_context(self, *, context_snapshot: dict) -> dict:
        actor_context = context_snapshot.get("actor_context")
        if not isinstance(actor_context, dict):
            return {}
        return actor_context

    def _prompt_size(self, *, message: str, context_snapshot: dict) -> int:
        serialized_context = json.dumps(context_snapshot, sort_keys=True, separators=(",", ":"), default=str)
        return len(message) + len(serialized_context)

    def _is_valid_intent_context(self, *, actor_context: dict, context_snapshot: dict) -> bool:
        del actor_context
        raw_intent = (
            context_snapshot.get("intent")
            or context_snapshot.get("intent_type")
            or context_snapshot.get("intent_category")
        )
        if raw_intent is None:
            return True
        normalized = str(raw_intent).strip().lower()
        return normalized in self._allowed_intents

    def _invoke_provider(self, *, message: str, context_snapshot: dict, household_id: str) -> LLMIntentResponse:
        signature = inspect.signature(self._provider.resolve_intent)
        parameters = signature.parameters
        kwargs: dict[str, object] = {"message": message}

        if "context_snapshot" in parameters:
            kwargs["context_snapshot"] = context_snapshot
        elif "context" in parameters:
            kwargs["context"] = context_snapshot

        if "household_id" in parameters:
            kwargs["household_id"] = household_id

        return self._provider.resolve_intent(**kwargs)

    def _validate_structured_response(self, result: LLMIntentResponse) -> LLMIntentResponse:
        intent = (result.intent_type or "").upper()
        if intent not in _ALLOWED_PROVIDER_INTENTS:
            return LLMIntentResponse(
                intent_type=None,
                confidence=0.0,
                clarification_request="I need a bit more detail to classify this safely.",
                resolved_by="fallback",
                raw_response="invalid_intent",
                extracted={},
            )

        confidence = result.confidence
        if confidence < 0.0:
            confidence = 0.0
        if confidence > 1.0:
            confidence = 1.0

        extracted = result.extracted if isinstance(result.extracted, dict) else {}

        return LLMIntentResponse(
            intent_type=intent,
            confidence=confidence,
            clarification_request=result.clarification_request,
            resolved_by=self._normalize_route_decision(result.resolved_by),
            raw_response=result.raw_response,
            extracted=extracted,
        )

    def _normalize_route_decision(self, decision: str | None) -> str:
        normalized = (decision or "").strip().lower()
        if normalized in _ALLOWED_ROUTE_DECISIONS:
            return normalized
        return "fallback"
