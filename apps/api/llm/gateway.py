from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass

from apps.api.llm.provider import LLMIntentResponse, LLMProvider


_ALLOWED_INTENTS = {
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
    ) -> None:
        self._provider = provider
        self._max_requests_per_minute = max_requests_per_minute
        self._max_prompt_chars = max_prompt_chars
        self._hard_timeout_seconds = hard_timeout_seconds

        self._rate_lock = threading.Lock()
        self._rate_windows: dict[str, RateWindow] = defaultdict(lambda: RateWindow(deque()))

    def resolve_intent(
        self,
        *,
        message: str,
        context_snapshot: dict,
        household_id: str,
    ) -> LLMIntentResponse:
        if not self._within_rate_limit(household_id):
            return LLMIntentResponse(
                intent_type=None,
                confidence=0.0,
                clarification_request=None,
                resolved_by="fallback",
                raw_response="rate_limit_exceeded",
                extracted={},
            )

        if len(message) + len(str(context_snapshot)) > self._max_prompt_chars:
            return LLMIntentResponse(
                intent_type=None,
                confidence=0.0,
                clarification_request=None,
                resolved_by="fallback",
                raw_response="prompt_budget_exceeded",
                extracted={},
            )

        result: LLMIntentResponse | None = None
        err: Exception | None = None

        def _run() -> None:
            nonlocal result, err
            try:
                result = self._provider.resolve_intent(
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
            return LLMIntentResponse(
                intent_type=None,
                confidence=0.0,
                clarification_request=None,
                resolved_by="fallback",
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

        validated = self._validate_structured_response(result)
        return validated

    def _within_rate_limit(self, household_id: str) -> bool:
        now = time.time()
        cutoff = now - 60.0
        with self._rate_lock:
            window = self._rate_windows[household_id].timestamps
            while window and window[0] < cutoff:
                window.popleft()
            if len(window) >= self._max_requests_per_minute:
                return False
            window.append(now)
            return True

    def _validate_structured_response(self, result: LLMIntentResponse) -> LLMIntentResponse:
        intent = (result.intent_type or "").upper()
        if intent not in _ALLOWED_INTENTS:
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
            resolved_by=result.resolved_by,
            raw_response=result.raw_response,
            extracted=extracted,
        )
