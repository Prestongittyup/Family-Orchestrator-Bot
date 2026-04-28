"""
LLM Provider Abstraction Layer
================================
Configurable LLM backend with automatic fallback to rule-based classifier.

Environment variables (set in .env):
  LLM_PROVIDER         = "openai" | "azure_openai" | "mock"   (default: "mock")
  OPENAI_API_KEY       = sk-...
  AZURE_OPENAI_KEY     = ...
  AZURE_OPENAI_ENDPOINT= https://YOUR_RESOURCE.openai.azure.com/
  AZURE_OPENAI_DEPLOYMENT = gpt-4o  (default deployment name)
  LLM_MODEL            = gpt-4o     (used for openai provider)
  LLM_TIMEOUT_SECONDS  = 8

Design:
  - All providers expose the same complete() interface.
  - ChatGatewayService imports LLMClient and calls client.intent_resolve().
  - If the provider errors or is unavailable, the rule-based classifier is
    used automatically (zero-break fallback).
"""
from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Response contract
# ---------------------------------------------------------------------------


@dataclass
class LLMIntentResponse:
    """
    Structured output from the LLM intent resolver.

    Fields mirror IntentClassification so downstream code can substitute
    either source without branching.
    """
    intent_type: str | None          # must be a valid IntentType string or None
    confidence: float                # 0.0–1.0
    clarification_request: str | None = None  # if LLM needs more info
    resolved_by: str = "llm"        # "llm" | "rule_based" | "mock"
    raw_response: str = ""           # raw LLM text (for debugging/audit)

    # Extracted structured fields from the LLM response
    extracted: dict = None           # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.extracted is None:
            self.extracted = {}


# ---------------------------------------------------------------------------
# Abstract provider interface
# ---------------------------------------------------------------------------


class LLMProvider(ABC):
    @abstractmethod
    def resolve_intent(
        self,
        *,
        message: str,
        context_snapshot: dict,
        household_id: str,
    ) -> LLMIntentResponse:
        """Classify message into structured intent."""


# ---------------------------------------------------------------------------
# System prompt shared across providers
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are the intent classification engine for HPAL, a family household coordination assistant.

Your ONLY job is to classify the user's message into a structured JSON intent object.
Do NOT answer conversationally. Do NOT add commentary. Return ONLY valid JSON.

Supported intent types:
  CREATE_TASK         – user wants to create a task or to-do item
  COMPLETE_TASK       – user wants to mark a task as done
  RESCHEDULE_TASK     – user wants to move/delay/reschedule a task
  CREATE_EVENT        – user wants to add a calendar event/appointment/meeting
  UPDATE_EVENT        – user wants to change an existing calendar event
  DELETE_EVENT        – user wants to remove a calendar event
  CREATE_PLAN         – user wants to create a multi-step household plan
  QUERY_SCHEDULE      – user is asking about upcoming schedule/events
  GENERAL_QUERY       – general question or chat not fitting above

Return format (always valid JSON):
{
  "intent_type": "<one of the above>",
  "confidence": <0.0-1.0>,
  "clarification_request": "<question to ask user if ambiguous, else null>",
  "extracted": {
    "title": "<event/task title if present>",
    "start_time": "<ISO datetime or natural language if present>",
    "end_time": "<ISO datetime or natural language if present>",
    "participants": ["<names>"],
    "due_time": "<due time if task>",
    "recurrence": "<daily|weekly|monthly|none>",
    "priority": "<low|medium|high or null>",
    "task_id": "<referenced task id if present>",
    "event_id": "<referenced event id if present>"
  }
}

Context provided to you (current household state):
"""


# ---------------------------------------------------------------------------
# OpenAI provider
# ---------------------------------------------------------------------------


class OpenAIProvider(LLMProvider):
    def __init__(self) -> None:
        self._api_key = os.environ.get("OPENAI_API_KEY", "")
        self._model = os.environ.get("LLM_MODEL", "gpt-4o")
        self._timeout = int(os.environ.get("LLM_TIMEOUT_SECONDS", "8"))

    def resolve_intent(
        self, *, message: str, context_snapshot: dict, household_id: str
    ) -> LLMIntentResponse:
        try:
            import openai  # local import – only required if provider is openai
        except ImportError:
            logger.warning("openai package not installed; falling back")
            return _fallback_response("openai_not_installed")

        client = openai.OpenAI(api_key=self._api_key, timeout=self._timeout)
        system_content = _SYSTEM_PROMPT + json.dumps(context_snapshot, default=str)[:2000]

        try:
            completion = client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": message},
                ],
                response_format={"type": "json_object"},
                max_tokens=400,
                temperature=0.0,
            )
            raw = completion.choices[0].message.content or ""
            return _parse_llm_json(raw, "openai")
        except Exception as exc:
            logger.warning("OpenAI call failed: %s", exc)
            return _fallback_response(f"openai_error: {exc}")


# ---------------------------------------------------------------------------
# Azure OpenAI provider
# ---------------------------------------------------------------------------


class AzureOpenAIProvider(LLMProvider):
    def __init__(self) -> None:
        self._api_key = os.environ.get("AZURE_OPENAI_KEY", "")
        self._endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
        self._deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
        self._timeout = int(os.environ.get("LLM_TIMEOUT_SECONDS", "8"))

    def resolve_intent(
        self, *, message: str, context_snapshot: dict, household_id: str
    ) -> LLMIntentResponse:
        try:
            import openai  # azure uses openai SDK
        except ImportError:
            logger.warning("openai package not installed; falling back")
            return _fallback_response("openai_not_installed")

        client = openai.AzureOpenAI(
            api_key=self._api_key,
            azure_endpoint=self._endpoint,
            api_version="2024-02-01",
            timeout=self._timeout,
        )
        system_content = _SYSTEM_PROMPT + json.dumps(context_snapshot, default=str)[:2000]

        try:
            completion = client.chat.completions.create(
                model=self._deployment,
                messages=[
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": message},
                ],
                response_format={"type": "json_object"},
                max_tokens=400,
                temperature=0.0,
            )
            raw = completion.choices[0].message.content or ""
            return _parse_llm_json(raw, "azure_openai")
        except Exception as exc:
            logger.warning("Azure OpenAI call failed: %s", exc)
            return _fallback_response(f"azure_error: {exc}")


# ---------------------------------------------------------------------------
# Mock provider (deterministic, for local dev / tests)
# ---------------------------------------------------------------------------


class MockLLMProvider(LLMProvider):
    """Deterministic mock — maps keyword patterns to known intents."""

    _KEYWORD_MAP = {
        ("add event", "create event", "schedule event", "new event", "book", "appointment", "meeting"):
            ("CREATE_EVENT", 0.88),
        ("update event", "change event", "edit event", "modify event", "move event"):
            ("UPDATE_EVENT", 0.85),
        ("delete event", "cancel event", "remove event"):
            ("DELETE_EVENT", 0.90),
        ("add task", "create task", "new task", "remind me to", "todo"):
            ("CREATE_TASK", 0.87),
        ("done", "complete", "finished", "mark complete", "check off"):
            ("COMPLETE_TASK", 0.85),
        ("reschedule", "postpone", "move task", "delay", "push back"):
            ("RESCHEDULE_TASK", 0.82),
        ("plan", "create plan", "organize"):
            ("CREATE_PLAN", 0.80),
        ("what", "when", "schedule", "shows", "show me", "upcoming", "today", "this week"):
            ("QUERY_SCHEDULE", 0.78),
    }

    def resolve_intent(
        self, *, message: str, context_snapshot: dict, household_id: str
    ) -> LLMIntentResponse:
        lower = message.lower()
        for keywords, (intent_type, conf) in self._KEYWORD_MAP.items():
            if any(kw in lower for kw in keywords):
                return LLMIntentResponse(
                    intent_type=intent_type,
                    confidence=conf,
                    resolved_by="mock",
                    extracted={},
                )
        return LLMIntentResponse(
            intent_type="GENERAL_QUERY",
            confidence=0.60,
            resolved_by="mock",
            extracted={},
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_llm_provider() -> LLMProvider:
    """Build the configured LLM provider from environment."""
    provider_name = os.environ.get("LLM_PROVIDER", "mock").lower()
    if provider_name == "openai":
        return OpenAIProvider()
    if provider_name == "azure_openai":
        return AzureOpenAIProvider()
    return MockLLMProvider()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_llm_json(raw: str, source: str) -> LLMIntentResponse:
    """Parse LLM JSON output into LLMIntentResponse."""
    try:
        data = json.loads(raw)
        return LLMIntentResponse(
            intent_type=data.get("intent_type"),
            confidence=float(data.get("confidence", 0.7)),
            clarification_request=data.get("clarification_request"),
            extracted=data.get("extracted", {}),
            resolved_by=source,
            raw_response=raw,
        )
    except Exception as exc:
        logger.warning("Failed to parse LLM JSON (%s): %s | raw=%s", source, exc, raw[:200])
        return _fallback_response(f"parse_error: {exc}")


def _fallback_response(reason: str) -> LLMIntentResponse:
    return LLMIntentResponse(
        intent_type=None,
        confidence=0.0,
        resolved_by="fallback",
        raw_response=reason,
    )
