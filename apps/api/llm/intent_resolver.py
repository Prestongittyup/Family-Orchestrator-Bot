"""
LLM Intent Resolver Service
==============================
Sits inside the IntentContract pipeline as a pre-classifier.

Replaces the pure rule-based IntentClassifier when an LLM provider is
configured.  Always falls back to rule-based if:
  - LLM provider returns None intent
  - LLM confidence < CONFIDENCE_THRESHOLD
  - Any exception occurs

Integration point:
  ChatGatewayService calls this before its existing decision engine.
  The resolved LLMIntentResponse is attached to the conversation graph
  so HouseholdOSDecisionEngine can use it.

Keys preserved:
  - Idempotency keys are generated downstream (ActionPlanner) — untouched
  - PolicyEngine gating is applied after this resolver — untouched
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
import os

from apps.api.intent_contract.classifier import IntentClassification, IntentClassifier
from apps.api.intent_contract.schema import ExtractedFields, IntentType
from apps.api.llm.gateway import LLMGateway
from apps.api.llm.provider import LLMIntentResponse, LLMProvider, build_llm_provider

logger = logging.getLogger(__name__)

# Minimum LLM confidence to trust LLM result over rule-based
CONFIDENCE_THRESHOLD = 0.65


@dataclass
class ResolvedIntent:
    """
    Unified intent result regardless of resolution source.

    Either source (LLM or rule-based) produces this contract.
    """
    intent_type: IntentType | None
    confidence: float
    extracted_fields: ExtractedFields
    resolution_source: str          # "llm" | "rule_based" | "mock" | "fallback"
    clarification_request: str | None = None


class LLMIntentResolver:
    """
    Dual-path intent resolver: LLM-primary, rule-based fallback.

    Usage:
        resolver = LLMIntentResolver()
        result = resolver.resolve(message="schedule dentist Tuesday 3pm",
                                  context_snapshot=graph_dict,
                                  household_id="hh-123")
    """

    def __init__(
        self,
        provider: LLMProvider | None = None,
        rule_classifier: IntentClassifier | None = None,
    ) -> None:
        self._provider: LLMProvider = provider or build_llm_provider()
        self._gateway = LLMGateway(
            self._provider,
            max_requests_per_minute=int(os.getenv("LLM_RATE_LIMIT_PER_MIN", "60")),
            max_prompt_chars=int(os.getenv("LLM_PROMPT_BUDGET_CHARS", "6000")),
            hard_timeout_seconds=float(os.getenv("LLM_TIMEOUT_SECONDS", "8")),
        )
        self._rule_classifier: IntentClassifier = rule_classifier or IntentClassifier()

    def resolve(
        self,
        *,
        message: str,
        context_snapshot: dict,
        household_id: str,
    ) -> ResolvedIntent:
        """
        Resolve intent using LLM first, rule-based fallback.

        Always returns a valid ResolvedIntent — never raises.
        """
        # --- 1. Try LLM ---
        llm_result: LLMIntentResponse | None = None
        try:
            llm_result = self._gateway.resolve_intent(
                message=message,
                context_snapshot=context_snapshot,
                household_id=household_id,
            )
        except Exception as exc:
            logger.warning("LLM resolver error; falling back to rule-based: %s", exc)

        if llm_result and llm_result.intent_type and llm_result.confidence >= CONFIDENCE_THRESHOLD:
            intent_type = _coerce_intent_type(llm_result.intent_type)
            extracted = _merge_extracted(llm_result.extracted)
            if intent_type is not None:
                logger.debug(
                    "LLM resolved intent=%s conf=%.2f source=%s",
                    intent_type.value,
                    llm_result.confidence,
                    llm_result.resolved_by,
                )
                return ResolvedIntent(
                    intent_type=intent_type,
                    confidence=llm_result.confidence,
                    extracted_fields=extracted,
                    resolution_source=llm_result.resolved_by,
                    clarification_request=llm_result.clarification_request,
                )

        # --- 2. Fallback: rule-based classifier ---
        logger.debug("Falling back to rule-based classifier for message: %s", message[:80])
        rule_result: IntentClassification = self._rule_classifier.classify(message)

        return ResolvedIntent(
            intent_type=rule_result.intent_type,
            confidence=rule_result.confidence_score,
            extracted_fields=rule_result.extracted_fields,
            resolution_source="rule_based",
            clarification_request=None,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _coerce_intent_type(raw: str | None) -> IntentType | None:
    """Convert string label from LLM to IntentType enum member, or None."""
    if raw is None:
        return None
    try:
        return IntentType[raw.upper()]
    except KeyError:
        logger.warning("LLM returned unknown intent_type=%s", raw)
        return None


def _merge_extracted(llm_extracted: dict) -> ExtractedFields:
    """Map LLM-returned extracted dict into ExtractedFields."""
    return ExtractedFields(
        {k: v for k, v in (llm_extracted or {}).items() if v is not None}
    )
