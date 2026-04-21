"""
Intent Lock Layer - Deterministic pre-decision intent classification and action space constraint.

This module runs BEFORE the decision engine and strictly constrains which domains
the engine can consider based on the user's explicit intent.

No action from a forbidden domain can be selected, making cross-domain contamination
mathematically impossible.

v2: IntentClassification adds secondary_intents, ambiguity_flag, and per-intent confidence
scores to support multi-route planning and clarification responses.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import re


class IntentType(str, Enum):
    """Explicit user intent classification."""

    DAILY_FOCUS = "daily_focus"  # "What should I focus on today?", "prioritize", "top tasks"
    FITNESS = "fitness"  # "get in shape", "workout", "exercise", "training"
    MEAL = "meal"  # "cook", "dinner", "meal plan", "grocery"
    MEDICAL = "medical"  # "doctor", "dentist", "appointment", "checkup"
    SCHEDULING = "scheduling"  # "schedule", "book a slot", "calendar"


@dataclass(frozen=True)
class IntentClassification:
    """
    Full probabilistic result from intent classification.

    Replaces the earlier single-intent IntentLockDecision with richer output
    that drives confidence-gated routing and multi-route planning.
    """

    primary_intent: IntentType
    confidence: float  # 0.0-1.0 — primary intent confidence
    secondary_intents: list[IntentType] = field(default_factory=list)  # ordered by score desc
    ambiguity_flag: bool = False  # True when multiple intents have close scores
    all_scores: dict[str, float] = field(default_factory=dict)  # raw scores per intent name
    matched_keywords: list[str] = field(default_factory=list)

    # ---------- computed properties ----------

    @property
    def allowed_domains(self) -> list[str]:
        """Primary domain only — used by single-domain (high-confidence) paths."""
        return IntentLock.INTENT_DOMAIN_MAP[self.primary_intent]

    @property
    def top2_domains(self) -> list[str]:
        """
        Domains for the top-2 intents (primary + first secondary).
        Used by medium-confidence routing to allow breadth without full cross-domain contamination.
        """
        domains: list[str] = list(IntentLock.INTENT_DOMAIN_MAP[self.primary_intent])
        for sec in self.secondary_intents[:1]:
            for d in IntentLock.INTENT_DOMAIN_MAP[sec]:
                if d not in domains:
                    domains.append(d)
        return domains

    @property
    def is_high_confidence(self) -> bool:
        return self.confidence > 0.7

    @property
    def is_medium_confidence(self) -> bool:
        return 0.4 <= self.confidence <= 0.7

    @property
    def is_low_confidence(self) -> bool:
        return self.confidence < 0.4


# Backward-compat alias so existing code referencing IntentLockDecision keeps working
@dataclass(frozen=True)
class IntentLockDecision:
    """Legacy single-intent classification result. New code should prefer IntentClassification."""

    intent: IntentType
    allowed_domains: list[str]
    confidence: float
    matched_keywords: list[str]


class IntentLock:
    """
    Deterministic pre-decision intent classifier and action space constrainer.

    Prevents cross-domain action selection by restricted which candidates
    the decision engine is allowed to consider.
    """

    # Intent-to-allowed-domains mapping (hard constraints)
    INTENT_DOMAIN_MAP = {
        IntentType.DAILY_FOCUS: ["general"],  # Only schedule review actions
        IntentType.FITNESS: ["fitness"],  # Only fitness actions
        IntentType.MEAL: ["meal"],  # Only meal actions
        IntentType.MEDICAL: ["calendar"],  # Only appointment actions
        IntentType.SCHEDULING: ["calendar"],  # Only calendar/booking actions
    }

    # Keyword patterns for each intent (case-insensitive, regex)
    # NOTE: Patterns are checked in order of priority (fitness/meal/medical before scheduling)
    INTENT_KEYWORDS = {
        IntentType.FITNESS: [
            r"\b(?:get\s+in\s+)?shape\b",
            r"\bwork\s*out\b",
            r"\bworking\s+out\b",
            r"\bfitness\b",
            r"\bexercise(?:s)?\b",
            r"\btraining\b",
            r"\brun(?:ning)?\b",
            r"\bcycle\b",
            r"\bcycling\b",
            r"\bswim(?:ming)?\b",
            r"\byoga\b",
            r"\bstrength\b",
            r"\bcardio\b",
            r"\bathletic\b",
        ],
        IntentType.MEAL: [
            r"cook",
            r"cooking",
            r"dinner",
            r"lunch",
            r"breakfast",
            r"meals?\b",  # "meal" or "meals"
            r"meal\s+(?:plan|prep)",
            r"grocery",
            r"recipe",
            r"hungry",
            r"food",
            r"prepare\s+(?:a\s+)?meal",
        ],
        IntentType.MEDICAL: [
            r"\bdoctor\b",
            r"\bdentist\b",
            r"\bappointment\b",
            r"\bcheckup\b",
            r"\bmedical\b",
            r"\bclinic\b",
            r"\bhealth(?:care)?\b",
        ],
        IntentType.DAILY_FOCUS: [
            r"focus.*today",
            r"today.*focus",
            r"what.*focus",
            r"prioritize",
            r"top\s+(?:task|priority|priorities)",
            r"most\s+important",
            r"plan\s+my\s+day",
            r"order\s+my\s+day",
            r"organize\s+(?:my\s+)?day",
        ],
        IntentType.SCHEDULING: [
            r"book\s+(?:a\s+)?(?:slot|time|appointment|meeting)",
            r"calendar",
            r"meeting",
        ],
    }

    @classmethod
    def _score_all(cls, message_lower: str) -> dict[IntentType, tuple[float, list[str]]]:
        """Return raw (confidence, matched_keywords) scores for every intent type."""
        intent_priority = [
            IntentType.FITNESS,
            IntentType.MEAL,
            IntentType.MEDICAL,
            IntentType.SCHEDULING,
            IntentType.DAILY_FOCUS,
        ]
        scores: dict[IntentType, tuple[float, list[str]]] = {}
        for intent_type in intent_priority:
            patterns = cls.INTENT_KEYWORDS[intent_type]
            matched: list[str] = []
            for pattern in patterns:
                if re.search(pattern, message_lower):
                    matched.append(pattern)
            match_count = len(matched)
            if match_count == 0:
                confidence = 0.0
            else:
                confidence = min(1.0, 0.5 + (match_count - 1) * 0.15)
            scores[intent_type] = (confidence, matched)
        return scores

    @classmethod
    def classify(cls, user_message: str) -> IntentClassification:
        """
        Classify user intent from natural language message.

        Returns IntentClassification with primary_intent, confidence, secondary_intents,
        and ambiguity_flag to support confidence-gated routing and multi-route planning.

        Patterns are checked in priority order so domain-specific intents win over generic ones
        on equal scores.
        """
        message_lower = user_message.lower().strip()

        intent_priority = [
            IntentType.FITNESS,
            IntentType.MEAL,
            IntentType.MEDICAL,
            IntentType.SCHEDULING,
            IntentType.DAILY_FOCUS,
        ]

        scores = cls._score_all(message_lower)

        # Sort intents by confidence descending, preserving priority order on ties
        ranked = sorted(
            intent_priority,
            key=lambda t: scores[t][0],
            reverse=True,
        )

        primary = ranked[0]
        primary_confidence, primary_matched = scores[primary]

        # Low-confidence fallback — no patterns matched at all
        if primary_confidence == 0.0:
            return IntentClassification(
                primary_intent=IntentType.DAILY_FOCUS,
                confidence=0.1,
                secondary_intents=[],
                ambiguity_flag=True,
                all_scores={t.value: scores[t][0] for t in intent_priority},
                matched_keywords=[],
            )

        # Collect secondary intents: intents with score > 0 that are not primary
        # Threshold: at least 50 % of the primary score to be considered a real secondary
        secondary_threshold = max(0.05, primary_confidence * 0.5)
        secondary_intents = [
            t for t in ranked[1:]
            if scores[t][0] >= secondary_threshold
        ]

        # Ambiguity flag — primary isn't clearly dominant
        # Triggered when confidence <= 0.7 OR a secondary intent is within 20 % of primary
        ambiguity_flag = primary_confidence <= 0.7 or bool(
            secondary_intents and scores[secondary_intents[0]][0] >= primary_confidence * 0.8
        )

        return IntentClassification(
            primary_intent=primary,
            confidence=primary_confidence,
            secondary_intents=secondary_intents,
            ambiguity_flag=ambiguity_flag,
            all_scores={t.value: scores[t][0] for t in intent_priority},
            matched_keywords=primary_matched,
        )

    @classmethod
    def classify_legacy(cls, user_message: str) -> IntentLockDecision:
        """
        Legacy compatibility wrapper — returns the old IntentLockDecision.
        Callers that only need single-intent routing can use this.
        """
        c = cls.classify(user_message)
        return IntentLockDecision(
            intent=c.primary_intent,
            allowed_domains=c.allowed_domains,
            confidence=c.confidence,
            matched_keywords=c.matched_keywords,
        )

    @classmethod
    def filter_candidates(
        cls,
        user_message: str,
        candidates: list[dict[str, Any]],
    ) -> IntentClassification | None:
        """
        Filter candidate actions based on user intent.

        Only candidates whose 'domain' is in the allowed list are kept.
        If no valid candidates remain after filtering, returns None.

        Args:
            user_message: Raw user input for intent classification
            candidates: List of candidate actions (each with 'domain' key)

        Returns:
            IntentClassification if filtering succeeded, None if no valid candidates
            (Caller should use fallback when None is returned)
        """
        decision = cls.classify(user_message)

        # Filter to only allowed domains
        allowed_candidates = [
            c for c in candidates if c.get("domain") in decision.allowed_domains
        ]

        # If all candidates filtered out, return None to signal fallback needed
        if not allowed_candidates:
            return None

        return decision

    @classmethod
    def get_fallback_recommendation(cls) -> dict[str, Any]:
        """
        Return fallback recommendation when no valid actions exist for the detected intent.

        This is the safe default: schedule review with no domain-specific content.
        """
        return {
            "action_id": "intent-lock-fallback",
            "target_domain": "general",
            "recommendation": "Block 30 minutes today to organize your priorities and reduce overload.",
            "why": [
                "Taking time to plan reduces decision fatigue",
                "Clear priorities help you focus on what matters most",
                "Organization prevents important tasks from slipping",
            ],
            "impact": "This gives you a simpler plan for the day and lowers cognitive load.",
            "approval_required": True,
        }
