from __future__ import annotations

import re
from collections import Counter

from app.services.rules_engine.models import EmailRuleResult, PantryRuleResult, RuleAction, ScheduleRuleResult


_URGENT_KEYWORDS = {
    "urgent",
    "asap",
    "deadline",
    "action required",
    "immediately",
    "due today",
    "past due",
}

_ACTION_KEYWORDS = {
    "please review",
    "confirm",
    "approve",
    "reply",
    "respond",
    "complete",
    "submit",
}

_PROMO_KEYWORDS = {
    "unsubscribe",
    "newsletter",
    "sale",
    "promo",
    "discount",
    "offer",
}

_AMBIGUOUS_KEYWORDS = {
    "thoughts",
    "maybe",
    "consider",
    "review",
    "when possible",
}

_PRIORITY_SENDERS = {
    "school",
    "doctor",
    "bank",
    "insurance",
    "landlord",
    "utilities",
}

_PANTRY_STATIC_MAP: dict[str, list[str]] = {
    "eggs": ["Veggie omelet", "Egg fried rice"],
    "rice": ["Egg fried rice", "Bean rice bowl"],
    "pasta": ["Simple pasta", "Tomato pasta"],
    "tomato": ["Tomato pasta", "Quick shakshuka"],
    "beans": ["Bean rice bowl", "Pantry chili"],
    "bread": ["Grilled cheese", "Pantry toast plate"],
    "cheese": ["Grilled cheese", "Simple pasta"],
    "tuna": ["Tuna melt", "Tuna rice bowl"],
    "chicken": ["Chicken rice bowl", "Simple chicken skillet"],
}


class RulesEngine:
    def analyze_email(
        self,
        *,
        sender: str,
        subject: str,
        messages: list[str],
        to_me: bool,
        cc_me: bool,
    ) -> EmailRuleResult:
        subject_text = subject.strip() or "Email update"
        thread_context = "\n".join(item.strip() for item in messages[-3:] if item.strip())
        combined = f"{subject_text}\n{thread_context}".strip().lower()
        sender_text = sender.strip().lower()

        if self._is_promotional(sender=sender_text, combined_text=combined):
            return EmailRuleResult(
                priority="low",
                needs_attention=False,
                actions=[],
                state_summary="Low priority via rules. Promotional or low-signal content detected.",
                reason="rules_promo_filter",
                ambiguous=False,
            )

        score = 0
        if to_me:
            score += 2
        if cc_me:
            score += 1

        if any(token in sender_text for token in _PRIORITY_SENDERS):
            score += 2

        score += 2 * self._hit_count(combined, _URGENT_KEYWORDS)
        score += self._hit_count(combined, _ACTION_KEYWORDS)

        due_value = self._extract_due_date(combined)
        actions: list[RuleAction] = []

        if "?" in combined or "reply" in combined or "confirm" in combined:
            actions.append(RuleAction(type="reply", title=f"Reply re: {subject_text}", due=due_value))

        if any(token in combined for token in ("approve", "submit", "complete", "review")):
            actions.append(RuleAction(type="task", title=f"Follow up: {subject_text}", due=due_value))

        if score >= 6:
            priority = "high"
        elif score >= 2:
            priority = "medium"
        else:
            priority = "low"

        needs_attention = priority == "high" or bool(actions)
        ambiguous = self._is_ambiguous_email(combined=combined, priority=priority, actions=actions)

        state_summary = {
            "high": "High priority via rules. Strong action or deadline cues detected.",
            "medium": "Medium priority via rules. Useful context found but limited certainty.",
            "low": "Low priority via rules. Mostly informational content.",
        }[priority]

        reason = {
            "high": "rules_high_confidence",
            "medium": "rules_medium_confidence",
            "low": "rules_low_confidence",
        }[priority]

        return EmailRuleResult(
            priority=priority,
            needs_attention=needs_attention,
            actions=actions[:4],
            state_summary=state_summary,
            reason=reason,
            ambiguous=ambiguous,
        )

    def analyze_pantry(self, *, items: list[str]) -> PantryRuleResult:
        normalized_items = [self._normalize_item(item) for item in items if self._normalize_item(item)]
        meal_counter: Counter[str] = Counter()

        for item in normalized_items:
            for meal in _PANTRY_STATIC_MAP.get(item, []):
                meal_counter[meal] += 1

        if meal_counter:
            suggested_meals = [meal for meal, _count in meal_counter.most_common(5)]
            ambiguous = len(suggested_meals) > 3
            return PantryRuleResult(
                suggested_meals=suggested_meals[:3],
                state_summary="Possible meals via static pantry mapping.",
                reason="rules_pantry_static_match",
                ambiguous=ambiguous,
            )

        defaults = [
            "Simple pasta with pantry seasoning",
            "Rice and beans bowl",
            "Basic toast and protein plate",
        ]
        return PantryRuleResult(
            suggested_meals=defaults,
            state_summary="Possible meals via static mapping.",
            reason="rules_pantry_fallback",
            ambiguous=bool(normalized_items),
        )

    def analyze_schedule(self, *, title: str, details: str) -> ScheduleRuleResult:
        title_text = title.strip() or "Household reminder"
        details_text = details.strip()
        combined = f"{title_text} {details_text}".lower().strip()

        suggestions: list[str] = []
        if any(token in combined for token in ("meeting", "call", "appointment", "session")):
            suggestions.append(f"Create a calendar block for {title_text}")

        if "tomorrow" in combined:
            suggestions.append("Set a reminder tonight for tomorrow")
        if "today" in combined or "tonight" in combined:
            suggestions.append("Set a reminder within the next hour")

        date_hint = self._extract_due_date(combined)
        if date_hint is not None:
            suggestions.append(f"Create reminder for {date_hint}")

        if not suggestions:
            suggestions = ["Basic reminder suggestion: add this to tomorrow planning."]

        explicit_time = bool(re.search(r"\b\d{1,2}(:\d{2})?\s?(am|pm)?\b", combined))
        has_trigger = any(token in combined for token in ("meeting", "call", "appointment", "tomorrow", "today", "next"))

        return ScheduleRuleResult(
            suggestions=suggestions[:3],
            state_summary="Basic reminder suggestions only.",
            reason="rules_schedule_triggered",
            ambiguous=has_trigger and not explicit_time,
        )

    @staticmethod
    def _hit_count(text: str, keywords: set[str]) -> int:
        return sum(1 for keyword in keywords if keyword in text)

    @staticmethod
    def _is_promotional(*, sender: str, combined_text: str) -> bool:
        if "no-reply" in sender or "noreply" in sender:
            return True
        return any(keyword in combined_text for keyword in _PROMO_KEYWORDS)

    @staticmethod
    def _is_ambiguous_email(*, combined: str, priority: str, actions: list[RuleAction]) -> bool:
        ambiguous_hits = sum(1 for token in _AMBIGUOUS_KEYWORDS if token in combined)
        if ambiguous_hits >= 1 and priority == "medium":
            return True
        if priority == "medium" and not actions and len(combined) > 180:
            return True
        return False

    @staticmethod
    def _extract_due_date(text: str) -> str | None:
        match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", text)
        if match is None:
            return None
        return match.group(0)

    @staticmethod
    def _normalize_item(value: str) -> str:
        return " ".join(value.strip().lower().split())
