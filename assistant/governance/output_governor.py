from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from household_os.core.contracts import HouseholdOSRunResponse


IntentType = Literal[
    "DAILY_FOCUS",
    "SCHEDULING",
    "FITNESS",
    "MEAL",
    "GENERAL_PLANNING",
]


_BANNED_PHRASES = {
    "low-conflict window": "open time slot",
    "planned commitments": "things you already have planned",
    "calendar events": "things on your calendar",
    "execution pipeline": "process",
    "graph state": "current plan",
    "decision engine": "assistant",
}


@dataclass(frozen=True)
class GovernedOutput:
    action_id: str
    recommendation: str
    why: list[str]
    impact: str
    approval_required: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "action_id": self.action_id,
            "recommendation": self.recommendation,
            "why": self.why,
            "impact": self.impact,
            "approval_required": self.approval_required,
        }


class OutputGovernor:
    def govern(
        self,
        *,
        user_message: str,
        payload: dict[str, object],
        decision_response: HouseholdOSRunResponse,
    ) -> GovernedOutput:
        intent = self.classify_intent(user_message)
        domain = self._decision_domain(decision_response)

        if intent == "DAILY_FOCUS" and not self._allowed_for_daily_focus(payload=payload, domain=domain):
            return self._fallback_output(payload)

        sanitized_recommendation = self._sanitize_recommendation(str(payload.get("recommendation", "")))
        sanitized_why = [self._sanitize_bullet(str(item)) for item in list(payload.get("why", []))[:3]]
        sanitized_impact = self._sanitize_text(str(payload.get("impact", "")))

        return GovernedOutput(
            action_id=str(payload.get("action_id", "")),
            recommendation=sanitized_recommendation,
            why=sanitized_why,
            impact=sanitized_impact,
            approval_required=bool(payload.get("approval_required", True)),
        )

    def classify_intent(self, user_message: str) -> IntentType:
        normalized = " ".join(user_message.lower().split())
        if any(token in normalized for token in ("focus on today", "focus today", "what should i focus on today", "top priorities today", "prioritize today")):
            return "DAILY_FOCUS"
        if any(token in normalized for token in ("schedule", "book", "appointment", "calendar", "meeting", "dentist", "doctor")):
            return "SCHEDULING"
        if any(token in normalized for token in ("work out", "working out", "workout", "fitness", "exercise", "gym", "run")):
            return "FITNESS"
        if any(token in normalized for token in ("meal", "dinner", "lunch", "breakfast", "cook", "grocery", "groceries")):
            return "MEAL"
        return "GENERAL_PLANNING"

    def _allowed_for_daily_focus(self, *, payload: dict[str, object], domain: str) -> bool:
        recommendation = str(payload.get("recommendation", "")).lower()
        if domain in {"fitness", "meal", "calendar"}:
            return False
        return any(
            token in recommendation
            for token in ("review your schedule", "prioritize", "top priorities", "review your day", "review your tasks")
        )

    def _fallback_output(self, payload: dict[str, object]) -> GovernedOutput:
        return GovernedOutput(
            action_id=str(payload.get("action_id", "")),
            recommendation="Block 30 minutes this morning to review your schedule and prioritize your most important tasks before your day fills up.",
            why=[
                "You need a clear plan for today",
                "This reduces decision fatigue",
                "It helps you focus on what matters first",
            ],
            impact="This gives you a simpler plan for the day and lowers cognitive load.",
            approval_required=bool(payload.get("approval_required", True)),
        )

    def _sanitize_recommendation(self, text: str) -> str:
        sanitized = self._sanitize_text(text)
        sanitized = self._ensure_strong_verb(sanitized)
        sanitized = self._single_sentence(sanitized)
        return sanitized

    def _sanitize_bullet(self, text: str) -> str:
        sanitized = self._sanitize_text(text)
        words = sanitized.split()
        if len(words) > 19:
            sanitized = " ".join(words[:19])
        return sanitized.rstrip(".")

    def _sanitize_text(self, text: str) -> str:
        updated = text
        for source, target in _BANNED_PHRASES.items():
            updated = updated.replace(source, target)
            updated = updated.replace(source.capitalize(), target.capitalize())
        updated = updated.replace("because your day already has", "before your day fills up")
        updated = updated.replace("because your day has", "before your day fills up")
        updated = " ".join(updated.split()).strip()
        if updated and updated[-1] not in {".", "!", "?"}:
            updated += "."
        return updated[0].upper() + updated[1:] if updated else updated

    def _ensure_strong_verb(self, text: str) -> str:
        if not text:
            return text
        strong_verbs = ("Schedule", "Block", "Move", "Cook", "Prepare", "Review", "Prioritize", "Adjust")
        if text.startswith(strong_verbs):
            return text
        return f"Block time to {text[0].lower() + text[1:]}"

    def _single_sentence(self, text: str) -> str:
        stripped = text.strip()
        if not stripped:
            return stripped
        first = stripped.split(". ", 1)[0].rstrip(".")
        return first + "."

    def _decision_domain(self, response: HouseholdOSRunResponse) -> str:
        title = response.recommended_action.title.lower()
        summary = response.intent_interpretation.summary.lower()
        if any(token in title or token in summary for token in ("workout", "fitness", "exercise")):
            return "fitness"
        if any(token in title or token in summary for token in ("cook", "meal", "dinner", "grocery")):
            return "meal"
        if any(token in title or token in summary for token in ("appointment", "book", "schedule")):
            return "calendar"
        return "general"
