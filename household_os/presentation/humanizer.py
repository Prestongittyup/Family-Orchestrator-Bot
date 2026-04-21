from __future__ import annotations

from dataclasses import dataclass
import re

from household_os.presentation.time_formatter import extract_and_format_relative_time


@dataclass(frozen=True)
class HumanizedRecommendation:
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


class RecommendationHumanizer:
    _PHRASE_MAP = {
        "this scheduled session closes the routine gap": "this helps you get back on track",
        "low-conflict window": "open time slot",
        "no recent executed workout-calendar updates were found": "you haven't worked out recently",
    }

    def humanize(self, payload: dict[str, object], *, reference_time: str | None = None) -> HumanizedRecommendation:
        action_id = str(payload.get("action_id", ""))
        recommendation = str(payload.get("recommendation", ""))
        impact = str(payload.get("impact", ""))
        approval_required = bool(payload.get("approval_required", True))

        rewritten_recommendation, relative_time = extract_and_format_relative_time(recommendation, reference_time=reference_time)
        recommendation_text = self._humanize_recommendation(rewritten_recommendation, relative_time=relative_time)

        source_why = payload.get("why", [])
        why_items = [str(item) for item in source_why if isinstance(item, str)]
        why = self._humanize_why(why_items)

        humanized_impact = self._sentence(impact)

        return HumanizedRecommendation(
            action_id=action_id,
            recommendation=recommendation_text,
            why=why,
            impact=humanized_impact,
            approval_required=approval_required,
        )

    def _humanize_recommendation(self, text: str, *, relative_time: str | None) -> str:
        lowered = text.lower()
        if "workout" in lowered:
            when = relative_time or "tomorrow morning at 6:00 AM"
            duration_match = re.search(r"(\d+)-minute workout", text, re.IGNORECASE)
            duration = duration_match.group(1) if duration_match else "45"
            if "evening" in when:
                context = "after work"
            elif "afternoon" in when:
                context = "while you still have energy"
            else:
                context = "before your day fills up"
            return f"Schedule a {duration}-minute workout {when} {context}."

        normalized = self._replace_phrases(text)
        normalized = normalized.replace("because the household has", "because your day already has")
        normalized = normalized.replace("scheduled calendar events", "planned commitments")
        return self._sentence(normalized)

    def _humanize_why(self, items: list[str]) -> list[str]:
        if not items:
            return [
                "Your schedule is already pretty full",
                "This helps you get back on track",
            ]

        lowered = " ".join(items).lower()
        output: list[str] = []

        if "schedule state" in lowered or "calendar" in lowered:
            output.append("Your schedule is already pretty full")

        if "goal or gap" in lowered or "consistency" in lowered or "routine gap" in lowered:
            output.append("You're trying to build consistency")

        if "recent behavior" in lowered or "no recent executed" in lowered or "momentum" in lowered:
            output.append("You haven't worked out recently")

        if "repeated morning workout rejections" in lowered or "approved more consistently" in lowered:
            output.append("Evening workouts have worked better for you")

        if not output:
            output = [self._short_reason(self._replace_phrases(item)) for item in items[:3]]

        return [self._trim_words(item, max_words=19) for item in output[:3]]

    def _short_reason(self, text: str) -> str:
        cleaned = text
        if ":" in cleaned:
            cleaned = cleaned.split(":", 1)[1].strip()
        return self._sentence(cleaned)

    def _replace_phrases(self, text: str) -> str:
        updated = text
        updated = updated.replace("household has X scheduled calendar events", "your day is already pretty full")
        for source, target in self._PHRASE_MAP.items():
            updated = updated.replace(source, target)
            updated = updated.replace(source.capitalize(), target.capitalize())
        return updated

    def _trim_words(self, text: str, *, max_words: int) -> str:
        words = text.split()
        if len(words) <= max_words:
            return text
        return " ".join(words[:max_words])

    def _sentence(self, text: str) -> str:
        cleaned = " ".join(text.split()).strip()
        if not cleaned:
            return cleaned
        if cleaned[-1] not in {".", "!", "?"}:
            cleaned += "."
        return cleaned[0].upper() + cleaned[1:]
