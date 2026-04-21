from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from household_os.core.contracts import HouseholdOSRunResponse
from household_os.learning.behavior_profile import BehaviorProfileBuilder, CategoryBehaviorProfile


_VAGUE_PHRASES = (
    "start routine",
    "consider doing",
    "you may want to",
)


@dataclass(frozen=True)
class EnrichedRecommendation:
    action_id: str
    recommendation: str
    why: list[str]
    impact: str
    approval_required: bool
    scheduled_for: str | None = None
    duration_minutes: int = 45
    category: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "action_id": self.action_id,
            "recommendation": self.recommendation,
            "why": self.why,
            "impact": self.impact,
            "approval_required": self.approval_required,
        }


class RecommendationBuilder:
    """Build deterministic, actionable recommendation text from decision output + state graph."""

    def __init__(self) -> None:
        self.behavior_profile_builder = BehaviorProfileBuilder()

    def build(self, *, response: HouseholdOSRunResponse, graph: dict[str, Any]) -> EnrichedRecommendation:
        action = response.recommended_action
        summary = response.current_state_summary
        domain = self._infer_domain(response)
        behavior_profile = self.behavior_profile_builder.build(graph)
        category = self._feedback_category(domain)
        category_profile = behavior_profile.for_category(category)
        scheduled_for, duration_minutes = self._adjust_schedule(
            action=action,
            graph=graph,
            domain=domain,
            profile=category_profile,
        )
        scheduled_phrase = self._format_schedule_phrase(scheduled_for)
        recommendation = self._build_recommendation_text(
            domain=domain,
            scheduled_phrase=scheduled_phrase,
            summary=summary,
            duration_minutes=duration_minutes,
        )

        why = [
            self._schedule_reason(summary=summary, scheduled_phrase=scheduled_phrase),
            self._goal_gap_reason(response=response, graph=graph, domain=domain),
            self._recent_behavior_reason(graph=graph, domain=domain, profile=category_profile),
        ]

        return EnrichedRecommendation(
            action_id=action.action_id,
            recommendation=self._sanitize(recommendation),
            why=[self._sanitize(item) for item in why][:3],
            impact=self._impact(domain=domain, duration_minutes=duration_minutes),
            approval_required=bool(action.approval_required),
            scheduled_for=scheduled_for,
            duration_minutes=duration_minutes,
            category=category,
        )

    def _build_recommendation_text(self, *, domain: str, scheduled_phrase: str, summary: Any, duration_minutes: int) -> str:
        context = f"because the household has {summary.calendar_events} scheduled calendar events"
        if domain == "fitness":
            text = f"Schedule a {duration_minutes}-minute workout {scheduled_phrase} to lock in a consistent training block {context}."
        elif domain == "meal":
            text = f"Create the dinner prep block {scheduled_phrase} and finalize the grocery list {context}."
        elif domain == "calendar":
            text = f"Book the appointment {scheduled_phrase} and reserve that slot in the shared calendar {context}."
        else:
            text = f"Adjust the highest-priority household task {scheduled_phrase} to reduce coordination backlog {context}."
        return text

    def _schedule_reason(self, *, summary: Any, scheduled_phrase: str) -> str:
        return (
            f"Schedule state: {summary.calendar_events} events are already on the calendar, and {scheduled_phrase} is a clear low-conflict window."
        )

    def _goal_gap_reason(self, *, response: HouseholdOSRunResponse, graph: dict[str, Any], domain: str) -> str:
        if domain == "fitness":
            routines = list(graph.get("fitness_routines", []))
            active_goal = routines[-1] if routines else "consistency"
            return f"Goal or gap: the active fitness goal is {active_goal}, and this scheduled session closes the routine gap."

        if domain == "meal":
            missing = sorted(response.current_state_summary.low_grocery_items)
            missing_text = ", ".join(missing) if missing else "no missing staples"
            return f"Goal or gap: meal execution depends on inventory coverage, and current gaps are {missing_text}."

        if domain == "calendar":
            return "Goal or gap: this action resolves a concrete appointment need before later time blocks become crowded."

        return f"Goal or gap: there are {response.current_state_summary.open_tasks} open tasks requiring explicit scheduling."

    def _recent_behavior_reason(self, *, graph: dict[str, Any], domain: str, profile: CategoryBehaviorProfile) -> str:
        execution_log = list(graph.get("execution_log", []))
        event_history = list(graph.get("event_history", []))
        if domain == "fitness":
            if profile.rejected_by_segment.get("morning", 0) > 3:
                return "Recent behavior: repeated morning workout rejections suggest an evening slot will work better."
            if profile.preferred_start_time and profile.approved_count > 0:
                return f"Recent behavior: workouts near {profile.preferred_start_time} are approved more consistently."
            recent = [item for item in execution_log if str(item.get("handler")) == "calendar_update"]
            if recent:
                return f"Recent behavior: {len(recent)} recently executed calendar updates show this workflow is being used consistently."
            return "Recent behavior: no recent executed workout-calendar updates were found, so scheduling this now creates momentum."

        if event_history:
            return f"Recent behavior: {len(event_history[-5:])} recent household events indicate active coordination that benefits from a specific next action."

        return "Recent behavior: no recent events are recorded yet, so a concrete action now establishes a baseline pattern."

    def _impact(self, *, domain: str, duration_minutes: int) -> str:
        impacts = {
            "fitness": f"This protects a repeatable {duration_minutes}-minute workout slot and improves follow-through over the next week.",
            "meal": "This reduces last-minute dinner decisions and lowers the chance of missed ingredients.",
            "calendar": "This secures a conflict-free appointment window and prevents downstream schedule collisions.",
            "general": "This reduces household coordination drift and makes the next decisions easier to sequence.",
        }
        return impacts.get(domain, impacts["general"])

    def _format_schedule_phrase(self, scheduled_for: str | None) -> str:
        if not scheduled_for:
            return "today between 09:00 and 10:00"

        if " " in scheduled_for and "-" in scheduled_for:
            date_part, time_block = scheduled_for.split(" ", 1)
            if "-" in time_block:
                start, end = time_block.split("-", 1)
                return f"on {date_part} from {start} to {end}"

        return f"at {scheduled_for}"

    def _infer_domain(self, response: HouseholdOSRunResponse) -> str:
        signals = " ".join(response.intent_interpretation.extracted_signals).lower()
        title = response.recommended_action.title.lower()
        description = response.recommended_action.description.lower()
        source = " ".join([signals, title, description])

        if any(token in source for token in ("workout", "fitness", "exercise", "training")):
            return "fitness"
        if any(token in source for token in ("cook", "meal", "dinner", "grocery")):
            return "meal"
        if any(token in source for token in ("appointment", "schedule", "book", "calendar")):
            return "calendar"
        return "general"

    def _adjust_schedule(
        self,
        *,
        action: Any,
        graph: dict[str, Any],
        domain: str,
        profile: CategoryBehaviorProfile,
    ) -> tuple[str | None, int]:
        default_duration = self._duration_from_slot(action.scheduled_for) or 45
        adjusted_duration = self.behavior_profile_builder.reduced_duration_minutes(profile, default_minutes=default_duration)

        if domain != "fitness":
            scheduled_for = self._apply_duration_to_slot(action.scheduled_for, adjusted_duration)
            return scheduled_for, adjusted_duration

        reference_time = str(graph.get("reference_time", ""))
        adjusted_slot = self.behavior_profile_builder.next_slot_for_profile(
            profile=profile,
            reference_time=reference_time,
            fallback_slot=action.scheduled_for,
            default_duration_minutes=adjusted_duration,
        )
        adjusted_slot = self._apply_duration_to_slot(adjusted_slot, adjusted_duration)
        return adjusted_slot, adjusted_duration

    def _feedback_category(self, domain: str) -> str:
        if domain == "appointment":
            return "calendar"
        if domain in {"fitness", "meal", "calendar"}:
            return domain
        return "calendar"

    def _duration_from_slot(self, scheduled_for: str | None) -> int | None:
        if not scheduled_for or " " not in scheduled_for or "-" not in scheduled_for:
            return None
        date_part, time_block = scheduled_for.split(" ", 1)
        start_raw, _sep, end_raw = time_block.partition("-")
        if not start_raw or not end_raw:
            return None
        from datetime import datetime

        start_dt = datetime.strptime(f"{date_part} {start_raw}", "%Y-%m-%d %H:%M")
        end_dt = datetime.strptime(f"{date_part} {end_raw}", "%Y-%m-%d %H:%M")
        return int((end_dt - start_dt).total_seconds() // 60)

    def _apply_duration_to_slot(self, scheduled_for: str | None, duration_minutes: int) -> str | None:
        if not scheduled_for or " " not in scheduled_for or "-" not in scheduled_for:
            return scheduled_for
        date_part, time_block = scheduled_for.split(" ", 1)
        start_raw, _sep, _end_raw = time_block.partition("-")
        if not start_raw:
            return scheduled_for
        from datetime import timedelta
        from household_os.learning.behavior_profile import BehaviorProfileBuilder

        start_dt = BehaviorProfileBuilder()._coerce_datetime(f"{date_part}T{start_raw}:00+00:00")
        end_dt = start_dt + timedelta(minutes=duration_minutes)
        return f"{start_dt.strftime('%Y-%m-%d %H:%M')}-{end_dt.strftime('%H:%M')}"

    def _sanitize(self, text: str) -> str:
        lowered = text.lower()
        cleaned = text
        for phrase in _VAGUE_PHRASES:
            if phrase in lowered:
                cleaned = cleaned.replace(phrase, "schedule the exact action")
                lowered = cleaned.lower()
        return cleaned
