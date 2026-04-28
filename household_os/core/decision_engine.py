from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any

from archive.apps.assistant_core.fitness_planner import generate_fitness_plan
from archive.apps.assistant_core.intent_parser import parse_intent
from archive.apps.assistant_core.meal_planner import plan_meal
from archive.apps.assistant_core.planning_engine import _find_available_windows
from household_os.core.contracts import (
    CurrentStateSummary,
    GroupedApprovalPayload,
    HouseholdOSRunResponse,
    IntentInterpretation,
    RecommendedNextAction,
    UrgencyLevel,
)
from household_os.security.trust_boundary_enforcer import enforce_import_boundary, validate_forbidden_call


enforce_import_boundary("household_os.core.decision_engine")


_MONTH_NAME_TO_INDEX = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

_WEEKDAY_TO_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


def _urgency_label_from_score(urgency_score: int) -> UrgencyLevel:
    if urgency_score >= 85:
        return "high"
    if urgency_score >= 65:
        return "medium"
    return "low"


class HouseholdOSDecisionEngine:
    """Unified cross-domain reasoning engine using canonical state graph."""

    def run(
        self,
        *,
        household_id: str,
        query: str,
        graph: dict[str, Any],
        request_id: str,
        allowed_domains: list[str] | None = None,
    ) -> HouseholdOSRunResponse:
        validate_forbidden_call(
            "HouseholdOSDecisionEngine.run",
            skip_modules={"household_os.core.decision_engine"},
        )
        if allowed_domains is not None and len(allowed_domains) == 0:
            raise ValueError("allowed_domains must be None or a non-empty list")

        # Perform unified household reasoning across calendar, meals, tasks, and fitness.

        # Parse intent from natural language
        intent = parse_intent(query)
        intent_summary = f"{intent.intent_type} query with {intent.priority} priority and {len(intent.entities)} signal(s)"

        # Read canonical state from graph
        reference_time = str(graph.get("reference_time", ""))
        calendar_events = list(graph.get("calendar_events", []))
        tasks = list(graph.get("tasks", []))
        meal_history = list(graph.get("meal_history", []))
        grocery_inventory = dict(graph.get("grocery_inventory", {}))
        fitness_routines = list(graph.get("fitness_routines", []))
        household_constraints = list(graph.get("household_constraints", []))

        # Cross-domain candidate generation (respecting intent lock constraints)
        candidates = []

        # Determine which domains to consider
        can_use_calendar = allowed_domains is None or "calendar" in allowed_domains
        can_use_meal = allowed_domains is None or "meal" in allowed_domains
        can_use_fitness = allowed_domains is None or "fitness" in allowed_domains
        can_use_general = allowed_domains is None or "general" in allowed_domains

        # Appointment/calendar candidate
        if can_use_calendar and intent.intent_type in ("appointment", "general"):
            candidates.append(
                self._calendar_candidate(query, calendar_events, reference_time, intent.priority)
            )

        # Meal candidate
        if can_use_meal and intent.intent_type in ("meal", "general"):
            candidates.append(
                self._meal_candidate(
                    query,
                    grocery_inventory,
                    meal_history,
                    calendar_events,
                    reference_time,
                    intent.priority,
                )
            )

        # Fitness candidate
        if can_use_fitness and intent.intent_type in ("fitness", "general"):
            candidates.append(
                self._fitness_candidate(query, calendar_events, reference_time, fitness_routines, intent.priority)
            )

        # Fallback: general household coordination
        if not candidates:
            candidates.append(self._general_candidate(tasks, household_constraints))

        # Select single top-ranked action
        ranked = sorted(
            candidates,
            key=lambda x: (-x["urgency_score"], x["domain"]),
        )
        selected = ranked[0]

        return HouseholdOSRunResponse(
            request_id=request_id,
            intent_interpretation=IntentInterpretation(
                summary=intent_summary,
                urgency=intent.priority,
                extracted_signals=intent.entities,
            ),
            current_state_summary=CurrentStateSummary(
                household_id=household_id,
                reference_time=reference_time,
                calendar_events=len(calendar_events),
                open_tasks=len([t for t in tasks if str(t.get("status", "")).lower() != "completed"]),
                meals_recorded=len(meal_history),
                low_grocery_items=sorted([str(item) for item, count in grocery_inventory.items() if int(count) <= 0]),
                fitness_routines=len(fitness_routines),
                constraints_count=len(household_constraints),
                pending_approvals=1,
                state_version=int(graph.get("state_version", 0)),
            ),
            recommended_action=RecommendedNextAction(
                action_id=f"{request_id}-primary",
                title=selected["title"],
                description=selected["description"],
                urgency=selected["urgency"],
                scheduled_for=selected.get("scheduled_for"),
                approval_required=True,
                approval_status="pending",
            ),
            follow_ups=[],
            grouped_approval_payload=GroupedApprovalPayload(
                group_id=f"{request_id}-group",
                label="Batch Household Action Execution",
                action_ids=[f"{request_id}-primary"],
                approval_status="pending",
            ),
            reasoning_trace=selected.get("reasoning", [])[:5],
        )

    def _calendar_candidate(
        self,
        query: str,
        calendar_events: list[dict[str, Any]],
        reference_time: str,
        intent_priority: str,
    ) -> dict[str, Any]:
        busy_count = sum(1 for evt in calendar_events if str(evt.get("start", ""))[:10] >= reference_time[:10])

        explicit_start = self._parse_explicit_requested_start(query, reference_time=reference_time)
        if explicit_start is not None:
            explicit_slot = self._format_slot(explicit_start)
            explicit_end = explicit_start + timedelta(minutes=45)
            has_conflict = self._has_calendar_conflict(
                calendar_events=calendar_events,
                start_dt=explicit_start,
                end_dt=explicit_end,
            )

            urgency = 90 if any(token in query.lower() for token in ("dentist", "doctor", "appointment")) else 70
            if intent_priority == "high":
                urgency = min(urgency + 10, 100)

            conflict_clause = (
                "but it overlaps known calendar commitments and may require adjustment"
                if has_conflict
                else "because it matches the requested time and avoids known calendar conflicts"
            )

            return {
                "domain": "calendar",
                "urgency_score": urgency,
                "urgency": _urgency_label_from_score(urgency),
                "title": f"Schedule appointment for {explicit_slot}",
                "description": f"Reserve {explicit_slot} for the requested appointment {conflict_clause}.",
                "scheduled_for": explicit_slot,
                "reasoning": [
                    f"Calendar analysis shows {busy_count} near-term commitments.",
                    f"The request explicitly included {explicit_slot}, so the engine preserved that window.",
                    "Human approval remains required before execution.",
                ],
            }

        windows = _find_available_windows(calendar_events, self._parse_iso(reference_time))
        chosen = next(
            (w for w in windows if w[0].lower() in {"monday", "tuesday", "wednesday", "thursday", "friday"}),
            windows[0] if windows else ("weekday", f"{reference_time[:10]} 10:00-11:00", "Default weekday window"),
        )

        urgency = 90 if any(token in query.lower() for token in ("dentist", "doctor", "appointment")) else 70
        if intent_priority == "high":
            urgency = min(urgency + 10, 100)

        return {
            "domain": "calendar",
            "urgency_score": urgency,
            "urgency": _urgency_label_from_score(urgency),
            "title": f"Schedule appointment for {chosen[1]}",
            "description": f"Reserve {chosen[1]} for the requested appointment because it avoids known calendar conflicts.",
            "scheduled_for": chosen[1],
            "reasoning": [
                f"Calendar analysis shows {busy_count} near-term commitments.",
                f"{chosen[1]} is the next low-conflict window.",
                "Scheduling protects meal and family time.",
            ],
        }

    def _meal_candidate(
        self,
        query: str,
        grocery_inventory: dict[str, int],
        meal_history: list[dict[str, Any]],
        calendar_events: list[dict[str, Any]],
        reference_time: str,
        intent_priority: str,
    ) -> dict[str, Any]:
        meal = plan_meal(
            inventory=grocery_inventory,
            recipe_history=meal_history,
            repeat_window_days=10,
        )

        dinner_conflict = any(
            "18:" in str(evt.get("start", "")) or "19:" in str(evt.get("start", ""))
            for evt in calendar_events
        )

        urgency = 85 if any(token in query.lower() for token in ("dinner", "meal", "cook", "grocery")) else 65
        if intent_priority == "high":
            urgency = min(urgency + 10, 100)

        reference_dt = self._parse_iso(reference_time)
        meal_start = reference_dt.replace(hour=18, minute=30, second=0, microsecond=0)
        meal_end = meal_start + timedelta(minutes=45)
        scheduled_for = f"{meal_start.strftime('%Y-%m-%d %H:%M')}-{meal_end.strftime('%H:%M')}"

        description = f"Prepare {meal.recipe_name} for {scheduled_for}"
        if meal.grocery_additions:
            description += f" and acquire: {', '.join(meal.grocery_additions)}"

        return {
            "domain": "meal",
            "urgency_score": urgency,
            "urgency": _urgency_label_from_score(urgency),
            "title": f"Cook {meal.recipe_name}",
            "description": description,
            "scheduled_for": scheduled_for,
            "reasoning": [
                f"{meal.recipe_name} balances nutrition with kitchen availability.",
                f"Grocery gaps: {', '.join(meal.grocery_additions) if meal.grocery_additions else 'None'}",
                f"Evening prep timing avoids calendar pressure: {dinner_conflict}",
            ],
        }

    def _fitness_candidate(
        self,
        query: str,
        calendar_events: list[dict[str, Any]],
        reference_time: str,
        fitness_routines: list[str],
        intent_priority: str,
    ) -> dict[str, Any]:
        goal = fitness_routines[-1] if fitness_routines else "consistency"
        windows = _find_available_windows(calendar_events, self._parse_iso(reference_time))
        plan = generate_fitness_plan(goal, windows)
        session = plan.insertion_suggestions[0] if plan.insertion_suggestions else None

        urgency = 80 if any(token in query.lower() for token in ("work out", "working out", "workout", "fitness", "exercise")) else 60
        if intent_priority == "high":
            urgency = min(urgency + 10, 100)

        scheduled = session.time_block if session else None

        return {
            "domain": "fitness",
            "urgency_score": urgency,
            "urgency": _urgency_label_from_score(urgency),
            "title": f"Start {goal} routine",
            "description": f"Use {scheduled or 'the next open morning slot'} for a repeatable {goal} session.",
            "scheduled_for": scheduled,
            "reasoning": [
                f"Active fitness goal: {goal}",
                f"Best insertion: {scheduled or 'morning'}",
                "Low-friction start improves adherence.",
            ],
        }

    def _general_candidate(
        self,
        tasks: list[dict[str, Any]],
        constraints: list[str],
    ) -> dict[str, Any]:
        open_tasks = [t for t in tasks if str(t.get("status", "")).lower() != "completed"]

        return {
            "domain": "general",
            "urgency_score": 50,
            "urgency": _urgency_label_from_score(50),
            "title": "Review household coordination",
            "description": f"Coordinate across {len(open_tasks)} open tasks and {len(constraints)} active constraints.",
            "scheduled_for": None,
            "reasoning": [
                f"Open tasks: {len(open_tasks)}",
                f"Active constraints: {len(constraints)}",
                "General coordination is the fallback when specific domains are unclear.",
            ],
        }

    def _parse_explicit_requested_start(self, query: str, *, reference_time: str) -> datetime | None:
        normalized = " ".join(query.strip().lower().split())
        reference_dt = self._parse_iso(reference_time)
        requested_date = self._extract_requested_date(normalized, reference_dt=reference_dt)
        requested_time = self._extract_requested_time(normalized)

        if requested_date is None and requested_time is None:
            return None

        if requested_date is None:
            requested_date = reference_dt.date()
        if requested_time is None:
            requested_time = (9, 0)

        hour, minute = requested_time
        try:
            return datetime(
                requested_date.year,
                requested_date.month,
                requested_date.day,
                hour,
                minute,
                tzinfo=reference_dt.tzinfo,
            )
        except ValueError:
            return None

    def _extract_requested_date(self, normalized: str, *, reference_dt: datetime):
        numeric_date_match = re.search(
            r"\b(?P<month>\d{1,2})[/-](?P<day>\d{1,2})(?:[/-](?P<year>\d{2,4}))?\b",
            normalized,
            flags=re.IGNORECASE,
        )
        if numeric_date_match is not None:
            month = int(numeric_date_match.group("month"))
            day = int(numeric_date_match.group("day"))
            year_raw = numeric_date_match.group("year")
            year = int(year_raw) if year_raw else reference_dt.year
            if year < 100:
                year += 2000
            try:
                return datetime(year, month, day, tzinfo=reference_dt.tzinfo).date()
            except ValueError:
                return None

        named_date_match = re.search(
            r"\b(?P<month_name>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(?P<day>\d{1,2})(?:\s*,?\s*(?P<year>\d{2,4}))?\b",
            normalized,
            flags=re.IGNORECASE,
        )
        if named_date_match is not None:
            month_name = named_date_match.group("month_name").lower()
            month = _MONTH_NAME_TO_INDEX.get(month_name)
            if month is None:
                return None
            day = int(named_date_match.group("day"))
            year_raw = named_date_match.group("year")
            year = int(year_raw) if year_raw else reference_dt.year
            if year < 100:
                year += 2000
            try:
                return datetime(year, month, day, tzinfo=reference_dt.tzinfo).date()
            except ValueError:
                return None

        if "tomorrow" in normalized:
            return reference_dt.date() + timedelta(days=1)

        if "today" in normalized or "tonight" in normalized:
            return reference_dt.date()

        for weekday_name, weekday_index in _WEEKDAY_TO_INDEX.items():
            if re.search(rf"\b{weekday_name}\b", normalized):
                offset = (weekday_index - reference_dt.weekday()) % 7
                if offset == 0:
                    offset = 7
                return reference_dt.date() + timedelta(days=offset)

        return None

    def _extract_requested_time(self, normalized: str) -> tuple[int, int] | None:
        if re.search(r"\bnoon\b", normalized):
            return (12, 0)
        if re.search(r"\bmidnight\b", normalized):
            return (0, 0)

        twelve_hour_match = re.search(
            r"\b(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<ampm>am|pm)\b",
            normalized,
            flags=re.IGNORECASE,
        )
        if twelve_hour_match is not None:
            hour = int(twelve_hour_match.group("hour"))
            minute = int(twelve_hour_match.group("minute") or 0)
            ampm = twelve_hour_match.group("ampm").lower()

            if hour < 1 or hour > 12:
                return None
            if ampm == "pm" and hour != 12:
                hour += 12
            if ampm == "am" and hour == 12:
                hour = 0
            return (hour, minute)

        twenty_four_hour_match = re.search(
            r"\b(?P<hour24>[01]?\d|2[0-3]):(?P<minute24>[0-5]\d)\b",
            normalized,
            flags=re.IGNORECASE,
        )
        if twenty_four_hour_match is not None:
            return (
                int(twenty_four_hour_match.group("hour24")),
                int(twenty_four_hour_match.group("minute24")),
            )

        if re.search(r"\bmorning\b", normalized):
            return (9, 0)
        if re.search(r"\bafternoon\b", normalized) or re.search(r"\bmidday\b", normalized):
            return (14, 0)
        if re.search(r"\bevening\b", normalized) or re.search(r"\btonight\b", normalized):
            return (18, 0)

        return None

    def _format_slot(self, start_dt: datetime) -> str:
        end_dt = start_dt + timedelta(minutes=45)
        return f"{start_dt.strftime('%Y-%m-%d %H:%M')}-{end_dt.strftime('%H:%M')}"

    def _has_calendar_conflict(
        self,
        *,
        calendar_events: list[dict[str, Any]],
        start_dt: datetime,
        end_dt: datetime,
    ) -> bool:
        for event in calendar_events:
            start_raw = str(event.get("start", "")).strip()
            end_raw = str(event.get("end", "")).strip()
            if not start_raw or not end_raw:
                continue

            try:
                event_start = self._parse_iso(start_raw)
                event_end = self._parse_iso(end_raw)
            except ValueError:
                continue

            if start_dt < event_end and event_start < end_dt:
                return True

        return False

    def _parse_iso(self, value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
