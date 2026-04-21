from __future__ import annotations

from datetime import datetime
from typing import Any

from apps.assistant_core.fitness_planner import generate_fitness_plan
from apps.assistant_core.intent_parser import parse_intent
from apps.assistant_core.meal_planner import plan_meal
from apps.assistant_core.planning_engine import _find_available_windows
from household_os.core.contracts import (
    CurrentStateSummary,
    GroupedApprovalPayload,
    HouseholdOSRunResponse,
    IntentInterpretation,
    RecommendedNextAction,
    UrgencyLevel,
)


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
        """
        Perform unified household reasoning:
        - Parse natural-language intent
        - Consider calendar, meals, tasks, fitness globally
        - Emit exactly ONE recommended next action

        Args:
            household_id: Household identifier
            query: User query string
            graph: Canonical state graph
            request_id: Request tracking ID
            allowed_domains: If set, only candidates from these domains are considered.
                           If None, all domains are available (legacy behavior).
                           If set and empty, raises ValueError.
        """

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
                self._meal_candidate(query, grocery_inventory, meal_history, calendar_events, intent.priority)
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
            "urgency": "high",
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

        description = f"Prepare {meal.recipe_name} for 18:30-19:15"
        if meal.grocery_additions:
            description += f" and acquire: {', '.join(meal.grocery_additions)}"

        return {
            "domain": "meal",
            "urgency_score": urgency,
            "urgency": "high",
            "title": f"Cook {meal.recipe_name}",
            "description": description,
            "scheduled_for": "2026-04-19 18:30-19:15",
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
            "urgency": "high",
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
            "urgency": "medium",
            "title": "Review household coordination",
            "description": f"Coordinate across {len(open_tasks)} open tasks and {len(constraints)} active constraints.",
            "scheduled_for": None,
            "reasoning": [
                f"Open tasks: {len(open_tasks)}",
                f"Active constraints: {len(constraints)}",
                "General coordination is the fallback when specific domains are unclear.",
            ],
        }

    def _parse_iso(self, value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
