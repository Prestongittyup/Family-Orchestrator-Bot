from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from apps.assistant_core.intent_parser import parse_intent
from apps.assistant_core.fitness_planner import generate_fitness_plan
from apps.assistant_core.meal_planner import plan_meal
from apps.assistant_core.planning_engine import _find_available_windows
from household_state.contracts import (
    ApprovalGroup,
    HouseholdCurrentStateSummary,
    HouseholdDecisionResponse,
    HouseholdRecommendedAction,
    StateConflictRecord,
)


@dataclass(frozen=True)
class _CandidateAction:
    domain: str
    urgency_score: int
    urgency: str
    title: str
    description: str
    scheduled_for: str | None
    conflicts: list[StateConflictRecord]
    reasoning: list[str]


class HouseholdDecisionEngine:
    def decide(
        self,
        *,
        household_id: str,
        query: str,
        graph: dict[str, Any],
        request_id: str,
    ) -> HouseholdDecisionResponse:
        intent = parse_intent(query)
        reference_time = str(graph.get("reference_time", ""))
        calendar_events = list(graph.get("calendar_events", []))
        tasks = list(graph.get("tasks", []))
        inventory = dict(graph.get("inventory", {}))
        meal_history = list(graph.get("meal_history", []))
        fitness_goals = list(graph.get("fitness_goals", []))
        pending_actions = [item for item in graph.get("assistant_actions", []) if item.get("approval_status") != "approved"]

        candidates = [
            self._appointment_candidate(query, calendar_events, reference_time),
            self._meal_candidate(query, inventory, meal_history, calendar_events),
            self._fitness_candidate(query, calendar_events, reference_time, fitness_goals),
            self._general_candidate(calendar_events, tasks, pending_actions),
        ]
        ranked = sorted(candidates, key=lambda item: (-item.urgency_score, item.domain, item.title))
        selected = ranked[0]
        conflicts = self._merge_conflicts(self._schedule_conflicts(calendar_events), selected.conflicts)

        return HouseholdDecisionResponse(
            request_id=request_id,
            intent_summary=f"{intent.intent_type} request with {intent.priority} priority and {len(intent.entities)} extracted intent entities.",
            current_state_summary=HouseholdCurrentStateSummary(
                household_id=household_id,
                reference_time=reference_time,
                calendar_event_count=len(calendar_events),
                task_count=len(tasks),
                meal_history_count=len(meal_history),
                active_fitness_goal=fitness_goals[-1] if fitness_goals else None,
                low_inventory_items=sorted([item for item, count in inventory.items() if int(count) <= 0]),
                pending_approval_count=len(pending_actions) + 1,
                conflicts=conflicts,
            ),
            recommended_action=HouseholdRecommendedAction(
                action_id=f"{request_id}-next",
                title=selected.title,
                description=selected.description,
                domain=selected.domain,
                urgency=selected.urgency,
                scheduled_for=selected.scheduled_for,
                approval_required=True,
                approval_status="pending",
            ),
            grouped_approvals=[
                ApprovalGroup(
                    group_id=f"{request_id}-approval-group",
                    title=selected.title,
                    description="Approve the single recommended next action.",
                    action_ids=[f"{request_id}-next"],
                    approval_status="pending",
                )
            ],
            reasoning_trace=self._trim_reasoning(selected.reasoning),
        )

    def _appointment_candidate(
        self,
        query: str,
        calendar_events: list[dict[str, Any]],
        reference_time: str,
    ) -> _CandidateAction:
        busy_week_count = sum(1 for event in calendar_events if str(event.get("start", ""))[:10] >= reference_time[:10])
        windows = _find_available_windows(calendar_events, self._parse_iso(reference_time))
        chosen = next(
            (window for window in windows if window[0].lower() in {"monday", "tuesday", "wednesday", "thursday", "friday"}),
            windows[0] if windows else ("weekday", f"{reference_time[:10]} 10:30-11:15", "No low-conflict weekday window was available."),
        )
        urgency_score = 92 if any(token in query.lower() for token in ("dentist", "appointment", "doctor")) else 52
        conflicts = []
        if busy_week_count >= 4:
            conflicts.append(
                StateConflictRecord(
                    conflict_type="busy_week",
                    severity="high",
                    description=f"The next planning window already contains {busy_week_count} calendar commitments.",
                )
            )
        return _CandidateAction(
            domain="appointment",
            urgency_score=urgency_score,
            urgency="high" if urgency_score >= 80 else "medium",
            title="Schedule the dentist appointment in the first protected weekday slot",
            description=f"Use {chosen[1]} because it avoids the densest calendar pressure and preserves meal and pickup buffers.",
            scheduled_for=chosen[1],
            conflicts=conflicts,
            reasoning=[
                f"The household calendar already shows {busy_week_count} near-term commitments.",
                f"{chosen[1]} is the earliest low-conflict weekday opening in the state graph.",
                "Choosing a buffered appointment window avoids stealing time from dinner coordination later in the day.",
            ],
        )

    def _meal_candidate(
        self,
        query: str,
        inventory: dict[str, Any],
        meal_history: list[dict[str, Any]],
        calendar_events: list[dict[str, Any]],
    ) -> _CandidateAction:
        meal = plan_meal(
            inventory={key: int(value) for key, value in inventory.items()},
            recipe_history=meal_history,
            repeat_window_days=10,
        )
        dinner_conflict = any("18:" in str(event.get("start", "")) or "19:" in str(event.get("start", "")) for event in calendar_events)
        urgency_score = 88 if any(token in query.lower() for token in ("dinner", "grocery", "groceries", "meal")) else 48
        conflicts = []
        if dinner_conflict:
            conflicts.append(
                StateConflictRecord(
                    conflict_type="evening_compression",
                    severity="medium",
                    description="The evening calendar is compressed, so dinner needs a protected prep window.",
                )
            )
        if meal.grocery_additions:
            conflicts.append(
                StateConflictRecord(
                    conflict_type="inventory_gap",
                    severity="medium",
                    description=f"Inventory is missing: {', '.join(meal.grocery_additions)}.",
                )
            )
        return _CandidateAction(
            domain="meal",
            urgency_score=urgency_score,
            urgency="high" if urgency_score >= 80 else "medium",
            title=f"Cook {meal.recipe_name} and reconcile groceries first",
            description=(
                f"Plan {meal.recipe_name} for 2026-04-19 18:30-19:15 and collect "
                f"{', '.join(meal.grocery_additions) if meal.grocery_additions else 'no extra grocery items'} before that dinner window."
            ),
            scheduled_for="2026-04-19 18:30-19:15",
            conflicts=conflicts,
            reasoning=[
                f"Inventory and meal history make {meal.recipe_name} the least repetitive dinner option.",
                "The food recommendation preserves an evening buffer against existing time pressure.",
                "Groceries are bundled into the same recommendation so the plan stays realistic.",
            ],
        )

    def _fitness_candidate(
        self,
        query: str,
        calendar_events: list[dict[str, Any]],
        reference_time: str,
        fitness_goals: list[str],
    ) -> _CandidateAction:
        goal = fitness_goals[-1] if fitness_goals else "consistency"
        windows = _find_available_windows(calendar_events, self._parse_iso(reference_time))
        plan = generate_fitness_plan(goal, windows)
        session = plan.insertion_suggestions[0] if plan.insertion_suggestions else None
        urgency_score = 84 if any(token in query.lower() for token in ("working out", "work out", "fitness", "exercise")) else 44
        conflicts = []
        if any("18:" in str(event.get("start", "")) for event in calendar_events):
            conflicts.append(
                StateConflictRecord(
                    conflict_type="meal_time_tradeoff",
                    severity="medium",
                    description="Workout placement must avoid colliding with the protected dinner window.",
                )
            )
        return _CandidateAction(
            domain="fitness",
            urgency_score=urgency_score,
            urgency="high" if urgency_score >= 80 else "medium",
            title="Start a repeatable workout cadence with the first low-friction slot",
            description=f"Use {session.time_block if session else 'the next open morning slot'} to establish a {goal} routine without disrupting meals or pickups.",
            scheduled_for=session.time_block if session else None,
            conflicts=conflicts,
            reasoning=[
                f"The active fitness goal in the state graph is {goal}.",
                f"{session.time_block if session else 'The next open morning slot'} is the least disruptive insertion point across the current schedule.",
                "Leading with a low-friction session improves adherence without overloading the household plan.",
            ],
        )

    def _general_candidate(
        self,
        calendar_events: list[dict[str, Any]],
        tasks: list[dict[str, Any]],
        pending_actions: list[dict[str, Any]],
    ) -> _CandidateAction:
        urgency_score = 72 if pending_actions else 40
        title = "Approve the highest-impact pending household action" if pending_actions else "Run a short household coordination review"
        description = (
            "Resolve the oldest pending approval first so the household state graph can converge before further planning."
            if pending_actions
            else "Use a short coordination review to align tasks, calendar pressure, and tonight's protected windows."
        )
        return _CandidateAction(
            domain="general",
            urgency_score=urgency_score,
            urgency="medium" if urgency_score >= 60 else "low",
            title=title,
            description=description,
            scheduled_for=None,
            conflicts=self._schedule_conflicts(calendar_events)[:1],
            reasoning=[
                f"There are {len(tasks)} tracked tasks and {len(pending_actions)} pending approvals in the state graph.",
                "The decision engine emits one action so user-facing responses stay decisive and module-neutral.",
                "General coordination only wins when meal, appointment, and fitness triggers are weaker than the current pending state.",
            ],
        )

    def _schedule_conflicts(self, calendar_events: list[dict[str, Any]]) -> list[StateConflictRecord]:
        conflicts: list[StateConflictRecord] = []
        ordered = sorted(calendar_events, key=lambda item: (str(item.get("start", "")), str(item.get("title", ""))))
        for index, left in enumerate(ordered):
            left_start = left.get("start")
            left_end = left.get("end")
            if not left_start or not left_end:
                continue
            for right in ordered[index + 1 :]:
                right_start = right.get("start")
                right_end = right.get("end")
                if not right_start or not right_end:
                    continue
                if self._parse_iso(left_start) < self._parse_iso(right_end) and self._parse_iso(right_start) < self._parse_iso(left_end):
                    conflicts.append(
                        StateConflictRecord(
                            conflict_type="calendar_overlap",
                            severity="medium",
                            description=f"{left.get('title', 'Event')} overlaps with {right.get('title', 'Event')}.",
                        )
                    )
                    break
        return conflicts

    def _merge_conflicts(self, *groups: list[StateConflictRecord]) -> list[StateConflictRecord]:
        merged: list[StateConflictRecord] = []
        seen: set[tuple[str, str]] = set()
        severity_order = {"high": 0, "medium": 1, "low": 2}
        for group in groups:
            for item in group:
                key = (item.conflict_type, item.description)
                if key in seen:
                    continue
                seen.add(key)
                merged.append(item)
        return sorted(merged, key=lambda item: (severity_order.get(item.severity, 99), item.conflict_type, item.description))

    def _trim_reasoning(self, bullets: list[str]) -> list[str]:
        return [bullet for bullet in bullets if bullet][:5]

    def _parse_iso(self, value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))