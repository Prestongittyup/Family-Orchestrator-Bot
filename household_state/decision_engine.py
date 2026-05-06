from __future__ import annotations

from datetime import datetime
from typing import Any

from household_os.core.decision_engine import HouseholdOSDecisionEngine
from household_state.contracts import (
    ApprovalGroup,
    HouseholdCurrentStateSummary,
    HouseholdDecisionResponse,
    HouseholdRecommendedAction,
    StateConflictRecord,
)


def _domain_from_response(*, query: str, intent_summary: str, title: str) -> str:
    normalized = " ".join(f"{query} {intent_summary} {title}".lower().split())
    if any(token in normalized for token in ("appointment", "doctor", "dentist", "calendar")):
        return "appointment"
    if any(token in normalized for token in ("meal", "dinner", "cook", "grocery")):
        return "meal"
    if any(token in normalized for token in ("fitness", "workout", "exercise")):
        return "fitness"
    return "general"


def _parse_iso(raw: str) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _derive_conflicts(*, query: str, graph: dict[str, Any], low_inventory_items: list[str]) -> list[StateConflictRecord]:
    normalized = " ".join(query.lower().split())
    meal_tokens = ("dinner", "meal", "grocery", "groceries", "cook", "tonight")
    fitness_tokens = ("fitness", "workout", "exercise", "gym", "run")
    meal_requested = any(token in normalized for token in meal_tokens)
    fitness_requested = any(token in normalized for token in fitness_tokens)

    event_ranges: list[tuple[datetime, datetime]] = []
    for item in graph.get("calendar_events", []):
        if not isinstance(item, dict):
            continue
        start = _parse_iso(str(item.get("start", "")))
        end = _parse_iso(str(item.get("end", "")))
        if start is None or end is None or start >= end:
            continue
        event_ranges.append((start, end))

    conflicts: list[StateConflictRecord] = []
    seen_types: set[str] = set()

    def add_conflict(conflict_type: str, severity: str, description: str) -> None:
        if conflict_type in seen_types:
            return
        seen_types.add(conflict_type)
        conflicts.append(
            StateConflictRecord(
                conflict_type=conflict_type,
                severity=severity,
                description=description,
            )
        )

    has_calendar_overlap = False
    for idx, (left_start, left_end) in enumerate(event_ranges):
        for right_start, right_end in event_ranges[idx + 1 :]:
            if left_start < right_end and right_start < left_end:
                has_calendar_overlap = True
                break
        if has_calendar_overlap:
            break
    if has_calendar_overlap:
        add_conflict(
            "calendar_overlap",
            "high",
            "Two or more calendar commitments overlap in the current planning window.",
        )

    evening_start_minutes = 17 * 60 + 30
    evening_end_minutes = 19 * 60 + 30

    def overlaps_evening_window(start: datetime, end: datetime) -> bool:
        start_minutes = start.hour * 60 + start.minute
        end_minutes = end.hour * 60 + end.minute
        return start_minutes < evening_end_minutes and end_minutes > evening_start_minutes

    evening_busy = any(overlaps_evening_window(start, end) for start, end in event_ranges)

    if meal_requested and low_inventory_items:
        add_conflict(
            "inventory_gap",
            "medium",
            f"Meal planning request detected while low inventory items remain: {', '.join(sorted(low_inventory_items))}.",
        )

    if meal_requested and evening_busy:
        add_conflict(
            "evening_compression",
            "medium",
            "Dinner planning overlaps with existing early-evening commitments.",
        )

    if meal_requested and fitness_requested and evening_busy:
        add_conflict(
            "meal_time_tradeoff",
            "medium",
            "Meal prep and workout windows both compete for the same evening block.",
        )

    return conflicts


class HouseholdDecisionEngine:
    def __init__(self, *, delegate: HouseholdOSDecisionEngine | None = None) -> None:
        self._delegate = delegate or HouseholdOSDecisionEngine()

    def decide(
        self,
        *,
        household_id: str,
        query: str,
        graph: dict[str, Any],
        request_id: str,
    ) -> HouseholdDecisionResponse:
        run_response = self._delegate.run(
            household_id=household_id,
            query=query,
            graph=graph,
            request_id=request_id,
        )

        state_summary = run_response.current_state_summary
        recommended = run_response.recommended_action
        derived_conflicts = _derive_conflicts(
            query=query,
            graph=graph,
            low_inventory_items=list(state_summary.low_grocery_items),
        )
        domain = _domain_from_response(
            query=query,
            intent_summary=run_response.intent_interpretation.summary,
            title=recommended.title,
        )

        return HouseholdDecisionResponse(
            request_id=run_response.request_id,
            intent_summary=run_response.intent_interpretation.summary,
            current_state_summary=HouseholdCurrentStateSummary(
                household_id=state_summary.household_id,
                reference_time=state_summary.reference_time,
                calendar_event_count=state_summary.calendar_events,
                task_count=state_summary.open_tasks,
                meal_history_count=state_summary.meals_recorded,
                active_fitness_goal=None,
                low_inventory_items=list(state_summary.low_grocery_items),
                pending_approval_count=state_summary.pending_approvals,
                conflicts=derived_conflicts,
            ),
            recommended_action=HouseholdRecommendedAction(
                action_id=recommended.action_id,
                title=recommended.title,
                description=recommended.description,
                domain=domain,
                urgency=recommended.urgency,
                scheduled_for=recommended.scheduled_for,
                approval_required=recommended.approval_required,
                approval_status=recommended.approval_status,
            ),
            grouped_approvals=[
                ApprovalGroup(
                    group_id=run_response.grouped_approval_payload.group_id,
                    title=run_response.grouped_approval_payload.label,
                    description="Approve the recommended household action.",
                    action_ids=list(run_response.grouped_approval_payload.action_ids),
                    approval_status=run_response.grouped_approval_payload.approval_status,
                )
            ],
            reasoning_trace=list(run_response.reasoning_trace),
        )
