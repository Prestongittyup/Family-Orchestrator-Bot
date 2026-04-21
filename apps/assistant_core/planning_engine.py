from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from apps.api.integration_core.models.household_state import CalendarEvent, HouseholdState
from apps.assistant_core.contracts import (
    AlternativeRecord,
    AssistantIntent,
    AssistantPlan,
    AssistantResponse,
    ConflictRecord,
    FallbackOption,
    ProposedAction,
    RecommendedPlan,
    ScheduleCandidate,
    TimelineBlock,
)
from apps.assistant_core.fitness_planner import generate_fitness_plan
from apps.assistant_core.intent_parser import parse_intent
from apps.assistant_core.meal_planner import default_inventory, default_recipe_history, plan_meal


REFERENCE_NOW = datetime(2026, 4, 19, 8, 0, tzinfo=UTC)
REPO_ROOT = Path(__file__).resolve().parents[2]


def _fallback_household_state(household_id: str) -> HouseholdState:
    return HouseholdState(
        user_id=household_id,
        calendar_events=[
            CalendarEvent(
                event_id="evt-school-dropoff",
                title="School drop-off",
                start="2026-04-20T08:00:00Z",
                end="2026-04-20T08:45:00Z",
            ),
            CalendarEvent(
                event_id="evt-standup",
                title="Work standup",
                start="2026-04-20T09:30:00Z",
                end="2026-04-20T10:00:00Z",
            ),
            CalendarEvent(
                event_id="evt-review",
                title="Project review",
                start="2026-04-20T13:00:00Z",
                end="2026-04-20T14:00:00Z",
            ),
            CalendarEvent(
                event_id="evt-pickup",
                title="School pickup",
                start="2026-04-20T15:15:00Z",
                end="2026-04-20T15:45:00Z",
            ),
            CalendarEvent(
                event_id="evt-soccer",
                title="Soccer practice",
                start="2026-04-22T17:30:00Z",
                end="2026-04-22T18:30:00Z",
            ),
        ],
        metadata={"reference_time": "2026-04-19T08:00:00Z"},
    )


def _parse_iso(value: str) -> datetime:
    raw = value.replace("Z", "+00:00")
    return datetime.fromisoformat(raw)


def _format_time_block(start_dt: datetime, end_dt: datetime) -> str:
    return f"{start_dt.strftime('%Y-%m-%d %H:%M')}-{end_dt.strftime('%H:%M')}"


def _load_artifact_summary() -> list[str]:
    traces: list[str] = []
    artifact_map = {
        "evaluation_results.json": "evaluation",
        "simulation_results.json": "simulation",
        "policy_engine_report.json": "policy",
        "insight_report.json": "insight",
    }
    for filename, label in artifact_map.items():
        path = REPO_ROOT / filename
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if label == "policy":
            traces.append(
                f"Read-only policy context loaded with {payload.get('policies_generated', 0)} prior recommendations for consistency checks."
            )
        elif label == "simulation":
            traces.append(
                f"Read-only simulation context loaded; stability score reference is {payload.get('stability_scores', {}).get('stability_score', 0)}."
            )
        elif label == "evaluation":
            traces.append(
                f"Read-only evaluation context loaded with {len(payload.get('failure_patterns', []))} tracked failure pattern groups."
            )
        else:
            recommendation_field = payload.get("recommendations", [])
            recommendation_count = (
                len(recommendation_field)
                if isinstance(recommendation_field, list)
                else int(recommendation_field)
                if isinstance(recommendation_field, int)
                else 0
            )
            traces.append(
                f"Read-only insight context loaded with {recommendation_count} cross-layer recommendations."
            )
    return traces


def _request_id(query: str, household_id: str, repeat_window_days: int, fitness_goal: str | None) -> str:
    normalized = json.dumps(
        {
            "query": " ".join(query.strip().lower().split()),
            "household_id": household_id,
            "repeat_window_days": repeat_window_days,
            "fitness_goal": fitness_goal or "",
        },
        sort_keys=True,
    )
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"assist-{digest[:12]}"


def _resolve_reference_time(state: HouseholdState) -> datetime:
    reference = str(state.metadata.get("reference_time", ""))
    if reference:
        return _parse_iso(reference)
    return REFERENCE_NOW


def _state_events(state: HouseholdState) -> list[dict[str, str]]:
    events = [event.as_dict() for event in state.calendar_events]
    if events:
        return sorted(events, key=lambda item: (item.get("start", ""), item.get("title", ""), item.get("event_id", "")))
    return [event.as_dict() for event in _fallback_household_state(state.user_id).calendar_events]


def _extract_requested_day(intent: AssistantIntent, reference_time: datetime) -> date:
    labels = {label.lower() for label in intent.time_constraints}
    if "tomorrow" in labels:
        return reference_time.date() + timedelta(days=1)
    if "today" in labels or "tonight" in labels:
        return reference_time.date()
    day_index = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    for label, target in day_index.items():
        if label in labels:
            delta = (target - reference_time.weekday()) % 7
            return reference_time.date() + timedelta(days=delta)
    return reference_time.date() + timedelta(days=1)


def _candidate_start_times(intent: AssistantIntent) -> list[time]:
    labels = {label.lower() for label in intent.time_constraints}
    if any(label in labels for label in ("morning", "9am", "10am")):
        return [time(9, 30), time(10, 30), time(11, 30)]
    if any(label in labels for label in ("afternoon", "midday", "lunch")):
        return [time(12, 0), time(14, 30), time(15, 30)]
    if any(label in labels for label in ("evening", "tonight")):
        return [time(17, 30), time(18, 30), time(19, 15)]
    return [time(10, 30), time(14, 30), time(16, 0)]


def _overlaps(start_dt: datetime, end_dt: datetime, event: dict[str, str]) -> bool:
    event_start = _parse_iso(event["start"])
    event_end = _parse_iso(event["end"])
    return start_dt < event_end and event_start < end_dt


def _event_label(event: dict[str, str]) -> str:
    return f"{event.get('title', 'Event')} ({event.get('start', '')})"


def _find_available_windows(events: list[dict[str, str]], reference_time: datetime) -> list[tuple[str, str, str]]:
    windows: list[tuple[str, str, str]] = []
    for offset in range(0, 7):
        day = reference_time.date() + timedelta(days=offset)
        for start_hour in (6, 7, 12, 16, 18):
            start_dt = datetime.combine(day, time(start_hour, 0), tzinfo=UTC)
            end_dt = start_dt + timedelta(minutes=45)
            if any(_overlaps(start_dt, end_dt, event) for event in events):
                continue
            windows.append(
                (
                    day.strftime("%A"),
                    _format_time_block(start_dt, end_dt),
                    "Window remains clear of known family and work commitments.",
                )
            )
    return windows


def _build_appointment_plan(intent: AssistantIntent, events: list[dict[str, str]], reference_time: datetime) -> tuple[AssistantPlan, list[ConflictRecord], list[AlternativeRecord], list[ProposedAction], list[str]]:
    requested_day = _extract_requested_day(intent, reference_time)
    duration = timedelta(minutes=45)
    candidate_schedules: list[ScheduleCandidate] = []
    conflicts: list[ConflictRecord] = []
    alternatives: list[AlternativeRecord] = []
    reasoning = ["Detected appointment intent and switched to schedule conflict analysis mode."]
    chosen_block: TimelineBlock | None = None
    chosen_target = ""

    for index, candidate_time in enumerate(_candidate_start_times(intent)):
        start_dt = datetime.combine(requested_day, candidate_time, tzinfo=UTC)
        end_dt = start_dt + duration
        overlapping = [event for event in events if _overlaps(start_dt, end_dt, event)]
        block = TimelineBlock(
            time_block=_format_time_block(start_dt, end_dt),
            title="Tentative appointment block",
            rationale="Selected from the nearest low-friction household window.",
            confidence=0.9 if not overlapping and index == 0 else 0.84 if not overlapping else 0.51,
        )
        candidate_schedules.append(
            ScheduleCandidate(
                candidate_id=f"appointment-{index + 1}",
                label=f"Option {index + 1}",
                blocks=[block],
                confidence=block.confidence,
            )
        )
        if overlapping:
            conflicts.append(
                ConflictRecord(
                    conflict_type="schedule_overlap",
                    severity="medium",
                    description=f"{block.time_block} overlaps with {', '.join(_event_label(event) for event in overlapping)}.",
                    impacted_blocks=[block.time_block],
                )
            )
            alternatives.append(
                AlternativeRecord(
                    label=f"Shift from {block.time_block}",
                    description="Move the appointment later in the day to avoid the detected overlap.",
                    confidence=0.68,
                )
            )
            continue
        if chosen_block is None:
            chosen_block = block
            chosen_target = start_dt.isoformat().replace("+00:00", "Z")

    if chosen_block is None:
        fallback_start = datetime.combine(requested_day + timedelta(days=1), time(10, 30), tzinfo=UTC)
        fallback_end = fallback_start + duration
        chosen_block = TimelineBlock(
            time_block=_format_time_block(fallback_start, fallback_end),
            title="Fallback appointment block",
            rationale="All same-day options conflicted, so the engine moved to the next clear morning slot.",
            confidence=0.73,
        )
        chosen_target = fallback_start.isoformat().replace("+00:00", "Z")
        alternatives.append(
            AlternativeRecord(
                label="Next-day morning",
                description="Use the next-day slot to preserve work and school commitments.",
                confidence=0.73,
            )
        )

    plan = AssistantPlan(
        domain="appointment",
        summary=f"Recommended appointment window: {chosen_block.time_block}.",
        candidate_schedules=candidate_schedules,
        recommended_plan=RecommendedPlan(
            summary=f"Book a tentative appointment hold for {chosen_block.time_block}.",
            timeline_blocks=[chosen_block],
            confidence=chosen_block.confidence,
            reasoning="The recommended option avoids current overlaps while keeping the request close to the preferred day and time.",
        ),
        fallback_options=[
            FallbackOption(
                option_id="fallback-later-day",
                description="Move to a later afternoon window if provider availability changes.",
                tradeoffs=["Less convenient for pickup logistics", "Lower family buffer before dinner"],
            ),
            FallbackOption(
                option_id="fallback-next-day",
                description="Shift to the following morning to remove same-day contention.",
                tradeoffs=["Adds one-day delay", "May require updated work coverage"],
            ),
        ],
    )
    actions = [
        ProposedAction(
            action_id=f"{_request_id(chosen_target, 'calendar', 0, None)}-hold",
            action_type="calendar_hold",
            description="Create a tentative appointment hold after human confirmation.",
            target=chosen_target,
            approval_status="pending",
            execution_mode="inert_until_approved",
        )
    ]
    reasoning.append(f"Selected {chosen_block.time_block} as the first candidate that cleared conflict resolution checks.")
    return plan, conflicts, alternatives, actions, reasoning


def _build_meal_plan(intent: AssistantIntent, repeat_window_days: int) -> tuple[AssistantPlan, list[ConflictRecord], list[AlternativeRecord], list[ProposedAction], list[str]]:
    meal = plan_meal(
        inventory=default_inventory(),
        recipe_history=default_recipe_history(),
        repeat_window_days=repeat_window_days,
    )
    meal_block = TimelineBlock(
        time_block="2026-04-19 18:30-19:15",
        title=meal.recipe_name,
        rationale="Placed in the evening meal prep window to protect work and school transitions.",
        confidence=0.86,
    )
    plan = AssistantPlan(
        domain="meal",
        summary=f"Recommend {meal.recipe_name} with only the required grocery additions.",
        candidate_schedules=[
            ScheduleCandidate(
                candidate_id="meal-1",
                label="Dinner prep window",
                blocks=[meal_block],
                confidence=meal_block.confidence,
            )
        ],
        recommended_plan=RecommendedPlan(
            summary=f"Cook {meal.recipe_name} in the main dinner window.",
            timeline_blocks=[meal_block],
            confidence=meal_block.confidence,
            reasoning="The selected meal avoids recent repeats, uses the strongest inventory coverage, and preserves nutrition balance.",
        ),
        fallback_options=[
            FallbackOption(
                option_id="meal-fallback-breakfast",
                description="Swap to a faster skillet option if the evening becomes compressed.",
                tradeoffs=["Lower dinner leftovers", "Less protein variety"],
            )
        ],
        meal_plan=meal,
    )
    actions = [
        ProposedAction(
            action_id="meal-grocery-review",
            action_type="grocery_review",
            description="Review the suggested grocery additions before any purchase decision.",
            target=", ".join(meal.grocery_additions) or "inventory-complete",
            approval_status="pending",
            execution_mode="inert_until_approved",
        )
    ]
    reasoning = [
        "Detected meal planning intent and evaluated inventory plus recipe history in read-only mode.",
        f"Applied a {repeat_window_days}-day repeat window before ranking meal options.",
    ]
    alternatives = [
        AlternativeRecord(
            label="Keep leftovers buffer",
            description="Delay the meal one day if the household needs a shorter prep cycle tonight.",
            confidence=0.64,
        )
    ]
    return plan, [], alternatives, actions, reasoning


def _build_fitness_plan(intent: AssistantIntent, events: list[dict[str, str]], reference_time: datetime, fitness_goal: str | None) -> tuple[AssistantPlan, list[ConflictRecord], list[AlternativeRecord], list[ProposedAction], list[str]]:
    goal = fitness_goal or "strength"
    windows = _find_available_windows(events, reference_time)
    fitness_plan = generate_fitness_plan(goal, windows)
    recommended_blocks = fitness_plan.insertion_suggestions[:2]
    plan = AssistantPlan(
        domain="fitness",
        summary=f"Prepared a weekly {goal} plan across the clearest schedule windows.",
        candidate_schedules=[
            ScheduleCandidate(
                candidate_id=f"fitness-{index + 1}",
                label=session.day,
                blocks=[fitness_plan.insertion_suggestions[index]],
                confidence=0.81,
            )
            for index, session in enumerate(fitness_plan.sessions)
        ],
        recommended_plan=RecommendedPlan(
            summary=f"Start with the first two {goal} sessions and preserve the third as overflow coverage.",
            timeline_blocks=recommended_blocks,
            confidence=0.81,
            reasoning="The plan favors windows with the lowest overlap risk and keeps workouts away from pickup and evening activity pressure.",
        ),
        fallback_options=[
            FallbackOption(
                option_id="fitness-fallback-shorter",
                description="Compress each workout to 30 minutes if family commitments tighten.",
                tradeoffs=["Reduced training volume", "Lower recovery demand"],
            )
        ],
        fitness_plan=fitness_plan,
    )
    actions = [
        ProposedAction(
            action_id="fitness-calendar-review",
            action_type="calendar_suggestion",
            description="Approve suggested workout holds before any calendar insertion is considered externally.",
            target=fitness_plan.sessions[0].time_block if fitness_plan.sessions else "no-window",
            approval_status="pending",
            execution_mode="inert_until_approved",
        )
    ]
    reasoning = [
        "Detected fitness planning intent and derived weekly availability from read-only calendar windows.",
        f"Mapped the goal to a {goal} session sequence without writing anything to the live schedule.",
    ]
    alternatives = [
        AlternativeRecord(
            label="Weekend emphasis",
            description="Move one session to the weekend to reduce weekday compression.",
            confidence=0.7,
        )
    ]
    return plan, [], alternatives, actions, reasoning


def _build_general_plan(events: list[dict[str, str]], reference_time: datetime) -> tuple[AssistantPlan, list[ConflictRecord], list[AlternativeRecord], list[ProposedAction], list[str]]:
    windows = _find_available_windows(events, reference_time)
    first_window = windows[0]
    block = TimelineBlock(
        time_block=first_window[1],
        title="Household coordination review",
        rationale="Uses the next shared low-conflict window for planning and follow-up.",
        confidence=0.77,
    )
    plan = AssistantPlan(
        domain="general",
        summary="Recommend a short household coordination block to align commitments and next actions.",
        candidate_schedules=[
            ScheduleCandidate(candidate_id="general-1", label=first_window[0], blocks=[block], confidence=block.confidence)
        ],
        recommended_plan=RecommendedPlan(
            summary="Use a dedicated coordination block instead of layering more tasks into already busy transitions.",
            timeline_blocks=[block],
            confidence=block.confidence,
            reasoning="A short planning block reduces follow-up errors when the schedule already contains multiple handoffs.",
        ),
        fallback_options=[
            FallbackOption(
                option_id="general-fallback-async",
                description="Switch to an asynchronous checklist review if no shared block remains.",
                tradeoffs=["Lower alignment quality", "Requires manual confirmation later"],
            )
        ],
    )
    actions = [
        ProposedAction(
            action_id="general-review",
            action_type="checklist_review",
            description="Approve the coordination checklist before any external follow-up occurs.",
            target=block.time_block,
            approval_status="pending",
            execution_mode="inert_until_approved",
        )
    ]
    return plan, [], [AlternativeRecord(label="Async fallback", description="Handle follow-up as a checklist if the block disappears.", confidence=0.61)], actions, ["Detected a general coordination request and switched to low-friction schedule alignment mode."]


class AssistantPlanningEngine:
    def build_response(
        self,
        *,
        query: str,
        household_id: str,
        intent: AssistantIntent,
        repeat_window_days: int,
        fitness_goal: str | None,
        state: HouseholdState | None,
    ) -> AssistantResponse:
        effective_state = state if state is not None else _fallback_household_state(household_id)
        events = _state_events(effective_state)
        reference_time = _resolve_reference_time(effective_state)
        artifact_traces = _load_artifact_summary()

        if intent.intent_type == "appointment":
            plan, conflicts, alternatives, proposed_actions, reasoning = _build_appointment_plan(intent, events, reference_time)
        elif intent.intent_type == "meal":
            plan, conflicts, alternatives, proposed_actions, reasoning = _build_meal_plan(intent, repeat_window_days)
        elif intent.intent_type == "fitness":
            plan, conflicts, alternatives, proposed_actions, reasoning = _build_fitness_plan(intent, events, reference_time, fitness_goal)
        else:
            plan, conflicts, alternatives, proposed_actions, reasoning = _build_general_plan(events, reference_time)

        reasoning_trace = [
            f"Normalized request into {intent.intent_type} intent with priority {intent.priority}.",
            f"Schedule analysis reviewed {len(events)} read-only calendar events.",
            *reasoning,
            *artifact_traces,
            "Generated inert proposed_actions that require explicit approval before any external follow-up.",
        ]

        request_id = _request_id(query, household_id, repeat_window_days, fitness_goal)
        return AssistantResponse(
            request_id=request_id,
            intent=intent,
            plan=plan,
            conflicts=conflicts,
            alternatives=alternatives,
            proposed_actions=proposed_actions,
            reasoning_trace=reasoning_trace,
        )


def generate_assistant_core_report() -> dict[str, Any]:
    engine = AssistantPlanningEngine()
    sample_queries = [
        ("Schedule a doctor appointment for Monday morning after school drop-off", None),
        ("Plan dinners for this week without repeating last week's meals", None),
        ("Build a fat loss workout plan around the family schedule", "fat loss"),
    ]
    outputs = []
    for query, goal in sample_queries:
        intent = parse_intent(query)
        outputs.append(
            engine.build_response(
                query=query,
                household_id="household-001",
                intent=intent,
                repeat_window_days=10,
                fitness_goal=goal,
                state=_fallback_household_state("household-001"),
            ).model_dump()
        )

    report = {
        "artifact": "assistant_core_report.json",
        "status": "generated",
        "module": "assistant_core",
        "deterministic": True,
        "approval_gate_enforced": True,
        "side_effects": "none",
        "endpoints": [
            "/assistant/query",
            "/assistant/suggestions/{request_id}",
            "/assistant/approve",
        ],
        "sample_request_ids": [item["request_id"] for item in outputs],
        "intent_types": [item["intent"]["intent_type"] for item in outputs],
    }
    path = REPO_ROOT / "assistant_core_report.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report