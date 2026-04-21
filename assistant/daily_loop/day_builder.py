from __future__ import annotations

from datetime import date, datetime

from assistant.contracts.assistant_plan import AssistantPlan as RuntimeAssistantPlan
from assistant.contracts.assistant_plan import AssistantProposal, MergedConflict, RankedPlanItem
from assistant.daily_loop.contracts import (
    DailyApprovalState,
    DailyConflict,
    DailyMeal,
    DailyPlan,
    DailyScheduleItem,
    DailyWorkout,
)
from assistant.daily_loop.time_slicer import (
    DEFAULT_BUFFER_MINUTES,
    SEGMENT_ORDER,
    allocate_time_block,
    detect_scheduling_gaps,
    parse_iso_datetime,
    parse_runtime_time_block,
    resolve_segment,
    to_iso_z,
    to_time_block,
)


class DayBuilder:
    def build(
        self,
        runtime_plan: RuntimeAssistantPlan,
        *,
        target_date: str | None = None,
        persisted: bool = False,
    ) -> DailyPlan:
        effective_date = target_date or self._resolve_target_date(runtime_plan)
        schedule, base_conflicts = self._build_calendar_constraints(runtime_plan, effective_date)
        conflicts = [*self._runtime_conflicts(runtime_plan.conflicts), *base_conflicts]
        meals: list[DailyMeal] = []
        workouts: list[DailyWorkout] = []

        for proposal in self._ordered_proposals(runtime_plan):
            proposal_items, proposal_conflicts = self._place_proposal(
                proposal,
                effective_date,
                existing_schedule=schedule,
            )
            schedule.extend(proposal_items)
            conflicts.extend(proposal_conflicts)
            meals.extend(self._meal_records(proposal_items, proposal))
            workouts.extend(self._workout_records(proposal_items, proposal))

        ordered_schedule = self._sort_schedule(schedule)
        gaps = detect_scheduling_gaps(ordered_schedule, target_date=date.fromisoformat(effective_date))
        return DailyPlan(
            date=effective_date,
            schedule=ordered_schedule,
            meals=meals,
            workouts=workouts,
            conflicts=self._sort_conflicts(conflicts),
            gaps=gaps,
            approval_state=DailyApprovalState(
                request_id=runtime_plan.request_id,
                approval_endpoint=runtime_plan.execution_payload.approval_endpoint,
                requires_approval=runtime_plan.requires_approval,
                approved=runtime_plan.execution_payload.approved,
                persisted=persisted,
                proposed_actions=[action.model_copy() for action in runtime_plan.execution_payload.proposed_actions],
            ),
        )

    def _resolve_target_date(self, runtime_plan: RuntimeAssistantPlan) -> str:
        reference_time = str(runtime_plan.state_snapshot.household_context.get("reference_time", ""))
        if reference_time:
            return reference_time[:10]
        for event in runtime_plan.state_snapshot.calendar_events:
            return event.start[:10]
        return datetime.utcnow().date().isoformat()

    def _ordered_proposals(self, runtime_plan: RuntimeAssistantPlan) -> list[AssistantProposal]:
        proposal_map = {proposal.proposal_id: proposal for proposal in runtime_plan.proposals}
        ranked_order: list[AssistantProposal] = []
        for item in runtime_plan.ranked_plan:
            proposal = proposal_map.pop(item.proposal_id, None)
            if proposal is not None:
                ranked_order.append(proposal)
        ranked_order.extend(sorted(proposal_map.values(), key=lambda proposal: proposal.proposal_id))
        return ranked_order

    def _runtime_conflicts(self, conflicts: list[MergedConflict]) -> list[DailyConflict]:
        return [
            DailyConflict(
                conflict_type=conflict.conflict_type,
                severity=conflict.severity,
                description=conflict.description,
                impacted_items=list(conflict.impacted_proposals),
            )
            for conflict in conflicts
        ]

    def _build_calendar_constraints(
        self,
        runtime_plan: RuntimeAssistantPlan,
        target_date: str,
    ) -> tuple[list[DailyScheduleItem], list[DailyConflict]]:
        events = sorted(
            [event for event in runtime_plan.state_snapshot.calendar_events if event.start[:10] == target_date],
            key=lambda event: (event.start, event.title, event.event_id),
        )
        schedule: list[DailyScheduleItem] = []
        conflicts: list[DailyConflict] = []

        if not events:
            return schedule, conflicts

        current_group = [events[0]]
        current_start = parse_iso_datetime(events[0].start)
        current_end = parse_iso_datetime(events[0].end)

        for event in events[1:]:
            event_start = parse_iso_datetime(event.start)
            event_end = parse_iso_datetime(event.end)
            if event_start < current_end:
                current_group.append(event)
                current_end = max(current_end, event_end)
                continue

            if len(current_group) > 1:
                conflicts.append(
                    DailyConflict(
                        conflict_type="calendar_constraint_overlap",
                        severity="medium",
                        description="Existing calendar constraints were merged into one protected block to preserve a conflict-free daily schedule.",
                        impacted_items=[item.event_id for item in current_group],
                    )
                )
            schedule.append(self._calendar_item(current_group, current_start, current_end))
            current_group = [event]
            current_start = event_start
            current_end = event_end

        if len(current_group) > 1:
            conflicts.append(
                DailyConflict(
                    conflict_type="calendar_constraint_overlap",
                    severity="medium",
                    description="Existing calendar constraints were merged into one protected block to preserve a conflict-free daily schedule.",
                    impacted_items=[item.event_id for item in current_group],
                )
            )
        schedule.append(self._calendar_item(current_group, current_start, current_end))
        return schedule, conflicts

    def _calendar_item(
        self,
        events: list,
        start_dt: datetime,
        end_dt: datetime,
    ) -> DailyScheduleItem:
        title = events[0].title if len(events) == 1 else "Calendar commitments: " + ", ".join(event.title for event in events)
        return DailyScheduleItem(
            item_id="calendar-" + "-".join(event.event_id for event in events),
            title=title,
            domain="calendar",
            segment=resolve_segment(start_dt, end_dt),
            start=to_iso_z(start_dt),
            end=to_iso_z(end_dt),
            time_block=to_time_block(start_dt, end_dt),
            locked=True,
            rationale="Existing calendar constraint preserved from the runtime state snapshot.",
            buffer_before_minutes=DEFAULT_BUFFER_MINUTES,
            buffer_after_minutes=DEFAULT_BUFFER_MINUTES,
        )

    def _place_proposal(
        self,
        proposal: AssistantProposal,
        target_date: str,
        *,
        existing_schedule: list[DailyScheduleItem],
    ) -> tuple[list[DailyScheduleItem], list[DailyConflict]]:
        placed_items: list[DailyScheduleItem] = []
        conflicts: list[DailyConflict] = []
        blocks = []
        for time_block in proposal.time_blocks:
            parsed = parse_runtime_time_block(time_block)
            if parsed is None:
                continue
            if parsed[0].date().isoformat() != target_date:
                continue
            blocks.append(parsed)
        blocks.sort(key=lambda item: (item[0], item[1]))

        if not blocks and proposal.time_blocks:
            conflicts.append(
                DailyConflict(
                    conflict_type="out_of_day_scope",
                    severity="low",
                    description=f"{proposal.title} was suggested outside the current daily loop date and was not placed into today's schedule.",
                    impacted_items=[proposal.proposal_id],
                )
            )
            return placed_items, conflicts

        for index, (desired_start, desired_end) in enumerate(blocks, start=1):
            segment = resolve_segment(desired_start, desired_end)
            allocated = allocate_time_block(
                [*existing_schedule, *placed_items],
                desired_start,
                desired_end,
                segment=segment,
                buffer_before_minutes=DEFAULT_BUFFER_MINUTES,
                buffer_after_minutes=DEFAULT_BUFFER_MINUTES,
            )
            if allocated is None:
                conflicts.append(
                    DailyConflict(
                        conflict_type="placement_conflict",
                        severity="medium",
                        description=f"{proposal.title} could not be placed into the {segment} segment without overlapping existing commitments or buffers.",
                        impacted_items=[proposal.proposal_id],
                    )
                )
                continue

            allocated_start, allocated_end = allocated
            placed_items.append(
                DailyScheduleItem(
                    item_id=f"{proposal.proposal_id}-{index}",
                    title=proposal.title,
                    domain=proposal.domain,
                    segment=resolve_segment(allocated_start, allocated_end),
                    start=to_iso_z(allocated_start),
                    end=to_iso_z(allocated_end),
                    time_block=to_time_block(allocated_start, allocated_end),
                    locked=False,
                    rationale=proposal.rationale,
                    buffer_before_minutes=DEFAULT_BUFFER_MINUTES,
                    buffer_after_minutes=DEFAULT_BUFFER_MINUTES,
                    source_proposal_id=proposal.proposal_id,
                )
            )

        return placed_items, conflicts

    def _meal_records(self, items: list[DailyScheduleItem], proposal: AssistantProposal) -> list[DailyMeal]:
        if proposal.domain != "meal":
            return []
        meal_type = str(proposal.details.get("meal_plan", {}).get("meal_type", "meal"))
        return [
            DailyMeal(
                item_id=item.item_id,
                title=item.title,
                meal_type=meal_type,
                segment=item.segment,
                time_block=item.time_block,
                source_proposal_id=proposal.proposal_id,
            )
            for item in items
        ]

    def _workout_records(self, items: list[DailyScheduleItem], proposal: AssistantProposal) -> list[DailyWorkout]:
        if proposal.domain != "fitness":
            return []
        session_map = {
            str(session.get("time_block", "")): str(session.get("focus", "workout"))
            for session in proposal.details.get("fitness_plan", {}).get("sessions", [])
        }
        default_focus = str(proposal.details.get("fitness_plan", {}).get("goal", "workout"))
        workouts: list[DailyWorkout] = []
        for item in items:
            workouts.append(
                DailyWorkout(
                    item_id=item.item_id,
                    title=item.title,
                    focus=session_map.get(item.time_block, default_focus),
                    segment=item.segment,
                    time_block=item.time_block,
                    source_proposal_id=proposal.proposal_id,
                )
            )
        return workouts

    def _sort_schedule(self, schedule: list[DailyScheduleItem]) -> list[DailyScheduleItem]:
        return sorted(
            schedule,
            key=lambda item: (
                parse_iso_datetime(item.start),
                SEGMENT_ORDER[item.segment],
                0 if item.locked else 1,
                item.domain,
                item.item_id,
            ),
        )

    def _sort_conflicts(self, conflicts: list[DailyConflict]) -> list[DailyConflict]:
        severity_order = {"high": 0, "medium": 1, "low": 2}
        return sorted(
            conflicts,
            key=lambda item: (
                severity_order.get(item.severity, 99),
                item.conflict_type,
                item.description,
                tuple(item.impacted_items),
            ),
        )