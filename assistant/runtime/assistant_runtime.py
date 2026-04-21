from __future__ import annotations

from dataclasses import dataclass

from apps.api.integration_core.models.household_state import HouseholdState
from apps.assistant_core.contracts import AssistantResponse, ConflictRecord
from apps.assistant_core.intent_parser import parse_intent
from apps.assistant_core.planning_engine import (
    AssistantPlanningEngine,
    _build_appointment_plan,
    _build_fitness_plan,
    _build_general_plan,
    _build_meal_plan,
    _fallback_household_state,
    _request_id,
    _resolve_reference_time,
    _state_events,
)
from assistant.contracts.assistant_plan import (
    AssistantPlan,
    AssistantProposal,
    ExecutionPayload,
    MergedConflict,
)
from assistant.planning.plan_merger import PlanMerger
from assistant.state.state_snapshot import StateSnapshotService
from household_state.contracts import HouseholdDecisionResponse
from household_state.decision_engine import HouseholdDecisionEngine
from household_state.household_state_manager import HouseholdStateManager


@dataclass(frozen=True)
class RuntimeResult:
    plan: AssistantPlan
    approval_response: AssistantResponse
    decision_response: HouseholdDecisionResponse


def _domain_proposal(plan_domain: str, plan, *, proposal_id: str) -> AssistantProposal:
    details: dict[str, object] = {
        "summary": plan.summary,
        "candidate_count": len(plan.candidate_schedules),
        "fallback_count": len(plan.fallback_options),
    }
    if plan.meal_plan is not None:
        details["meal_plan"] = plan.meal_plan.model_dump()
    if plan.fitness_plan is not None:
        details["fitness_plan"] = plan.fitness_plan.model_dump()
    return AssistantProposal(
        proposal_id=proposal_id,
        domain=plan_domain,
        title=plan.recommended_plan.summary,
        summary=plan.summary,
        confidence=plan.recommended_plan.confidence,
        rationale=plan.recommended_plan.reasoning,
        time_blocks=[block.time_block for block in plan.recommended_plan.timeline_blocks],
        details=details,
    )


def _as_merged_conflict(conflict: ConflictRecord, proposal_id: str) -> MergedConflict:
    return MergedConflict(
        conflict_type=conflict.conflict_type,
        severity=conflict.severity,
        description=conflict.description,
        impacted_proposals=[proposal_id],
    )


def _select_domains(query: str, intent_type: str) -> list[str]:
    normalized = " ".join(query.strip().lower().split())
    domains: list[str] = []

    if intent_type == "appointment" or any(token in normalized for token in ("appointment", "doctor", "meeting", "schedule")):
        domains.append("calendar")
    if intent_type == "meal" or any(token in normalized for token in ("meal", "dinner", "lunch", "breakfast", "cook", "recipe", "grocery")):
        domains.append("meal")
    if intent_type == "fitness" or any(token in normalized for token in ("fitness", "workout", "exercise", "gym", "run", "strength", "cardio")):
        domains.append("fitness")
    if any(token in normalized for token in ("household", "family", "today", "plan", "coordinate", "kids", "chores")) or not domains:
        domains.append("household")

    ordered: list[str] = []
    for domain in domains:
        if domain not in ordered:
            ordered.append(domain)
    return ordered


class AssistantRuntimeEngine:
    def __init__(self) -> None:
        self._snapshot_service = StateSnapshotService()
        self._merger = PlanMerger()
        self._planning_engine = AssistantPlanningEngine()
        self._state_manager = HouseholdStateManager()
        self._decision_engine = HouseholdDecisionEngine()

    def run(
        self,
        *,
        query: str,
        household_id: str,
        repeat_window_days: int,
        fitness_goal: str | None,
        state: HouseholdState | None,
    ) -> RuntimeResult:
        effective_state = state if state is not None else _fallback_household_state(household_id)
        request_id = _request_id(query, household_id, repeat_window_days, fitness_goal)
        existing_response = self._state_manager.get_response(household_id, request_id)
        if existing_response is None:
            graph = self._state_manager.refresh_graph(
                household_id=household_id,
                state=effective_state,
                query=query,
                fitness_goal=fitness_goal,
            )
            decision_response = self._decision_engine.decide(
                household_id=household_id,
                query=query,
                graph=graph,
                request_id=request_id,
            )
            self._state_manager.store_decision(household_id, query, decision_response.model_dump())
        else:
            hydrated = HouseholdDecisionResponse.model_validate(existing_response)
            if hydrated.recommended_action.approval_status == "approved":
                graph = self._state_manager.refresh_graph(
                    household_id=household_id,
                    state=effective_state,
                    query=query,
                    fitness_goal=fitness_goal,
                )
                decision_response = self._decision_engine.decide(
                    household_id=household_id,
                    query=query,
                    graph=graph,
                    request_id=request_id,
                )
                self._state_manager.store_decision(household_id, query, decision_response.model_dump())
            else:
                decision_response = hydrated
                if decision_response.current_state_summary.pending_approval_count < 1:
                    decision_response = decision_response.model_copy(
                        update={
                            "current_state_summary": decision_response.current_state_summary.model_copy(
                                update={"pending_approval_count": 1}
                            )
                        }
                    )

        intent = parse_intent(query)
        base_response = self._planning_engine.build_response(
            query=query,
            household_id=household_id,
            intent=intent,
            repeat_window_days=repeat_window_days,
            fitness_goal=fitness_goal,
            state=effective_state,
        )

        state_snapshot = self._snapshot_service.build(effective_state, fitness_goal=fitness_goal)
        events = _state_events(effective_state)
        reference_time = _resolve_reference_time(effective_state)
        selected_domains = _select_domains(query, intent.intent_type)

        proposals = [
            _domain_proposal(base_response.plan.domain, base_response.plan, proposal_id=f"{request_id}-{base_response.plan.domain}"),
        ]
        merged_conflicts = [
            _as_merged_conflict(conflict, proposals[0].proposal_id)
            for conflict in base_response.conflicts
        ]
        all_actions = list(base_response.proposed_actions)
        runtime_reasoning = list(base_response.reasoning_trace)
        runtime_reasoning.append(f"Assistant runtime selected domains: {', '.join(selected_domains)}.")

        existing_domains = {base_response.plan.domain}

        if "calendar" in selected_domains and "appointment" not in existing_domains:
            plan, conflicts, _alternatives, actions, reasoning = _build_appointment_plan(intent, events, reference_time)
            proposal = _domain_proposal("appointment", plan, proposal_id=f"{request_id}-appointment")
            proposals.append(proposal)
            merged_conflicts.extend(_as_merged_conflict(conflict, proposal.proposal_id) for conflict in conflicts)
            all_actions.extend(actions)
            runtime_reasoning.extend(reasoning)
            existing_domains.add("appointment")

        if "meal" in selected_domains and "meal" not in existing_domains:
            plan, conflicts, _alternatives, actions, reasoning = _build_meal_plan(intent, repeat_window_days)
            proposal = _domain_proposal("meal", plan, proposal_id=f"{request_id}-meal")
            proposals.append(proposal)
            merged_conflicts.extend(_as_merged_conflict(conflict, proposal.proposal_id) for conflict in conflicts)
            all_actions.extend(actions)
            runtime_reasoning.extend(reasoning)
            existing_domains.add("meal")

        if "fitness" in selected_domains and "fitness" not in existing_domains:
            plan, conflicts, _alternatives, actions, reasoning = _build_fitness_plan(intent, events, reference_time, fitness_goal)
            proposal = _domain_proposal("fitness", plan, proposal_id=f"{request_id}-fitness")
            proposals.append(proposal)
            merged_conflicts.extend(_as_merged_conflict(conflict, proposal.proposal_id) for conflict in conflicts)
            all_actions.extend(actions)
            runtime_reasoning.extend(reasoning)
            existing_domains.add("fitness")

        if "household" in selected_domains and "general" not in existing_domains:
            plan, conflicts, _alternatives, actions, reasoning = _build_general_plan(events, reference_time)
            proposal = _domain_proposal("general", plan, proposal_id=f"{request_id}-general")
            proposals.append(proposal)
            merged_conflicts.extend(_as_merged_conflict(conflict, proposal.proposal_id) for conflict in conflicts)
            all_actions.extend(actions)
            runtime_reasoning.extend(reasoning)

        unified_conflicts, ranked_plan = self._merger.merge(
            proposals,
            existing_conflicts=merged_conflicts,
            primary_domains=selected_domains,
        )

        plan = AssistantPlan(
            request_id=request_id,
            intent=intent,
            state_snapshot=state_snapshot,
            proposals=proposals,
            conflicts=unified_conflicts,
            ranked_plan=ranked_plan,
            requires_approval=bool(all_actions),
            execution_payload=ExecutionPayload(
                request_id=request_id,
                approval_endpoint="/assistant/approve",
                execution_mode="inert_until_approved",
                approved=False,
                proposed_actions=all_actions,
            ),
        )

        approval_response = base_response.model_copy(
            update={
                "request_id": request_id,
                "conflicts": [
                    conflict.model_copy()
                    for conflict in base_response.conflicts
                ],
                "proposed_actions": all_actions,
                "reasoning_trace": [
                    *runtime_reasoning,
                    "Assistant runtime delegated user-facing reasoning to HouseholdStateManager and HouseholdDecisionEngine.",
                ],
            }
        )
        return RuntimeResult(plan=plan, approval_response=approval_response, decision_response=decision_response)
