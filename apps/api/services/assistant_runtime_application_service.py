from __future__ import annotations

from typing import Any

from apps.assistant_core.planning_engine import _fallback_household_state
from assistant.governance.output_governor import OutputGovernor
from household_os.core import HouseholdOSRunResponse
from household_os.core.lifecycle_state import LifecycleState, enforce_boundary_state
from household_os.presentation.humanizer import RecommendationHumanizer
from household_os.presentation.lifecycle_presentation_mapper import LifecyclePresentationMapper
from household_os.presentation.recommendation_builder import RecommendationBuilder
from household_os.runtime.orchestrator import HouseholdOSOrchestrator
from household_os.runtime.orchestrator import OrchestratorRequest, RequestActionType


class AssistantRuntimeApplicationService:
    """Application service that encapsulates assistant runtime orchestration calls."""

    def __init__(
        self,
        runtime_orchestrator: HouseholdOSOrchestrator | None = None,
        recommendation_builder: RecommendationBuilder | None = None,
        recommendation_humanizer: RecommendationHumanizer | None = None,
        output_governor: OutputGovernor | None = None,
    ) -> None:
        self._runtime_orchestrator = runtime_orchestrator or HouseholdOSOrchestrator()
        self._recommendation_builder = recommendation_builder or RecommendationBuilder()
        self._recommendation_humanizer = recommendation_humanizer or RecommendationHumanizer()
        self._output_governor = output_governor or OutputGovernor()

    def run_assistant(
        self,
        *,
        household_id: str,
        message: str | None,
        query: str | None,
        fitness_goal: str | None,
        raw_actor: dict[str, Any],
    ) -> dict[str, Any]:
        if query and not message:
            return {
                "kind": "legacy",
                "response": self._run_legacy_household_os(
                    household_id=household_id,
                    message=message,
                    query=query,
                    fitness_goal=fitness_goal,
                    raw_actor=raw_actor,
                ),
            }

        state = _fallback_household_state(household_id)
        resolved_message = message or query or ""
        result = self._runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.RUN,
                household_id=household_id,
                actor=raw_actor,
                state=state,
                user_input=resolved_message,
                fitness_goal=fitness_goal,
                context=self._request_context_from_actor(raw_actor),
            )
        )

        if result.clarification_text:
            return {
                "kind": "clarification",
                "clarification": result.clarification_text,
                "routing_case": "low_confidence",
            }

        response = result.response
        action = result.action_record
        if response is None or action is None:
            raise RuntimeError("Orchestrator did not emit an action")

        enrichment_graph = self._runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.READ_SENSITIVE_STATE,
                household_id=household_id,
                actor=raw_actor,
                resource_type="recommendation_enrichment",
                context=self._request_context_from_actor(raw_actor),
            )
        )
        enriched = self._recommendation_builder.build(response=response, graph=enrichment_graph)

        humanization_graph = self._runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.READ_SENSITIVE_STATE,
                household_id=household_id,
                actor=raw_actor,
                resource_type="recommendation_humanization",
                context=self._request_context_from_actor(raw_actor),
            )
        )
        humanized = self._recommendation_humanizer.humanize(
            enriched.as_dict(),
            reference_time=humanization_graph.get("reference_time"),
        )
        governed = self._output_governor.govern(
            user_message=resolved_message,
            payload=humanized.as_dict(),
            decision_response=response,
        )

        return {
            "kind": "action",
            "action_id": governed.action_id,
            "recommendation": governed.recommendation,
            "why": governed.why,
            "impact": governed.impact,
            "approval_required": governed.approval_required,
            "routing_case": result.routing_case or "high_confidence",
            "secondary_suggestions": result.secondary_suggestions,
        }

    def approve_action(
        self,
        *,
        household_id: str,
        action_id: str,
        raw_actor: dict[str, Any],
    ) -> dict[str, Any]:
        graph = self._runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.READ_SENSITIVE_STATE,
                household_id=household_id,
                actor=raw_actor,
                resource_type="action_lifecycle",
                context=self._request_context_from_actor(raw_actor),
            )
        )
        action_payload = graph.get("action_lifecycle", {}).get("actions", {}).get(action_id)
        if action_payload is None:
            raise LookupError("Action not found")

        request_id = str(action_payload.get("request_id", ""))
        if not request_id:
            raise ValueError("Action is missing request association")

        approval_result = self._runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.APPROVE,
                household_id=household_id,
                actor=raw_actor,
                request_id=request_id,
                action_ids=[action_id],
                context=self._request_context_from_actor(raw_actor),
            )
        )
        effects = [
            {
                "action_id": action.action_id,
                "handler": action.execution_result.get("handler") if action.execution_result else None,
                "result": action.execution_result or {},
            }
            for action in approval_result.executed_actions
        ]
        return {
            "status": LifecyclePresentationMapper.to_api_state(LifecycleState.COMMITTED),
            "effects": effects,
        }

    def reject_action(
        self,
        *,
        household_id: str,
        action_id: str,
        raw_actor: dict[str, Any],
    ) -> dict[str, Any]:
        graph = self._runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.READ_SENSITIVE_STATE,
                household_id=household_id,
                actor=raw_actor,
                resource_type="action_lifecycle",
                context=self._request_context_from_actor(raw_actor),
            )
        )
        action_payload = graph.get("action_lifecycle", {}).get("actions", {}).get(action_id)
        if action_payload is None:
            raise LookupError("Action not found")

        request_id = str(action_payload.get("request_id", ""))
        if not request_id:
            raise ValueError("Action is missing request association")

        rejected = self._runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.REJECT,
                household_id=household_id,
                actor=raw_actor,
                request_id=request_id,
                action_ids=[action_id],
                now=graph.get("reference_time"),
                context=self._request_context_from_actor(raw_actor),
            )
        )
        if not rejected:
            raise RuntimeError("Action could not be rejected")

        return {
            "status": LifecyclePresentationMapper.to_api_state(LifecycleState.REJECTED),
        }

    def execute_action(
        self,
        *,
        household_id: str,
        action_id: str,
        raw_actor: dict[str, Any],
    ) -> dict[str, Any]:
        graph = self._runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.READ_SENSITIVE_STATE,
                household_id=household_id,
                actor=raw_actor,
                resource_type="action_lifecycle",
                context=self._request_context_from_actor(raw_actor),
            )
        )
        action_payload = graph.get("action_lifecycle", {}).get("actions", {}).get(action_id)
        if action_payload is None:
            raise LookupError("Action not found")

        executed = self._runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.EXECUTE,
                household_id=household_id,
                actor=raw_actor,
                now=graph.get("reference_time"),
                context=self._request_context_from_actor(raw_actor),
            )
        )
        effects = [
            {
                "action_id": action.action_id,
                "handler": action.execution_result.get("handler") if action.execution_result else None,
                "result": action.execution_result or {},
            }
            for action in executed
        ]
        return {
            "status": LifecyclePresentationMapper.to_api_state(LifecycleState.COMMITTED),
            "effects": effects,
        }

    def get_today_summary(
        self,
        *,
        household_id: str,
        raw_actor: dict[str, Any],
    ) -> dict[str, Any]:
        graph = self._runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.READ_SENSITIVE_STATE,
                household_id=household_id,
                actor=raw_actor,
                resource_type="assistant_today",
                context=self._request_context_from_actor(raw_actor),
            )
        )

        actions = graph.get("action_lifecycle", {}).get("actions", {})
        pending_actions = []
        for action in actions.values():
            state = enforce_boundary_state(action.get("current_state"))
            if state in {
                LifecycleState.PROPOSED,
                LifecycleState.PENDING_APPROVAL,
                LifecycleState.APPROVED,
            }:
                pending_actions.append(
                    {
                        "action_id": action.get("action_id"),
                        "title": action.get("title"),
                        "state": LifecyclePresentationMapper.to_api_state(state),
                        "approval_required": bool(action.get("approval_required", True)),
                    }
                )
        pending_actions.sort(key=lambda item: str(item.get("action_id", "")))

        last_recommendation = None
        responses = graph.get("responses", {})
        if responses:
            latest_key = sorted(responses.keys())[-1]
            payload = responses.get(latest_key, {})
            recommendation = payload.get("recommended_action", {})
            last_recommendation = {
                "action_id": recommendation.get("action_id"),
                "recommendation": recommendation.get("title"),
                "approval_status": recommendation.get("approval_status"),
            }

        return {
            "household_id": household_id,
            "events": list(graph.get("calendar_events", [])),
            "pending_actions": pending_actions,
            "last_recommendation": last_recommendation,
        }

    def _run_legacy_household_os(
        self,
        *,
        household_id: str,
        message: str | None,
        query: str | None,
        fitness_goal: str | None,
        raw_actor: dict[str, Any],
    ) -> HouseholdOSRunResponse:
        resolved_query = query or message or ""
        state = _fallback_household_state(household_id)
        result = self._runtime_orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.LEGACY_EXECUTION,
                household_id=household_id,
                actor=raw_actor,
                state=state,
                user_input=resolved_query,
                fitness_goal=fitness_goal,
                context={"legacy_execution": True, **self._request_context_from_actor(raw_actor)},
            )
        )
        if result.response is None:
            raise RuntimeError("Legacy execution did not produce a response")
        return result.response

    @staticmethod
    def _request_context_from_actor(raw_actor: dict[str, Any]) -> dict[str, Any]:
        actor_type = str(raw_actor.get("actor_type") or "").strip().lower()
        return {
            "system_worker_verified": actor_type in {"system_worker", "scheduler"},
            "auth_scope": str(raw_actor.get("auth_scope") or "household"),
        }
