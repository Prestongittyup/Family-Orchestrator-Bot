from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from apps.api.integration_core.models.household_state import HouseholdState
from apps.assistant_core.planning_engine import _request_id
from assistant.governance.intent_router import IntentRouter, RoutingDecision, RoutingCase
from assistant.state.life_state_model import LifeStateModel
from apps.api.observability.execution_trace import trace_function
from household_os.core.contracts import HouseholdOSRunResponse
from household_os.core.decision_engine import HouseholdOSDecisionEngine
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.runtime.action_pipeline import ActionPipeline, LifecycleAction
from household_os.runtime.trigger_detector import RuntimeTrigger, TriggerDetector


class RuntimeTickResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    household_id: str
    detected_triggers: list[RuntimeTrigger] = Field(default_factory=list)
    processed_trigger: RuntimeTrigger | None = None
    response: HouseholdOSRunResponse | None = None
    action_record: LifecycleAction | None = None
    executed_actions: list[LifecycleAction] = Field(default_factory=list)
    graph_state_version: int = 0
    routing_case: str | None = None  # "high_confidence" | "medium_confidence" | "low_confidence"
    clarification_text: str | None = None  # set only on low-confidence routing
    secondary_suggestions: list[dict[str, Any]] = Field(default_factory=list)


class RuntimeApprovalResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    household_id: str
    request_id: str
    response: HouseholdOSRunResponse | None = None
    approved_actions: list[LifecycleAction] = Field(default_factory=list)
    executed_actions: list[LifecycleAction] = Field(default_factory=list)


class HouseholdOSOrchestrator:
    def __init__(
        self,
        *,
        state_store: HouseholdStateGraphStore | None = None,
        decision_engine: HouseholdOSDecisionEngine | None = None,
        trigger_detector: TriggerDetector | None = None,
        action_pipeline: ActionPipeline | None = None,
    ) -> None:
        self.state_store = state_store or HouseholdStateGraphStore()
        self.decision_engine = decision_engine or HouseholdOSDecisionEngine()
        self.trigger_detector = trigger_detector or TriggerDetector()
        self.action_pipeline = action_pipeline or ActionPipeline()
        self.life_state_model = LifeStateModel()

    @trace_function(entrypoint="orchestrator.tick", actor_type="system_worker", source="orchestrator")
    def tick(
        self,
        *,
        household_id: str,
        state: HouseholdState | None = None,
        user_input: str | None = None,
        fitness_goal: str | None = None,
        now: str | datetime | None = None,
    ) -> RuntimeTickResult:
        graph = self._prepare_graph(
            household_id=household_id,
            state=state,
            user_input=user_input,
            fitness_goal=fitness_goal,
        )
        timestamp = self._coerce_datetime(now or graph.get("reference_time"))
        triggers = self.trigger_detector.detect(
            household_id=household_id,
            graph=graph,
            user_input=user_input,
            now=timestamp,
        )
        processed = self._select_trigger(triggers)

        if processed is None:
            self.state_store.save_graph(graph)
            return RuntimeTickResult(
                household_id=household_id,
                detected_triggers=triggers,
                graph_state_version=int(graph.get("state_version", 0)),
            )

        runtime = graph.setdefault("runtime", {})
        runtime.setdefault("processed_trigger_ids", []).append(processed.trigger_id)
        runtime["last_processed_state_version"] = int(graph.get("state_version", 0))

        if processed.trigger_type == "TIME_TICK":
            segment = str(processed.metadata.get("segment", ""))
            if segment:
                runtime.setdefault("last_time_tick", {})[segment] = timestamp.date().isoformat()

        if processed.trigger_type == "APPROVAL_PENDING_TIMEOUT":
            self.action_pipeline.reject_action_timeout(graph=graph, trigger=processed, now=timestamp)
            self.state_store.save_graph(graph)
            return RuntimeTickResult(
                household_id=household_id,
                detected_triggers=triggers,
                processed_trigger=processed,
                graph_state_version=int(graph.get("state_version", 0)),
            )

        query = self._query_for_trigger(graph=graph, trigger=processed)
        request_id = _request_id(query, household_id, 10, fitness_goal)

        # ----------------------------------------------------------------
        # Intent Router: classify intent, apply confidence gating, constrain
        # domain space BEFORE the decision engine sees any candidates.
        # ----------------------------------------------------------------
        routing: RoutingDecision | None = None
        life_state = self.life_state_model.load(household_id)
        if user_input:
            routing = IntentRouter.route_message(user_input, life_state=life_state)

            # Case C — low confidence: block execution, return clarification
            if routing.routing_case == RoutingCase.LOW_CONFIDENCE:
                self.state_store.save_graph(graph)
                self.life_state_model.update_after_run(
                    household_id=household_id,
                    graph=graph,
                    classification=routing.classification,
                    timestamp=timestamp,
                )
                return RuntimeTickResult(
                    household_id=household_id,
                    detected_triggers=triggers,
                    processed_trigger=processed,
                    graph_state_version=int(graph.get("state_version", 0)),
                    routing_case=routing.routing_case.value,
                    clarification_text=routing.clarification_text,
                )

        allowed_domains = routing.allowed_domains if routing else None

        response = self.decision_engine.run(
            household_id=household_id,
            query=query,
            graph=graph,
            request_id=request_id,
            allowed_domains=allowed_domains,
        )
        action_record = self.action_pipeline.register_proposed_action(
            graph=graph,
            trigger=processed,
            response=response,
            now=timestamp,
        )
        self._store_response_in_graph(graph=graph, response=response, timestamp=timestamp)
        self._consume_follow_up_if_used(graph=graph, trigger=processed, query=query)
        self.state_store.save_graph(graph)
        self.life_state_model.update_after_run(
            household_id=household_id,
            graph=graph,
            classification=routing.classification if routing else None,
            timestamp=timestamp,
        )

        # Build secondary suggestions for multi-intent routing
        secondary_suggestions: list[dict[str, Any]] = []
        if routing and routing.is_multi_intent:
            raw = IntentRouter.build_secondary_suggestions(
                secondary_intents=routing.classification.secondary_intents,
                graph=graph,
                life_state=life_state,
            )
            secondary_suggestions = [
                {
                    "intent": s.intent.value,
                    "domain": s.domain,
                    "title": s.title,
                    "description": s.description,
                    "why": s.why,
                }
                for s in raw
            ]

        return RuntimeTickResult(
            household_id=household_id,
            detected_triggers=triggers,
            processed_trigger=processed,
            response=response,
            action_record=action_record,
            graph_state_version=int(graph.get("state_version", 0)),
            routing_case=routing.routing_case.value if routing else None,
            secondary_suggestions=secondary_suggestions,
        )

    @trace_function(entrypoint="orchestrator.approve_and_execute", actor_type="system_worker", source="orchestrator")
    def approve_and_execute(
        self,
        *,
        household_id: str,
        request_id: str,
        action_ids: list[str],
        now: str | datetime | None = None,
    ) -> RuntimeApprovalResult:
        graph = self.state_store.load_graph(household_id)
        timestamp = self._coerce_datetime(now or graph.get("reference_time"))
        approved_actions = self.action_pipeline.approve_actions(
            graph=graph,
            request_id=request_id,
            action_ids=action_ids,
            now=timestamp,
        )
        executed_actions = self.action_pipeline.execute_approved_actions(graph=graph, now=timestamp)
        self._mark_response_approved(graph=graph, request_id=request_id, action_ids=action_ids, executed_actions=executed_actions)
        self.state_store.save_graph(graph)
        self.life_state_model.update_after_approval(
            household_id=household_id,
            graph=graph,
            timestamp=timestamp,
        )

        payload = graph.get("responses", {}).get(request_id)
        response = None if payload is None else HouseholdOSRunResponse.model_validate(payload)
        return RuntimeApprovalResult(
            household_id=household_id,
            request_id=request_id,
            response=response,
            approved_actions=approved_actions,
            executed_actions=executed_actions,
        )

    def _prepare_graph(
        self,
        *,
        household_id: str,
        state: HouseholdState | None,
        user_input: str | None,
        fitness_goal: str | None,
    ) -> dict[str, Any]:
        if state is None:
            return self.state_store.load_graph(household_id)
        return self.state_store.refresh_graph(
            household_id=household_id,
            state=state,
            query=user_input or "runtime_tick",
            fitness_goal=fitness_goal,
        )

    def _query_for_trigger(self, *, graph: dict[str, Any], trigger: RuntimeTrigger) -> str:
        if trigger.trigger_type == "USER_INPUT":
            return str(trigger.metadata.get("query", "Review household coordination"))

        if trigger.trigger_type == "TIME_TICK":
            segment = str(trigger.metadata.get("segment", ""))
            pending_follow_ups = graph.get("runtime", {}).get("daily_cycle", {}).get("pending_follow_up_queries", [])
            if segment == "morning":
                for item in pending_follow_ups:
                    if item.get("due_on") == self._coerce_datetime(trigger.detected_at).date().isoformat():
                        return str(item.get("query", "Plan today with appointments, meals, and a workout around the family schedule"))
                return "Plan today with appointments, meals, and a workout around the family schedule"
            return "Review today's outcomes and adjust tomorrow's plan"

        return "Review household changes and recommend the next coordination step"

    def _store_response_in_graph(
        self,
        *,
        graph: dict[str, Any],
        response: HouseholdOSRunResponse,
        timestamp: datetime,
    ) -> None:
        graph.setdefault("responses", {})[response.request_id] = response.model_dump()
        graph.setdefault("approval_actions", []).append(
            {
                "request_id": response.request_id,
                "action_id": response.recommended_action.action_id,
                "approval_status": response.recommended_action.approval_status,
            }
        )
        graph.setdefault("event_history", []).append(
            {
                "event_type": "response_emitted",
                "request_id": response.request_id,
                "recorded_at": self._iso(timestamp),
            }
        )

    def _mark_response_approved(
        self,
        *,
        graph: dict[str, Any],
        request_id: str,
        action_ids: list[str],
        executed_actions: list[LifecycleAction],
    ) -> None:
        payload = graph.get("responses", {}).get(request_id)
        if payload is None:
            return

        requested = set(action_ids)
        recommended = dict(payload.get("recommended_action", {}))
        if recommended.get("action_id") in requested:
            recommended["approval_status"] = "approved"
        approval_payload = dict(payload.get("grouped_approval_payload", {}))
        if requested.intersection(set(approval_payload.get("action_ids", []))):
            approval_payload["approval_status"] = "approved"

        reasoning_trace = list(payload.get("reasoning_trace", []))
        for action in executed_actions:
            reasoning_trace.append(f"Action executed via {action.execution_handler}.")

        payload["recommended_action"] = recommended
        payload["grouped_approval_payload"] = approval_payload
        payload["reasoning_trace"] = reasoning_trace[:8]
        graph.setdefault("responses", {})[request_id] = payload

        for approval_action in graph.get("approval_actions", []):
            if approval_action.get("request_id") == request_id and approval_action.get("action_id") in requested:
                approval_action["approval_status"] = "approved"

    def _consume_follow_up_if_used(self, *, graph: dict[str, Any], trigger: RuntimeTrigger, query: str) -> None:
        if trigger.trigger_type != "TIME_TICK":
            return
        daily_cycle = graph.get("runtime", {}).get("daily_cycle", {})
        follow_ups = list(daily_cycle.get("pending_follow_up_queries", []))
        daily_cycle["pending_follow_up_queries"] = [item for item in follow_ups if item.get("query") != query]

    def _select_trigger(self, triggers: list[RuntimeTrigger]) -> RuntimeTrigger | None:
        priority = {
            "USER_INPUT": 0,
            "APPROVAL_PENDING_TIMEOUT": 1,
            "TIME_TICK": 2,
            "STATE_CHANGE": 3,
        }
        if not triggers:
            return None
        return sorted(triggers, key=lambda item: (priority[item.trigger_type], item.trigger_id))[0]

    def _coerce_datetime(self, value: str | datetime | None) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)
        if isinstance(value, str) and value:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        return datetime.now(UTC)

    def _iso(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
