from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
import inspect
import logging
from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field

from apps.api.integration_core.models.household_state import HouseholdState
from apps.assistant_core.planning_engine import _request_id
from assistant.governance.intent_router import IntentRouter, RoutingDecision, RoutingCase
from assistant.state.life_state_model import LifeStateModel
from apps.api.observability.execution_trace import trace_function
from household_os.core.contracts import HouseholdOSRunResponse
from household_os.core.decision_engine import HouseholdOSDecisionEngine
from household_os.core.execution_context import ExecutionContext
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.security.authorization_gate import ActorIdentity, AuthorizationGate
from household_os.runtime.action_pipeline import ActionPipeline, LifecycleAction
from household_os.runtime.trigger_detector import RuntimeTrigger, TriggerDetector


logger = logging.getLogger(__name__)


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


class RequestActionType(str, Enum):
    RUN = "RUN"
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    EXECUTE = "EXECUTE"
    READ_SENSITIVE_STATE = "READ_SENSITIVE_STATE"
    WRITE_SENSITIVE_STATE = "WRITE_SENSITIVE_STATE"
    QUEUE_FOLLOW_UPS = "QUEUE_FOLLOW_UPS"
    UPDATE_DAILY_CYCLE_MARKER = "UPDATE_DAILY_CYCLE_MARKER"
    LEGACY_EXECUTION = "LEGACY_EXECUTION"


class OrchestratorRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    action_type: RequestActionType
    household_id: str
    actor: dict[str, Any] | ActorIdentity
    request_id: str | None = None
    action_ids: list[str] = Field(default_factory=list)
    user_input: str | None = None
    state: HouseholdState | None = None
    fitness_goal: str | None = None
    now: str | datetime | None = None
    resource_type: str | None = None
    graph: dict[str, Any] | None = None
    cycle_marker: str | None = None
    cycle_timestamp: str | datetime | None = None
    context: dict[str, Any] = Field(default_factory=dict)


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
        self._pipeline_internal_token = object()
        self.action_pipeline.bind_internal_gate_token(self._pipeline_internal_token)
        self.authorization_gate = AuthorizationGate(
            verify_household_owner=self.state_store.verify_household_owner,
            require_system_worker_proof=True,
        )

    def handle_request(self, request: OrchestratorRequest) -> Any:
        actor = self.authorization_gate.normalize_actor_identity(request.actor)

        if request.action_type == RequestActionType.READ_SENSITIVE_STATE:
            auth = self.authorization_gate.authorize_read(
                actor,
                request.household_id,
                request.resource_type or "state",
            )
            if not auth.allowed:
                raise PermissionError(auth.reason)
            return self._read_sensitive_state(household_id=request.household_id)

        auth = self.authorization_gate.authorize_action(
            actor,
            request.action_type.value,
            request.household_id,
            context=request.context,
        )
        if not auth.allowed:
            raise PermissionError(auth.reason)

        if request.action_type in {RequestActionType.RUN, RequestActionType.LEGACY_EXECUTION}:
            return self._tick_authorized(
                household_id=request.household_id,
                state=request.state,
                user_input=request.user_input,
                fitness_goal=request.fitness_goal,
                actor=actor,
                now=request.now,
            )

        if request.action_type == RequestActionType.APPROVE:
            if not request.request_id:
                raise ValueError("request_id is required for APPROVE")
            return self._approve_and_execute_authorized(
                household_id=request.household_id,
                request_id=request.request_id,
                action_ids=request.action_ids,
                actor=actor,
                now=request.now,
            )

        if request.action_type == RequestActionType.REJECT:
            if not request.request_id:
                raise ValueError("request_id is required for REJECT")
            return self._reject_authorized(
                household_id=request.household_id,
                request_id=request.request_id,
                action_ids=request.action_ids,
                actor=actor,
                now=request.now,
            )

        if request.action_type == RequestActionType.EXECUTE:
            return self._execute_authorized(
                household_id=request.household_id,
                actor=actor,
                now=request.now,
            )

        if request.action_type == RequestActionType.WRITE_SENSITIVE_STATE:
            if request.graph is None:
                raise ValueError("graph is required for WRITE_SENSITIVE_STATE")
            return self._write_sensitive_state(household_id=request.household_id, graph=request.graph)

        if request.action_type == RequestActionType.QUEUE_FOLLOW_UPS:
            return self._queue_follow_ups(
                household_id=request.household_id,
                actor=actor,
                now=request.now,
            )

        if request.action_type == RequestActionType.UPDATE_DAILY_CYCLE_MARKER:
            if not request.cycle_marker:
                raise ValueError("cycle_marker is required for UPDATE_DAILY_CYCLE_MARKER")
            marker_timestamp = request.cycle_timestamp if request.cycle_timestamp is not None else request.now
            if marker_timestamp is None:
                raise ValueError("cycle_timestamp is required for UPDATE_DAILY_CYCLE_MARKER")
            return self._update_daily_cycle_marker(
                household_id=request.household_id,
                cycle_marker=request.cycle_marker,
                marker_timestamp=marker_timestamp,
            )

        raise ValueError(f"Unsupported request action_type: {request.action_type}")

    @trace_function(entrypoint="orchestrator.tick", actor_type="system_worker", source="orchestrator")
    def tick(
        self,
        *,
        household_id: str,
        state: HouseholdState | None = None,
        user_input: str | None = None,
        fitness_goal: str | None = None,
        actor_type: str | None = None,
        user_id: str | None = None,
        now: str | datetime | None = None,
    ) -> RuntimeTickResult:
        if actor_type is None:
            raise PermissionError("actor_type is required")

        actor: dict[str, Any] = {
            "actor_type": actor_type,
            "subject_id": user_id or ("system" if actor_type == "system_worker" else ""),
            "session_id": None,
            "verified": True,
        }
        request = OrchestratorRequest(
            action_type=RequestActionType.RUN,
            household_id=household_id,
            actor=actor,
            state=state,
            user_input=user_input,
            fitness_goal=fitness_goal,
            now=now,
            context={"system_worker_verified": actor_type == "system_worker"},
        )
        return self.handle_request(request)

    def _tick_authorized(
        self,
        *,
        household_id: str,
        state: HouseholdState | None,
        user_input: str | None,
        fitness_goal: str | None,
        actor: ActorIdentity,
        now: str | datetime | None,
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
            timeout_context = self._execution_context_from_actor(
                actor=actor,
                household_id=household_id,
                request_id="",
            )
            self.action_pipeline.reject_action_timeout(
                graph=graph,
                trigger=processed,
                now=timestamp,
                context=timeout_context,
                internal_token=self._pipeline_internal_token,
            )
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
            context=self._execution_context_from_actor(
                actor=actor,
                household_id=household_id,
                request_id=response.request_id,
            ),
            internal_token=self._pipeline_internal_token,
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
        context: ExecutionContext | None = None,
        actor_type: str | None = None,
        user_id: str | None = None,
        now: str | datetime | None = None,
    ) -> RuntimeApprovalResult:
        if context is not None:
            raw_actor: dict[str, Any] = {
                "actor_type": context.actor_type,
                "subject_id": context.user_id,
                "session_id": context.trace_id or None,
                "verified": True,
            }
        else:
            if actor_type is None:
                raise PermissionError("actor_type is required")
            raw_actor = {
                "actor_type": actor_type,
                "subject_id": user_id or ("system" if actor_type == "system_worker" else ""),
                "session_id": None,
                "verified": True,
            }

        request = OrchestratorRequest(
            action_type=RequestActionType.APPROVE,
            household_id=household_id,
            actor=raw_actor,
            request_id=request_id,
            action_ids=action_ids,
            now=now,
            context={"system_worker_verified": raw_actor.get("actor_type") == "system_worker"},
        )
        return self.handle_request(request)

    def _approve_and_execute_authorized(
        self,
        *,
        household_id: str,
        request_id: str,
        action_ids: list[str],
        actor: ActorIdentity,
        now: str | datetime | None,
    ) -> RuntimeApprovalResult:
        graph = self.state_store.load_graph(household_id)
        timestamp = self._coerce_datetime(now or graph.get("reference_time"))
        execution_context = self._execution_context_from_actor(
            actor=actor,
            household_id=household_id,
            request_id=request_id,
        )
        approve_kwargs: dict[str, Any] = {
            "graph": graph,
            "request_id": request_id,
            "action_ids": action_ids,
            "now": timestamp,
            "actor_type": actor.actor_type.value,
        }
        approve_signature = inspect.signature(self.action_pipeline.approve_actions)
        if "context" in approve_signature.parameters:
            approve_kwargs["context"] = execution_context
        if "internal_token" in approve_signature.parameters:
            approve_kwargs["internal_token"] = self._pipeline_internal_token
        approved_actions = self.action_pipeline.approve_actions(**approve_kwargs)
        executed_actions = self.action_pipeline.execute_approved_actions(
            graph=graph,
            now=timestamp,
            context=execution_context,
            internal_token=self._pipeline_internal_token,
        )
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

    def _reject_authorized(
        self,
        *,
        household_id: str,
        request_id: str,
        action_ids: list[str],
        actor: ActorIdentity,
        now: str | datetime | None,
    ) -> list[LifecycleAction]:
        graph = self.state_store.load_graph(household_id)
        timestamp = self._coerce_datetime(now or graph.get("reference_time"))
        execution_context = self._execution_context_from_actor(
            actor=actor,
            household_id=household_id,
            request_id=request_id,
        )
        rejected = self.action_pipeline.reject_actions(
            graph=graph,
            request_id=request_id,
            action_ids=action_ids,
            now=timestamp,
            context=execution_context,
            internal_token=self._pipeline_internal_token,
        )
        self.state_store.save_graph(graph)
        return rejected

    def _execute_authorized(
        self,
        *,
        household_id: str,
        actor: ActorIdentity,
        now: str | datetime | None,
    ) -> list[LifecycleAction]:
        graph = self.state_store.load_graph(household_id)
        timestamp = self._coerce_datetime(now or graph.get("reference_time"))
        execution_context = self._execution_context_from_actor(
            actor=actor,
            household_id=household_id,
            request_id="",
        )
        executed = self.action_pipeline.execute_approved_actions(
            graph=graph,
            now=timestamp,
            context=execution_context,
            internal_token=self._pipeline_internal_token,
        )
        self.state_store.save_graph(graph)
        return executed

    def _read_sensitive_state(self, *, household_id: str) -> dict[str, Any]:
        return self.state_store.load_graph(household_id)

    def _write_sensitive_state(self, *, household_id: str, graph: dict[str, Any]) -> dict[str, Any]:
        if str(graph.get("household_id") or "") not in {"", household_id}:
            raise PermissionError("graph household_id mismatch")
        merged = dict(graph)
        merged["household_id"] = household_id
        return self.state_store.save_graph(merged)

    def _queue_follow_ups(
        self,
        *,
        household_id: str,
        actor: ActorIdentity,
        now: str | datetime | None,
    ) -> list[dict[str, Any]]:
        graph = self.state_store.load_graph(household_id)
        timestamp = self._coerce_datetime(now or graph.get("reference_time"))
        self._execution_context_from_actor(
            actor=actor,
            household_id=household_id,
            request_id="",
        )
        queued = self.action_pipeline.queue_next_day_follow_ups(
            graph=graph,
            now=timestamp,
            internal_token=self._pipeline_internal_token,
        )
        self.state_store.save_graph(graph)
        return queued

    def _update_daily_cycle_marker(
        self,
        *,
        household_id: str,
        cycle_marker: str,
        marker_timestamp: str | datetime,
    ) -> dict[str, Any]:
        graph = self.state_store.load_graph(household_id)
        timestamp = self._coerce_datetime(marker_timestamp)
        key = f"last_{cycle_marker}_run"
        graph.setdefault("runtime", {}).setdefault("daily_cycle", {})[key] = self._iso(timestamp)
        self.state_store.save_graph(graph)
        return graph

    def _execution_context_from_actor(
        self,
        *,
        actor: ActorIdentity,
        household_id: str,
        request_id: str,
    ) -> ExecutionContext:
        actor_type = actor.actor_type.value
        canonical_actor_type = "user" if actor_type == "api_user" else actor_type
        auth_scope = "system" if canonical_actor_type in {"system_worker", "scheduler"} else "household"
        return ExecutionContext(
            actor_type=canonical_actor_type,
            user_id=actor.subject_id or None,
            household_id=household_id,
            request_id=request_id,
            trace_id=actor.session_id or "",
            metadata={"subject_id": actor.subject_id, "verified": actor.verified, "auth_scope": auth_scope},
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
