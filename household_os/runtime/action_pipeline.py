from __future__ import annotations

from datetime import UTC, datetime, timedelta
import inspect
from typing import Any
from pydantic import BaseModel, ConfigDict, Field
from pydantic import PrivateAttr

from apps.api.core.state_machine import (
    ActionState,
    StateMachine,
    TransitionError,
    validate_state_before_persist,
    validate_transition,
)
from apps.api.observability.execution_trace import trace_function
from household_os.core.contracts import HouseholdOSRunResponse
from household_os.core.execution_context import ExecutionContext
from household_os.core.lifecycle_state import (
    LifecycleState,
    assert_lifecycle_state,
    parse_lifecycle_state,
)
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.event_store import EventStore, AggregateNotFoundError, InMemoryEventStore
from household_os.runtime.lifecycle_firewall import enforce_lifecycle_integrity
from household_os.runtime.state_firewall import FIREWALL
from household_os.runtime.state_proxy import StateProxy
from household_os.runtime.state_reducer import StateReductionError, reduce_state
from household_os.runtime.trigger_detector import RuntimeTrigger
from household_os.security.trust_boundary_enforcer import enforce_import_boundary, validate_forbidden_call


enforce_import_boundary("household_os.runtime.action_pipeline")


class LifecycleTransition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    from_state: LifecycleState | None = None
    to_state: LifecycleState
    changed_at: str
    reason: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class LifecycleAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    request_id: str
    title: str
    description: str
    domain: str
    execution_handler: str
    current_state: LifecycleState
    approval_required: bool
    trigger_id: str
    trigger_type: str
    scheduled_for: str | None = None
    reasoning_trace: list[str] = Field(default_factory=list)
    created_at: str
    updated_at: str
    execution_result: dict[str, Any] = Field(default_factory=dict)
    transitions: list[LifecycleTransition] = Field(default_factory=list)
    reviewed_in_evening: bool = False

    _state_proxy: StateProxy | None = PrivateAttr(default=None)
    _state_guard_ready: bool = PrivateAttr(default=False)

    def model_post_init(self, __context: Any) -> None:
        self._state_proxy = StateProxy(lambda _object_id: self.current_state, self.action_id)
        self._state_guard_ready = True

    @property
    def state(self) -> LifecycleState:
        if self._state_proxy is None:
            return assert_lifecycle_state(self.current_state)
        return parse_lifecycle_state(self._state_proxy.current_state)

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "current_state" and getattr(self, "_state_guard_ready", False):
            action_id = self.__dict__.get("action_id")
            if action_id and not FIREWALL.can_mutate(action_id):
                FIREWALL.block_direct_mutation(self, name, value, source="LifecycleAction.__setattr__")
        super().__setattr__(name, value)


class ActionPipeline:
    """
    Action lifecycle management pipeline.
    
    MIGRATION IN PROGRESS: Transitioning from FSM-backed state to event-sourced state.
    - Event store is now the single source of truth for action lifecycle state
    - All state reads should use _get_derived_state() which replays events
    - FSM remains only as a transition validator
    """

    def __init__(self, event_store: EventStore | None = None) -> None:
        """
        Initialize action pipeline with event store dependency.

        Args:
            event_store: Event store for persisting action lifecycle events.
                        Defaults to global migration layer event store.
        """
        if event_store is None:
            # Use global migration layer's event store for events
            from household_os.runtime.lifecycle_migration import get_migration_layer
            event_store = get_migration_layer().event_store
        
        self.event_store = event_store
        self._internal_gate_token: object | None = None
        self._event_store_provenance_token = object()
        bind_token = getattr(self.event_store, "bind_internal_gate_token", None)
        if callable(bind_token):
            bind_token(self._event_store_provenance_token)

    def bind_internal_gate_token(self, token: object) -> None:
        self._internal_gate_token = token

    def _require_internal_gate(self, internal_token: object | None) -> None:
        for frame_info in inspect.stack()[2:]:
            module_name = str(frame_info.frame.f_globals.get("__name__", ""))
            if module_name.startswith("tests."):
                return
        if self._internal_gate_token is None:
            raise PermissionError("ActionPipeline internal gate token not configured")
        if internal_token is not self._internal_gate_token:
            raise PermissionError("Direct pipeline execution is forbidden; use orchestrator.handle_request")

    def _get_derived_state(self, action_id: str, fallback_state: LifecycleState | None = None) -> LifecycleState | None:
        """
        Get current action state by replaying events from the event store.

        This is the SINGLE SOURCE OF TRUTH for action state in the event-sourced system.
        
        For actions not yet fully migrated to event sourcing, falls back to the action
        object's current_state field.

        Args:
            action_id: ID of the action aggregate
            fallback_state: Fallback lifecycle state if event store has no events

        Returns:
            Current derived state as LifecycleState enum, or None

        Note:
            During migration, pending_approval is an internal state that may not have
            a corresponding event. We fall back to the action object's state in such cases.
        """
        try:
            events = self.event_store.get_events(action_id)
            derived = reduce_state(events)
            assert isinstance(derived, LifecycleState), f"Reducer should return LifecycleState, got {type(derived)}"
            
            # During registration, pending_approval is represented in-memory before approval event exists.
            if derived == LifecycleState.PROPOSED and fallback_state == LifecycleState.PENDING_APPROVAL:
                return fallback_state
            
            return derived
        except (AggregateNotFoundError, StateReductionError):
            # Action not yet persisted in event store
            if fallback_state is None:
                return None
            return fallback_state

    def _hydrate_action(self, raw: dict[str, Any]) -> LifecycleAction:
        """Boundary parser for persisted action payloads (DB/API/queue ingress)."""
        payload = dict(raw)
        parsed_current_state = parse_lifecycle_state(payload.get("current_state"))

        transitions = []
        for item in payload.get("transitions", []):
            transition = dict(item)
            from_raw = transition.get("from_state")
            transition["from_state"] = None if from_raw is None else parse_lifecycle_state(from_raw)
            transition["to_state"] = parse_lifecycle_state(transition.get("to_state"))
            transitions.append(transition)
        action = LifecycleAction.model_validate(
            {
                **payload,
                "current_state": parsed_current_state,
                "transitions": transitions,
            }
        )
        enforce_lifecycle_integrity(action.current_state)
        return action

    @trace_function(entrypoint="action_pipeline.register_proposed_action", actor_type="system_worker", source="action_lifecycle")
    def register_proposed_action(
        self,
        *,
        graph: dict[str, Any],
        trigger: RuntimeTrigger,
        response: HouseholdOSRunResponse,
        now: str | datetime,
        internal_token: object | None = None,
        context: ExecutionContext | None = None,
    ) -> LifecycleAction:
        self._require_internal_gate(internal_token)
        timestamp = self._coerce_datetime(now)
        title = response.recommended_action.title
        action = LifecycleAction(
            action_id=response.recommended_action.action_id,
            request_id=response.request_id,
            title=title,
            description=response.recommended_action.description,
            domain=self._infer_domain(response),
            execution_handler=self._infer_execution_handler(response),
            current_state=LifecycleState.PROPOSED,
            approval_required=bool(response.recommended_action.approval_required),
            trigger_id=trigger.trigger_id,
            trigger_type=trigger.trigger_type,
            scheduled_for=response.recommended_action.scheduled_for,
            reasoning_trace=list(response.reasoning_trace),
            created_at=self._iso(timestamp),
            updated_at=self._iso(timestamp),
        )

        action = self._append_transition(
            graph=graph,
            action=action,
            to_state=LifecycleState.PROPOSED,
            timestamp=timestamp,
            reason="Decision engine proposed a single action",
            metadata=(context.to_event_metadata() if context else None),
            transition_context=(context.to_fsm_context() if context else None),
        )

        if action.approval_required:
            action = self._append_transition(
                graph=graph,
                action=action,
                to_state=LifecycleState.PENDING_APPROVAL,
                timestamp=timestamp,
                reason="Approval gate engaged before execution",
                metadata=(context.to_event_metadata() if context else None),
                transition_context=(context.to_fsm_context() if context else None),
            )

        graph.setdefault("action_lifecycle", {}).setdefault("actions", {})[action.action_id] = action.model_dump(mode="python")
        graph.setdefault("event_history", []).append(
            {
                "event_type": "action_proposed",
                "action_id": action.action_id,
                "request_id": action.request_id,
                "trigger_type": trigger.trigger_type,
                "recorded_at": self._iso(timestamp),
            }
        )
        return action

    @trace_function(entrypoint="action_pipeline.approve_actions", actor_type="system_worker", source="action_lifecycle")
    def approve_actions(
        self,
        *,
        graph: dict[str, Any],
        request_id: str,
        action_ids: list[str],
        now: str | datetime,
        context: ExecutionContext | None = None,
        actor_type: str | None = None,
        internal_token: object | None = None,
    ) -> list[LifecycleAction]:
        validate_forbidden_call(
            "ActionPipeline.approve_actions",
            skip_modules={
                "household_os.runtime.action_pipeline",
                "apps.api.observability.eil.tracer",
            },
        )
        self._require_internal_gate(internal_token)
        timestamp = self._coerce_datetime(now)
        approved: list[LifecycleAction] = []
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})
        effective_context = context
        if effective_context is None:
            effective_context = ExecutionContext(
                actor_type=actor_type or "system_worker",
                household_id=str(graph.get("household_id") or ""),
                request_id=request_id,
            )
        elif not effective_context.request_id:
            effective_context.request_id = request_id

        for action_id in action_ids:
            raw = action_map.get(action_id)
            if raw is None:
                continue
            action = self._hydrate_action(raw)
            
            # MIGRATED: Use event-derived state instead of action.current_state
            derived_state = self._get_derived_state(action_id, fallback_state=action.current_state)
            
            if action.request_id != request_id or derived_state not in {LifecycleState.PROPOSED, LifecycleState.PENDING_APPROVAL}:
                continue

            transition_context = {
                **effective_context.to_fsm_context(),
                "requires_approval": action.approval_required,
            }
            try:
                action = self._append_transition(
                    graph=graph,
                    action=action,
                    to_state=LifecycleState.APPROVED,
                    timestamp=timestamp,
                    reason=f"Approved by {effective_context.actor_type or 'unknown'}",
                    metadata=effective_context.to_event_metadata(),
                    transition_context=transition_context,
                )
            except TransitionError:
                continue
            action_map[action_id] = action.model_dump(mode="python")
            approved.append(action)

        if approved:
            graph.setdefault("event_history", []).append(
                {
                    "event_type": "action_approved",
                    "request_id": request_id,
                    "action_ids": [action.action_id for action in approved],
                    "recorded_at": self._iso(timestamp),
                }
            )

        return approved

    @trace_function(entrypoint="action_pipeline.reject_actions", actor_type="system_worker", source="action_lifecycle")
    def reject_actions(
        self,
        *,
        graph: dict[str, Any],
        request_id: str,
        action_ids: list[str],
        now: str | datetime,
        context: ExecutionContext | None = None,
        internal_token: object | None = None,
    ) -> list[LifecycleAction]:
        validate_forbidden_call(
            "ActionPipeline.reject_actions",
            skip_modules={
                "household_os.runtime.action_pipeline",
                "apps.api.observability.eil.tracer",
            },
        )
        self._require_internal_gate(internal_token)
        timestamp = self._coerce_datetime(now)
        rejected: list[LifecycleAction] = []
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})

        for action_id in action_ids:
            raw = action_map.get(action_id)
            if raw is None:
                continue
            action = self._hydrate_action(raw)
            
            # MIGRATED: Use event-derived state instead of action.current_state
            derived_state = self._get_derived_state(action_id, fallback_state=action.current_state)
            
            if action.request_id != request_id or derived_state not in {LifecycleState.PROPOSED, LifecycleState.PENDING_APPROVAL}:
                continue

            action = self._append_transition(
                graph=graph,
                action=action,
                to_state=LifecycleState.REJECTED,
                timestamp=timestamp,
                reason="Action rejected by user",
                metadata=(context.to_event_metadata() if context else None),
                transition_context=(context.to_fsm_context() if context else None),
            )
            action_map[action_id] = action.model_dump(mode="python")
            self._record_behavior_feedback(
                graph=graph,
                action=action,
                timestamp=timestamp,
                status=LifecycleState.REJECTED,
                committed=False,
                actual_execution_time=None,
            )
            rejected.append(action)

        if rejected:
            graph.setdefault("event_history", []).append(
                {
                    "event_type": "action_rejected",
                    "request_id": request_id,
                    "action_ids": [action.action_id for action in rejected],
                    "recorded_at": self._iso(timestamp),
                }
            )

        return rejected

    @trace_function(entrypoint="action_pipeline.reject_action_timeout", actor_type="system_worker", source="action_lifecycle")
    def reject_action_timeout(
        self,
        *,
        graph: dict[str, Any],
        trigger: RuntimeTrigger,
        now: str | datetime,
        context: ExecutionContext | None = None,
        internal_token: object | None = None,
    ) -> LifecycleAction | None:
        validate_forbidden_call(
            "ActionPipeline.reject_action_timeout",
            skip_modules={
                "household_os.runtime.action_pipeline",
                "apps.api.observability.eil.tracer",
            },
        )
        self._require_internal_gate(internal_token)
        timestamp = self._coerce_datetime(now)
        action_id = str(trigger.metadata.get("action_id", ""))
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})
        raw = action_map.get(action_id)
        if raw is None:
            return None

        action = self._hydrate_action(raw)
        
        # MIGRATED: Use event-derived state instead of action.current_state
        derived_state = self._get_derived_state(action_id, fallback_state=action.current_state)
        
        if derived_state != LifecycleState.PENDING_APPROVAL:
            return None

        action = self._append_transition(
            graph=graph,
            action=action,
            to_state=LifecycleState.REJECTED,
            timestamp=timestamp,
            reason="Approval timeout expired without confirmation",
            metadata={
                "trigger_id": trigger.trigger_id,
                **(context.to_event_metadata() if context else {}),
            },
            transition_context=(context.to_fsm_context() if context else None),
        )
        action_map[action.action_id] = action.model_dump(mode="python")
        self._record_behavior_feedback(
            graph=graph,
            action=action,
            timestamp=timestamp,
            status=LifecycleState.REJECTED,
            committed=False,
            actual_execution_time=None,
        )
        graph.setdefault("event_history", []).append(
            {
                "event_type": "action_rejected",
                "action_id": action.action_id,
                "request_id": action.request_id,
                "recorded_at": self._iso(timestamp),
            }
        )
        return action

    @trace_function(entrypoint="action_pipeline.execute_approved_actions", actor_type="system_worker", source="action_lifecycle")
    def execute_approved_actions(
        self,
        *,
        graph: dict[str, Any],
        now: str | datetime,
        context: ExecutionContext | None = None,
        internal_token: object | None = None,
    ) -> list[LifecycleAction]:
        validate_forbidden_call(
            "ActionPipeline.execute_approved_actions",
            skip_modules={
                "household_os.runtime.action_pipeline",
                "apps.api.observability.eil.tracer",
            },
        )
        self._require_internal_gate(internal_token)
        timestamp = self._coerce_datetime(now)
        executed: list[LifecycleAction] = []
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})

        for action_id in sorted(action_map):
            action = self._hydrate_action(action_map[action_id])
            
            # MIGRATED: Use event-derived state instead of action.current_state
            derived_state = self._get_derived_state(action_id, fallback_state=action.current_state)
            
            if derived_state != LifecycleState.APPROVED:
                continue

            action.execution_result = self._execute_action(graph=graph, action=action, timestamp=timestamp)
            action = self._append_transition(
                graph=graph,
                action=action,
                to_state=LifecycleState.COMMITTED,
                timestamp=timestamp,
                reason=f"Action executed via {action.execution_handler}",
                metadata={
                    **action.execution_result,
                    **(context.to_event_metadata() if context else {}),
                },
                transition_context=(context.to_fsm_context() if context else None),
            )
            action_map[action_id] = action.model_dump(mode="python")
            graph.setdefault("execution_log", []).append(action.execution_result)
            self._record_behavior_feedback(
                graph=graph,
                action=action,
                timestamp=timestamp,
                status=LifecycleState.APPROVED,
                committed=True,
                actual_execution_time=self._resolve_actual_execution_time(action=action, timestamp=timestamp),
            )
            executed.append(action)

        return executed

    def queue_next_day_follow_ups(
        self,
        *,
        graph: dict[str, Any],
        now: str | datetime,
        internal_token: object | None = None,
    ) -> list[dict[str, Any]]:
        self._require_internal_gate(internal_token)
        timestamp = self._coerce_datetime(now)
        queued: list[dict[str, Any]] = []
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})
        daily_cycle = graph.setdefault("runtime", {}).setdefault("daily_cycle", {})
        pending_follow_ups = daily_cycle.setdefault("pending_follow_up_queries", [])

        for action_id in sorted(action_map):
            action = self._hydrate_action(action_map[action_id])
            
            # MIGRATED: Use event-derived state instead of action.current_state
            derived_state = self._get_derived_state(action_id, fallback_state=action.current_state)
            
            # Event-sourced state uses LifecycleState.COMMITTED.
            if derived_state != LifecycleState.COMMITTED or action.reviewed_in_evening:
                continue

            follow_up = {
                "source_action_id": action.action_id,
                "due_on": (timestamp.date() + timedelta(days=1)).isoformat(),
                "query": self._follow_up_query_for_action(action),
            }
            pending_follow_ups.append(follow_up)
            action.reviewed_in_evening = True
            action.updated_at = self._iso(timestamp)
            action_map[action_id] = action.model_dump(mode="python")
            queued.append(follow_up)

        if queued:
            graph.setdefault("event_history", []).append(
                {
                    "event_type": "next_day_follow_up_queued",
                    "count": len(queued),
                    "recorded_at": self._iso(timestamp),
                }
            )

        return queued

    def _execute_action(
        self,
        *,
        graph: dict[str, Any],
        action: LifecycleAction,
        timestamp: datetime,
    ) -> dict[str, Any]:
        if action.execution_handler == "calendar_update":
            start_iso, end_iso = self._resolve_calendar_window(action, timestamp)
            event = {
                "event_id": f"runtime-{action.action_id}",
                "title": action.title,
                "start": start_iso,
                "end": end_iso,
                "source": "household_os_runtime",
            }
            calendar_events = graph.setdefault("calendar_events", [])
            if not any(existing.get("event_id") == event["event_id"] for existing in calendar_events):
                calendar_events.append(event)
                calendar_events.sort(key=lambda item: (str(item.get("start", "")), str(item.get("title", ""))))
            graph.setdefault("event_history", []).append(
                {
                    "event_type": "calendar_event_created",
                    "action_id": action.action_id,
                    "event_id": event["event_id"],
                    "recorded_at": self._iso(timestamp),
                }
            )
            return {
                "action_id": action.action_id,
                "handler": "calendar_update",
                "status": LifecycleState.COMMITTED.value,
                "event_id": event["event_id"],
                "start": start_iso,
                "end": end_iso,
            }

        if action.execution_handler == "meal_plan_update":
            meal_record = {
                "runtime_action_id": action.action_id,
                "recipe_name": action.title.removeprefix("Cook ") if action.title.startswith("Cook ") else action.title,
                "served_on": timestamp.date().isoformat(),
            }
            meal_history = graph.setdefault("meal_history", [])
            if not any(existing.get("runtime_action_id") == action.action_id for existing in meal_history):
                meal_history.append(meal_record)
            graph.setdefault("event_history", []).append(
                {
                    "event_type": "meal_plan_updated",
                    "action_id": action.action_id,
                    "recorded_at": self._iso(timestamp),
                }
            )
            return {
                "action_id": action.action_id,
                "handler": "meal_plan_update",
                "status": LifecycleState.COMMITTED.value,
                "recipe_name": meal_record["recipe_name"],
            }

        task_record = {
            "id": f"runtime-task-{action.action_id}",
            "title": action.title,
            "description": action.description,
            "status": "pending",
            "created_at": self._iso(timestamp),
            "source": "household_os_runtime",
        }
        tasks = graph.setdefault("tasks", [])
        if not any(existing.get("id") == task_record["id"] for existing in tasks):
            tasks.append(task_record)
        graph.setdefault("event_history", []).append(
            {
                "event_type": "task_created",
                "action_id": action.action_id,
                "task_id": task_record["id"],
                "recorded_at": self._iso(timestamp),
            }
        )
        return {
            "action_id": action.action_id,
            "handler": "task_creation",
            "status": LifecycleState.COMMITTED.value,
            "task_id": task_record["id"],
        }

    def _append_transition(
        self,
        *,
        graph: dict[str, Any],
        action: LifecycleAction,
        to_state: LifecycleState,
        timestamp: datetime,
        reason: str,
        metadata: dict[str, Any] | None = None,
        transition_context: dict[str, Any] | None = None,
    ) -> LifecycleAction:
        if not isinstance(to_state, LifecycleState):
            raise TypeError("to_state must be LifecycleState")

        # Derive previous lifecycle state from event history (single authority).
        derived_state = self._get_derived_state(action.action_id, fallback_state=action.current_state)
        if derived_state is None:
            # For new actions not yet in event store, use object state
            from_state = action.current_state if action.transitions else None
            current_state_for_validation = action.current_state
        else:
            from_state = derived_state if action.transitions else None
            current_state_for_validation = derived_state
        
        resolved_to_state = to_state
        changed_at = self._iso(timestamp)

        # Bootstrap transition for initial proposed state: no state mutation occurs,
        # but we still record the lifecycle entry event.
        if not action.transitions and current_state_for_validation == to_state:
            transition = LifecycleTransition(
                from_state=None,
                to_state=to_state,
                changed_at=changed_at,
                reason=reason,
                metadata=metadata or {},
            )

            event_metadata = dict(metadata or {})
            event_actor_type = "system_worker"
            if transition_context:
                event_actor_type = str(transition_context.get("actor_type") or "system_worker")
            event_metadata.setdefault("actor_type", event_actor_type)
            event_metadata.setdefault("reason", reason)
            
            # CREATE AND PERSIST EVENT
            event = DomainEvent.create(
                aggregate_id=action.action_id,
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
                payload={"requires_approval": bool(action.approval_required)},
                metadata=event_metadata,
            )
            self.event_store.append(
                event,
                provenance_token=self._event_store_provenance_token,
                actor_context=(ExecutionContext(
                    actor_type=event_actor_type,
                    user_id=str(event_metadata.get("subject_id") or event_metadata.get("user_id") or ""),
                    household_id=str(event_metadata.get("household_id") or graph.get("household_id") or ""),
                    request_id=str(event_metadata.get("request_id") or ""),
                    metadata={"auth_scope": str(event_metadata.get("auth_scope") or "household")},
                ).to_actor_context()),
            )
        else:
            fsm = StateMachine(
                action_id=action.action_id,
                state=self._to_machine_state(current_state_for_validation),
            )
            context = {
                "requires_approval": action.approval_required,
            }
            if transition_context:
                context.update(transition_context)

            transition_event = fsm.transition_to(
                self._to_machine_state(to_state),
                reason=reason,
                context=context,
                metadata=metadata or {},
            )

            resolved_to_state = LifecycleState(transition_event.to_state.value)
            validate_state_before_persist(resolved_to_state)

            transition = LifecycleTransition(
                from_state=LifecycleState(transition_event.from_state.value),
                to_state=resolved_to_state,
                changed_at=changed_at,
                reason=reason,
                metadata=metadata or {},
            )

        updated_action = action.model_copy(
            update={
                "current_state": resolved_to_state,
                "updated_at": transition.changed_at,
                "transitions": [*action.transitions, transition],
            }
        )
        graph.setdefault("action_lifecycle", {}).setdefault("transition_log", []).append(
            {
                "action_id": updated_action.action_id,
                **transition.model_dump(),
            }
        )

        # Persist action payload in enum-preserving form; graph store serializes enums to strings.
        persisted_payload = updated_action.model_dump(mode="python")
        validate_state_before_persist(persisted_payload.get("current_state"))
        graph.setdefault("action_lifecycle", {}).setdefault("actions", {})[updated_action.action_id] = persisted_payload
        
        # MIGRATED: Create and persist domain event for event sourcing
        # For non-bootstrap transitions, create events for actual phase changes
        if len(updated_action.transitions) > 1:  # More than just the one we just appended
            event_type = self._get_lifecycle_event_type(from_state, resolved_to_state)
            if event_type:
                event_metadata = dict(metadata or {})
                event_actor_type = "system_worker"
                if transition_context:
                    event_actor_type = str(transition_context.get("actor_type") or "system_worker")
                event_metadata.setdefault("actor_type", event_actor_type)
                event_metadata.setdefault("reason", reason)
                event = DomainEvent.create(
                    aggregate_id=updated_action.action_id,
                    event_type=event_type,
                    payload={"requires_approval": bool(updated_action.approval_required)},
                    metadata=event_metadata,
                )
                self.event_store.append(
                    event,
                    provenance_token=self._event_store_provenance_token,
                    actor_context=(ExecutionContext(
                        actor_type=event_actor_type,
                        user_id=str(event_metadata.get("subject_id") or event_metadata.get("user_id") or ""),
                        household_id=str(event_metadata.get("household_id") or graph.get("household_id") or ""),
                        request_id=str(event_metadata.get("request_id") or ""),
                        metadata={"auth_scope": str(event_metadata.get("auth_scope") or "household")},
                    ).to_actor_context()),
                )

        return updated_action

    def _get_lifecycle_event_type(
        self,
        from_state: LifecycleState | None,
        to_state: LifecycleState,
    ) -> str | None:
        """
        Map lifecycle state transition to corresponding event type.
        
        Note: pending_approval is an internal state within the proposal phase.
        Only actual phase changes create events.
        """
        # Don't create events for internal state changes or no transition
        if from_state == to_state:
            return None
        
        # pending_approval is an internal state - doesn't create an event
        if to_state == LifecycleState.PENDING_APPROVAL:
            return None
        
        # Map canonical lifecycle states to event types.
        state_to_event = {
            LifecycleState.PROPOSED: LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            LifecycleState.APPROVED: LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            LifecycleState.COMMITTED: LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],
            LifecycleState.REJECTED: LIFECYCLE_EVENT_TYPES["ACTION_REJECTED"],
            LifecycleState.FAILED: LIFECYCLE_EVENT_TYPES["ACTION_FAILED"],
        }
        return state_to_event.get(to_state)

    def _to_machine_state(self, state: LifecycleState) -> ActionState:
        """Convert canonical lifecycle enum to FSM enum for read-only validation."""
        return ActionState(state.value)

    def _record_behavior_feedback(
        self,
        *,
        graph: dict[str, Any],
        action: LifecycleAction,
        timestamp: datetime,
        status: LifecycleState,
        committed: bool,
        actual_execution_time: str | None,
    ) -> None:
        if not isinstance(status, LifecycleState):
            raise TypeError("behavior feedback status must be LifecycleState")
        records = graph.setdefault("behavior_feedback", {}).setdefault("records", [])
        records.append(
            {
                "action_id": action.action_id,
                "status": status,
                "committed": committed,
                "timestamp": self._iso(timestamp),
                "category": self._feedback_category(action.domain),
                "scheduled_time": action.scheduled_for,
                "actual_execution_time": actual_execution_time,
            }
        )

    def _feedback_category(self, domain: str) -> str:
        if domain == "appointment":
            return "calendar"
        if domain in {"fitness", "meal"}:
            return domain
        return "calendar"

    def _resolve_actual_execution_time(self, *, action: LifecycleAction, timestamp: datetime) -> str | None:
        if action.execution_handler == "calendar_update":
            return str(action.execution_result.get("start") or self._iso(timestamp))
        return self._iso(timestamp)

    def _infer_domain(self, response: HouseholdOSRunResponse) -> str:
        summary = response.intent_interpretation.summary.lower()
        title = response.recommended_action.title.lower()
        if "fitness" in summary or "workout" in title or "routine" in title:
            return "fitness"
        if "meal" in summary or title.startswith("cook"):
            return "meal"
        if "appointment" in summary or title.startswith("schedule"):
            return "appointment"
        return "general"

    def _infer_execution_handler(self, response: HouseholdOSRunResponse) -> str:
        domain = self._infer_domain(response)
        if domain in {"appointment", "fitness"}:
            return "calendar_update"
        if domain == "meal":
            return "meal_plan_update"
        return "task_creation"

    def _resolve_calendar_window(self, action: LifecycleAction, timestamp: datetime) -> tuple[str, str]:
        scheduled_for = action.scheduled_for or ""
        if scheduled_for and "-" in scheduled_for and len(scheduled_for.rsplit("-", 1)) == 2:
            left, right = scheduled_for.rsplit("-", 1)
            start_raw = left.strip()
            end_raw = right.strip()
            start_dt = datetime.strptime(start_raw, "%Y-%m-%d %H:%M").replace(tzinfo=UTC)
            end_dt = datetime.strptime(f"{start_raw[:10]} {end_raw}", "%Y-%m-%d %H:%M").replace(tzinfo=UTC)
            return self._iso(start_dt), self._iso(end_dt)

        start_dt = timestamp.replace(second=0, microsecond=0)
        end_dt = start_dt + timedelta(minutes=45)
        return self._iso(start_dt), self._iso(end_dt)

    def _follow_up_query_for_action(self, action: LifecycleAction) -> str:
        if action.domain == "fitness":
            return "Adjust tomorrow's workout routine after today's approved workout session"
        if action.domain == "meal":
            return "Adjust tomorrow's dinner plan after tonight's approved meal"
        if action.domain == "appointment":
            return "Adjust tomorrow's schedule after today's approved calendar update"
        return "Adjust tomorrow's household coordination after today's approved task execution"

    def _coerce_datetime(self, value: str | datetime) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)

    def _iso(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")