"""
Refactoring Action Pipeline to use Centralized StateMachine.

This module demonstrates how to integrate the new StateMachine into the existing
action_pipeline while maintaining backward compatibility.

Changes:
1. Replace inline LifecycleState with ActionState enum
2. Route all transitions through StateMachine.transition_to()
3. Emit structured events via EventBus
4. Add retry logic for failed actions
5. Add timeout enforcement
6. Add idempotency key tracking

NON-BREAKING CHANGES:
- Existing graph structure preserved
- Existing method signatures preserved (where possible)
- New fields added to action model for retry tracking
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from apps.api.core.state_machine import (
    ActionState,
    StateMachine,
    StateTransitionEvent,
    RETRY_POLICY,
    classify_error,
    TransitionError,
)


# ─────────────────────────────────────────────────────────────────────────────
# INTEGRATION PATTERN: Existing action_pipeline refactoring guidance
# ─────────────────────────────────────────────────────────────────────────────

class ActionPipelineStateMachineIntegration:
    """
    Example of how to integrate StateMachine into ActionPipeline.

    This is a reference implementation showing the expected changes.
    """

    # Old code (to be replaced):
    # def approve_actions(self, *, graph, request_id, action_ids, now):
    #     for action_id in action_ids:
    #         raw = action_map.get(action_id)
    #         if raw is None: continue
    #         action = LifecycleAction.model_validate(raw)
    #
    #         # OLD: Direct state mutation
    #         action.current_state = "approved"
    #
    #         self._append_transition(graph, action, "approved", timestamp, "User approved")
    #         action_map[action_id] = action.model_dump()

    # New code pattern:
    def approve_actions_refactored(
        self,
        *,
        graph: dict[str, Any],
        request_id: str,
        action_ids: list[str],
        now: str | datetime,
    ) -> list[Any]:
        """
        REFACTORED: Route through StateMachine validator.

        Key changes:
        1. Create StateMachine instance from action payload
        2. Call transition_to() instead of direct state mutation
        3. Emit event via EventBus
        4. Capture retry count and error tracking
        """
        timestamp = self._coerce_datetime(now)
        approved: list[Any] = []
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})

        for action_id in action_ids:
            raw = action_map.get(action_id)
            if raw is None:
                continue

            # NEW: Reconstruct state machine from persisted state
            fsm = self._load_state_machine(raw)

            try:
                # NEW: Validate and execute transition through state machine
                event: StateTransitionEvent = fsm.transition_to(
                    ActionState.APPROVED,
                    reason="Action approved for execution",
                    correlation_id=request_id,
                    context={"requires_approval": raw.get("approval_required", False)},
                )

                # NEW: Emit event to EventBus
                self._emit_transition_event(event, graph, request_id)

                # Update action payload with new state and retry metadata
                raw["current_state"] = fsm.state.value
                raw["retry_count"] = fsm.retry_count
                raw["transitions"] = [t.to_dict() for t in fsm.transitions]
                action_map[action_id] = raw

                approved.append(raw)

            except TransitionError as e:
                # NEW: Explicit error handling
                self._handle_transition_error(
                    graph=graph,
                    action_id=action_id,
                    error=str(e),
                    timestamp=timestamp,
                )
                continue

        return approved

    def execute_approved_actions_refactored(
        self,
        *,
        graph: dict[str, Any],
        now: str | datetime,
    ) -> list[Any]:
        """
        REFACTORED: Execute with failure handling and retry logic.

        Key changes:
        1. Capture execution errors with classification
        2. Route failed actions through StateMachine to failed state
        3. Log retry metadata for automatic retry
        """
        timestamp = self._coerce_datetime(now)
        executed: list[Any] = []
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})

        for action_id in sorted(action_map):
            raw = action_map[action_id]
            if raw.get("current_state") != "approved":
                continue

            fsm = self._load_state_machine(raw)

            try:
                # Execute action (existing logic)
                execution_result = self._execute_action_logic(graph, raw, timestamp)

                # NEW: Transition to committed on success
                event: StateTransitionEvent = fsm.transition_to(
                    ActionState.COMMITTED,
                    reason=f"Action executed via {raw.get('execution_handler')}",
                    metadata=execution_result,
                )

                # Emit success event
                self._emit_transition_event(event, graph, raw.get("request_id"))

                raw["current_state"] = fsm.state.value
                raw["execution_result"] = execution_result
                raw["transitions"] = [t.to_dict() for t in fsm.transitions]
                action_map[action_id] = raw
                executed.append(raw)

            except Exception as e:
                # NEW: Classify error and route to failed state
                error_code = self._classify_exception(e)
                error_classification = classify_error(error_code)

                try:
                    event: StateTransitionEvent = fsm.transition_to(
                        ActionState.FAILED,
                        reason=f"Execution failed: {str(e)[:100]}",
                        error_code=error_code,
                    )

                    # Emit failure event
                    self._emit_transition_event(
                        event,
                        graph,
                        raw.get("request_id"),
                        visibility="user_alert" if error_classification == "non_retryable" else "silent",
                    )

                    raw["current_state"] = fsm.state.value
                    raw["last_error"] = {"code": error_code, "message": str(e)}
                    raw["retry_count"] = fsm.retry_count
                    raw["next_retry_time"] = (
                        (timestamp + fsm.get_retry_delay()).isoformat()
                        if fsm.can_retry()
                        else None
                    )
                    raw["transitions"] = [t.to_dict() for t in fsm.transitions]
                    action_map[action_id] = raw

                except TransitionError:
                    # Already in terminal state, cannot fail
                    pass

        return executed

    def process_failed_actions_refactored(
        self,
        *,
        graph: dict[str, Any],
        now: str | datetime,
    ) -> list[Any]:
        """
        NEW: Process failed actions for retry.

        Key features:
        1. Check can_retry() before retrying
        2. Enforce exponential backoff via get_retry_delay()
        3. Transition failed → proposed for retry
        4. Emit retry event
        """
        timestamp = self._coerce_datetime(now)
        retried: list[Any] = []
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})

        for action_id in sorted(action_map):
            raw = action_map[action_id]
            if raw.get("current_state") != "failed":
                continue

            fsm = self._load_state_machine(raw)

            # Check if can retry
            if not fsm.can_retry():
                # Max retries exceeded, final failure
                continue

            # Check if enough time has passed for retry delay
            next_retry_time_str = raw.get("next_retry_time")
            if next_retry_time_str:
                next_retry_time = datetime.fromisoformat(
                    next_retry_time_str.replace("Z", "+00:00")
                )
                if timestamp < next_retry_time:
                    continue  # Not time to retry yet

            try:
                # Transition: failed → proposed (for retry)
                event: StateTransitionEvent = fsm.transition_to(
                    ActionState.PROPOSED,
                    reason=f"Retrying action (attempt {fsm.retry_count + 1}/{RETRY_POLICY['max_retries']})",
                )

                # Emit retry event
                self._emit_transition_event(event, graph, raw.get("request_id"))

                raw["current_state"] = fsm.state.value
                raw["retry_count"] = fsm.retry_count
                raw["transitions"] = [t.to_dict() for t in fsm.transitions]
                raw["next_retry_time"] = None
                action_map[action_id] = raw
                retried.append(raw)

            except TransitionError:
                pass

        return retried

    # ─────────────────────────────────────────────────────────────────────────
    # Helper methods for integration
    # ─────────────────────────────────────────────────────────────────────────

    def _load_state_machine(self, action_payload: dict[str, Any]) -> StateMachine:
        """Reconstruct StateMachine from action payload."""
        state_value = action_payload.get("current_state", "proposed")
        state = ActionState(state_value)

        fsm = StateMachine(
            action_id=action_payload.get("action_id", ""),
            state=state,
            retry_count=action_payload.get("retry_count", 0),
        )

        # Restore transition history
        for transition_dict in action_payload.get("transitions", []):
            # (In full implementation, reconstruct StateTransitionEvent)
            pass

        return fsm

    def _emit_transition_event(
        self,
        event: StateTransitionEvent,
        graph: dict[str, Any],
        request_id: str,
        visibility: str = "silent",
    ) -> None:
        """Emit a transition event to EventBus and append to graph history."""
        # Append to graph event history for backward compatibility
        graph.setdefault("event_history", []).append(
            {
                "event_id": event.event_id,
                "event_type": f"action.{event.to_state.value}",
                "action_id": event.action_id,
                "request_id": request_id,
                "recorded_at": event.timestamp.isoformat(),
                "reason": event.reason,
                "retry_attempt": event.retry_attempt,
                "error_code": event.error_code,
            }
        )

        # In full implementation: emit to EventBus
        # event_bus.emit(
        #     event_type=f"action.{event.to_state.value}",
        #     action_id=event.action_id,
        #     household_id=...,
        #     state=event.to_state.value,
        # )

    def _handle_transition_error(
        self,
        *,
        graph: dict[str, Any],
        action_id: str,
        error: str,
        timestamp: datetime,
    ) -> None:
        """Handle a transition error."""
        graph.setdefault("transition_errors", []).append(
            {
                "action_id": action_id,
                "error": error,
                "timestamp": timestamp.isoformat(),
            }
        )

    def _classify_exception(self, exc: Exception) -> str:
        """Classify an exception to an error code."""
        exc_name = type(exc).__name__
        if "Connection" in exc_name or "Connection" in str(exc):
            return "database_connection_error"
        if "Timeout" in exc_name or "Timeout" in str(exc):
            return "network_timeout"
        if "NotImplemented" in exc_name:
            return "not_implemented"
        return "internal_server_error"

    def _execute_action_logic(
        self,
        graph: dict[str, Any],
        action_payload: dict[str, Any],
        timestamp: datetime,
    ) -> dict[str, Any]:
        """Existing action execution logic (unchanged)."""
        # This is existing code from action_pipeline._execute_action
        # Refactor returns only the result dict
        return {
            "action_id": action_payload.get("action_id"),
            "handler": action_payload.get("execution_handler"),
            "status": "executed",
        }

    def _coerce_datetime(self, value: str | datetime) -> datetime:
        """Convert value to datetime."""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


# ─────────────────────────────────────────────────────────────────────────────
# Migration Checklist
# ─────────────────────────────────────────────────────────────────────────────

"""
MIGRATION CHECKLIST FOR action_pipeline.py:

Phase 1: Preparation
  □ Add import: from apps.api.core.state_machine import ActionState, StateMachine, TransitionError
  □ Add import: from apps.api.core.event_bus import get_event_bus
  □ Keep old LifecycleState for backward compatibility (deprecate gradually)
  □ Add _load_state_machine() helper method
  □ Add _emit_transition_event() helper method

Phase 2: Refactor Core Methods
  □ register_proposed_action() - emit via StateMachine + EventBus
  □ approve_actions() - route through StateMachine.transition_to()
  □ reject_actions() - route through StateMachine.transition_to()
  □ execute_approved_actions() - add error handling, failed state transitions
  □ Add new: process_failed_actions_refactored() - retry logic
  □ reject_action_timeout() - use StateMachine, emit event

Phase 3: Persistence
  □ Update action model to store retry_count, last_error, next_retry_time
  □ Update transition storage to use StateTransitionEvent.to_dict()
  □ Add idempotency_key to action model
  □ Add retry_metadata to persisted action

Phase 4: Testing
  □ Test invalid transitions raise TransitionError
  □ Test retry logic (count, backoff delay)
  □ Test idempotency (duplicate requests)
  □ Test timeout enforcement
  □ Test error classification
  □ Test event emission
  □ Test backward compatibility

Phase 5: Orchestrator Integration
  □ Update orchestrator to call process_failed_actions()
  □ Add retry background task/trigger
  □ Add timeout monitoring/trigger
  □ Pass execution errors through classify_error()

Phase 6: Async Hardening
  □ Add tests for concurrent transitions
  □ Add tests for race conditions on action updates
  □ Verify idempotency under load
"""
