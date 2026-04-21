"""
Intent Action Planner - Action Plan Generation
===============================================

Transforms validated intents into deterministic action plans.

Design:
  - 1:1 mapping from intent to actions
  - Deterministic idempotency keys (SHA-256 hash)
  - Sequence numbering for multi-action intents
  - All fields propagated from intent to action parameters
  - Safe-by-default: no guessing, explicit error handling
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List

from apps.api.intent_contract.schema import IntentType
from apps.api.intent_contract.validator import ValidatedIntent, ValidationError_


# ---------------------------------------------------------------------------
# ACTION PLAN TYPES
# ---------------------------------------------------------------------------


@dataclass
class Action:
    """
    Single action in an action plan.

    Fields:
      action_type: string identifying the action to take
      parameters: dict of parameters for the action
      idempotency_key: deterministic key for idempotent retry
      sequence_number: order in multi-action plans
    """

    action_type: str
    parameters: Dict[str, Any]
    idempotency_key: str
    sequence_number: int

    def __str__(self):
        return f"Action({self.action_type}, seq={self.sequence_number}, key={self.idempotency_key[:8]}...)"


@dataclass
class ActionPlan:
    """
    Complete action plan generated from a validated intent.

    Fields:
      intent_type: the intent that generated this plan
      actions: list of Action objects in execution order
      validated_data: the original validated intent data
      generated_at: timestamp when the plan was generated
    """

    intent_type: IntentType
    actions: List[Action] = field(default_factory=list)
    validated_data: Dict[str, Any] = field(default_factory=dict)
    generated_at: datetime = field(default_factory=datetime.utcnow)

    def __str__(self):
        return f"ActionPlan({self.intent_type.value}, {len(self.actions)} action(s))"


# ---------------------------------------------------------------------------
# ACTION PLANNER
# ---------------------------------------------------------------------------


class ActionPlanner:
    """
    Generates deterministic action plans from validated intents.

    Safe-by-default: returns None if intent type is not recognized.
    """

    def plan(self, validated: ValidatedIntent) -> ActionPlan | None:
        """
        Generate an action plan from a validated intent.

        Args:
            validated: ValidatedIntent from validator

        Returns:
            ActionPlan with all actions sequenced and idempotent, or None if unknown intent

        Raises:
            No exceptions; all errors are handled gracefully
        """
        # Route to per-intent handler
        handler = self._get_handler(validated.intent_type)
        if not handler:
            return None

        # Generate actions using the handler
        actions = handler(validated.validated_data)

        # Build and return the plan
        return ActionPlan(
            intent_type=validated.intent_type,
            actions=actions,
            validated_data=validated.validated_data,
            generated_at=datetime.utcnow(),
        )

    def _get_handler(self, intent_type: IntentType):
        """Get the handler function for the given intent type."""
        handlers = {
            IntentType.CREATE_TASK: self._handle_create_task,
            IntentType.COMPLETE_TASK: self._handle_complete_task,
            IntentType.RESCHEDULE_TASK: self._handle_reschedule_task,
            IntentType.CREATE_EVENT: self._handle_create_event,
            IntentType.UPDATE_EVENT: self._handle_update_event,
            IntentType.DELETE_EVENT: self._handle_delete_event,
            IntentType.CREATE_PLAN: self._handle_create_plan,
            IntentType.UPDATE_PLAN: self._handle_update_plan,
            IntentType.RECOMPUTE_PLAN: self._handle_recompute_plan,
        }
        return handlers.get(intent_type)

    # -----------------------------------------------
    # TASK INTENT HANDLERS
    # -----------------------------------------------

    def _handle_create_task(self, data: Dict[str, Any]) -> List[Action]:
        """CREATE_TASK: single action to create task."""
        idempotency_key = self._generate_idempotency_key(IntentType.CREATE_TASK, data)
        return [
            Action(
                action_type="create_task",
                parameters={
                    "task_name": data.get("task_name"),
                    "due_time": data.get("due_time"),
                    "plan_id": data.get("plan_id"),
                },
                idempotency_key=idempotency_key,
                sequence_number=1,
            )
        ]

    def _handle_complete_task(self, data: Dict[str, Any]) -> List[Action]:
        """COMPLETE_TASK: single action to mark task complete."""
        idempotency_key = self._generate_idempotency_key(IntentType.COMPLETE_TASK, data)
        return [
            Action(
                action_type="mark_task_complete",
                parameters={"task_id": data.get("task_id")},
                idempotency_key=idempotency_key,
                sequence_number=1,
            )
        ]

    def _handle_reschedule_task(self, data: Dict[str, Any]) -> List[Action]:
        """RESCHEDULE_TASK: single action to reschedule task."""
        idempotency_key = self._generate_idempotency_key(IntentType.RESCHEDULE_TASK, data)
        return [
            Action(
                action_type="reschedule_task",
                parameters={
                    "task_id": data.get("task_id"),
                    "new_time": data.get("new_time"),
                    "reason": data.get("reason"),
                },
                idempotency_key=idempotency_key,
                sequence_number=1,
            )
        ]

    # -----------------------------------------------
    # EVENT INTENT HANDLERS
    # -----------------------------------------------

    def _handle_create_event(self, data: Dict[str, Any]) -> List[Action]:
        """CREATE_EVENT: single action to create event."""
        idempotency_key = self._generate_idempotency_key(IntentType.CREATE_EVENT, data)
        return [
            Action(
                action_type="create_event",
                parameters={
                    "event_name": data.get("event_name"),
                    "start_time": data.get("start_time"),
                    "end_time": data.get("end_time"),
                    "description": data.get("description"),
                },
                idempotency_key=idempotency_key,
                sequence_number=1,
            )
        ]

    def _handle_update_event(self, data: Dict[str, Any]) -> List[Action]:
        """UPDATE_EVENT: single action to update event."""
        idempotency_key = self._generate_idempotency_key(IntentType.UPDATE_EVENT, data)
        return [
            Action(
                action_type="update_event",
                parameters={
                    "event_id": data.get("event_id"),
                    "event_name": data.get("event_name"),
                    "start_time": data.get("start_time"),
                    "end_time": data.get("end_time"),
                    "description": data.get("description"),
                },
                idempotency_key=idempotency_key,
                sequence_number=1,
            )
        ]

    def _handle_delete_event(self, data: Dict[str, Any]) -> List[Action]:
        """DELETE_EVENT: single action to delete event."""
        idempotency_key = self._generate_idempotency_key(IntentType.DELETE_EVENT, data)
        return [
            Action(
                action_type="delete_event",
                parameters={
                    "event_id": data.get("event_id"),
                    "reason": data.get("reason"),
                },
                idempotency_key=idempotency_key,
                sequence_number=1,
            )
        ]

    # -----------------------------------------------
    # PLAN INTENT HANDLERS
    # -----------------------------------------------

    def _handle_create_plan(self, data: Dict[str, Any]) -> List[Action]:
        """CREATE_PLAN: single action to create plan."""
        idempotency_key = self._generate_idempotency_key(IntentType.CREATE_PLAN, data)
        return [
            Action(
                action_type="create_plan",
                parameters={
                    "plan_name": data.get("plan_name"),
                    "description": data.get("description"),
                    "start_date": data.get("start_date"),
                    "end_date": data.get("end_date"),
                },
                idempotency_key=idempotency_key,
                sequence_number=1,
            )
        ]

    def _handle_update_plan(self, data: Dict[str, Any]) -> List[Action]:
        """UPDATE_PLAN: single action to update plan."""
        idempotency_key = self._generate_idempotency_key(IntentType.UPDATE_PLAN, data)
        return [
            Action(
                action_type="update_plan",
                parameters={
                    "plan_id": data.get("plan_id"),
                    "plan_name": data.get("plan_name"),
                    "description": data.get("description"),
                    "start_date": data.get("start_date"),
                    "end_date": data.get("end_date"),
                },
                idempotency_key=idempotency_key,
                sequence_number=1,
            )
        ]

    def _handle_recompute_plan(self, data: Dict[str, Any]) -> List[Action]:
        """RECOMPUTE_PLAN: single action to recompute plan."""
        idempotency_key = self._generate_idempotency_key(IntentType.RECOMPUTE_PLAN, data)
        return [
            Action(
                action_type="recompute_plan",
                parameters={
                    "plan_id": data.get("plan_id"),
                    "reason": data.get("reason"),
                },
                idempotency_key=idempotency_key,
                sequence_number=1,
            )
        ]

    # -----------------------------------------------
    # IDEMPOTENCY KEY GENERATION
    # -----------------------------------------------

    def _generate_idempotency_key(self, intent_type: IntentType, data: Dict[str, Any]) -> str:
        """
        Generate deterministic idempotency key.

        Uses SHA-256 hash of (intent_type, fields, timestamp_second).
        Same input always produces same key.

        Args:
            intent_type: the intent type
            data: the validated data dict

        Returns:
            40-char hex string (first 40 chars of SHA-256 hash)
        """
        # Build hashable dict with intent type and all fields
        hashable = {
            "intent_type": intent_type.value,
            "fields": self._serialize_for_hash(data),
        }

        # Hash to generate key
        key_bytes = json.dumps(hashable, sort_keys=True).encode("utf-8")
        full_hash = hashlib.sha256(key_bytes).hexdigest()

        # Return first 40 chars (160 bits)
        return full_hash[:40]

    def _serialize_for_hash(self, obj: Any) -> Any:
        """
        Prepare an object for JSON serialization in hash generation.

        Converts datetime objects to ISO strings, handles nested dicts/lists.
        """
        if isinstance(obj, dict):
            return {k: self._serialize_for_hash(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._serialize_for_hash(v) for v in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj
