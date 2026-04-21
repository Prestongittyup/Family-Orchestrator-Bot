from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from household_os.core.contracts import HouseholdOSRunResponse
from household_os.runtime.trigger_detector import RuntimeTrigger


LifecycleState = Literal[
    "proposed",
    "pending_approval",
    "approved",
    "executed",
    "rejected",
    "ignored",
]


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


class ActionPipeline:
    def register_proposed_action(
        self,
        *,
        graph: dict[str, Any],
        trigger: RuntimeTrigger,
        response: HouseholdOSRunResponse,
        now: str | datetime,
    ) -> LifecycleAction:
        timestamp = self._coerce_datetime(now)
        title = response.recommended_action.title
        action = LifecycleAction(
            action_id=response.recommended_action.action_id,
            request_id=response.request_id,
            title=title,
            description=response.recommended_action.description,
            domain=self._infer_domain(response),
            execution_handler=self._infer_execution_handler(response),
            current_state="proposed",
            approval_required=bool(response.recommended_action.approval_required),
            trigger_id=trigger.trigger_id,
            trigger_type=trigger.trigger_type,
            scheduled_for=response.recommended_action.scheduled_for,
            reasoning_trace=list(response.reasoning_trace),
            created_at=self._iso(timestamp),
            updated_at=self._iso(timestamp),
        )

        self._append_transition(
            graph=graph,
            action=action,
            to_state="proposed",
            timestamp=timestamp,
            reason="Decision engine proposed a single action",
        )

        if action.approval_required:
            self._append_transition(
                graph=graph,
                action=action,
                to_state="pending_approval",
                timestamp=timestamp,
                reason="Approval gate engaged before execution",
            )

        graph.setdefault("action_lifecycle", {}).setdefault("actions", {})[action.action_id] = action.model_dump()
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

    def approve_actions(
        self,
        *,
        graph: dict[str, Any],
        request_id: str,
        action_ids: list[str],
        now: str | datetime,
    ) -> list[LifecycleAction]:
        timestamp = self._coerce_datetime(now)
        approved: list[LifecycleAction] = []
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})

        for action_id in action_ids:
            raw = action_map.get(action_id)
            if raw is None:
                continue
            action = LifecycleAction.model_validate(raw)
            if action.request_id != request_id or action.current_state not in {"proposed", "pending_approval"}:
                continue

            self._append_transition(
                graph=graph,
                action=action,
                to_state="approved",
                timestamp=timestamp,
                reason="Action approved for execution",
            )
            action_map[action_id] = action.model_dump()
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

    def reject_actions(
        self,
        *,
        graph: dict[str, Any],
        request_id: str,
        action_ids: list[str],
        now: str | datetime,
    ) -> list[LifecycleAction]:
        timestamp = self._coerce_datetime(now)
        rejected: list[LifecycleAction] = []
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})

        for action_id in action_ids:
            raw = action_map.get(action_id)
            if raw is None:
                continue
            action = LifecycleAction.model_validate(raw)
            if action.request_id != request_id or action.current_state not in {"proposed", "pending_approval"}:
                continue

            self._append_transition(
                graph=graph,
                action=action,
                to_state="rejected",
                timestamp=timestamp,
                reason="Action rejected by user",
            )
            action_map[action_id] = action.model_dump()
            self._record_behavior_feedback(
                graph=graph,
                action=action,
                timestamp=timestamp,
                status="rejected",
                executed=False,
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

    def reject_action_timeout(
        self,
        *,
        graph: dict[str, Any],
        trigger: RuntimeTrigger,
        now: str | datetime,
    ) -> LifecycleAction | None:
        timestamp = self._coerce_datetime(now)
        action_id = str(trigger.metadata.get("action_id", ""))
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})
        raw = action_map.get(action_id)
        if raw is None:
            return None

        action = LifecycleAction.model_validate(raw)
        if action.current_state != "pending_approval":
            return None

        self._append_transition(
            graph=graph,
            action=action,
            to_state="ignored",
            timestamp=timestamp,
            reason="Approval timeout expired without confirmation",
            metadata={"trigger_id": trigger.trigger_id},
        )
        action_map[action.action_id] = action.model_dump()
        self._record_behavior_feedback(
            graph=graph,
            action=action,
            timestamp=timestamp,
            status="ignored",
            executed=False,
            actual_execution_time=None,
        )
        graph.setdefault("event_history", []).append(
            {
                "event_type": "action_ignored",
                "action_id": action.action_id,
                "request_id": action.request_id,
                "recorded_at": self._iso(timestamp),
            }
        )
        return action

    def execute_approved_actions(
        self,
        *,
        graph: dict[str, Any],
        now: str | datetime,
    ) -> list[LifecycleAction]:
        timestamp = self._coerce_datetime(now)
        executed: list[LifecycleAction] = []
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})

        for action_id in sorted(action_map):
            action = LifecycleAction.model_validate(action_map[action_id])
            if action.current_state != "approved":
                continue

            action.execution_result = self._execute_action(graph=graph, action=action, timestamp=timestamp)
            self._append_transition(
                graph=graph,
                action=action,
                to_state="executed",
                timestamp=timestamp,
                reason=f"Action executed via {action.execution_handler}",
                metadata=action.execution_result,
            )
            action_map[action_id] = action.model_dump()
            graph.setdefault("execution_log", []).append(action.execution_result)
            self._record_behavior_feedback(
                graph=graph,
                action=action,
                timestamp=timestamp,
                status="approved",
                executed=True,
                actual_execution_time=self._resolve_actual_execution_time(action=action, timestamp=timestamp),
            )
            executed.append(action)

        return executed

    def queue_next_day_follow_ups(
        self,
        *,
        graph: dict[str, Any],
        now: str | datetime,
    ) -> list[dict[str, Any]]:
        timestamp = self._coerce_datetime(now)
        queued: list[dict[str, Any]] = []
        action_map = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})
        daily_cycle = graph.setdefault("runtime", {}).setdefault("daily_cycle", {})
        pending_follow_ups = daily_cycle.setdefault("pending_follow_up_queries", [])

        for action_id in sorted(action_map):
            action = LifecycleAction.model_validate(action_map[action_id])
            if action.current_state != "executed" or action.reviewed_in_evening:
                continue

            follow_up = {
                "source_action_id": action.action_id,
                "due_on": (timestamp.date() + timedelta(days=1)).isoformat(),
                "query": self._follow_up_query_for_action(action),
            }
            pending_follow_ups.append(follow_up)
            action.reviewed_in_evening = True
            action.updated_at = self._iso(timestamp)
            action_map[action_id] = action.model_dump()
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
                "status": "executed",
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
                "status": "executed",
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
            "status": "executed",
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
    ) -> None:
        transition = LifecycleTransition(
            from_state=action.current_state if action.transitions else None,
            to_state=to_state,
            changed_at=self._iso(timestamp),
            reason=reason,
            metadata=metadata or {},
        )
        action.current_state = to_state
        action.updated_at = transition.changed_at
        action.transitions.append(transition)
        graph.setdefault("action_lifecycle", {}).setdefault("transition_log", []).append(
            {
                "action_id": action.action_id,
                **transition.model_dump(),
            }
        )

    def _record_behavior_feedback(
        self,
        *,
        graph: dict[str, Any],
        action: LifecycleAction,
        timestamp: datetime,
        status: str,
        executed: bool,
        actual_execution_time: str | None,
    ) -> None:
        records = graph.setdefault("behavior_feedback", {}).setdefault("records", [])
        records.append(
            {
                "action_id": action.action_id,
                "status": status,
                "executed": executed,
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