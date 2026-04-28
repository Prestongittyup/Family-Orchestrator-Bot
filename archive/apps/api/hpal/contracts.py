from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


PlanStatus = Literal["active", "paused", "completed", "failed"]
TaskStatus = Literal["pending", "in_progress", "completed", "failed", "stale_projection"]
PlanStability = Literal["stable", "adjusting", "blocked"]
EventSource = Literal["manual", "system_generated"]


class TimeWindow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: str
    end: str


class PersonModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    person_id: str
    name: str
    role: str
    availability_constraints: list[str] = Field(default_factory=list)
    preferences: dict[str, Any] = Field(default_factory=dict)
    assigned_tasks: list[str] = Field(default_factory=list)
    schedule_overlay: list[dict[str, str]] = Field(default_factory=list)


class PlanModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plan_id: str
    family_id: str
    title: str
    intent_origin: str
    status: PlanStatus
    linked_tasks: list[str] = Field(default_factory=list)
    schedule_window: TimeWindow
    last_recomputed_at: str | None = None
    revision: int = 1
    stability_state: PlanStability = "stable"


class TaskModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: str
    plan_id: str
    assigned_to: str
    status: TaskStatus
    due_time: str | None = None
    auto_generated: bool = True
    priority: str = "medium"
    title: str


class EventModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    family_id: str
    title: str
    time_window: TimeWindow
    participants: list[str] = Field(default_factory=list)
    linked_plans: list[str] = Field(default_factory=list)
    source: EventSource = "manual"


class FamilyModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    family_id: str
    members: list[PersonModel] = Field(default_factory=list)
    shared_calendar_ref: str
    default_time_zone: str
    household_preferences: dict[str, Any] = Field(default_factory=dict)
    active_plans: list[str] = Field(default_factory=list)
    system_state_summary: dict[str, Any] = Field(default_factory=dict)


class HouseholdOverview(BaseModel):
    model_config = ConfigDict(extra="forbid")

    family: FamilyModel
    today_events: list[EventModel] = Field(default_factory=list)
    active_plan_count: int = 0
    pending_task_count: int = 0
    completed_task_count: int = 0


class CreateFamilyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    family_id: str
    name: str = "Family"
    shared_calendar_ref: str = "primary"
    default_time_zone: str = "UTC"
    household_preferences: dict[str, Any] = Field(default_factory=dict)
    initial_members: list[PersonModel] = Field(default_factory=list)


class UpdateMemberRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    role: str | None = None
    availability_constraints: list[str] | None = None
    preferences: dict[str, Any] | None = None


class CreatePlanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str
    intent_origin: str
    schedule_window: TimeWindow
    participants: list[str] = Field(default_factory=list)
    priority_hint: str = "medium"
    idempotency_key: str


class UpdatePlanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str | None = None
    schedule_window: TimeWindow | None = None
    participants: list[str] | None = None
    status: Literal["active", "paused"] | None = None
    idempotency_key: str


class RecomputePlanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str
    idempotency_key: str


class CreateEventRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str
    time_window: TimeWindow
    participants: list[str] = Field(default_factory=list)
    source: EventSource = "manual"
    idempotency_key: str


class LinkEventPlanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plan_id: str
    idempotency_key: str


class InternalTaskStatusRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_status: TaskStatus
    reason_code: str


class InternalTaskRescheduleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    due_time: str
    reason_code: str


class CommandResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_id: str
    status: Literal["accepted", "replayed"]
    submitted_at: str


class ProductFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    message: str


FAILURE_TRANSLATION: dict[str, ProductFailure] = {
    "dag_conflict": ProductFailure(code="plan_adjusting", message="Plan adjusting"),
    "lease_conflict": ProductFailure(code="task_delayed", message="Task delayed"),
    "invariant_violation": ProductFailure(code="plan_blocked", message="Plan blocked"),
    "reconciliation": ProductFailure(code="system_updating_plan", message="System updating plan"),
    "outbox_lag": ProductFailure(code="external_sync_pending", message="External sync pending"),
}


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"
