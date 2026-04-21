from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class FamilySummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    family_id: str
    member_count: int
    member_names: list[str] = Field(default_factory=list)
    default_time_zone: str


class TodayOverview(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: str
    open_task_count: int
    scheduled_event_count: int
    active_plan_count: int
    notification_count: int


class PlanSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plan_id: str
    title: str
    status: str
    revision: int
    linked_task_count: int


class TaskSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: str
    title: str
    plan_id: str
    assigned_to: str
    status: str
    priority: str
    due_time: str | None = None


class TaskBoardState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pending: list[TaskSummary] = Field(default_factory=list)
    in_progress: list[TaskSummary] = Field(default_factory=list)
    completed: list[TaskSummary] = Field(default_factory=list)
    failed: list[TaskSummary] = Field(default_factory=list)


class CalendarEventSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    title: str
    start: str
    end: str
    participants: list[str] = Field(default_factory=list)


class CalendarState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    window_start: str
    window_end: str
    events: list[CalendarEventSummary] = Field(default_factory=list)


class Notification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    notification_id: str
    title: str
    message: str
    level: Literal["info", "warning", "critical"]
    related_entity: str | None = None


class XAIExplanationSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    explanation_id: str
    entity_type: str
    entity_id: str
    summary: str
    timestamp: str


class SystemHealthSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["healthy", "degraded"]
    pending_actions: int
    stale_projection: bool
    state_version: int
    last_updated: str


class UIBootstrapState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    snapshot_version: int
    source_watermark: str
    family: FamilySummary
    today_overview: TodayOverview
    active_plans: list[PlanSummary] = Field(default_factory=list)
    task_board: TaskBoardState
    calendar: CalendarState
    notifications: list[Notification] = Field(default_factory=list)
    explanation_digest: list[XAIExplanationSummary] = Field(default_factory=list)
    system_health: SystemHealthSnapshot


class UIPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entity_type: Literal["task", "plan", "event", "family", "notification"]
    entity_id: str
    change_type: Literal["create", "update", "delete", "replace"]
    payload: dict[str, Any] = Field(default_factory=dict)
    version: int
    source_timestamp: datetime


class ActionCard(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: Literal["confirm", "reschedule", "approve", "reject", "edit"]
    title: str
    description: str
    related_entity: str
    required_action_payload: dict[str, Any] = Field(default_factory=dict)
    risk_level: Literal["low", "medium", "high"]


class ChatMessageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    family_id: str
    message: str
    session_id: str


class ChatResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    assistant_message: str
    action_cards: list[ActionCard] = Field(default_factory=list)
    ui_patch: list[UIPatch] = Field(default_factory=list)
    requires_confirmation: bool
    explanation_summary: list[XAIExplanationSummary] = Field(default_factory=list)


class ActionExecutionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    family_id: str
    session_id: str
    action_card_id: str
    payload: dict[str, Any] = Field(default_factory=dict)
