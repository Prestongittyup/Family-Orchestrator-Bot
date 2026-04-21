from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


UrgencyLevel = Literal["low", "medium", "high"]
ApprovalStatus = Literal["pending", "approved"]


class StateConflictRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conflict_type: str
    severity: UrgencyLevel
    description: str


class HouseholdCurrentStateSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    household_id: str
    reference_time: str
    calendar_event_count: int
    task_count: int
    meal_history_count: int
    active_fitness_goal: str | None = None
    low_inventory_items: list[str] = Field(default_factory=list)
    pending_approval_count: int = 0
    conflicts: list[StateConflictRecord] = Field(default_factory=list)


class HouseholdRecommendedAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    title: str
    description: str
    domain: str
    urgency: UrgencyLevel
    scheduled_for: str | None = None
    approval_required: bool = True
    approval_status: ApprovalStatus = "pending"


class ApprovalGroup(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: str
    title: str
    description: str
    action_ids: list[str] = Field(default_factory=list)
    approval_status: ApprovalStatus = "pending"


class HouseholdDecisionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    intent_summary: str
    current_state_summary: HouseholdCurrentStateSummary
    recommended_action: HouseholdRecommendedAction
    grouped_approvals: list[ApprovalGroup] = Field(default_factory=list)
    reasoning_trace: list[str] = Field(default_factory=list)