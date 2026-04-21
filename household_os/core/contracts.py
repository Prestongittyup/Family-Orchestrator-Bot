from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


UrgencyLevel = Literal["low", "medium", "high"]
ApprovalStatus = Literal["pending", "approved"]


class IntentInterpretation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str
    urgency: UrgencyLevel
    extracted_signals: list[str] = Field(default_factory=list)


class CurrentStateSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    household_id: str
    reference_time: str
    calendar_events: int
    open_tasks: int
    meals_recorded: int
    low_grocery_items: list[str] = Field(default_factory=list)
    fitness_routines: int
    constraints_count: int
    pending_approvals: int
    state_version: int


class RecommendedNextAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    title: str
    description: str
    urgency: UrgencyLevel
    scheduled_for: str | None = None
    approval_required: bool = True
    approval_status: ApprovalStatus = "pending"


class GroupedApprovalPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: str
    label: str
    action_ids: list[str] = Field(default_factory=list)
    execution_mode: str = "inert_until_approved"
    approval_status: ApprovalStatus = "pending"


class HouseholdOSRunResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    intent_interpretation: IntentInterpretation
    current_state_summary: CurrentStateSummary
    recommended_action: RecommendedNextAction
    follow_ups: list[str] = Field(default_factory=list, max_length=3)
    grouped_approval_payload: GroupedApprovalPayload
    reasoning_trace: list[str] = Field(default_factory=list)
