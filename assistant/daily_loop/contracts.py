from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from apps.assistant_core.contracts import ProposedAction


DaySegment = Literal["morning", "midday", "evening"]


class DailyScheduleItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_id: str
    title: str
    domain: str
    segment: DaySegment
    start: str
    end: str
    time_block: str
    locked: bool
    rationale: str
    buffer_before_minutes: int = 15
    buffer_after_minutes: int = 15
    source_proposal_id: str | None = None


class DailyMeal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_id: str
    title: str
    meal_type: str
    segment: DaySegment
    time_block: str
    source_proposal_id: str | None = None


class DailyWorkout(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_id: str
    title: str
    focus: str
    segment: DaySegment
    time_block: str
    source_proposal_id: str | None = None


class DailyConflict(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conflict_type: str
    severity: str
    description: str
    impacted_items: list[str] = Field(default_factory=list)


class SchedulingGap(BaseModel):
    model_config = ConfigDict(extra="forbid")

    segment: DaySegment
    start: str
    end: str
    time_block: str
    duration_minutes: int


class DailyApprovalState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    approval_endpoint: str
    requires_approval: bool
    approved: bool = False
    persisted: bool = False
    proposed_actions: list[ProposedAction] = Field(default_factory=list)


class DailyPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: str
    schedule: list[DailyScheduleItem]
    meals: list[DailyMeal]
    workouts: list[DailyWorkout]
    conflicts: list[DailyConflict]
    gaps: list[SchedulingGap]
    approval_state: DailyApprovalState