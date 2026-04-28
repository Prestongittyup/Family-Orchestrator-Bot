from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


IntentType = Literal["appointment", "meal", "fitness", "general"]
PriorityLevel = Literal["low", "medium", "high"]
SeverityLevel = Literal["low", "medium", "high"]
ApprovalStatus = Literal["pending", "approved"]


class AssistantIntent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    intent_type: IntentType
    entities: list[str]
    time_constraints: list[str]
    priority: PriorityLevel
    context_flags: list[str]


class TimelineBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    time_block: str
    title: str
    rationale: str
    confidence: float


class ScheduleCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_id: str
    label: str
    blocks: list[TimelineBlock]
    confidence: float


class RecommendedPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str
    timeline_blocks: list[TimelineBlock]
    confidence: float
    reasoning: str


class FallbackOption(BaseModel):
    model_config = ConfigDict(extra="forbid")

    option_id: str
    description: str
    tradeoffs: list[str]


class MealSuggestion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recipe_name: str
    meal_type: str
    ingredients_used: list[str]
    grocery_additions: list[str]
    nutrition_balance: list[str]
    repeat_window_days: int


class FitnessSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    day: str
    time_block: str
    focus: str
    duration_minutes: int
    rationale: str


class FitnessPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    goal: str
    weekly_summary: str
    sessions: list[FitnessSession]
    insertion_suggestions: list[TimelineBlock]


class AssistantPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    domain: str
    summary: str
    candidate_schedules: list[ScheduleCandidate]
    recommended_plan: RecommendedPlan
    fallback_options: list[FallbackOption]
    meal_plan: MealSuggestion | None = None
    fitness_plan: FitnessPlan | None = None


class ConflictRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conflict_type: str
    severity: SeverityLevel
    description: str
    impacted_blocks: list[str]


class AlternativeRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: str
    description: str
    confidence: float


class ProposedAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    action_type: str
    description: str
    target: str
    approval_status: ApprovalStatus
    execution_mode: str


class AssistantResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    intent: AssistantIntent
    plan: AssistantPlan
    conflicts: list[ConflictRecord]
    alternatives: list[AlternativeRecord]
    proposed_actions: list[ProposedAction]
    reasoning_trace: list[str]


class AssistantQueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(min_length=3)
    household_id: str = "household-001"
    repeat_window_days: int = Field(default=10, ge=7, le=14)
    fitness_goal: str | None = None


class AssistantApprovalRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    action_ids: list[str] = Field(default_factory=list)