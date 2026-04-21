from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from apps.assistant_core.contracts import AssistantIntent, ProposedAction


class SnapshotCalendarEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    title: str
    start: str
    end: str


class SnapshotMealRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recipe_name: str
    served_on: str


class SnapshotFitnessSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    day: str
    time_block: str
    focus: str


class AssistantStateSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    calendar_events: list[SnapshotCalendarEvent]
    recent_meals: list[SnapshotMealRecord]
    fitness_schedule: list[SnapshotFitnessSession]
    household_context: dict[str, Any]


class AssistantProposal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    proposal_id: str
    domain: str
    title: str
    summary: str
    confidence: float
    rationale: str
    time_blocks: list[str] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)


class MergedConflict(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conflict_type: str
    severity: str
    description: str
    impacted_proposals: list[str]


class RankedPlanItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rank: int
    proposal_id: str
    domain: str
    title: str
    confidence: float
    rationale: str


class ExecutionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    approval_endpoint: str
    execution_mode: str
    approved: bool = False
    proposed_actions: list[ProposedAction] = Field(default_factory=list)


class AssistantPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    intent: AssistantIntent
    state_snapshot: AssistantStateSnapshot
    proposals: list[AssistantProposal]
    conflicts: list[MergedConflict]
    ranked_plan: list[RankedPlanItem]
    requires_approval: bool
    execution_payload: ExecutionPayload