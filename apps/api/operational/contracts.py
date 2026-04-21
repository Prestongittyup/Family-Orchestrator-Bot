from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class PriorityItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str
    priority_level: str
    reason: str


class ScheduleActionItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: str
    time: str
    confidence: float


class ConflictItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conflict_type: str
    severity: str
    description: str


class OperationalResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    timestamp: str
    household_id: str
    top_priorities: list[PriorityItem]
    schedule_actions: list[ScheduleActionItem]
    conflicts: list[ConflictItem]
    system_notes: list[str]
