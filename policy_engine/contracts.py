from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


Priority = Literal["low", "medium", "high"]
ImpactArea = Literal["operational", "evaluation", "simulation"]


class HouseholdMemoryBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    preferences: list[str]
    patterns: list[str]
    constraints: list[str]
    routines: list[str]


class HouseholdMemorySnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    household_id: str
    updated_at: str
    memory: HouseholdMemoryBody


class PolicySuggestion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    policy_type: str
    description: str
    reasoning: str
    confidence: float
    impact_area: list[ImpactArea]


class PolicySummaryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    policies: list[PolicySuggestion]


class ItineraryBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    time_block: str
    event: str
    reason: str
    priority: Priority


class ItineraryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: str
    recommended_itinerary: list[ItineraryBlock]
    conflicts_detected: list[str]
    optimization_notes: list[str]


class PolicyRecomputeResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    memory_snapshot: HouseholdMemorySnapshot
    policy_summary: PolicySummaryResponse
    itinerary: ItineraryResponse