from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ScenarioEvent:
    title: str
    start_time: str
    end_time: str
    participants: list[str]
    type: str
    priority_hint: str | None = None


@dataclass(frozen=True)
class HouseholdScenario:
    scenario_id: str
    description: str
    household_members: list[str] = field(default_factory=list)
    events: list[ScenarioEvent] = field(default_factory=list)
    expected_signals: dict[str, Any] = field(default_factory=dict)
    expected_outcomes: dict[str, Any] = field(default_factory=dict)
