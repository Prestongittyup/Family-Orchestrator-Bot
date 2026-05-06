from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


PriorityLabel = Literal["high", "medium", "low"]


@dataclass(frozen=True)
class RuleAction:
    type: Literal["reply", "task"]
    title: str
    due: str | None = None

    def to_payload(self) -> dict[str, str | None]:
        return {
            "type": self.type,
            "title": self.title,
            "due": self.due,
        }


@dataclass(frozen=True)
class EmailRuleResult:
    priority: PriorityLabel
    needs_attention: bool
    actions: list[RuleAction]
    state_summary: str
    reason: str
    ambiguous: bool

    def to_payload(self) -> dict[str, object]:
        return {
            "priority": self.priority,
            "needs_attention": self.needs_attention,
            "actions": [action.to_payload() for action in self.actions],
            "state_summary": self.state_summary,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class PantryRuleResult:
    suggested_meals: list[str]
    state_summary: str
    reason: str
    ambiguous: bool

    def to_payload(self) -> dict[str, object]:
        return {
            "state_summary": self.state_summary,
            "suggested_meals": list(self.suggested_meals),
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ScheduleRuleResult:
    suggestions: list[str]
    state_summary: str
    reason: str
    ambiguous: bool

    def to_payload(self) -> dict[str, object]:
        return {
            "state_summary": self.state_summary,
            "suggestions": list(self.suggestions),
            "reason": self.reason,
        }
