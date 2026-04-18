from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

SignalSeverity = Literal["low", "medium", "high"]
EffortLevel = Literal["low", "medium", "high"]
ProposalCategory = Literal["task", "event_prep", "maintenance", "health", "other"]


@dataclass(frozen=True)
class Proposal:
    id: str
    type: str
    title: str
    description: str
    priority: int
    source_module: str
    duration: int = 1
    effort: EffortLevel = "medium"
    category: ProposalCategory = "other"

    def __post_init__(self) -> None:
        if not (1 <= self.priority <= 5):
            raise ValueError("Proposal.priority must be in range 1..5")
        if self.duration < 1:
            raise ValueError("Proposal.duration must be >= 1")
        if self.effort not in {"low", "medium", "high"}:
            raise ValueError("Proposal.effort must be one of: low, medium, high")
        if self.category not in {"task", "event_prep", "maintenance", "health", "other"}:
            raise ValueError("Proposal.category must be one of: task, event_prep, maintenance, health, other")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "title": self.title,
            "description": self.description,
            "priority": self.priority,
            "source_module": self.source_module,
            "duration": self.duration,
            "effort": self.effort,
            "category": self.category,
        }


@dataclass(frozen=True)
class Signal:
    id: str
    type: str
    message: str
    severity: SignalSeverity
    source_module: str

    def __post_init__(self) -> None:
        if self.severity not in {"low", "medium", "high"}:
            raise ValueError("Signal.severity must be one of: low, medium, high")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "message": self.message,
            "severity": self.severity,
            "source_module": self.source_module,
        }


@dataclass(frozen=True)
class ModuleOutput:
    module: str
    proposals: list[Proposal] = field(default_factory=list)
    signals: list[Signal] = field(default_factory=list)
    confidence: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not (0.0 <= self.confidence <= 1.0):
            raise ValueError("ModuleOutput.confidence must be in range 0.0..1.0")
        if self.proposals is None:
            raise ValueError("ModuleOutput.proposals cannot be None")
        if self.signals is None:
            raise ValueError("ModuleOutput.signals cannot be None")
        if self.metadata is None:
            raise ValueError("ModuleOutput.metadata cannot be None")

    def to_dict(self) -> dict[str, Any]:
        return {
            "module": self.module,
            "proposals": [proposal.to_dict() for proposal in self.proposals],
            "signals": [signal.to_dict() for signal in self.signals],
            "confidence": self.confidence,
            "metadata": self.metadata,
        }


def validate_module_output(output: ModuleOutput) -> ModuleOutput:
    if not isinstance(output, ModuleOutput):
        raise TypeError("Each module must return ModuleOutput")

    for proposal in output.proposals:
        if not isinstance(proposal, Proposal):
            raise TypeError("All proposals must be Proposal")

    for signal in output.signals:
        if not isinstance(signal, Signal):
            raise TypeError("All signals must be Signal")

    return output
