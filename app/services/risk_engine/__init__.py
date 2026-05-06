from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RiskDecision:
    decision: str
    reason: str


class RiskEngine:
    """Deterministic risk gate placeholder aligned with RFC-001 boundary contracts."""

    def evaluate(self, *, context: dict[str, Any] | None = None) -> RiskDecision:
        _ = context or {}
        return RiskDecision(decision="allow", reason="default_allow")


__all__ = ["RiskDecision", "RiskEngine"]
