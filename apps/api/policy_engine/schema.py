"""
Policy Engine - Schema Definitions
===================================

Defines policy decisions, inputs, and outputs for the guardrails layer.

Design principles:
  - Deterministic decision-making
  - Explicit rule-driven (no AI/heuristics)
  - Safe-by-default (unknown actions require confirmation/block)
  - Immutable frozen dataclasses
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from apps.api.intent_contract.schema import IntentType


# ---------------------------------------------------------------------------
# POLICY DECISION ENUM
# ---------------------------------------------------------------------------


class PolicyDecision(str, Enum):
    """Enumeration of all possible policy decisions."""

    ALLOW = "allow"
    REQUIRE_CONFIRMATION = "require_confirmation"
    BLOCK = "block"

    @property
    def allows_execution(self) -> bool:
        """Returns True if this decision allows action to execute (without confirmation)."""
        return self == PolicyDecision.ALLOW

    @property
    def requires_user_input(self) -> bool:
        """Returns True if this decision requires user confirmation."""
        return self == PolicyDecision.REQUIRE_CONFIRMATION

    @property
    def prevents_execution(self) -> bool:
        """Returns True if this decision blocks execution entirely."""
        return self == PolicyDecision.BLOCK


# ---------------------------------------------------------------------------
# POLICY INPUT
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PolicyInput:
    """
    Input to the policy engine for decision-making.

    Represents the action being evaluated for safety/approval.

    Fields:
      intent_type: the intent type (from intent contract)
      action_count: number of actions in the plan
      entity_ids: set of entity IDs being referenced (task_id, event_id, plan_id)
      plan_id: optional plan ID if this is part of a plan operation
      family_id: optional family/user context for multi-user systems
      scope_estimate: optional estimate of action scope (e.g., "single", "bulk", "all")
    """

    intent_type: IntentType
    action_count: int = 1
    entity_ids: List[str] = field(default_factory=list)
    plan_id: Optional[str] = None
    family_id: Optional[str] = None
    scope_estimate: Optional[str] = None

    def __str__(self):
        return f"PolicyInput({self.intent_type.value}, {self.action_count} action(s))"


# ---------------------------------------------------------------------------
# POLICY RESULT
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PolicyResult:
    """
    Result of policy evaluation.

    Fields:
      decision: ALLOW, REQUIRE_CONFIRMATION, or BLOCK
      reason_code: machine-readable code for XAI/logging (e.g., "destructive_operation")
      message: human-readable explanation for user/logs
      rule_name: which rule was matched
      evaluated_at: timestamp of evaluation
    """

    decision: PolicyDecision
    reason_code: str
    message: str
    rule_name: str = "default"

    def __str__(self):
        return f"PolicyResult({self.decision.value}: {self.message})"

    @property
    def is_allowed(self) -> bool:
        """Shorthand: decision == ALLOW."""
        return self.decision == PolicyDecision.ALLOW

    @property
    def is_blocked(self) -> bool:
        """Shorthand: decision == BLOCK."""
        return self.decision == PolicyDecision.BLOCK

    @property
    def needs_confirmation(self) -> bool:
        """Shorthand: decision == REQUIRE_CONFIRMATION."""
        return self.decision == PolicyDecision.REQUIRE_CONFIRMATION


# ---------------------------------------------------------------------------
# POLICY RULE DEFINITION
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PolicyRule:
    """
    A single policy rule for evaluation.

    Defines when a rule matches and what decision to make.

    Fields:
      rule_name: unique identifier (e.g., "create_task_allowed")
      intent_types: set of intent types this rule applies to (None = all)
      decision: what decision to make when this rule matches
      reason_code: machine-readable code for this decision
      message: human-readable explanation
      priority: higher priority rules are evaluated first (default: 0)
      requires_confirmation_reason: optional reason code for REQUIRE_CONFIRMATION
    """

    rule_name: str
    intent_types: List[IntentType]
    decision: PolicyDecision
    reason_code: str
    message: str
    priority: int = 0

    def matches(self, intent_type: IntentType) -> bool:
        """Check if this rule applies to the given intent type."""
        return intent_type in self.intent_types

    def __str__(self):
        types = ", ".join(t.value for t in self.intent_types[:2])
        if len(self.intent_types) > 2:
            types += f", +{len(self.intent_types) - 2}"
        return f"PolicyRule({self.rule_name}: {types} → {self.decision.value})"


# ---------------------------------------------------------------------------
# POLICY CONFIGURATION
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PolicyConfig:
    """
    Configuration for the policy engine.

    Fields:
      default_decision: decision if no rule matches (default: REQUIRE_CONFIRMATION for safety)
      allow_bulk_operations: whether to allow bulk operations without confirmation
      require_deletion_confirmation: always require confirmation for deletions
      max_scope_without_confirmation: max entity count before requiring confirmation
      block_unknown_intents: whether to block intents not in the rule set
    """

    default_decision: PolicyDecision = PolicyDecision.REQUIRE_CONFIRMATION
    allow_bulk_operations: bool = False
    require_deletion_confirmation: bool = True
    max_scope_without_confirmation: int = 10
    block_unknown_intents: bool = False
