from __future__ import annotations

from typing import Any, Literal, TypedDict

# brief_v1 top-level frozen contract keys.
BRIEF_V1_REQUIRED_FIELDS: tuple[str, ...] = (
    "scheduled_actions",
    "unscheduled_actions",
    "priorities",
    "warnings",
    "risks",
    "summary",
)

BRIEF_V1_ALLOWED_FIELDS: frozenset[str] = frozenset(BRIEF_V1_REQUIRED_FIELDS)

# brief_v1 deterministic ordering rules (documentation/specification only).
# These are intentionally not enforced in this module.
BRIEF_V1_ORDERING_RULES: dict[str, str] = {
    "scheduled_actions": (
        "Sort ascending by ordering_position; then by start_time; then by title; "
        "then by source_module. Tie-break fallback must ignore proposal_id."
    ),
    "unscheduled_actions": (
        "Sort ascending by ordering_position; then descending by normalized_priority; "
        "then by title; then by source_module. Tie-break fallback must ignore proposal_id."
    ),
    "priorities": (
        "Sort descending by normalized_priority; then descending by score; "
        "then ascending by title; then ascending by source_module; then ascending by rank. "
        "Tie-break fallback must ignore proposal_id."
    ),
}

BRIEF_V1_TIE_BREAK_RULE: str = (
    "For semantic equality and deterministic ordering verification, proposal_id is ignored "
    "as a tie-breaker because it can vary across equivalent runs."
)


class BriefV1Action(TypedDict, total=False):
    proposal_id: str
    title: str
    description: str
    source_module: str
    decision_type: Literal["scheduled", "deferred"]
    reason: str
    confidence: float
    normalized_priority: float
    ordering_position: int
    time_bucket: Literal["morning", "afternoon", "evening"] | str
    score: float
    duration_units: int
    duration: int
    start_time: str
    end_time: str


class BriefV1Priority(TypedDict, total=False):
    rank: int
    proposal_id: str
    title: str
    source_module: str
    normalized_priority: float
    score: float
    urgency_score: float
    context_score: float


class BriefV1Warning(TypedDict, total=False):
    code: str
    message: str
    severity: str


class BriefV1Risk(TypedDict, total=False):
    code: str
    message: str
    severity: str


class BriefV1(TypedDict):
    scheduled_actions: list[BriefV1Action]
    unscheduled_actions: list[BriefV1Action]
    priorities: list[BriefV1Priority]
    warnings: list[BriefV1Warning | dict[str, Any]]
    risks: list[BriefV1Risk | dict[str, Any]]
    summary: str
