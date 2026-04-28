"""
Unified UI Bootstrap Read Models
================================

Deterministic, read-only models for /v1/ui/bootstrap.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Literal


@dataclass(frozen=True)
class FamilyView:
    family_id: str
    shared_calendar_ref: str
    default_time_zone: str
    member_count: int
    active_plan_ids: list[str]


@dataclass(frozen=True)
class PlanView:
    plan_id: str
    family_id: str
    title: str
    status: str
    linked_tasks: list[str]
    revision: int
    stability_state: str
    last_recomputed_at: str | None


@dataclass(frozen=True)
class TaskView:
    task_id: str
    plan_id: str
    assigned_to: str
    status: str
    due_time: str | None
    priority: str
    title: str


@dataclass(frozen=True)
class EventView:
    event_id: str
    family_id: str
    title: str
    start: str
    end: str
    participants: list[str]
    source: str


@dataclass(frozen=True)
class ConversationSessionView:
    session_id: str
    user_id: str
    state: str
    active_intent_summary: dict[str, Any]
    last_user_message: str | None
    last_updated: datetime


@dataclass(frozen=True)
class PartialIntentView:
    session_id: str
    intent_type: str | None
    extracted_fields: dict[str, Any]
    missing_fields: list[str]
    ambiguous_fields: list[str]
    confidence: float


@dataclass(frozen=True)
class XAIExplanationView:
    explanation_id: str
    entity_type: str
    entity_id: str
    change_type: str
    reason_code: str
    explanation_text: str
    timestamp: datetime


@dataclass(frozen=True)
class SystemStateView:
    mode: Literal["NORMAL", "DEGRADED", "RECONCILIATION_HEAVY", "QUARANTINE_FOCUSED", "HALTED"]
    health_score: float
    active_repair_count: int
    last_reconciliation_at: datetime


@dataclass(frozen=True)
class BootstrapMetadata:
    projection_version: int
    projection_epoch: str
    source_watermark: str
    generated_at: datetime
    staleness_ms: int
    degraded_components: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class UIBootstrapResponse:
    family: FamilyView
    plans: list[PlanView]
    tasks: list[TaskView]
    events: list[EventView]
    conversation_sessions: list[ConversationSessionView]
    pending_intents: list[PartialIntentView]
    system_state: SystemStateView
    xai_recent: list[XAIExplanationView]
    metadata: BootstrapMetadata

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        return _serialize_datetimes(payload)


def _serialize_datetimes(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, list):
        return [_serialize_datetimes(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _serialize_datetimes(v) for k, v in obj.items()}
    return obj
