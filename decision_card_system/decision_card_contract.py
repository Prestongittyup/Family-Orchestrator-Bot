from __future__ import annotations

from typing import Literal, NotRequired, TypedDict


DecisionCardContractVersion = Literal["v1"]
DecisionCardState = Literal["generated", "surfaced", "acknowledged", "resolved", "applied"]
DecisionCardEventType = Literal[
    "DecisionCardGenerated",
    "DecisionCardSurfaced",
    "DecisionCardAcknowledged",
    "DecisionCardResolved",
    "DecisionCardApplied",
    "DecisionDeferred",
    "DecisionIgnored",
    "DecisionCompleted",
]


class DecisionCardRecord(TypedDict):
    decision_card_id: str
    household_id: str
    title: str
    root_cause_key: str
    contract_version: DecisionCardContractVersion
    origin_api: str
    dedupe_key: str
    state: DecisionCardState
    created_at: str
    updated_at: str
    actor_id: str
    last_event_type: DecisionCardEventType
    metadata: NotRequired[dict[str, object]]
    resolved_at: NotRequired[str]
    applied_at: NotRequired[str]
    defer_to_date: NotRequired[str]


class DecisionCardEventPayload(TypedDict, total=False):
    event_type: DecisionCardEventType
    decision_card_id: str
    household_id: str
    actor_id: str
    title: str
    root_cause_key: str
    contract_version: DecisionCardContractVersion
    origin_api: str
    dedupe_key: str
    timestamp: str
    metadata: dict[str, object]
    defer_to_date: str
    resolution_kind: str
    decision_id: str
