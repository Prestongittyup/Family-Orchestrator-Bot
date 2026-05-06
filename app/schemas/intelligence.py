from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class UpgradeMetadata(BaseModel):
    upgrade_available: bool = False
    message: str = ""
    benefit: list[str] = Field(default_factory=list)
    recommended_tier: Literal["pro", "premium"] | None = None
    recommended_price: float | None = None


class EmailAction(BaseModel):
    type: Literal["reply", "task"]
    title: str
    due: str | None = None


class EmailMessageIn(BaseModel):
    body: str = Field(default="")


class EmailAnalyzeStreamRequest(BaseModel):
    user_id: str
    thread_id: str
    latest_message_id: str
    sender: str
    subject: str
    to_me: bool = True
    cc_me: bool = False
    prefer_ai: bool = False
    messages: list[EmailMessageIn] = Field(default_factory=list)


class PantrySuggestRequest(BaseModel):
    user_id: str
    items: list[str] = Field(default_factory=list)
    prefer_ai: bool = False


class ScheduleSuggestRequest(BaseModel):
    user_id: str
    title: str
    details: str = ""
    prefer_ai: bool = False


class PantrySuggestResponse(BaseModel):
    mode: Literal["RULE_ONLY", "NEEDS_LLM", "BLOCKED"]
    state_summary: str
    suggested_meals: list[str] = Field(default_factory=list)
    reason: str
    upgrade_available: bool = False
    metadata: UpgradeMetadata = Field(default_factory=UpgradeMetadata)


class ScheduleSuggestResponse(BaseModel):
    mode: Literal["RULE_ONLY", "NEEDS_LLM", "BLOCKED"]
    state_summary: str
    suggestions: list[str] = Field(default_factory=list)
    reason: str
    upgrade_available: bool = False
    metadata: UpgradeMetadata = Field(default_factory=UpgradeMetadata)


class EvaluationEmailInput(BaseModel):
    sender: str
    subject: str
    body: str
    thread: list[str] = Field(default_factory=list)


class EmailEvaluateRequest(BaseModel):
    user_id: str
    email: EvaluationEmailInput


class EmailComparisonMetrics(BaseModel):
    action_agreement: float
    action_match: bool
    priority_delta: bool
    missed_intent_detection: bool
    false_positive_rate: float
    llm_only_actions: list[str] = Field(default_factory=list)
    rules_only_actions: list[str] = Field(default_factory=list)


class EmailEvaluateResponse(BaseModel):
    evaluation_id: str
    user_id: str
    email_id: str
    rule_output: dict[str, object]
    llm_output: dict[str, object]
    comparison: EmailComparisonMetrics
    dls: float
    delta_score: float
    latency_ms: float
    tokens_in: int
    tokens_out: int
    estimated_cost: float
    llm_used: bool
    timestamp: str


class EmailEvaluateBatchRequest(BaseModel):
    user_id: str
    emails: list[EvaluationEmailInput] = Field(min_length=1, max_length=200)


class BatchAggregateMetrics(BaseModel):
    total_emails: int
    avg_dls: float
    avg_delta_score: float
    disagreement_percent: float
    llm_only_detections_percent: float
    llm_new_actions_percent: float
    meaningful_priority_change_percent: float
    positive_lift_ratio: float
    negative_lift_ratio: float
    llm_cost_per_email: float
    llm_usage_percentage: float


class EmailEvaluateBatchResponse(BaseModel):
    aggregate: BatchAggregateMetrics
    evaluations: list[EmailEvaluateResponse]


class AnalyticsDLSResponse(BaseModel):
    avg_dls: float
    positive_lift_ratio: float
    negative_lift_ratio: float
    llm_cost_per_email: float
    rule_only_cost: float
    llm_usage_percentage: float
    total_emails_processed: int
    rolling_avg_dls: float
    rolling_cost_per_email: float
    lift_to_cost: float
    efficiency_score: float
    action_discovery_rate: float
    system_mode: Literal["LLM_CORE", "HYBRID", "RULE_ONLY"]
    alerts: list[str] = Field(default_factory=list)
