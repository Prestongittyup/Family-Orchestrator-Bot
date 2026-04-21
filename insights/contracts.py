from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


Severity = Literal["low", "medium", "high"]
EvidenceSource = Literal["evaluation", "simulation", "operational"]


class EvaluationScenarioScore(BaseModel):
    model_config = ConfigDict(extra="forbid")

    priority_score: float
    relevance_score: float
    completeness_score: float
    clarity_score: float
    priority_correctness: float
    conflict_handling_score: float
    omission_score: float
    noise_penalty: float


class EvaluationScenario(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scenario_id: str
    description: str
    scores: EvaluationScenarioScore
    issues: list[str]


class EvaluationAggregate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    avg_priority: float
    avg_relevance: float
    avg_completeness: float
    avg_clarity: float
    avg_priority_correctness: float
    avg_conflict_handling: float
    avg_omission: float
    avg_noise_penalty: float


class EvaluationFailurePattern(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str
    count: int
    scenarios: list[str]


class EvaluationArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scenarios: list[EvaluationScenario]
    aggregate: EvaluationAggregate
    comparison: dict
    failure_patterns: list[EvaluationFailurePattern]
    decision_gaps: list[dict]
    recommended_adjustments: list[dict]


class SimulationDecisionDriftMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_drift_score: float
    priority_flip_rate: float
    brief_instability_index: float


class SimulationStabilityScores(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stability_score: float


class SimulationSystemHealth(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_latency_ms: float
    decision_distribution: dict
    conflict_frequency: float
    stability_trend: list[float]


class SimulationAssertions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    priority_stability_under_change: bool
    correct_reordering_of_urgent_events: bool
    no_stale_event_persistence: bool
    correct_conflict_resolution_behavior: bool
    priority_flip_count: int
    stale_event_failures: list[str]


class SimulationArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    simulation_id: str
    generated_at: str
    config: dict
    event_timeline: list[dict]
    brief_outputs_over_time: list[dict]
    brief_evolution: list[dict]
    decision_drift_metrics: SimulationDecisionDriftMetrics
    stability_scores: SimulationStabilityScores
    system_recovery_metrics: dict
    failure_patterns: list[str]
    assertions: SimulationAssertions
    system_health: SimulationSystemHealth


class OperationalSuiteResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command: str
    result: str
    passed: int
    failed: int


class OperationalSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operational_layer_isolated: bool
    strict_contract_enforced: bool
    ui_today_operational_mode_added: bool
    integration_core_logic_unchanged: bool
    evaluation_logic_unchanged: bool


class OperationalTests(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operational_suite: OperationalSuiteResult
    regression_suite: OperationalSuiteResult


class OperationalArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifact: str
    status: str
    summary: OperationalSummary
    operational_endpoints: list[str]
    tests: OperationalTests
    notes: list[str]


class PatternRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str
    frequency: int
    affected_scenarios: list[str]
    severity: Severity
    evidence_sources: list[EvidenceSource]


class InsightItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str
    severity: Severity
    description: str
    evidence_sources: list[EvidenceSource]
    affected_components: list[str]


class SystemHealthSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stability_score: float
    conflict_rate: float
    priority_accuracy_estimate: float


class RecommendationItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recommendation: str
    reason: str
    priority: Severity


class InsightBridgeResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    timestamp: str
    insights: list[InsightItem]
    system_health_summary: SystemHealthSummary
    recommendations: list[RecommendationItem]