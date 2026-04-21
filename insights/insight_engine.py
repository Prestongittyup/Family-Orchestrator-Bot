from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from insights.contracts import (
    EvaluationArtifact,
    InsightBridgeResponse,
    OperationalArtifact,
    SimulationArtifact,
    SystemHealthSummary,
)
from insights.insight_generator import build_insights, build_recommendations
from insights.pattern_analyzer import detect_patterns


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class InsightEngine:
    def __init__(self, root_path: Path | None = None) -> None:
        self.root_path = root_path or Path(__file__).resolve().parent.parent

    def _load_json_file(self, file_name: str) -> dict:
        file_path = self.root_path / file_name
        return json.loads(file_path.read_text(encoding="utf-8"))

    def load_evaluation_artifact(self) -> EvaluationArtifact:
        return EvaluationArtifact.model_validate(self._load_json_file("evaluation_results.json"))

    def load_simulation_artifact(self) -> SimulationArtifact:
        return SimulationArtifact.model_validate(self._load_json_file("simulation_results.json"))

    def load_operational_artifact(self) -> OperationalArtifact:
        return OperationalArtifact.model_validate(self._load_json_file("operational_mode_report.json"))

    def build_system_health_summary(
        self,
        evaluation: EvaluationArtifact,
        simulation: SimulationArtifact,
    ) -> SystemHealthSummary:
        event_count = max(1, len(simulation.event_timeline))
        stability_score = round(float(simulation.stability_scores.stability_score), 4)
        conflict_rate = round(float(simulation.system_health.conflict_frequency) / event_count, 4)
        priority_accuracy_estimate = round(float(evaluation.aggregate.avg_priority_correctness) / 10.0, 4)
        return SystemHealthSummary(
            stability_score=stability_score,
            conflict_rate=conflict_rate,
            priority_accuracy_estimate=priority_accuracy_estimate,
        )

    def build_response(self) -> InsightBridgeResponse:
        evaluation = self.load_evaluation_artifact()
        simulation = self.load_simulation_artifact()
        operational = self.load_operational_artifact()
        patterns = detect_patterns(evaluation, simulation, operational)
        insights = build_insights(patterns)
        recommendations = build_recommendations(patterns)
        return InsightBridgeResponse(
            timestamp=_utc_now_iso(),
            insights=insights,
            system_health_summary=self.build_system_health_summary(evaluation, simulation),
            recommendations=recommendations,
        )

    def build_report(self) -> dict:
        response = self.build_response()
        top_risks = [item.description for item in response.insights if item.severity in {"high", "medium"}][:5]
        cross_layer_findings = [item.description for item in response.insights if "evaluation" in item.evidence_sources and "simulation" in item.evidence_sources]
        return {
            "generated_at": response.timestamp,
            "patterns_detected": len(response.insights),
            "insights_generated": len(response.insights),
            "recommendations": len(response.recommendations),
            "system_health_summary": response.system_health_summary.model_dump(),
            "top_risks": top_risks,
            "cross_layer_findings": cross_layer_findings,
        }


def build_insight_response() -> InsightBridgeResponse:
    return InsightEngine().build_response()


def generate_insight_report(output_path: Path | None = None) -> dict:
    engine = InsightEngine()
    report = engine.build_report()
    target = output_path or (engine.root_path / "insight_report.json")
    target.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report