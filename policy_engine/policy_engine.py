from __future__ import annotations

import json
from pathlib import Path

from policy_engine.contracts import HouseholdMemorySnapshot, PolicySuggestion, PolicySummaryResponse
from policy_engine.itinerary_generator import generate_daily_itinerary
from policy_engine.memory_store import PolicyMemoryStore


def _confidence(value: float) -> float:
    return round(max(0.0, min(1.0, value)), 2)


class PolicyRecommendationEngine:
    def __init__(self, root_path: Path | None = None) -> None:
        self.root_path = root_path or Path(__file__).resolve().parent.parent
        self.memory_store = PolicyMemoryStore(self.root_path)

    def _load_json(self, file_name: str) -> dict:
        return json.loads((self.root_path / file_name).read_text(encoding="utf-8"))

    def build_policy_summary(self, memory_snapshot: HouseholdMemorySnapshot | None = None) -> PolicySummaryResponse:
        memory_snapshot = memory_snapshot or self.memory_store.load_or_build_memory_snapshot()
        insight = self._load_json("insight_report.json")
        evaluation = self._load_json("evaluation_results.json")
        simulation = self._load_json("simulation_results.json")
        operational = self._load_json("operational_mode_report.json")

        priority_accuracy = float(evaluation.get("aggregate", {}).get("avg_priority_correctness", 0.0)) / 10.0
        stability_score = float(simulation.get("stability_scores", {}).get("stability_score", 0.0))
        conflict_signal = 1.0 if any("conflict" in note.lower() for note in operational.get("notes", [])) else 0.4
        drift_signal = 1.0 if insight.get("cross_layer_findings") else 0.5

        policies = [
            PolicySuggestion(
                policy_type="priority_adjustment_suggestion",
                description="Review household priority weighting whenever high-conflict or medical scenarios recur.",
                reasoning="Priority alignment remains below a full-confidence threshold in artifact summaries.",
                confidence=_confidence(1.0 - (priority_accuracy * 0.5)),
                impact_area=["operational", "evaluation"],
            ),
            PolicySuggestion(
                policy_type="scheduling_heuristic_suggestion",
                description="Protect school and work blocks with an explicit midday constraint review.",
                reasoning="Recurring routines and cross-layer drift suggest schedule pressure builds during dense days.",
                confidence=_confidence(0.55 + (drift_signal * 0.25)),
                impact_area=["operational", "simulation"],
            ),
            PolicySuggestion(
                policy_type="conflict_handling_strategy",
                description="Escalate overlap-prone days for manual confirmation before finalizing daily plans.",
                reasoning="Operational and insight artifacts both surface conflict-related caution signals.",
                confidence=_confidence(0.45 + (conflict_signal * 0.35)),
                impact_area=["operational", "simulation"],
            ),
            PolicySuggestion(
                policy_type="routine_optimization_suggestion",
                description="Standardize morning readiness and evening review routines to reduce drift across the day.",
                reasoning="Memory-derived routines and stable simulation scores support a repeatable household cadence.",
                confidence=_confidence(0.4 + (stability_score * 0.4)),
                impact_area=["operational", "evaluation", "simulation"],
            ),
        ]
        return PolicySummaryResponse(policies=policies)

    def build_behavior_summary(self, policy_summary: PolicySummaryResponse, itinerary_date: str | None = None) -> dict:
        memory_snapshot = self.memory_store.load_memory_snapshot() or self.memory_store.build_memory_snapshot()
        itinerary = generate_daily_itinerary(memory_snapshot, target_date=itinerary_date)
        evaluation = self._load_json("evaluation_results.json")
        simulation = self._load_json("simulation_results.json")

        priority_alignment_score = round(float(evaluation.get("aggregate", {}).get("avg_priority_correctness", 0.0)) / 10.0, 4)
        schedule_efficiency_score = round(float(simulation.get("stability_scores", {}).get("stability_score", 0.0)), 4)
        conflict_reduction_score = round(max(0.0, 1.0 - min(1.0, len(itinerary.conflicts_detected) / 5.0)), 4)

        return {
            "generated_at": memory_snapshot.updated_at,
            "policies_generated": len(policy_summary.policies),
            "memory_entries": sum(len(getattr(memory_snapshot.memory, key)) for key in ("preferences", "patterns", "constraints", "routines")),
            "itinerary_blocks": len(itinerary.recommended_itinerary),
            "conflicts_detected": len(itinerary.conflicts_detected),
            "system_behavior_summary": {
                "priority_alignment_score": priority_alignment_score,
                "schedule_efficiency_score": schedule_efficiency_score,
                "conflict_reduction_score": conflict_reduction_score,
            },
            "top_recommendations": [policy.description for policy in policy_summary.policies[:5]],
        }


def generate_policy_engine_report(root_path: Path | None = None) -> dict:
    engine = PolicyRecommendationEngine(root_path)
    memory_snapshot = engine.memory_store.load_memory_snapshot() or engine.memory_store.build_memory_snapshot()
    policy_summary = engine.build_policy_summary(memory_snapshot)
    report = engine.build_behavior_summary(policy_summary)
    return engine.memory_store.persist_policy_report(report)