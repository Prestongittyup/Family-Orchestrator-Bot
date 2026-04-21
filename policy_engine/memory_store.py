from __future__ import annotations

import json
from pathlib import Path

from policy_engine.contracts import HouseholdMemoryBody, HouseholdMemorySnapshot


class PolicyMemoryStore:
    def __init__(self, root_path: Path | None = None) -> None:
        self.root_path = root_path or Path(__file__).resolve().parent.parent
        self.memory_path = self.root_path / "policy_memory.json"
        self.report_path = self.root_path / "policy_engine_report.json"

    def _load_json(self, file_name: str) -> dict:
        return json.loads((self.root_path / file_name).read_text(encoding="utf-8"))

    def build_memory_snapshot(self, household_id: str = "household-001") -> HouseholdMemorySnapshot:
        evaluation = self._load_json("evaluation_results.json")
        simulation = self._load_json("simulation_results.json")
        operational = self._load_json("operational_mode_report.json")
        insight = self._load_json("insight_report.json")

        descriptions = [str(item.get("description", "")) for item in evaluation.get("scenarios", [])]
        issue_text = " ".join(
            str(issue)
            for scenario in evaluation.get("scenarios", [])
            for issue in scenario.get("issues", [])
        ).lower()
        risks = [str(item) for item in insight.get("top_risks", [])]
        notes = [str(item) for item in operational.get("notes", [])]
        failure_patterns = [str(item) for item in simulation.get("failure_patterns", [])]

        preferences = []
        if any("doctor" in text.lower() or "health" in text.lower() or "medical" in text.lower() for text in descriptions + risks):
            preferences.append("medical_events_first")
        if any("school" in text.lower() for text in descriptions + risks):
            preferences.append("school_events_high_visibility")
        if any("work" in text.lower() or "client" in text.lower() for text in descriptions + risks):
            preferences.append("work_blocks_protected")

        patterns = []
        if failure_patterns:
            patterns.append("simulation_instability_under_replay")
        if any("priority" in text.lower() for text in risks):
            patterns.append("priority_reordering_observed")
        if any("drift" in text.lower() for text in risks):
            patterns.append("cross_layer_drift_detected")

        constraints = []
        if issue_text:
            constraints.append("avoid_priority_conflicts_in_dense_days")
        if any("conflict" in note.lower() for note in notes + risks):
            constraints.append("surface_conflicts_before_schedule_commitment")
        if simulation.get("assertions", {}).get("correct_reordering_of_urgent_events") is False:
            constraints.append("urgent_events_require_manual_review")

        routines = []
        if "medical_events_first" in preferences:
            routines.append("08:00 medical readiness review")
        if "school_events_high_visibility" in preferences:
            routines.append("08:30 school launch block")
        if "work_blocks_protected" in preferences:
            routines.append("09:00 work focus block")
        routines.append("18:00 household coordination review")

        updated_at = str(insight.get("generated_at", simulation.get("generated_at", "")))

        return HouseholdMemorySnapshot(
            household_id=household_id,
            updated_at=updated_at,
            memory=HouseholdMemoryBody(
                preferences=sorted(set(preferences)),
                patterns=sorted(set(patterns)),
                constraints=sorted(set(constraints)),
                routines=sorted(set(routines)),
            ),
        )

    def persist_memory_snapshot(self, snapshot: HouseholdMemorySnapshot) -> HouseholdMemorySnapshot:
        self.memory_path.write_text(
            json.dumps(snapshot.model_dump(), indent=2),
            encoding="utf-8",
        )
        return snapshot

    def load_memory_snapshot(self) -> HouseholdMemorySnapshot | None:
        if not self.memory_path.exists():
            return None
        return HouseholdMemorySnapshot.model_validate(json.loads(self.memory_path.read_text(encoding="utf-8")))

    def load_or_build_memory_snapshot(self, household_id: str = "household-001") -> HouseholdMemorySnapshot:
        snapshot = self.load_memory_snapshot()
        if snapshot is not None:
            return snapshot
        snapshot = self.build_memory_snapshot(household_id=household_id)
        return self.persist_memory_snapshot(snapshot)

    def persist_policy_report(self, report: dict) -> dict:
        self.report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report