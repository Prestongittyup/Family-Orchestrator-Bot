from __future__ import annotations

from insights.contracts import EvaluationArtifact, OperationalArtifact, PatternRecord, SimulationArtifact


def _severity_for_frequency(frequency: int) -> str:
    if frequency >= 3:
        return "high"
    if frequency >= 1:
        return "medium"
    return "low"


def detect_patterns(
    evaluation: EvaluationArtifact,
    simulation: SimulationArtifact,
    operational: OperationalArtifact,
) -> list[PatternRecord]:
    evaluation_issue_scenarios = [scenario.scenario_id for scenario in evaluation.scenarios if scenario.issues]
    priority_failure = next(
        (pattern for pattern in evaluation.failure_patterns if pattern.type == "priority_misordering"),
        None,
    )
    priority_frequency = priority_failure.count if priority_failure is not None else len(evaluation_issue_scenarios)
    priority_scenarios = list(priority_failure.scenarios) if priority_failure is not None else evaluation_issue_scenarios

    conflict_note_hits = [note for note in operational.notes if "conflict" in note.lower()]
    conflict_frequency = len(conflict_note_hits)
    if conflict_frequency == 0:
        conflict_frequency = 1
    conflict_scenarios = ["operational_mode_report"]

    instability_frequency = len(simulation.failure_patterns)
    if not simulation.assertions.correct_reordering_of_urgent_events:
        instability_frequency += 1
    instability_scenarios = [str(simulation.config.get("scenario_preset", simulation.simulation_id))]

    omission_frequency = 0
    omission_scenarios: list[str] = []
    if evaluation.aggregate.avg_omission < 10:
        omission_frequency += max(1, int(round(10 - evaluation.aggregate.avg_omission)))
        omission_scenarios.extend(evaluation_issue_scenarios)
    if all("omission" not in note.lower() for note in operational.notes):
        omission_frequency += 1
        omission_scenarios.append("operational_mode_report")

    drift_frequency = 0
    drift_scenarios: list[str] = []
    priority_accuracy_estimate = round(evaluation.aggregate.avg_priority_correctness / 10.0, 3)
    stability_score = simulation.stability_scores.stability_score
    if priority_accuracy_estimate < 0.75:
        drift_frequency += 1
        drift_scenarios.extend(priority_scenarios or evaluation_issue_scenarios)
    if simulation.failure_patterns:
        drift_frequency += 1
        drift_scenarios.extend(instability_scenarios)
    if operational.summary.strict_contract_enforced and operational.summary.operational_layer_isolated:
        drift_frequency += 1
        drift_scenarios.append("operational_mode_report")

    return [
        PatternRecord(
            type="priority_misalignment",
            frequency=priority_frequency,
            affected_scenarios=priority_scenarios,
            severity=_severity_for_frequency(priority_frequency),
            evidence_sources=["evaluation"],
        ),
        PatternRecord(
            type="conflict_resolution_failure",
            frequency=conflict_frequency,
            affected_scenarios=conflict_scenarios,
            severity="low" if conflict_frequency == 1 else _severity_for_frequency(conflict_frequency),
            evidence_sources=["operational"],
        ),
        PatternRecord(
            type="decision_instability",
            frequency=instability_frequency,
            affected_scenarios=instability_scenarios,
            severity=_severity_for_frequency(instability_frequency),
            evidence_sources=["simulation"],
        ),
        PatternRecord(
            type="omission_bias",
            frequency=omission_frequency,
            affected_scenarios=sorted(set(omission_scenarios)),
            severity=_severity_for_frequency(omission_frequency),
            evidence_sources=["evaluation", "operational"],
        ),
        PatternRecord(
            type="system_drift",
            frequency=drift_frequency,
            affected_scenarios=sorted(set(drift_scenarios)),
            severity="high" if drift_frequency >= 3 else _severity_for_frequency(drift_frequency),
            evidence_sources=["evaluation", "simulation", "operational"],
        ),
    ]