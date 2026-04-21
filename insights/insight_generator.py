from __future__ import annotations

from insights.contracts import InsightItem, PatternRecord, RecommendationItem


_COMPONENTS = {
    "priority_misalignment": ["evaluation_artifact", "priority_ranking_surface"],
    "conflict_resolution_failure": ["operational_artifact", "conflict_reporting_surface"],
    "decision_instability": ["simulation_artifact", "timeline_replay_surface"],
    "omission_bias": ["evaluation_artifact", "operational_artifact"],
    "system_drift": ["cross_layer_alignment", "insight_bridge"],
}


def _build_description(pattern: PatternRecord) -> str:
    if pattern.type == "priority_misalignment":
        return "Priority weighting appears inconsistent in high-conflict evaluation scenarios."
    if pattern.type == "conflict_resolution_failure":
        return "Operational reporting does not yet provide strong conflict-resolution evidence coverage."
    if pattern.type == "decision_instability":
        return "Simulation artifacts indicate instability or urgent-event reordering weakness under replayed load."
    if pattern.type == "omission_bias":
        return "Coverage for omitted critical items is uneven between evaluation evidence and operational reporting."
    return "Cross-layer signals indicate drift between measured evaluation quality, simulation behavior, and operational assurances."


def build_insights(patterns: list[PatternRecord]) -> list[InsightItem]:
    insights: list[InsightItem] = []
    for pattern in patterns:
        if pattern.frequency <= 0:
            continue
        insights.append(
            InsightItem(
                type=pattern.type,
                severity=pattern.severity,
                description=_build_description(pattern),
                evidence_sources=pattern.evidence_sources,
                affected_components=_COMPONENTS.get(pattern.type, ["insight_bridge"]),
            )
        )
    return insights


def build_recommendations(patterns: list[PatternRecord]) -> list[RecommendationItem]:
    recommendations: list[RecommendationItem] = []
    for pattern in patterns:
        if pattern.frequency <= 0:
            continue
        if pattern.type == "priority_misalignment":
            recommendations.append(
                RecommendationItem(
                    recommendation="Review priority outcomes in high-conflict scenarios before promoting future changes.",
                    reason="Evaluation artifacts show repeated priority misalignment evidence.",
                    priority=pattern.severity,
                )
            )
        elif pattern.type == "conflict_resolution_failure":
            recommendations.append(
                RecommendationItem(
                    recommendation="Expand operational reporting coverage for conflict outcomes and resolution confidence.",
                    reason="Operational artifacts provide limited direct evidence about conflict handling quality.",
                    priority=pattern.severity,
                )
            )
        elif pattern.type == "decision_instability":
            recommendations.append(
                RecommendationItem(
                    recommendation="Track instability indicators across additional simulation runs before changing release confidence.",
                    reason="Simulation artifacts show replay-time decision instability signals.",
                    priority=pattern.severity,
                )
            )
        elif pattern.type == "omission_bias":
            recommendations.append(
                RecommendationItem(
                    recommendation="Compare evaluation omission metrics with operational reporting coverage during reviews.",
                    reason="Cross-artifact omission visibility is weaker than other signals.",
                    priority=pattern.severity,
                )
            )
        elif pattern.type == "system_drift":
            recommendations.append(
                RecommendationItem(
                    recommendation="Use this bridge output as a release-review checkpoint when cross-layer signals diverge.",
                    reason="Evaluation, simulation, and operational artifacts are not fully aligned.",
                    priority=pattern.severity,
                )
            )
    return recommendations