"""Decision Feedback Layer — converts evaluation results into actionable intelligence."""
from __future__ import annotations


def extract_failure_patterns(results: dict) -> dict:
    """Aggregate issues from scenario scores into categorised failure patterns."""
    pattern_map: dict[str, dict] = {}

    for scenario in results.get("scenarios", []):
        sid = scenario.get("scenario_id", "unknown")
        for issue in scenario.get("issues", []):
            if not issue:
                continue
            issue_lower = issue.lower()
            if "priority" in issue_lower:
                category = "priority_misordering"
            elif "conflict" in issue_lower:
                category = "conflict_handling_failure"
            elif "missing" in issue_lower or "omission" in issue_lower:
                category = "omission_failure"
            elif "noise" in issue_lower:
                category = "noise_inclusion"
            else:
                category = "other"

            if category not in pattern_map:
                pattern_map[category] = {"type": category, "count": 0, "scenarios": []}
            pattern_map[category]["count"] += 1
            if sid not in pattern_map[category]["scenarios"]:
                pattern_map[category]["scenarios"].append(sid)

    failure_patterns = sorted(
        pattern_map.values(), key=lambda p: p["count"], reverse=True
    )
    return {"failure_patterns": list(failure_patterns)}


_FAILURE_TO_GAP: dict[str, str] = {
    "priority_misordering": "priority_weighting_logic_weak",
    "conflict_handling_failure": "conflict_detection_missing_or_insufficient",
    "omission_failure": "event_selection_filtering_too_aggressive",
    "noise_inclusion": "relevance_filtering_too_permissive",
    "other": "unclassified_decision_gap",
}

_GAP_RECOMMENDATIONS: dict[str, str] = {
    "priority_weighting_logic_weak": (
        "Increase weighting of time-sensitive and high-impact events during prioritization."
    ),
    "conflict_detection_missing_or_insufficient": (
        "Introduce explicit schedule conflict detection before final decision ranking."
    ),
    "event_selection_filtering_too_aggressive": (
        "Relax filtering thresholds to ensure critical events are not omitted."
    ),
    "relevance_filtering_too_permissive": (
        "Tighten relevance criteria to eliminate low-signal or non-actionable items."
    ),
    "unclassified_decision_gap": (
        "Perform manual review of decision logic for unidentified failure patterns."
    ),
}


def map_failure_to_decision_gaps(patterns: dict) -> list:
    """Map failure patterns to named decision gaps."""
    gaps: list[dict] = []
    for pattern in patterns.get("failure_patterns", []):
        gap_type = _FAILURE_TO_GAP.get(pattern["type"], "unclassified_decision_gap")
        gaps.append(
            {
                "gap_type": gap_type,
                "source_failure": pattern["type"],
                "frequency": pattern["count"],
            }
        )
    return gaps


def generate_recommendations(gaps: list) -> list:
    """Produce prioritised recommendations from decision gaps."""
    recommendations: list[dict] = []
    for gap in gaps:
        gap_type = gap["gap_type"]
        frequency = gap["frequency"]
        if frequency >= 3:
            priority = "high"
        elif frequency == 2:
            priority = "medium"
        else:
            priority = "low"
        recommendations.append(
            {
                "recommendation": _GAP_RECOMMENDATIONS.get(
                    gap_type,
                    "Perform manual review of decision logic for unidentified failure patterns.",
                ),
                "based_on": gap_type,
                "priority": priority,
            }
        )
    return recommendations
