from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tests.evaluation.brief_runner import run_scenario
from tests.evaluation.feedback_engine import (
    extract_failure_patterns,
    generate_recommendations,
    map_failure_to_decision_gaps,
)
from tests.evaluation.scenario_generator import generate_scenarios
from tests.evaluation.scoring_engine import evaluate_brief


_METRIC_KEYS = [
    "priority_score",
    "relevance_score",
    "completeness_score",
    "clarity_score",
    "priority_correctness",
    "conflict_handling_score",
    "omission_score",
    "noise_penalty",
]


def _average(rows: list[int | float]) -> float:
    if not rows:
        return 0.0
    return round(sum(float(x) for x in rows) / len(rows), 2)


def _metric_delta(current: float, previous: float) -> float:
    return round(float(current) - float(previous), 2)


def compare_results(current: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
    current_aggregate = current.get("aggregate", {}) if isinstance(current, dict) else {}
    previous_aggregate = previous.get("aggregate", {}) if isinstance(previous, dict) else {}

    metric_map = {
        "priority_score": ("avg_priority", "avg_priority"),
        "relevance_score": ("avg_relevance", "avg_relevance"),
        "completeness_score": ("avg_completeness", "avg_completeness"),
        "clarity_score": ("avg_clarity", "avg_clarity"),
        "priority_correctness": ("avg_priority_correctness", "avg_priority_correctness"),
        "conflict_handling_score": ("avg_conflict_handling", "avg_conflict_handling"),
        "omission_score": ("avg_omission", "avg_omission"),
        "noise_penalty": ("avg_noise_penalty", "avg_noise_penalty"),
    }

    score_deltas: dict[str, float] = {}
    regressions: list[str] = []

    for metric_name, (current_key, previous_key) in metric_map.items():
        current_value = float(current_aggregate.get(current_key, 0.0))
        previous_value = float(previous_aggregate.get(previous_key, 0.0))
        delta = _metric_delta(current_value, previous_value)
        score_deltas[metric_name] = delta
        if delta < 0:
            regressions.append(metric_name)

    return {
        "improved": not regressions and any(value > 0 for value in score_deltas.values()),
        "regressions": regressions,
        "score_deltas": score_deltas,
    }


def run_full_evaluation() -> dict[str, Any]:
    output_path = Path("evaluation_results.json")
    previous_payload: dict[str, Any] = {}
    if output_path.exists():
        previous_payload = json.loads(output_path.read_text(encoding="utf-8"))

    scenarios = generate_scenarios()
    results: list[dict[str, Any]] = []

    for scenario in scenarios:
        brief = run_scenario(scenario)
        expected_outcomes = dict(scenario.expected_outcomes)
        expected_outcomes["scenario_events"] = [
            {
                "title": row.title,
                "start_time": row.start_time,
                "end_time": row.end_time,
            }
            for row in scenario.events
        ]

        scores = evaluate_brief(
            brief["brief_output"],
            scenario.expected_signals,
            expected_outcomes,
        )
        results.append(
            {
                "scenario_id": scenario.scenario_id,
                "description": scenario.description,
                "scores": {
                    key: scores[key] for key in _METRIC_KEYS
                },
                "issues": list(scores.get("issues", [])),
            }
        )

    aggregate = {
        "avg_priority": _average([row["scores"]["priority_score"] for row in results]),
        "avg_relevance": _average([row["scores"]["relevance_score"] for row in results]),
        "avg_completeness": _average([row["scores"]["completeness_score"] for row in results]),
        "avg_clarity": _average([row["scores"]["clarity_score"] for row in results]),
        "avg_priority_correctness": _average([row["scores"]["priority_correctness"] for row in results]),
        "avg_conflict_handling": _average([row["scores"]["conflict_handling_score"] for row in results]),
        "avg_omission": _average([row["scores"]["omission_score"] for row in results]),
        "avg_noise_penalty": _average([row["scores"]["noise_penalty"] for row in results]),
    }

    current_payload = {
        "scenarios": results,
        "aggregate": aggregate,
    }

    comparison = compare_results(current_payload, previous_payload) if previous_payload else {
        "improved": True,
        "regressions": [],
        "score_deltas": {metric: 0.0 for metric in _METRIC_KEYS},
    }
    current_payload["comparison"] = comparison

    # --- Feedback layer ---
    pattern_result = extract_failure_patterns(current_payload)
    gaps = map_failure_to_decision_gaps(pattern_result)
    recommendations = generate_recommendations(gaps)

    current_payload["failure_patterns"] = pattern_result["failure_patterns"]
    current_payload["decision_gaps"] = gaps
    current_payload["recommended_adjustments"] = recommendations

    output_path.write_text(
        json.dumps(current_payload, indent=2),
        encoding="utf-8",
    )

    print("DECISION_FEEDBACK_COMPLETE")
    return current_payload
