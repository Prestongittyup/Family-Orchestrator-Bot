from __future__ import annotations

from pathlib import Path

from tests.evaluation.evaluation_runner import run_full_evaluation


def test_brief_evaluation_pipeline_runs_end_to_end() -> None:
    result = run_full_evaluation()

    assert isinstance(result, dict)
    assert "scenarios" in result
    assert "aggregate" in result
    assert "comparison" in result

    scenarios = result["scenarios"]
    assert isinstance(scenarios, list)
    assert len(scenarios) >= 5

    metric_keys = (
        "priority_score",
        "relevance_score",
        "completeness_score",
        "clarity_score",
        "priority_correctness",
        "conflict_handling_score",
        "omission_score",
        "noise_penalty",
    )

    for row in scenarios:
        assert "scenario_id" in row
        assert "scores" in row
        scores = row["scores"]
        for key in metric_keys:
            assert key in scores
            assert 0 <= int(scores[key]) <= 10

    aggregate = result["aggregate"]
    for key in (
        "avg_priority",
        "avg_relevance",
        "avg_completeness",
        "avg_clarity",
        "avg_priority_correctness",
        "avg_conflict_handling",
        "avg_omission",
        "avg_noise_penalty",
    ):
        assert key in aggregate
        assert 0.0 <= float(aggregate[key]) <= 10.0

    assert result["aggregate"]["avg_priority_correctness"] >= 5
    assert result["aggregate"]["avg_omission"] >= 5

    comparison = result["comparison"]
    assert isinstance(comparison, dict)
    assert "improved" in comparison
    assert "regressions" in comparison
    assert "score_deltas" in comparison

    score_deltas = comparison["score_deltas"]
    for key in (
        "priority_score",
        "relevance_score",
        "completeness_score",
        "clarity_score",
        "priority_correctness",
        "conflict_handling_score",
        "omission_score",
        "noise_penalty",
    ):
        assert key in score_deltas

    artifact = Path("evaluation_results.json")
    assert artifact.exists()

    # --- Feedback layer assertions ---
    assert "failure_patterns" in result
    assert "decision_gaps" in result
    assert "recommended_adjustments" in result

    assert isinstance(result["failure_patterns"], list)
    assert isinstance(result["decision_gaps"], list)
    assert isinstance(result["recommended_adjustments"], list)

    # If any scenario emitted issues, failure_patterns must be non-empty
    any_issues = any(row.get("issues") for row in result["scenarios"])
    if any_issues:
        assert len(result["failure_patterns"]) > 0

    for item in result["recommended_adjustments"]:
        assert "recommendation" in item
        assert "priority" in item
        assert item["priority"] in ("high", "medium", "low")
