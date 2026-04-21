from __future__ import annotations

from tests.simulation.stress_tests.runner import run_stress_scenarios
from tests.simulation.stress_tests.stability_metrics import compute_stability_metrics


def test_stress_runner_emits_all_chaos_scenarios() -> None:
    payload = run_stress_scenarios(seed=77)

    assert payload["seed"] == 77
    scenarios = payload["stress_scenarios"]
    assert len(scenarios) == 3

    names = {item["scenario"] for item in scenarios}
    assert names == {"low_noise", "moderate_chaos", "high_chaos"}

    for row in scenarios:
        metrics = row["metrics"]
        assert 0.0 <= float(metrics["stability_score"]) <= 1.0
        assert float(metrics["decision_drift_score"]) >= 0.0
        assert float(metrics["priority_flip_rate"]) >= 0.0
        assert int(metrics["recovery_time_steps"]) >= 0


def test_stability_metrics_are_computed_correctly() -> None:
    evolution = [
        {"top_event_titles": ["A", "B", "C"], "conflict_count": 0},
        {"top_event_titles": ["A", "B", "C"], "conflict_count": 1},
        {"top_event_titles": ["X", "B", "C"], "conflict_count": 1},
        {"top_event_titles": ["X", "B", "C"], "conflict_count": 0},
    ]

    metrics = compute_stability_metrics(evolution)

    assert "decision_drift_score" in metrics
    assert "priority_flip_rate" in metrics
    assert "brief_instability_index" in metrics
    assert "recovery_time_steps" in metrics
    assert "stability_score" in metrics

    assert metrics["priority_flip_rate"] > 0
    assert metrics["stability_score"] < 1
