from __future__ import annotations

from tests.simulation.live_orchestration.engine import run_live_simulation


def test_live_orchestration_simulation_is_deterministic_for_same_seed() -> None:
    left = run_live_simulation(
        seed=123,
        household_size=4,
        chaos_level="medium",
        event_density=16,
        scenario_preset="school_work_balance",
        persist=False,
    )
    right = run_live_simulation(
        seed=123,
        household_size=4,
        chaos_level="medium",
        event_density=16,
        scenario_preset="school_work_balance",
        persist=False,
    )

    assert left["event_timeline"] == right["event_timeline"]
    assert left["decision_drift_metrics"] == right["decision_drift_metrics"]
    assert left["stability_scores"] == right["stability_scores"]


def test_live_orchestration_assertions_are_computed() -> None:
    result = run_live_simulation(
        seed=9,
        household_size=3,
        chaos_level="high",
        event_density=14,
        scenario_preset="health_interruption_day",
        persist=False,
    )

    assertions = result["assertions"]

    assert "priority_stability_under_change" in assertions
    assert "correct_reordering_of_urgent_events" in assertions
    assert "no_stale_event_persistence" in assertions
    assert "correct_conflict_resolution_behavior" in assertions
    assert isinstance(assertions["priority_flip_count"], int)
    assert isinstance(result["brief_outputs_over_time"], list)
    assert len(result["brief_outputs_over_time"]) == len(result["event_timeline"])
