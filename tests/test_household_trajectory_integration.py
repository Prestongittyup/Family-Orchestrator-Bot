from __future__ import annotations

from app.surfaces.household_trajectory_surface import build_household_trajectory_surface


def test_trajectory_surface_groups_and_aggregates_feedback_window_deterministically() -> None:
    feedback_rows = [
        {
            "household_id": "home-1",
            "date": "2026-05-04",
            "decision": {"id": "d-2"},
            "execution": {"executed": True},
            "outcome": {"completed": False, "failed": True, "pending": False},
            "feedback": {"decision_fulfilled": False},
            "drift_impact": {"drift_delta": -0.4},
        },
        {
            "household_id": "home-1",
            "date": "2026-05-03",
            "decision": {"id": "d-1"},
            "execution": {"executed": True},
            "outcome": {"completed": True, "failed": False, "pending": False},
            "feedback": {"decision_fulfilled": True},
            "drift_impact": {"drift_delta": -0.2},
        },
        {
            "household_id": "home-1",
            "date": "2026-05-03",
            "decision": {"id": "d-3"},
            "execution": {"executed": False},
            "outcome": {"completed": False, "failed": False, "pending": True},
            "feedback": {"decision_fulfilled": False},
            "drift_impact": {"drift_delta": 0.3},
        },
    ]

    first = build_household_trajectory_surface(feedback_rows)
    second = build_household_trajectory_surface(feedback_rows)

    assert first == second
    assert first["household_id"] == "home-1"
    assert first["time_window"] == {
        "start_date": "2026-05-03",
        "end_date": "2026-05-04",
    }

    trajectory = first["trajectory"]
    assert [row["date"] for row in trajectory] == ["2026-05-03", "2026-05-04"]

    assert trajectory[0]["decision_fulfillment_rate"] == 0.5
    assert trajectory[0]["execution_success_rate"] == 1.0
    assert trajectory[0]["drift_delta"] == 0.1
    assert trajectory[0]["state_direction"] == "degrading"

    assert trajectory[1]["decision_fulfillment_rate"] == 0.0
    assert trajectory[1]["execution_success_rate"] == 0.0
    assert trajectory[1]["drift_delta"] == -0.4
    assert trajectory[1]["state_direction"] == "improving"

    aggregate = first["aggregate"]
    assert aggregate["avg_fulfillment_rate"] == 0.25
    assert aggregate["avg_execution_success_rate"] == 0.5
    assert aggregate["cumulative_drift"] == -0.3
    assert aggregate["overall_direction"] == "improving"