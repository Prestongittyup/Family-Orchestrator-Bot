from __future__ import annotations

from app.surfaces.household_insight_surface import build_household_insight_surface


def test_insight_surface_compresses_deterministically_with_rule_ordered_summary() -> None:
    trajectory_surface = {
        "household_id": "home-4",
        "time_window": {
            "start_date": "2026-05-03",
            "end_date": "2026-05-05",
        },
        "trajectory": [
            {
                "date": "2026-05-03",
                "decision_fulfillment_rate": 1.0,
                "execution_success_rate": 1.0,
                "drift_delta": -0.2,
                "state_direction": "stable",
            },
            {
                "date": "2026-05-04",
                "decision_fulfillment_rate": 0.0,
                "execution_success_rate": 0.0,
                "drift_delta": 0.6,
                "state_direction": "degrading",
            },
            {
                "date": "2026-05-05",
                "decision_fulfillment_rate": 0.0,
                "execution_success_rate": 0.0,
                "drift_delta": 1.2,
                "state_direction": "degrading",
            },
        ],
        "aggregate": {
            "avg_fulfillment_rate": 0.3333,
            "avg_execution_success_rate": 0.3333,
            "cumulative_drift": 1.6,
            "overall_direction": "degrading",
        },
    }
    feedback_surfaces = [
        {
            "household_id": "home-4",
            "date": "2026-05-03",
            "decision": {"id": "decision-a", "type": "ACTION"},
            "execution": {"executed": True},
            "outcome": {"completed": True, "failed": False, "pending": False},
            "feedback": {"decision_fulfilled": True},
            "drift_impact": {"drift_delta": -0.2},
        },
        {
            "household_id": "home-4",
            "date": "2026-05-04",
            "decision": {"id": "decision-b", "type": "ACTION"},
            "execution": {"executed": True},
            "outcome": {"completed": False, "failed": True, "pending": False},
            "feedback": {"decision_fulfilled": False},
            "drift_impact": {"drift_delta": 0.6},
        },
        {
            "household_id": "home-4",
            "date": "2026-05-05",
            "decision": {"id": "decision-c", "type": "ACTION"},
            "execution": {"executed": False},
            "outcome": {"completed": False, "failed": False, "pending": True},
            "feedback": {"decision_fulfilled": False},
            "drift_impact": {"drift_delta": 1.2},
        },
    ]
    decision_surface = {
        "household_id": "home-4",
        "date": "2026-05-05",
        "decision": {
            "id": "decision-c",
            "type": "ACTION",
            "source": "priority",
            "metadata": {
                "execution_plan_id": None,
                "action_id": None,
            },
        },
        "context": {
            "top_overdue_count": 4,
            "top_conflict_count": 2,
            "system_load_index": 2.5,
        },
    }

    first = build_household_insight_surface(
        trajectory_surface,
        feedback_surfaces,
        decision_surface,
    )
    second = build_household_insight_surface(
        trajectory_surface,
        feedback_surfaces,
        decision_surface,
    )

    assert first == second
    assert set(first.keys()) == {
        "household_id",
        "date",
        "focus_items",
        "daily_state",
        "compressed_summary",
    }

    assert first["household_id"] == "home-4"
    assert first["date"] == "2026-05-05"

    daily_state = first["daily_state"]
    assert daily_state["direction"] == "degrading"
    assert daily_state["drift_signal"] == 1.2
    assert daily_state["execution_health"] == 0.5
    assert daily_state["attention_pressure"] == 0.7

    severity_rank = {"high": 0, "medium": 1, "low": 2}
    ranked = [severity_rank[item["severity"]] for item in first["focus_items"]]
    assert ranked == sorted(ranked)

    expected_summary = " | ".join(
        f"{item['severity'].upper()} {item['type']}: {item['summary']}"
        for item in first["focus_items"][:3]
    )
    assert first["compressed_summary"] == expected_summary


def test_insight_surface_returns_stable_empty_summary_when_no_focus_items() -> None:
    insight = build_household_insight_surface(
        {
            "household_id": "home-5",
            "time_window": {"start_date": "2026-05-05", "end_date": "2026-05-05"},
            "trajectory": [
                {
                    "date": "2026-05-05",
                    "decision_fulfillment_rate": 1.0,
                    "execution_success_rate": 1.0,
                    "drift_delta": 0.0,
                    "state_direction": "stable",
                }
            ],
            "aggregate": {
                "avg_fulfillment_rate": 1.0,
                "avg_execution_success_rate": 1.0,
                "cumulative_drift": 0.0,
                "overall_direction": "stable",
            },
        },
        [],
        {
            "household_id": "home-5",
            "date": "2026-05-05",
            "decision": {
                "id": "decision-z",
                "type": "ACTION",
                "source": "execution",
                "metadata": {"execution_plan_id": "plan-z", "action_id": "action-z"},
            },
            "context": {
                "top_overdue_count": 0,
                "top_conflict_count": 0,
                "system_load_index": 1.0,
            },
        },
    )

    assert insight["focus_items"] == []
    assert insight["compressed_summary"] == "2026-05-05: no focus items."