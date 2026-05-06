from __future__ import annotations

from app.surfaces.household_decision_feedback_surface import build_household_decision_feedback_surface
from app.surfaces.household_decision_surface import build_household_decision_surface


def test_decision_surface_and_feedback_surface_remain_reference_aligned() -> None:
    loop_surface = {
        "household_id": "home-3",
        "date": "2026-05-05",
        "attention": {
            "overdue": [],
            "conflicts": [],
            "priority": [],
        },
        "actions": [
            {
                "action_id": "action-10",
                "priority_rank": 1,
                "title": "Ship groceries",
            }
        ],
        "execution_plans": [
            {
                "job_id": "plan-10",
                "action_id": "action-10",
                "status": "SUCCESS",
                "payload": {"timestamp": "2026-05-05T08:00:00Z"},
            }
        ],
        "drift": {"system_load_index": 2.0},
    }

    decision_surface = build_household_decision_surface(loop_surface)

    assert decision_surface["decision"]["source"] == "action"
    assert decision_surface["decision"]["metadata"]["action_id"] == "action-10"

    feedback_surface = build_household_decision_feedback_surface(
        decision_surface,
        {
            "household_id": "home-3",
            "date": "2026-05-05",
            "execution_plans": loop_surface["execution_plans"],
        },
        {
            "household_id": "home-3",
            "date": "2026-05-05",
            "actions": loop_surface["actions"],
        },
        loop_surface,
        {
            "household_id": "home-3",
            "date": "2026-05-05",
            "drift": {"system_load_index": 1.5},
        },
    )

    assert feedback_surface["decision"]["id"] == decision_surface["decision"]["id"]
    assert (
        feedback_surface["execution"]["action_id"]
        == decision_surface["decision"]["metadata"]["action_id"]
    )
    assert feedback_surface["outcome"]["completed"] is True
    assert feedback_surface["feedback"]["decision_fulfilled"] is True