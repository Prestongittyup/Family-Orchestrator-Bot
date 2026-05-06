from __future__ import annotations

from app.surfaces.household_decision_feedback_surface import build_household_decision_feedback_surface


def test_feedback_surface_reconciles_outcome_and_drift_with_exact_refs() -> None:
    decision_surface = {
        "household_id": "home-2",
        "date": "2026-05-05",
        "decision": {
            "id": "decision-1",
            "type": "ACTION",
            "metadata": {
                "execution_plan_id": "plan-1",
                "action_id": "action-1",
            },
        },
    }
    execution_plans = {
        "household_id": "home-2",
        "date": "2026-05-05",
        "execution_plans": [
            {
                "job_id": "plan-1",
                "action_id": "action-1",
                "status": "FAILED",
                "payload": {"timestamp": "2026-05-05T10:00:00Z"},
            }
        ],
    }
    actions = {
        "household_id": "home-2",
        "date": "2026-05-05",
        "actions": [
            {
                "action_id": "action-1",
                "failed": True,
            }
        ],
    }
    pre_loop_surface = {
        "household_id": "home-2",
        "date": "2026-05-05",
        "drift": {"system_load_index": 1.0},
    }
    post_loop_surface = {
        "household_id": "home-2",
        "date": "2026-05-05",
        "drift": {"system_load_index": 1.8},
    }

    first = build_household_decision_feedback_surface(
        decision_surface,
        execution_plans,
        actions,
        pre_loop_surface,
        post_loop_surface,
    )
    second = build_household_decision_feedback_surface(
        decision_surface,
        execution_plans,
        actions,
        pre_loop_surface,
        post_loop_surface,
    )

    assert first == second
    assert first["decision"] == {"id": "decision-1", "type": "ACTION"}
    assert first["execution"]["executed"] is True
    assert first["execution"]["execution_plan_id"] == "plan-1"
    assert first["execution"]["action_id"] == "action-1"

    assert first["outcome"]["completed"] is False
    assert first["outcome"]["failed"] is True
    assert first["outcome"]["pending"] is False

    assert first["feedback"]["decision_fulfilled"] is False
    assert first["drift_impact"]["pre_drift_index"] == 1.0
    assert first["drift_impact"]["post_drift_index"] == 1.8
    assert first["drift_impact"]["drift_delta"] == 0.8