from __future__ import annotations

import pytest

from app.main import app as household_surface_app
from app.services.commands import get_command_runtime_service
from tests.harness.household_contract_harness import (
    assert_home_structure_valid,
    assert_only_expected_diff,
    assert_projection_deterministic,
    create_test_client,
    get_home,
    post_decision_complete,
    reset_projection_state,
)

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]


@pytest.mark.integration
def test_golden_household_scenario_contract() -> None:
    household_id = "test-house-001"
    scenario_date = "2026-05-05"

    reset_projection_state(household_id)

    with create_test_client(household_surface_app) as client:
        created = client.post(
            "/command",
            json={
                "command_type": "task.create",
                "household_id": household_id,
                "payload": {
                    "title": "Resolve household decision",
                    "priority": "medium",
                },
            },
        )
        assert created.status_code == 200, created.text
        created_payload = created.json()
        assert str(created_payload.get("status") or "") in {"accepted", "pending_approval"}
        decision_id = str(created_payload["response"]["task"]["task_id"])

        email_ingest = client.post(
            "/ingest/email",
            json={
                "household_id": household_id,
                "email": {
                    "email_id": "email-001",
                    "subject": "School pickup confirmation",
                    "body": "Action: confirm pickup time with coach",
                    "from": "coach@example.com",
                    "received_at": "2026-05-05T08:00:00Z",
                },
            },
        )
        assert email_ingest.status_code == 200, email_ingest.text
        assert str(email_ingest.json().get("status") or "") == "accepted"

        calendar_ingest = client.post(
            "/ingest/calendar",
            json={
                "household_id": household_id,
                "events": [
                    {
                        "event_id": "cal-1",
                        "title": "Parent-teacher call",
                        "start_at": "2026-05-05T15:00:00Z",
                        "end_at": "2026-05-05T15:30:00Z",
                    },
                    {
                        "event_id": "cal-2",
                        "title": "Soccer pickup",
                        "start_at": "2026-05-05T15:15:00Z",
                        "end_at": "2026-05-05T16:00:00Z",
                    },
                ],
            },
        )
        assert calendar_ingest.status_code == 200, calendar_ingest.text
        assert str(calendar_ingest.json().get("status") or "") == "accepted"

        baseline_home = get_home(client, household_id, date=scenario_date)
        assert_home_structure_valid(baseline_home)
        assert isinstance(baseline_home.get("actions"), list)
        assert len(baseline_home.get("actions") or []) >= 1
        assert isinstance(baseline_home.get("needs_decision"), list)
        assert len(baseline_home.get("needs_decision") or []) >= 1

        baseline_repeat = get_home(client, household_id, date=scenario_date)
        assert_projection_deterministic(baseline_home, baseline_repeat)

        projection_before = get_command_runtime_service().get_projection(household_id, force_replay=True)

        complete_result = post_decision_complete(client, household_id, decision_id)
        assert complete_result.get("status") == "accepted"

        projection_after = get_command_runtime_service().get_projection(household_id, force_replay=True)
        decisions_after = projection_after.get("decisions")
        assert isinstance(decisions_after, dict)
        decision_projection = decisions_after.get(decision_id)
        assert isinstance(decision_projection, dict)
        assert decision_projection.get("state") == "completed"
        assert projection_after.get("last_event_id") != projection_before.get("last_event_id")

        home_after = get_home(client, household_id, date=scenario_date)
        assert_home_structure_valid(home_after)

        home_after_repeat = get_home(client, household_id, date=scenario_date)
        assert_projection_deterministic(home_after, home_after_repeat)

        assert_only_expected_diff(
            baseline_home,
            home_after,
            allowed_diff_keys={"summary", "calendar", "actions", "needs_decision"},
        )

        decision_response = client.get(f"/household/{household_id}/decision", params={"date": scenario_date})
        loop_response = client.get(f"/household/{household_id}/loop", params={"date": scenario_date})

        assert decision_response.status_code == 200, decision_response.text
        assert loop_response.status_code == 200, loop_response.text

        decision_surface = decision_response.json()
        loop_surface = loop_response.json()
        assert isinstance(decision_surface, dict)
        assert isinstance(loop_surface, dict)
        assert decision_surface.get("household_id") == household_id
        assert loop_surface.get("household_id") == household_id
