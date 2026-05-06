from __future__ import annotations

import json

import pytest

from app.services.commands import get_command_runtime_service
from tests.harness.production_readiness_harness import (
    call_post_decision_complete,
    create_test_client,
    deep_diff_entries,
    household_surface_app,
    log_debug,
    rebuild_projection_from_event_log,
    reset_projection_state,
    to_plain_data,
)

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]


@pytest.mark.integration
def test_phase2_event_replay_integrity() -> None:
    household_id = "prod-ready-phase2-household"
    scenario_date = "2026-05-05"
    decision_id = "phase2-decision-complete-001"

    reset_projection_state(household_id)

    runtime = get_command_runtime_service()

    with create_test_client(household_surface_app) as client:
        baseline_home = client.get(f"/household/{household_id}/home", params={"date": scenario_date})
        assert baseline_home.status_code == 200, baseline_home.text
        log_debug(
            "phase2_baseline_home",
            household_id=household_id,
            scenario_date=scenario_date,
            payload=baseline_home.json(),
        )

        command_record = call_post_decision_complete(
            client,
            household_id=household_id,
            decision_id=decision_id,
        )
        assert command_record.status_code == 200

    projection_uncached = runtime.get_projection(household_id, force_replay=False)
    projection_cached = runtime.get_projection(household_id, force_replay=False)
    projection_replayed = runtime.get_projection(household_id, force_replay=True)

    log_debug(
        "phase2_runtime_projection_snapshots",
        household_id=household_id,
        uncached=to_plain_data(projection_uncached),
        cached=to_plain_data(projection_cached),
        replayed=to_plain_data(projection_replayed),
    )

    reconstructed_projection, replay_events = rebuild_projection_from_event_log(household_id)

    log_debug(
        "phase2_replay_events",
        household_id=household_id,
        event_count=len(replay_events),
        events=replay_events,
    )

    cached_plain = to_plain_data(projection_cached)
    replayed_plain = to_plain_data(reconstructed_projection)

    diff_entries = deep_diff_entries(cached_plain, replayed_plain)

    log_debug(
        "phase2_projection_diff",
        household_id=household_id,
        diff_count=len(diff_entries),
        diffs=diff_entries,
    )

    assert cached_plain == replayed_plain, (
        "Cached projection did not match direct replay projection. "
        f"Diffs: {json.dumps(diff_entries, indent=2, default=str)}"
    )
