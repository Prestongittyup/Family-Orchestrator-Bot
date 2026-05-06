from __future__ import annotations

import json

import pytest

from app.services.commands import get_command_runtime_service
from tests.harness.production_readiness_harness import (
    allowed_home_diff_paths,
    assert_surface_deterministic,
    call_get_surface,
    call_post_decision_complete,
    create_test_client,
    deep_diff_entries,
    fetch_surface_bundle,
    household_surface_app,
    log_debug,
    normalize_surface_payload,
    projection_version_token,
    reset_projection_state,
    validate_cross_surface_consistency,
)

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]


@pytest.mark.integration
def test_phase4_production_shippability_validation() -> None:
    household_id = "prod-ready-phase4-household"
    scenario_date = "2026-05-05"
    decision_id = "phase4-shippability-decision-001"

    reset_projection_state(household_id)
    runtime = get_command_runtime_service()

    with create_test_client(household_surface_app) as client:
        home_first = call_get_surface(
            client,
            name="home",
            household_id=household_id,
            scenario_date=scenario_date,
        )
        home_second = call_get_surface(
            client,
            name="home",
            household_id=household_id,
            scenario_date=scenario_date,
        )

        assert_surface_deterministic(home_first, home_second, allowed_diff_paths=set())

        before_bundle, before_token_start, before_token_end = fetch_surface_bundle(
            client,
            household_id=household_id,
            scenario_date=scenario_date,
        )
        validate_cross_surface_consistency(
            before_bundle,
            household_id=household_id,
            scenario_date=scenario_date,
            projection_token_before=before_token_start,
            projection_token_after=before_token_end,
        )

        projection_before_command = runtime.get_projection(household_id, force_replay=True)
        token_before_command = projection_version_token(projection_before_command)

        command_record = call_post_decision_complete(
            client,
            household_id=household_id,
            decision_id=decision_id,
        )
        assert command_record.status_code == 200

        projection_after_command = runtime.get_projection(household_id, force_replay=True)
        token_after_command = projection_version_token(projection_after_command)

        log_debug(
            "phase4_projection_transition",
            household_id=household_id,
            token_before=token_before_command,
            token_after=token_after_command,
            projection_after=projection_after_command,
        )

        assert token_before_command != token_after_command, "Projection version token did not change after command"

        decisions_after = projection_after_command.get("decisions")
        assert isinstance(decisions_after, dict)
        decision_snapshot = decisions_after.get(decision_id)
        assert isinstance(decision_snapshot, dict)
        assert decision_snapshot.get("state") == "completed"

        after_bundle, after_token_start, after_token_end = fetch_surface_bundle(
            client,
            household_id=household_id,
            scenario_date=scenario_date,
        )
        validate_cross_surface_consistency(
            after_bundle,
            household_id=household_id,
            scenario_date=scenario_date,
            projection_token_before=after_token_start,
            projection_token_after=after_token_end,
        )

        home_before = before_bundle["home"]
        home_after = after_bundle["home"]

        normalized_home_before = normalize_surface_payload("home", home_before.response_payload)
        normalized_home_after = normalize_surface_payload("home", home_after.response_payload)
        home_diffs = deep_diff_entries(normalized_home_before, normalized_home_after)

        allowed_paths = allowed_home_diff_paths()
        allowed_diffs = []
        unexpected_diffs = []
        for entry in home_diffs:
            path = str(entry.get("path") or "")
            is_allowed = False
            for allowed in allowed_paths:
                if path == allowed or path.startswith(f"{allowed}.") or path.startswith(f"{allowed}["):
                    is_allowed = True
                    break
            if is_allowed:
                allowed_diffs.append(entry)
            else:
                unexpected_diffs.append(entry)

        log_debug(
            "phase4_home_transition_diff",
            allowed_paths=sorted(allowed_paths),
            allowed_diffs=allowed_diffs,
            unexpected_diffs=unexpected_diffs,
        )
        assert not unexpected_diffs, (
            "Home response changed in unexpected fields after state transition. "
            f"Diffs: {json.dumps(unexpected_diffs, indent=2, default=str)}"
        )
