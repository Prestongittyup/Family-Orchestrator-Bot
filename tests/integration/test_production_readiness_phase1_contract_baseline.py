from __future__ import annotations

import pytest

from tests.harness.production_readiness_harness import (
    SURFACE_NAMES,
    assert_surface_deterministic,
    create_test_client,
    fetch_surface_bundle,
    household_surface_app,
    log_debug,
    reset_projection_state,
    validate_phase1_contract,
)

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]


@pytest.mark.integration
def test_phase1_contract_baseline_lock() -> None:
    household_id = "prod-ready-phase1-household"
    scenario_date = "2026-05-05"

    reset_projection_state(household_id)

    with create_test_client(household_surface_app) as client:
        baseline_records, baseline_token_before, baseline_token_after = fetch_surface_bundle(
            client,
            household_id=household_id,
            scenario_date=scenario_date,
        )
        validate_phase1_contract(baseline_records, household_id=household_id, scenario_date=scenario_date)

        log_debug(
            "phase1_baseline_projection_gate",
            household_id=household_id,
            scenario_date=scenario_date,
            token_before=baseline_token_before,
            token_after=baseline_token_after,
        )
        assert baseline_token_before == baseline_token_after

        repeated_records, repeated_token_before, repeated_token_after = fetch_surface_bundle(
            client,
            household_id=household_id,
            scenario_date=scenario_date,
        )
        validate_phase1_contract(repeated_records, household_id=household_id, scenario_date=scenario_date)

        log_debug(
            "phase1_repeated_projection_gate",
            household_id=household_id,
            scenario_date=scenario_date,
            token_before=repeated_token_before,
            token_after=repeated_token_after,
        )
        assert repeated_token_before == repeated_token_after

        for name in SURFACE_NAMES:
            allowed_paths: set[str] = set()
            assert_surface_deterministic(
                baseline_records[name],
                repeated_records[name],
                allowed_diff_paths=allowed_paths,
            )
