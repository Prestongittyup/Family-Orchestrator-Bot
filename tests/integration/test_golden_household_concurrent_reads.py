from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.main import app as household_surface_app
from tests.harness.household_contract_harness import (
    assert_home_structure_valid,
    assert_projection_deterministic,
    create_test_client,
    get_home,
    reset_projection_state,
)

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]


def _fetch_home_snapshot(household_id: str, scenario_date: str) -> dict:
    with create_test_client(household_surface_app) as client:
        payload = get_home(client, household_id, date=scenario_date)
    assert_home_structure_valid(payload)
    return payload


@pytest.mark.integration
def test_golden_household_concurrent_reads() -> None:
    household_id = "test-house-concurrent-001"
    scenario_date = "2026-05-05"
    total_reads = 20

    reset_projection_state(household_id)

    with ThreadPoolExecutor(max_workers=total_reads) as executor:
        futures = [
            executor.submit(_fetch_home_snapshot, household_id, scenario_date)
            for _ in range(total_reads)
        ]
        snapshots = [future.result(timeout=30) for future in futures]

    assert len(snapshots) == total_reads

    baseline = snapshots[0]
    for snapshot in snapshots[1:]:
        assert_projection_deterministic(baseline, snapshot)
