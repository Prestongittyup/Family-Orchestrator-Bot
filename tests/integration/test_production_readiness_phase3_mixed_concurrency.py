from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from tests.harness.household_contract_harness import assert_home_structure_valid
from tests.harness.production_readiness_harness import (
    RequestLogRecord,
    call_get_surface,
    call_post_decision_complete,
    create_test_client,
    household_surface_app,
    log_debug,
    normalize_surface_payload,
    report_hash_divergence,
    reset_projection_state,
    stable_hash,
    structure_signature,
)

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]


def _run_home_get_request(index: int, *, household_id: str, scenario_date: str) -> RequestLogRecord:
    with create_test_client(household_surface_app) as client:
        record = call_get_surface(
            client,
            name="home",
            household_id=household_id,
            scenario_date=scenario_date,
        )
        log_debug(
            "phase3_get_home_result",
            index=index,
            status_code=record.status_code,
            duration_ms=record.duration_ms,
            response_hash=record.response_hash,
            normalized_hash=record.normalized_hash,
        )
        return record


def _run_post_complete_request(index: int, *, household_id: str) -> RequestLogRecord:
    decision_id = f"phase3-concurrent-decision-{index:03d}"
    with create_test_client(household_surface_app) as client:
        record = call_post_decision_complete(
            client,
            household_id=household_id,
            decision_id=decision_id,
        )
        log_debug(
            "phase3_post_complete_result",
            index=index,
            decision_id=decision_id,
            status_code=record.status_code,
            duration_ms=record.duration_ms,
            response_hash=record.response_hash,
        )
        return record


def _run_concurrently(function, count: int, *, kwargs: dict[str, Any]) -> list[RequestLogRecord]:
    with ThreadPoolExecutor(max_workers=count) as executor:
        futures = [executor.submit(function, index, **kwargs) for index in range(count)]
        return [future.result(timeout=60) for future in futures]


@pytest.mark.integration
def test_phase3_mixed_concurrency_load() -> None:
    household_id = "prod-ready-phase3-household"
    scenario_date = "2026-05-05"

    reset_projection_state(household_id)

    with create_test_client(household_surface_app) as client:
        baseline_home = call_get_surface(
            client,
            name="home",
            household_id=household_id,
            scenario_date=scenario_date,
        )
        assert baseline_home.status_code == 200

    pre_read_records = _run_concurrently(
        _run_home_get_request,
        10,
        kwargs={"household_id": household_id, "scenario_date": scenario_date},
    )

    write_records = _run_concurrently(
        _run_post_complete_request,
        5,
        kwargs={"household_id": household_id},
    )

    post_read_records = _run_concurrently(
        _run_home_get_request,
        10,
        kwargs={"household_id": household_id, "scenario_date": scenario_date},
    )

    for record in pre_read_records + post_read_records:
        assert record.status_code == 200, f"GET /home failed: {record.status_code}"
        payload = record.response_payload
        assert isinstance(payload, dict)
        assert_home_structure_valid(payload)

    for record in write_records:
        assert record.status_code == 200, f"POST /decision/complete failed: {record.status_code}"
        payload = record.response_payload
        assert isinstance(payload, dict)
        assert payload.get("status") == "accepted"

    pre_divergence = report_hash_divergence(pre_read_records, label="phase3_pre_read")
    post_divergence = report_hash_divergence(post_read_records, label="phase3_post_read")

    assert len(pre_divergence) == 1, "Pre-write concurrent reads were not deterministic after normalization"
    assert len(post_divergence) == 1, "Post-write concurrent reads were not deterministic after normalization"

    pre_signatures = {
        stable_hash(structure_signature(normalize_surface_payload("home", record.response_payload)))
        for record in pre_read_records
    }
    post_signatures = {
        stable_hash(structure_signature(normalize_surface_payload("home", record.response_payload)))
        for record in post_read_records
    }

    log_debug(
        "phase3_structure_signatures",
        pre_signatures=sorted(pre_signatures),
        post_signatures=sorted(post_signatures),
    )

    assert len(pre_signatures) == 1, "Pre-write responses showed structural divergence"
    assert len(post_signatures) == 1, "Post-write responses showed structural divergence"

    with create_test_client(household_surface_app) as client:
        health = client.get("/health")
        assert health.status_code == 200, "Health endpoint failed after mixed concurrency run"
