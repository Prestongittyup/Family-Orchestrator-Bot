from __future__ import annotations

"""Frozen /tasks contract regression suite for post-Sprint 5 stabilization."""

from typing import Any

import pytest

from app.api import tasks as tasks_api
from core.replay.event_replay_engine import replay, validate_replay
from tests.test_sprint4_tasks_pagination_search import (
    _build_runtime,
    _build_test_client,
    _complete_task,
    _create_task,
)


# FEATURE_INTAKE:
#   projection_impact: no
#   read_model_impact: yes
#   kernel_interaction: none
FEATURE_INTAKE_DECLARATION = {
    "projection_impact": "no",
    "read_model_impact": "yes",
    "kernel_interaction": "none",
}


def test_tasks_response_schema_shape_is_frozen() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s5-schema-lock"
        _create_task(client, household_id=household_id, title="Schema lock task")

        response = client.get("/tasks", params={"household_id": household_id})

    assert response.status_code == 200
    payload = response.json()
    tasks = payload["tasks"]

    assert list(payload.keys()) == ["tasks", "summary", "pagination"]
    assert list(payload["summary"].keys()) == ["total", "pending", "completed"]
    assert list(payload["pagination"].keys()) == ["limit", "offset", "returned"]
    assert len(tasks) == 1
    assert list(tasks[0].keys()) == [
        "task_id",
        "title",
        "description",
        "priority",
        "due_at",
        "status",
        "created_at",
        "completed_at",
    ]


def test_tasks_pagination_contract_stable_after_dataset_change() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s5-pagination-post-mutation"
        for index in range(6):
            _create_task(
                client,
                household_id=household_id,
                title=f"Stable page {index}",
            )

        params = {
            "household_id": household_id,
            "sort_by": "created_at",
            "order": "desc",
            "limit": 2,
            "offset": 1,
        }
        _ = client.get("/tasks", params=params)

        _create_task(
            client,
            household_id=household_id,
            title="Newly added task",
        )

        first = client.get("/tasks", params=params)
        second = client.get("/tasks", params=params)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert first.json()["pagination"] == {"limit": 2, "offset": 1, "returned": 2}


def test_tasks_pagination_is_stable_for_identical_query() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s5-pagination-stability"
        for index in range(8):
            _create_task(
                client,
                household_id=household_id,
                title=f"Task {index}",
            )

        params = {
            "household_id": household_id,
            "sort_by": "created_at",
            "order": "desc",
            "limit": 3,
            "offset": 2,
        }
        first = client.get("/tasks", params=params)
        second = client.get("/tasks", params=params)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()


def test_tasks_sorting_is_stable_when_sort_values_duplicate() -> None:
    tasks: list[dict[str, Any]] = [
        {
            "task_id": "task-b",
            "created_at": "2026-01-01T00:00:00Z",
            "completed_at": "2026-02-01T00:00:00Z",
        },
        {
            "task_id": "task-a",
            "created_at": "2026-01-01T00:00:00Z",
            "completed_at": "2026-02-01T00:00:00Z",
        },
        {
            "task_id": "task-c",
            "created_at": "2026-01-02T00:00:00Z",
            "completed_at": "2026-02-01T00:00:00Z",
        },
    ]

    ascending = tasks_api._sorted_tasks(tasks, sort_by="completed_at", order="asc")
    descending = tasks_api._sorted_tasks(tasks, sort_by="completed_at", order="desc")

    assert [str(task.get("task_id")) for task in ascending] == ["task-a", "task-b", "task-c"]
    assert [str(task.get("task_id")) for task in descending] == ["task-c", "task-b", "task-a"]


def test_tasks_search_partial_case_insensitive_is_preserved() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s5-search"
        _create_task(
            client,
            household_id=household_id,
            title="Book Dentist Visit",
            description="Call clinic for next-week slot",
        )
        _create_task(
            client,
            household_id=household_id,
            title="Buy groceries",
            description="Get milk and bread",
        )

        title_match = client.get(
            "/tasks",
            params={"household_id": household_id, "search": "DENT"},
        )
        description_match = client.get(
            "/tasks",
            params={"household_id": household_id, "search": "clinic"},
        )

    assert title_match.status_code == 200
    assert description_match.status_code == 200

    title_tasks = title_match.json()["tasks"]
    description_tasks = description_match.json()["tasks"]

    assert len(title_tasks) == 1
    assert len(description_tasks) == 1
    assert "dent" in title_tasks[0]["title"].lower()
    assert "clinic" in description_tasks[0]["description"].lower()


def test_tasks_replay_invariance_under_hardened_read_pipeline() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s5-replay"
        first = _create_task(
            client,
            household_id=household_id,
            title="Plan camping trip",
            description="trip logistics checklist",
        )
        _create_task(
            client,
            household_id=household_id,
            title="Trip budget",
            description="trip cost planning",
        )
        _complete_task(client, household_id=household_id, task_id=first)

        response = client.get(
            "/tasks",
            params={
                "household_id": household_id,
                "search": "TRIP",
                "sort_by": "created_at",
                "order": "desc",
                "limit": "1",
                "offset": "0",
            },
        )

    assert response.status_code == 200
    payload = response.json()

    live_projection = runtime.get_projection(household_id, force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True

    projection_tasks = [
        task
        for task in list(live_projection.get("tasks_list") or [])
        if isinstance(task, dict)
    ]
    records = tasks_api._materialized_task_records(projection_tasks)
    filtered, summary = tasks_api._filtered_records_with_summary(
        records,
        status=tasks_api._normalized_status_param(None),
        search=tasks_api._normalized_search_param("TRIP"),
    )
    sorted_tasks = tasks_api._sorted_tasks(filtered, sort_by="created_at", order="desc")
    expected_tasks = tasks_api._paginated_tasks(sorted_tasks, limit=1, offset=0)

    assert payload["tasks"] == expected_tasks
    assert payload["summary"] == summary
    assert payload["pagination"] == {"limit": 1, "offset": 0, "returned": len(expected_tasks)}


def test_tasks_pipeline_materializes_projection_once(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s5-performance"
        for index in range(7):
            _create_task(
                client,
                household_id=household_id,
                title=f"Perf Task {index}",
                description="steady query profile",
            )

        projection = runtime.get_projection(household_id)
        projection_task_count = len(
            [
                task
                for task in list(projection.get("tasks_list") or [])
                if isinstance(task, dict)
            ]
        )

        call_count = 0
        original = tasks_api._normalize_task_row

        def _counting_normalizer(task: Any) -> dict[str, Any]:
            nonlocal call_count
            call_count += 1
            return original(task)

        monkeypatch.setattr(tasks_api, "_normalize_task_row", _counting_normalizer)

        response = client.get(
            "/tasks",
            params={
                "household_id": household_id,
                "status": "pending",
                "search": "perf",
                "sort_by": "created_at",
                "order": "desc",
                "limit": 3,
                "offset": 1,
            },
        )

    assert response.status_code == 200
    assert call_count == projection_task_count


def test_tasks_invalid_query_inputs_are_normalized_deterministically() -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s5-normalization"
        _create_task(client, household_id=household_id, title="A")
        _create_task(client, household_id=household_id, title="B")

        baseline = client.get("/tasks", params={"household_id": household_id})
        invalid = client.get(
            "/tasks",
            params={
                "household_id": household_id,
                "status": "not-a-status",
                "sort_by": "unknown-field",
                "order": "weird-order",
                "limit": "-999",
                "offset": "-5",
                "search": "   ",
            },
        )
        malformed = client.get(
            "/tasks",
            params={
                "household_id": household_id,
                "limit": "not-an-int",
                "offset": "not-an-int",
            },
        )

    assert baseline.status_code == 200
    assert invalid.status_code == 200
    assert malformed.status_code == 200

    baseline_body = baseline.json()
    invalid_body = invalid.json()
    malformed_body = malformed.json()

    assert invalid_body["tasks"] == baseline_body["tasks"]
    assert invalid_body["summary"] == baseline_body["summary"]
    assert invalid_body["pagination"] == {
        "limit": tasks_api._DEFAULT_PAGE_LIMIT,
        "offset": tasks_api._DEFAULT_PAGE_OFFSET,
        "returned": len(baseline_body["tasks"]),
    }

    assert malformed_body["tasks"] == baseline_body["tasks"]
    assert malformed_body["summary"] == baseline_body["summary"]
    assert malformed_body["pagination"] == {
        "limit": tasks_api._DEFAULT_PAGE_LIMIT,
        "offset": tasks_api._DEFAULT_PAGE_OFFSET,
        "returned": len(baseline_body["tasks"]),
    }


def test_tasks_read_cache_reuses_computed_view_for_identical_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s5-cache-hit"
        for index in range(6):
            _create_task(
                client,
                household_id=household_id,
                title=f"Cache task {index}",
                description="cache profile",
            )

        materialized_calls = 0
        filtered_calls = 0
        sorted_calls = 0

        original_materialized = tasks_api._materialized_task_records
        original_filtered = tasks_api._filtered_records_with_summary
        original_sorted = tasks_api._sorted_tasks

        def _count_materialized(tasks: list[dict[str, Any]]) -> list[tuple[dict[str, Any], str]]:
            nonlocal materialized_calls
            materialized_calls += 1
            return original_materialized(tasks)

        def _count_filtered(
            records: list[tuple[dict[str, Any], str]],
            *,
            status: str | None,
            search: str | None,
        ) -> tuple[list[dict[str, Any]], dict[str, int]]:
            nonlocal filtered_calls
            filtered_calls += 1
            return original_filtered(records, status=status, search=search)

        def _count_sorted(
            tasks: list[dict[str, Any]],
            *,
            sort_by: str,
            order: str,
        ) -> list[dict[str, Any]]:
            nonlocal sorted_calls
            sorted_calls += 1
            return [dict(task) for task in original_sorted(tasks, sort_by=sort_by, order=order)]

        monkeypatch.setattr(tasks_api, "_materialized_task_records", _count_materialized)
        monkeypatch.setattr(tasks_api, "_filtered_records_with_summary", _count_filtered)
        monkeypatch.setattr(tasks_api, "_sorted_tasks", _count_sorted)

        params = {
            "household_id": household_id,
            "status": "pending",
            "search": "cache",
            "sort_by": "created_at",
            "order": "desc",
            "limit": 3,
            "offset": 1,
        }
        first = client.get("/tasks", params=params)
        second = client.get("/tasks", params=params)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert materialized_calls == 1
    assert filtered_calls == 1
    assert sorted_calls == 1


def test_tasks_read_cache_invalidates_when_new_event_arrives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _ = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s5-cache-invalidation"
        for index in range(3):
            _create_task(
                client,
                household_id=household_id,
                title=f"Invalidate task {index}",
            )

        materialized_calls = 0
        original_materialized = tasks_api._materialized_task_records

        def _count_materialized(tasks: list[dict[str, Any]]) -> list[tuple[dict[str, Any], str]]:
            nonlocal materialized_calls
            materialized_calls += 1
            return original_materialized(tasks)

        monkeypatch.setattr(tasks_api, "_materialized_task_records", _count_materialized)

        params = {
            "household_id": household_id,
            "sort_by": "created_at",
            "order": "desc",
            "limit": 2,
            "offset": 0,
        }
        first = client.get("/tasks", params=params)
        second = client.get("/tasks", params=params)

        _create_task(client, household_id=household_id, title="Invalidate task new")
        third = client.get("/tasks", params=params)

    assert first.status_code == 200
    assert second.status_code == 200
    assert third.status_code == 200
    assert first.json() == second.json()
    assert materialized_calls == 2


def test_tasks_cached_read_matches_force_replay_equivalence() -> None:
    runtime, event_log = _build_runtime()

    with _build_test_client(runtime) as client:
        household_id = "household-s5-cache-replay-equivalence"
        first_task = _create_task(
            client,
            household_id=household_id,
            title="Replay cache task one",
            description="replay cache profile",
        )
        _create_task(
            client,
            household_id=household_id,
            title="Replay cache task two",
            description="replay cache profile",
        )
        _complete_task(client, household_id=household_id, task_id=first_task)

        params = {
            "household_id": household_id,
            "search": "replay",
            "sort_by": "created_at",
            "order": "desc",
            "limit": 1,
            "offset": 0,
        }
        _ = client.get("/tasks", params=params)
        cached_response = client.get("/tasks", params=params)

    assert cached_response.status_code == 200
    cached_payload = cached_response.json()

    live_projection = runtime.get_projection(household_id, force_replay=True)
    replayed = replay(event_log.insert_order)
    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True

    projection_tasks = [
        task
        for task in list(live_projection.get("tasks_list") or [])
        if isinstance(task, dict)
    ]
    projection_fingerprint = tasks_api._projection_fingerprint(
        household_id=household_id,
        projection=live_projection,
    )
    records = tasks_api._get_or_build_materialized_records(
        projection_fingerprint=projection_fingerprint,
        projection_tasks=projection_tasks,
    )
    sorted_tasks, summary_counts = tasks_api._get_or_build_sorted_view(
        projection_fingerprint=projection_fingerprint,
        records=records,
        status=tasks_api._normalized_status_param(None),
        search=tasks_api._normalized_search_param("replay"),
        sort_by="created_at",
        order="desc",
        limit=1,
        offset=0,
    )
    expected_tasks = [dict(task) for task in tasks_api._paginated_tasks(sorted_tasks, limit=1, offset=0)]
    expected_summary = tasks_api._summary_from_counts(summary_counts)

    assert cached_payload["tasks"] == expected_tasks
    assert cached_payload["summary"] == expected_summary
    assert cached_payload["pagination"] == {"limit": 1, "offset": 0, "returned": len(expected_tasks)}
