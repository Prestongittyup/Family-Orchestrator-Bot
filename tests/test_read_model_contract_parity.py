from __future__ import annotations

import inspect
from typing import Any, Callable

import pytest

from app.api import notifications as notifications_api
from app.api import read_model_shared as read_model_shared
from app.api import reminders as reminders_api
from app.api import schedule as schedule_api
from app.api import tasks as tasks_api
from core.replay.event_replay_engine import replay, validate_replay
from tests.governance.contract_kernel import (
    READ_MODEL_DOMAIN_SPECS,
    READ_MODEL_ENDPOINT_MARKERS,
    READ_MODEL_SORTED_VIEW_PREFIX_MARKERS,
    READ_MODEL_SORTED_VIEW_SUFFIX_MARKERS,
    assert_marker_order,
)
from tests.test_notification_guardrails import (
    _build_runtime,
    _build_test_client,
    _create_reminder,
    _post_command,
    _trigger_reminder,
)


# FEATURE_INTAKE:
#   projection_impact: yes
#   read_model_impact: yes
#   kernel_interaction: reference
FEATURE_INTAKE_DECLARATION = {
    "projection_impact": "yes",
    "read_model_impact": "yes",
    "kernel_interaction": "reference",
}


_DOMAIN_MODULES: dict[str, Any] = {
    "tasks": tasks_api,
    "schedule": schedule_api,
    "reminders": reminders_api,
    "notifications": notifications_api,
}

_TIE_BREAK_SORT_FIELDS: dict[str, str] = {
    "tasks": "completed_at",
    "schedule": "start_at",
    "reminders": "trigger_at",
    "notifications": "created_at",
}


def _clear_read_model_caches() -> None:
    tasks_api._MATERIALIZED_RECORDS_CACHE.clear()
    tasks_api._VIEW_CACHE.clear()

    schedule_api._MATERIALIZED_RECORDS_CACHE.clear()
    schedule_api._VIEW_CACHE.clear()

    reminders_api._MATERIALIZED_RECORDS_CACHE.clear()
    reminders_api._VIEW_CACHE.clear()

    notifications_api._MATERIALIZED_RECORDS_CACHE.clear()
    notifications_api._VIEW_CACHE.clear()


def _id_sequence(rows: list[dict[str, Any]], *, id_field: str) -> list[str]:
    return [str(row.get(id_field) or "") for row in rows]


def _sorted_with_case(
    *,
    sorter: Callable[..., list[dict[str, Any]]],
    sort_by: str,
    id_field: str,
    rows: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    asc = sorter(rows, sort_by=sort_by, order="asc")
    desc = sorter(rows, sort_by=sort_by, order="desc")
    return _id_sequence(asc, id_field=id_field), _id_sequence(desc, id_field=id_field)


def _assert_repeated_sorted_results(
    *,
    sorter: Callable[..., list[dict[str, Any]]],
    sort_by: str,
    id_field: str,
    rows: list[dict[str, Any]],
    runs: int = 5,
) -> tuple[list[str], list[str]]:
    assert runs >= 5
    outcomes = [
        _sorted_with_case(
            sorter=sorter,
            sort_by=sort_by,
            id_field=id_field,
            rows=[dict(row) for row in rows],
        )
        for _ in range(runs)
    ]
    baseline = outcomes[0]
    for outcome in outcomes[1:]:
        assert outcome == baseline
    return baseline


def _assert_identical_query_results(
    *,
    client: Any,
    path: str,
    params: dict[str, Any],
    runs: int = 5,
) -> list[dict[str, Any]]:
    assert runs >= 5
    responses = [client.get(path, params=params) for _ in range(runs)]
    payloads: list[dict[str, Any]] = []
    for response in responses:
        assert response.status_code == 200
        payloads.append(response.json())

    baseline = payloads[0]
    for payload in payloads[1:]:
        assert payload == baseline
    return payloads


def test_pipeline_order_parity_across_domains() -> None:
    for module_name, endpoint_name, sorted_symbol, _id_field in READ_MODEL_DOMAIN_SPECS:
        module = _DOMAIN_MODULES[module_name]

        endpoint_source = inspect.getsource(getattr(module, endpoint_name))
        assert_marker_order(endpoint_source, READ_MODEL_ENDPOINT_MARKERS)

        sorted_view_source = inspect.getsource(module._get_or_build_sorted_view)
        assert_marker_order(
            sorted_view_source,
            [
                *READ_MODEL_SORTED_VIEW_PREFIX_MARKERS,
                f"{sorted_symbol}(",
                *READ_MODEL_SORTED_VIEW_SUFFIX_MARKERS,
            ],
        )


def test_sorting_contract_parity_across_domains() -> None:
    shared_rows = [
        {
            "created_at": "2032-01-01T00:00:00Z",
            "source_event_id": "event-b",
            "task_id": "id-c",
            "schedule_id": "id-c",
            "reminder_id": "id-c",
            "notification_id": "id-c",
        },
        {
            "created_at": "2032-01-01T00:00:00Z",
            "source_event_id": "event-a",
            "task_id": "id-b",
            "schedule_id": "id-b",
            "reminder_id": "id-b",
            "notification_id": "id-b",
        },
        {
            "created_at": "2032-01-01T00:00:00Z",
            "source_event_id": "event-a",
            "task_id": "id-a",
            "schedule_id": "id-a",
            "reminder_id": "id-a",
            "notification_id": "id-a",
        },
    ]

    for module_name, _endpoint_name, sorted_symbol, id_field in READ_MODEL_DOMAIN_SPECS:
        module = _DOMAIN_MODULES[module_name]
        sorter = getattr(module, sorted_symbol)
        asc_ids, desc_ids = _assert_repeated_sorted_results(
            sorter=sorter,
            sort_by="created_at",
            id_field=id_field,
            rows=shared_rows,
        )
        assert asc_ids == ["id-a", "id-b", "id-c"]
        assert desc_ids == ["id-c", "id-b", "id-a"]


def test_tie_break_determinism_created_event_entity_order_is_consistent() -> None:
    tie_rows = [
        {
            "completed_at": "2032-02-01T00:00:00Z",
            "start_at": "2032-02-01T00:00:00Z",
            "trigger_at": "2032-02-01T00:00:00Z",
            "created_at": "2032-02-02T00:00:00Z",
            "source_event_id": "event-a",
            "task_id": "id-z",
            "schedule_id": "id-z",
            "reminder_id": "id-z",
            "notification_id": "id-z",
        },
        {
            "completed_at": "2032-02-01T00:00:00Z",
            "start_at": "2032-02-01T00:00:00Z",
            "trigger_at": "2032-02-01T00:00:00Z",
            "created_at": "2032-02-01T00:00:00Z",
            "source_event_id": "event-z",
            "task_id": "id-a",
            "schedule_id": "id-a",
            "reminder_id": "id-a",
            "notification_id": "id-a",
        },
        {
            "completed_at": "2032-02-01T00:00:00Z",
            "start_at": "2032-02-01T00:00:00Z",
            "trigger_at": "2032-02-01T00:00:00Z",
            "created_at": "2032-02-01T00:00:00Z",
            "source_event_id": "event-a",
            "task_id": "id-c",
            "schedule_id": "id-c",
            "reminder_id": "id-c",
            "notification_id": "id-c",
        },
        {
            "completed_at": "2032-02-01T00:00:00Z",
            "start_at": "2032-02-01T00:00:00Z",
            "trigger_at": "2032-02-01T00:00:00Z",
            "created_at": "2032-02-01T00:00:00Z",
            "source_event_id": "event-a",
            "task_id": "id-b",
            "schedule_id": "id-b",
            "reminder_id": "id-b",
            "notification_id": "id-b",
        },
    ]

    for module_name, _endpoint_name, sorted_symbol, id_field in READ_MODEL_DOMAIN_SPECS:
        module = _DOMAIN_MODULES[module_name]
        sorter = getattr(module, sorted_symbol)
        asc_ids, _desc_ids = _assert_repeated_sorted_results(
            sorter=sorter,
            sort_by=_TIE_BREAK_SORT_FIELDS[module_name],
            id_field=id_field,
            rows=tie_rows,
        )
        assert asc_ids == ["id-b", "id-c", "id-a", "id-z"]


def test_cache_fingerprint_consistency_and_non_authority() -> None:
    projection = {
        "last_event_id": "evt-1",
        "state_version": 7,
        "checksum": "abc123",
    }

    tasks_fp = tasks_api._projection_fingerprint(household_id="household-x", projection=projection)
    schedule_fp = schedule_api._projection_fingerprint(household_id="household-x", projection=projection)
    reminders_fp = reminders_api._projection_fingerprint(household_id="household-x", projection=projection)
    notifications_fp = notifications_api._projection_fingerprint(household_id="household-x", projection=projection)

    assert tasks_fp == schedule_fp == reminders_fp == notifications_fp

    query_state = read_model_shared.query_cache_state(
        filter_state={"status": "pending"},
        search="alpha",
        sort_by="created_at",
        order="desc",
        limit=2,
        offset=1,
    )
    assert len(query_state) == 6
    assert query_state[0] == "status=pending"

    runtime, _event_log = _build_runtime()
    _clear_read_model_caches()

    with _build_test_client(runtime) as client:
        household_id = "household-contract-cache"

        _post_command(
            client,
            command_type="task.create",
            household_id=household_id,
            payload={"title": "Cache baseline task", "priority": "low"},
        )
        _post_command(
            client,
            command_type="schedule.create",
            household_id=household_id,
            payload={
                "title": "Cache baseline schedule",
                "start_at": "2033-01-01T08:00:00Z",
                "end_at": "2033-01-01T09:00:00Z",
            },
        )
        reminder_created = _create_reminder(
            client,
            household_id=household_id,
            title="Cache baseline reminder",
            message="cache baseline",
            trigger_at="2033-01-01T10:00:00Z",
        )
        reminder_id = str(reminder_created["response"]["reminder"]["reminder_id"])
        _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=reminder_id,
            system_trigger=True,
        )

        projection_before = runtime.get_projection(household_id, force_replay=True)
        before_fp = tasks_api._projection_fingerprint(household_id=household_id, projection=projection_before)

        task_query_params = {"household_id": household_id, "limit": 5, "offset": 0}
        tasks_before_runs = _assert_identical_query_results(
            client=client,
            path="/tasks",
            params=task_query_params,
        )
        task_page_0 = _assert_identical_query_results(
            client=client,
            path="/tasks",
            params={"household_id": household_id, "limit": 1, "offset": 0},
        )
        task_page_1 = _assert_identical_query_results(
            client=client,
            path="/tasks",
            params={"household_id": household_id, "limit": 1, "offset": 1},
        )
        assert task_page_0[0] != task_page_1[0]

        _clear_read_model_caches()
        runtime.get_projection(household_id, force_replay=True)
        tasks_before_recomputed = _assert_identical_query_results(
            client=client,
            path="/tasks",
            params=task_query_params,
        )[0]
        assert tasks_before_recomputed == tasks_before_runs[0]

        _post_command(
            client,
            command_type="task.create",
            household_id=household_id,
            payload={"title": "Cache mutation task", "priority": "low"},
        )

        projection_after = runtime.get_projection(household_id, force_replay=True)
        after_fp = tasks_api._projection_fingerprint(household_id=household_id, projection=projection_after)

        tasks_after_runs = _assert_identical_query_results(
            client=client,
            path="/tasks",
            params=task_query_params,
        )

        _clear_read_model_caches()
        runtime.get_projection(household_id, force_replay=True)
        tasks_after_recomputed = _assert_identical_query_results(
            client=client,
            path="/tasks",
            params=task_query_params,
        )[0]
        assert tasks_after_recomputed == tasks_after_runs[0]

    assert before_fp != after_fp
    assert tasks_before_runs[0]["summary"]["total"] + 1 == tasks_after_runs[0]["summary"]["total"]


def test_cross_domain_replay_equivalence_with_read_models() -> None:
    runtime, event_log = _build_runtime()
    _clear_read_model_caches()

    with _build_test_client(runtime) as client:
        household_id = "household-contract-replay"

        for index in range(6):
            created = _post_command(
                client,
                command_type="task.create",
                household_id=household_id,
                payload={"title": f"Replay task {index}", "priority": "medium"},
            )
            task_id = str(created["response"]["task"]["task_id"])
            if index % 2 == 0:
                _post_command(
                    client,
                    command_type="task_completed",
                    household_id=household_id,
                    payload={"task_id": task_id},
                )

        for index in range(6):
            created = _post_command(
                client,
                command_type="schedule.create",
                household_id=household_id,
                payload={
                    "title": f"Replay schedule {index}",
                    "start_at": f"2034-02-{(index % 8) + 1:02d}T08:00:00Z",
                    "end_at": f"2034-02-{(index % 8) + 1:02d}T09:00:00Z",
                },
            )
            schedule_id = str(created["response"]["schedule"]["schedule_id"])
            if index % 3 == 0:
                _post_command(
                    client,
                    command_type="schedule.cancel",
                    household_id=household_id,
                    payload={"schedule_id": schedule_id},
                )

        for index in range(6):
            created = _create_reminder(
                client,
                household_id=household_id,
                title=f"Replay reminder {index}",
                message=f"Replay message {index}",
                trigger_at=f"2034-03-{(index % 8) + 1:02d}T10:00:00Z",
            )
            reminder_id = str(created["response"]["reminder"]["reminder_id"])
            if index % 2 == 1:
                _trigger_reminder(
                    client,
                    household_id=household_id,
                    reminder_id=reminder_id,
                    system_trigger=True,
                )

        tasks_params = {"household_id": household_id, "limit": 6, "offset": 0}
        schedule_params = {"household_id": household_id, "limit": 6, "offset": 0}
        reminders_params = {"household_id": household_id, "limit": 6, "offset": 0}
        notifications_params = {"household_id": household_id, "limit": 6, "offset": 0}

        tasks_first = _assert_identical_query_results(client=client, path="/tasks", params=tasks_params)[0]
        schedule_first = _assert_identical_query_results(client=client, path="/schedule", params=schedule_params)[0]
        reminders_first = _assert_identical_query_results(client=client, path="/reminders", params=reminders_params)[0]
        notifications_first = _assert_identical_query_results(
            client=client,
            path="/notifications",
            params=notifications_params,
        )[0]

        replay_runs = [replay(event_log.insert_order) for _ in range(5)]
        replayed = replay_runs[0]
        for replayed_run in replay_runs[1:]:
            assert replayed_run == replayed

        _clear_read_model_caches()
        runtime.get_projection(household_id, force_replay=True)

        tasks_second = _assert_identical_query_results(client=client, path="/tasks", params=tasks_params)[0]
        schedule_second = _assert_identical_query_results(client=client, path="/schedule", params=schedule_params)[0]
        reminders_second = _assert_identical_query_results(client=client, path="/reminders", params=reminders_params)[0]
        notifications_second = _assert_identical_query_results(
            client=client,
            path="/notifications",
            params=notifications_params,
        )[0]

    assert tasks_first == tasks_second
    assert schedule_first == schedule_second
    assert reminders_first == reminders_second
    assert notifications_first == notifications_second

    live_projection = runtime.get_projection(household_id, force_replay=True)
    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True
    assert live_projection == replayed["derived_state"]


def test_projection_object_isolation_across_logical_requests() -> None:
    runtime, _event_log = _build_runtime()
    _clear_read_model_caches()

    with _build_test_client(runtime) as client:
        household_a = "household-projection-isolation-a"
        household_b = "household-projection-isolation-b"

        _post_command(
            client,
            command_type="task.create",
            household_id=household_a,
            payload={"title": "projection-a-anchor", "priority": "medium"},
        )
        _post_command(
            client,
            command_type="task.create",
            household_id=household_b,
            payload={"title": "projection-b-anchor", "priority": "medium"},
        )

        projection_a_first = runtime.get_projection(household_a)
        projection_b_first = runtime.get_projection(household_b)
        projection_a_second = runtime.get_projection(household_a)

        assert id(projection_a_first) != id(projection_a_second)
        assert id(projection_a_first) != id(projection_b_first)

        tasks_a_first = projection_a_first.get("tasks")
        tasks_a_second = projection_a_second.get("tasks")
        tasks_b_first = projection_b_first.get("tasks")

        assert isinstance(tasks_a_first, dict)
        assert isinstance(tasks_a_second, dict)
        assert isinstance(tasks_b_first, dict)

        assert id(tasks_a_first) != id(tasks_a_second)
        assert id(tasks_a_first) != id(tasks_b_first)

        tasks_a_first["synthetic-local-only"] = {"task_id": "synthetic-local-only"}

        projection_a_after_local_mutation = runtime.get_projection(household_a)
        projection_b_after_local_mutation = runtime.get_projection(household_b)

        tasks_after_a = projection_a_after_local_mutation.get("tasks")
        tasks_after_b = projection_b_after_local_mutation.get("tasks")
        assert isinstance(tasks_after_a, dict)
        assert isinstance(tasks_after_b, dict)
        assert "synthetic-local-only" not in tasks_after_a
        assert "synthetic-local-only" not in tasks_after_b

        _post_command(
            client,
            command_type="schedule.create",
            household_id=household_a,
            payload={
                "title": "projection-a-schedule",
                "start_at": "2037-01-01T08:00:00Z",
                "end_at": "2037-01-01T09:00:00Z",
            },
        )
        _post_command(
            client,
            command_type="schedule.create",
            household_id=household_b,
            payload={
                "title": "projection-b-schedule",
                "start_at": "2037-01-02T08:00:00Z",
                "end_at": "2037-01-02T09:00:00Z",
            },
        )

        projection_a_interleaved = runtime.get_projection(household_a, force_replay=True)
        projection_b_interleaved = runtime.get_projection(household_b, force_replay=True)

    assert id(projection_a_interleaved) != id(projection_b_interleaved)
    assert projection_a_interleaved != projection_b_interleaved

    tasks_list_a = list(projection_a_interleaved.get("tasks_list") or [])
    tasks_list_b = list(projection_b_interleaved.get("tasks_list") or [])
    assert all(str(row.get("title") or "") != "projection-b-anchor" for row in tasks_list_a if isinstance(row, dict))
    assert all(str(row.get("title") or "") != "projection-a-anchor" for row in tasks_list_b if isinstance(row, dict))


def test_pipeline_structural_drift_detection_guards() -> None:
    modules = [tasks_api, schedule_api, reminders_api, notifications_api]

    for module in modules:
        source = inspect.getsource(module.get_tasks if module is tasks_api else module.get_schedule if module is schedule_api else module.get_reminders if module is reminders_api else module.get_notifications)
        assert "_get_or_build_materialized_records" in source
        assert "_get_or_build_sorted_view" in source
        assert "_paginated" in source

    for module in modules:
        sorted_view_source = inspect.getsource(module._get_or_build_sorted_view)
        assert "_filtered_records_with_summary" in sorted_view_source
        assert "_sorted" in sorted_view_source
        assert "_shared_query_cache_state" in sorted_view_source


@pytest.mark.parametrize(
    "module,sort_symbol",
    [
        (_DOMAIN_MODULES[module_name], sorted_symbol)
        for module_name, _endpoint_name, sorted_symbol, _id_field in READ_MODEL_DOMAIN_SPECS
    ],
)
def test_sorting_helpers_delegate_to_shared_tie_break_contract(module: Any, sort_symbol: str) -> None:
    source = inspect.getsource(getattr(module, sort_symbol))
    assert "_shared_sort_records_with_tie_break(" in source
