from __future__ import annotations

import ast
import inspect
from typing import Any

from app.api import notifications as notifications_api
from app.api import read_model_shared as read_model_shared
from app.api import reminders as reminders_api
from app.api import schedule as schedule_api
from app.api import tasks as tasks_api
from core.replay.event_replay_engine import replay, validate_replay
from tests.governance.contract_kernel import (
    EXPECTED_SHARED_HELPER_EXPORTS,
    EXPECTED_SHARED_IMPORT_ALIASES,
    READ_MODEL_DOMAIN_SPECS,
    READ_MODEL_ENDPOINT_MARKERS,
    READ_MODEL_SORTED_VIEW_PREFIX_MARKERS,
    READ_MODEL_SORTED_VIEW_SUFFIX_MARKERS,
    SHARED_LAYER_FORBIDDEN_DOMAIN_TOKENS,
    SHARED_LAYER_FORBIDDEN_ENFORCEMENT_TOKENS,
    SHARED_LAYER_FORBIDDEN_INTERPRETATION_TOKENS,
    SHARED_LAYER_FORBIDDEN_POLICY_TOKENS,
    SHARED_LAYER_FORBIDDEN_UTILITY_TOKENS,
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

_DOMAIN_CASES: list[tuple[Any, str, str]] = [
    (_DOMAIN_MODULES[module_name], endpoint_name, sorted_symbol)
    for module_name, endpoint_name, sorted_symbol, _id_field in READ_MODEL_DOMAIN_SPECS
]


def _shared_import_aliases(module: Any) -> set[str]:
    tree = ast.parse(inspect.getsource(module))
    aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "app.api.read_model_shared":
            for imported in node.names:
                aliases.add(imported.asname or imported.name)
    return aliases


def _clear_domain_caches() -> None:
    tasks_api._MATERIALIZED_RECORDS_CACHE.clear()
    tasks_api._VIEW_CACHE.clear()

    schedule_api._MATERIALIZED_RECORDS_CACHE.clear()
    schedule_api._VIEW_CACHE.clear()

    reminders_api._MATERIALIZED_RECORDS_CACHE.clear()
    reminders_api._VIEW_CACHE.clear()

    notifications_api._MATERIALIZED_RECORDS_CACHE.clear()
    notifications_api._VIEW_CACHE.clear()


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


def _get_tasks_payload(
    *,
    client: Any,
    household_id: str,
    search: str,
) -> dict[str, Any]:
    response = client.get(
        "/tasks",
        params={
            "household_id": household_id,
            "search": search,
            "sort_by": "created_at",
            "order": "desc",
            "limit": 10,
            "offset": 0,
        },
    )
    assert response.status_code == 200
    return response.json()


def _events_for_household(event_log: Any, *, household_id: str) -> list[Any]:
    return [row for row in event_log.insert_order if row.household_id == household_id]


def test_shared_layer_usage_and_exports_are_frozen() -> None:
    exported = {
        name
        for name, value in inspect.getmembers(read_model_shared, inspect.isfunction)
        if getattr(value, "__module__", "") == read_model_shared.__name__
    }
    assert exported == EXPECTED_SHARED_HELPER_EXPORTS

    for module, _endpoint_name, _sort_name in _DOMAIN_CASES:
        assert _shared_import_aliases(module) == EXPECTED_SHARED_IMPORT_ALIASES


def test_shared_layer_is_utility_only_and_not_orchestration() -> None:
    source = inspect.getsource(read_model_shared).lower()
    for token in SHARED_LAYER_FORBIDDEN_UTILITY_TOKENS:
        assert token not in source
    for token in SHARED_LAYER_FORBIDDEN_DOMAIN_TOKENS:
        assert token not in source
    for token in SHARED_LAYER_FORBIDDEN_POLICY_TOKENS:
        assert token not in source
    for token in SHARED_LAYER_FORBIDDEN_ENFORCEMENT_TOKENS:
        assert token not in source
    for token in SHARED_LAYER_FORBIDDEN_INTERPRETATION_TOKENS:
        assert token not in source

    for name, value in inspect.getmembers(read_model_shared, inspect.isfunction):
        if getattr(value, "__module__", "") != read_model_shared.__name__:
            continue
        lowered = name.lower()
        assert "pipeline" not in lowered
        assert "orches" not in lowered
        assert "endpoint" not in lowered
        assert "router" not in lowered
        assert "assemble" not in lowered


def test_domains_own_pipeline_order_and_cache_policy() -> None:
    for module, endpoint_name, sorted_name in _DOMAIN_CASES:
        endpoint_source = inspect.getsource(getattr(module, endpoint_name))
        assert_marker_order(
            endpoint_source,
            [*READ_MODEL_ENDPOINT_MARKERS, "return {"],
        )

        sorted_view_source = inspect.getsource(module._get_or_build_sorted_view)
        assert_marker_order(
            sorted_view_source,
            [
                READ_MODEL_SORTED_VIEW_PREFIX_MARKERS[0],
                "view_key =",
                READ_MODEL_SORTED_VIEW_PREFIX_MARKERS[1],
                f"{sorted_name}(",
                *READ_MODEL_SORTED_VIEW_SUFFIX_MARKERS,
            ],
        )
        assert "max_entries=_VIEW_CACHE_MAX_ENTRIES" in sorted_view_source

        sorted_source = inspect.getsource(getattr(module, sorted_name))
        assert "_shared_sort_records_with_tie_break(" in sorted_source
        assert "_shared_sort_records_with_tie_break(" not in endpoint_source


def test_cache_key_structure_projection_and_query_state_is_consistent() -> None:
    projection = {
        "last_event_id": "evt-123",
        "state_version": 9,
        "checksum": "sum-123",
    }

    fingerprints = [
        tasks_api._projection_fingerprint(household_id="household-x", projection=projection),
        schedule_api._projection_fingerprint(household_id="household-x", projection=projection),
        reminders_api._projection_fingerprint(household_id="household-x", projection=projection),
        notifications_api._projection_fingerprint(household_id="household-x", projection=projection),
    ]
    assert fingerprints[0] == fingerprints[1] == fingerprints[2] == fingerprints[3]
    assert fingerprints[0] == ("household-x", "evt-123", 9, "sum-123")

    _clear_domain_caches()

    task_fp = tasks_api._projection_fingerprint(household_id="household-x", projection=projection)
    task_records = (({
        "task_id": "task-1",
        "title": "Task",
        "description": "Task",
        "priority": "medium",
        "due_at": None,
        "status": "pending",
        "created_at": "2035-01-01T00:00:00Z",
        "completed_at": None,
    }, "task"),)
    tasks_api._get_or_build_sorted_view(
        projection_fingerprint=task_fp,
        records=task_records,
        status="pending",
        search="task",
        sort_by="created_at",
        order="desc",
        limit=2,
        offset=1,
    )

    schedule_fp = schedule_api._projection_fingerprint(household_id="household-x", projection=projection)
    schedule_records = (({
        "schedule_id": "schedule-1",
        "title": "Schedule",
        "start_at": "2035-01-01T08:00:00Z",
        "end_at": "2035-01-01T09:00:00Z",
        "status": "scheduled",
        "created_at": "2035-01-01T00:00:00Z",
        "cancelled_at": None,
    }, "schedule"),)
    schedule_api._get_or_build_sorted_view(
        projection_fingerprint=schedule_fp,
        records=schedule_records,
        status="scheduled",
        search="schedule",
        start_from="2035-01-01T00:00:00Z",
        end_to="2035-01-31T00:00:00Z",
        sort_by="start_at",
        order="asc",
        limit=3,
        offset=2,
    )

    reminder_fp = reminders_api._projection_fingerprint(household_id="household-x", projection=projection)
    reminder_records = (({
        "reminder_id": "reminder-1",
        "title": "Reminder",
        "message": "Reminder",
        "trigger_at": "2035-01-01T10:00:00Z",
        "status": "active",
        "created_at": "2035-01-01T00:00:00Z",
        "triggered_at": None,
    }, "reminder"),)
    reminders_api._get_or_build_sorted_view(
        projection_fingerprint=reminder_fp,
        records=reminder_records,
        status="active",
        search="reminder",
        sort_by="trigger_at",
        order="desc",
        limit=4,
        offset=3,
    )

    notification_fp = notifications_api._projection_fingerprint(household_id="household-x", projection=projection)
    notification_records = (({
        "notification_id": "notification-1",
        "source_event_id": "event-1",
        "source_type": "reminder",
        "source_id": "reminder-1",
        "message": "Notification",
        "created_at": "2035-01-01T00:00:00Z",
        "delivery_status": "pending",
    }, "notification"),)
    notifications_api._get_or_build_sorted_view(
        projection_fingerprint=notification_fp,
        records=notification_records,
        delivery_status="pending",
        source_type="reminder",
        search="notification",
        sort_by="created_at",
        order="desc",
        limit=5,
        offset=4,
    )

    task_key = next(iter(tasks_api._VIEW_CACHE.keys()))
    schedule_key = next(iter(schedule_api._VIEW_CACHE.keys()))
    reminder_key = next(iter(reminders_api._VIEW_CACHE.keys()))
    notification_key = next(iter(notifications_api._VIEW_CACHE.keys()))

    assert len(task_key) == len(schedule_key) == len(reminder_key) == len(notification_key) == 10
    assert task_key[:4] == task_fp
    assert schedule_key[:4] == schedule_fp
    assert reminder_key[:4] == reminder_fp
    assert notification_key[:4] == notification_fp

    assert task_key[4:] == ("status=pending", "task", "created_at", "desc", 2, 1)
    assert schedule_key[4:] == (
        "end_to=2035-01-31T00:00:00Z|start_from=2035-01-01T00:00:00Z|status=scheduled",
        "schedule",
        "start_at",
        "asc",
        3,
        2,
    )
    assert reminder_key[4:] == ("status=active", "reminder", "trigger_at", "desc", 4, 3)
    assert notification_key[4:] == (
        "delivery_status=pending|source_type=reminder",
        "notification",
        "created_at",
        "desc",
        5,
        4,
    )


def test_no_cross_domain_pipeline_imports_or_calls() -> None:
    module_names = [module_name for module_name, *_ in READ_MODEL_DOMAIN_SPECS]
    forbidden_imports = [
        *(f"from app.api.{module_name} import" for module_name in module_names),
        *(f"import app.api.{module_name}" for module_name in module_names),
    ]

    for module, _endpoint_name, _sort_name in _DOMAIN_CASES:
        source = inspect.getsource(module)
        for token in forbidden_imports:
            assert token not in source


def test_abstraction_creep_detection_for_shared_layer() -> None:
    source = inspect.getsource(read_model_shared)
    assert "def orchestr" not in source
    assert "def pipeline" not in source
    assert "def assemble" not in source
    assert "def build_response" not in source

    # Boundary freeze: any expansion here should require explicit governance update.
    exported = {
        name
        for name, value in inspect.getmembers(read_model_shared, inspect.isfunction)
        if getattr(value, "__module__", "") == read_model_shared.__name__
    }
    assert exported == EXPECTED_SHARED_HELPER_EXPORTS


def test_deterministic_behavior_across_repeated_queries_for_all_domains() -> None:
    runtime, event_log = _build_runtime()
    _clear_domain_caches()

    with _build_test_client(runtime) as client:
        household_id = "household-boundary-determinism"

        task_created = _post_command(
            client,
            command_type="task.create",
            household_id=household_id,
            payload={"title": "Boundary task", "priority": "medium"},
        )
        task_id = str(task_created["response"]["task"]["task_id"])
        _post_command(
            client,
            command_type="task_completed",
            household_id=household_id,
            payload={"task_id": task_id},
        )
        _post_command(
            client,
            command_type="task.create",
            household_id=household_id,
            payload={"title": "Boundary task extra 1", "priority": "low"},
        )
        _post_command(
            client,
            command_type="task.create",
            household_id=household_id,
            payload={"title": "Boundary task extra 2", "priority": "high"},
        )

        schedule_created = _post_command(
            client,
            command_type="schedule.create",
            household_id=household_id,
            payload={
                "title": "Boundary schedule",
                "start_at": "2036-01-01T08:00:00Z",
                "end_at": "2036-01-01T09:00:00Z",
            },
        )
        schedule_id = str(schedule_created["response"]["schedule"]["schedule_id"])
        _post_command(
            client,
            command_type="schedule.cancel",
            household_id=household_id,
            payload={"schedule_id": schedule_id},
        )

        reminder_created = _create_reminder(
            client,
            household_id=household_id,
            title="Boundary reminder",
            message="Boundary reminder message",
            trigger_at="2036-01-01T10:00:00Z",
        )
        reminder_id = str(reminder_created["response"]["reminder"]["reminder_id"])
        _trigger_reminder(
            client,
            household_id=household_id,
            reminder_id=reminder_id,
            system_trigger=True,
        )

        task_params = {"household_id": household_id, "sort_by": "created_at", "order": "desc", "limit": 5, "offset": 0}
        schedule_params = {"household_id": household_id, "sort_by": "start_at", "order": "asc", "limit": 5, "offset": 0}
        reminder_params = {"household_id": household_id, "sort_by": "trigger_at", "order": "desc", "limit": 5, "offset": 0}
        notification_params = {"household_id": household_id, "sort_by": "created_at", "order": "desc", "limit": 5, "offset": 0}

        tasks_runs = _assert_identical_query_results(client=client, path="/tasks", params=task_params)
        _assert_identical_query_results(client=client, path="/schedule", params=schedule_params)
        _assert_identical_query_results(client=client, path="/reminders", params=reminder_params)
        _assert_identical_query_results(client=client, path="/notifications", params=notification_params)

        task_page_0 = _assert_identical_query_results(
            client=client,
            path="/tasks",
            params={"household_id": household_id, "sort_by": "created_at", "order": "desc", "limit": 1, "offset": 0},
        )
        task_page_1 = _assert_identical_query_results(
            client=client,
            path="/tasks",
            params={"household_id": household_id, "sort_by": "created_at", "order": "desc", "limit": 1, "offset": 1},
        )
        assert task_page_0[0] != task_page_1[0]

        cached_tasks = tasks_runs[0]
        _clear_domain_caches()
        runtime.get_projection(household_id, force_replay=True)
        recomputed_tasks = _assert_identical_query_results(client=client, path="/tasks", params=task_params)[0]
        assert cached_tasks == recomputed_tasks

        interleaving_query_params = {
            "household_id": household_id,
            "search": "interleave-a",
            "sort_by": "created_at",
            "order": "desc",
            "limit": 10,
            "offset": 0,
        }
        _post_command(
            client,
            command_type="task.create",
            household_id=household_id,
            payload={"title": "interleave-a one", "priority": "medium"},
        )
        interleave_first = _assert_identical_query_results(
            client=client,
            path="/tasks",
            params=interleaving_query_params,
        )[0]

        _post_command(
            client,
            command_type="schedule.create",
            household_id=household_id,
            payload={
                "title": "interleave-b schedule",
                "start_at": "2036-01-02T08:00:00Z",
                "end_at": "2036-01-02T09:00:00Z",
            },
        )
        interleave_after_b = _assert_identical_query_results(
            client=client,
            path="/tasks",
            params=interleaving_query_params,
        )[0]
        assert interleave_after_b == interleave_first

        _post_command(
            client,
            command_type="task.create",
            household_id=household_id,
            payload={"title": "interleave-a two", "priority": "medium"},
        )
        interleave_after_second_a = _assert_identical_query_results(
            client=client,
            path="/tasks",
            params=interleaving_query_params,
        )[0]
        assert interleave_after_second_a != interleave_after_b
        assert len(interleave_after_second_a["tasks"]) == len(interleave_after_b["tasks"]) + 1

        replay_runs = [replay(event_log.insert_order) for _ in range(5)]
        replayed = replay_runs[0]
        for replayed_run in replay_runs[1:]:
            assert replayed_run == replayed

    live_projection = runtime.get_projection(household_id, force_replay=True)
    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True
    assert live_projection == replayed["derived_state"]


def test_controlled_concurrency_interleaving_isolation_cache_and_replay_repeatability() -> None:
    runtime, event_log = _build_runtime()
    _clear_domain_caches()

    with _build_test_client(runtime) as client:
        household_a = "household-controlled-concurrency-a"
        household_b = "household-controlled-concurrency-b"
        anchor_a = "stable-a-anchor"
        anchor_b = "stable-b-anchor"

        _post_command(
            client,
            command_type="task.create",
            household_id=household_a,
            payload={"title": anchor_a, "priority": "medium"},
        )
        _post_command(
            client,
            command_type="task.create",
            household_id=household_b,
            payload={"title": anchor_b, "priority": "medium"},
        )

        scenario_runs: list[dict[str, Any]] = []
        for round_index in range(5):
            # A1: mutation -> read
            _post_command(
                client,
                command_type="task.create",
                household_id=household_a,
                payload={"title": f"a-noise-round-{round_index}-1", "priority": "low"},
            )
            a1 = _get_tasks_payload(client=client, household_id=household_a, search=anchor_a)

            # B1: mutation -> read
            _post_command(
                client,
                command_type="task.create",
                household_id=household_b,
                payload={"title": f"b-noise-round-{round_index}-1", "priority": "low"},
            )
            b1 = _get_tasks_payload(client=client, household_id=household_b, search=anchor_b)

            # A read after B mutation must remain isolated.
            a_after_b1 = _get_tasks_payload(client=client, household_id=household_a, search=anchor_a)
            assert a_after_b1 == a1

            # A2: mutation -> read
            _post_command(
                client,
                command_type="task.create",
                household_id=household_a,
                payload={"title": f"a-noise-round-{round_index}-2", "priority": "low"},
            )
            a2 = _get_tasks_payload(client=client, household_id=household_a, search=anchor_a)

            # B read after A mutation must remain isolated.
            b_after_a2 = _get_tasks_payload(client=client, household_id=household_b, search=anchor_b)
            assert b_after_a2 == b1

            # B2: mutation -> read
            _post_command(
                client,
                command_type="task.create",
                household_id=household_b,
                payload={"title": f"b-noise-round-{round_index}-2", "priority": "low"},
            )
            b2 = _get_tasks_payload(client=client, household_id=household_b, search=anchor_b)

            # A3: mutation -> read
            _post_command(
                client,
                command_type="task.create",
                household_id=household_a,
                payload={"title": f"a-noise-round-{round_index}-3", "priority": "low"},
            )
            a3 = _get_tasks_payload(client=client, household_id=household_a, search=anchor_a)

            # Required interleaving invariants.
            assert a1 == a2 == a3
            assert b1 == b2

            # Cache isolation: A(warm) -> B(cold) -> A(cold).
            a_warm = _get_tasks_payload(client=client, household_id=household_a, search=anchor_a)
            _clear_domain_caches()
            b_cold = _get_tasks_payload(client=client, household_id=household_b, search=anchor_b)
            _clear_domain_caches()
            a_cold = _get_tasks_payload(client=client, household_id=household_a, search=anchor_a)
            assert a_warm == a_cold

            _clear_domain_caches()
            b_cold_repeat = _get_tasks_payload(client=client, household_id=household_b, search=anchor_b)
            assert b_cold_repeat == b_cold

            events_a = _events_for_household(event_log, household_id=household_a)
            events_b = _events_for_household(event_log, household_id=household_b)
            assert events_a
            assert events_b

            # Event ordering within each logical stream is stable.
            assert [row.event_id for row in events_a] == [
                row.event_id for row in _events_for_household(event_log, household_id=household_a)
            ]
            assert [row.event_id for row in events_b] == [
                row.event_id for row in _events_for_household(event_log, household_id=household_b)
            ]

            replayed_a = replay(events_a)
            replayed_b = replay(events_b)
            runtime_projection_a = runtime.get_projection(household_a, force_replay=True)
            runtime_projection_b = runtime.get_projection(household_b, force_replay=True)

            assert replayed_a["derived_state"] == runtime_projection_a
            assert replayed_b["derived_state"] == runtime_projection_b
            assert replayed_a["last_event_id"] == events_a[-1].event_id
            assert replayed_b["last_event_id"] == events_b[-1].event_id
            assert validate_replay(runtime_projection_a, replayed_a["derived_state"])["matches"] is True
            assert validate_replay(runtime_projection_b, replayed_b["derived_state"])["matches"] is True

            scenario_runs.append(
                {
                    "a1": a1,
                    "a2": a2,
                    "a3": a3,
                    "b1": b1,
                    "b2": b2,
                    "a_warm": a_warm,
                    "a_cold": a_cold,
                    "b_cold": b_cold,
                }
            )

        baseline = scenario_runs[0]
        for run in scenario_runs[1:]:
            assert run == baseline
