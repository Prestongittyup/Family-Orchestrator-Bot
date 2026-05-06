from __future__ import annotations

import json
from pathlib import Path
import re
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app
from app.services.commands import get_command_runtime_service, reset_command_runtime_service


def _ingest_email(client: TestClient, *, household_id: str, email: dict[str, str]) -> None:
    response = client.post(
        "/ingest/email",
        json={"household_id": household_id, "email": email},
    )
    assert response.status_code == 200, response.text


def _ingest_calendar(client: TestClient, *, household_id: str, events: list[dict[str, str]]) -> None:
    response = client.post(
        "/ingest/calendar",
        json={"household_id": household_id, "events": events},
    )
    assert response.status_code == 200, response.text


def _get_home(client: TestClient, *, household_id: str, scenario_date: str) -> dict:
    response = client.get(
        "/home",
        params={"household_id": household_id, "date": scenario_date},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert isinstance(payload, dict)
    return payload


def _to_plain(value: object) -> object:
    return json.loads(json.dumps(value, sort_keys=True, default=str))


def test_home_v0_determinism_same_state_same_output() -> None:
    household_id = f"home-v0-det-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        _ingest_email(
            client,
            household_id=household_id,
            email={
                "email_id": "email-det-1",
                "subject": "School reminder",
                "body": "Action: confirm pickup by end of day.",
                "from": "school@example.com",
                "received_at": f"{scenario_date}T08:00:00Z",
            },
        )
        _ingest_calendar(
            client,
            household_id=household_id,
            events=[
                {
                    "event_id": "cal-det-2",
                    "title": "Pickup",
                    "start_at": f"{scenario_date}T15:30:00Z",
                    "end_at": f"{scenario_date}T16:00:00Z",
                },
                {
                    "event_id": "cal-det-1",
                    "title": "Parent call",
                    "start_at": f"{scenario_date}T15:00:00Z",
                    "end_at": f"{scenario_date}T15:45:00Z",
                },
            ],
        )

        first = _get_home(client, household_id=household_id, scenario_date=scenario_date)
        second = _get_home(client, household_id=household_id, scenario_date=scenario_date)

    assert first == second
    assert list(first.keys()) == ["needs_decision", "actions", "calendar", "summary"]


def test_home_v0_duplicate_detection_single_builder_only() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    app_files = list(repo_root.joinpath("app").rglob("*.py"))

    home_route_count = 0
    home_builder_count = 0
    contract_freezer_count = 0

    for path in app_files:
        content = path.read_text(encoding="utf-8")
        home_route_count += content.count('@router.get("/home")')
        home_builder_count += content.count("def _build_home_v0(")
        contract_freezer_count += content.count("def freeze_home_v0_contract(")

    assert home_route_count == 1, f"Expected one /home route in app/, found {home_route_count}"
    assert home_builder_count == 1, f"Expected one _build_home_v0 in app/, found {home_builder_count}"
    assert contract_freezer_count == 1, (
        "Expected one freeze_home_v0_contract in app/, "
        f"found {contract_freezer_count}"
    )


def test_home_v0_ingestion_to_home_consistency() -> None:
    household_id = f"home-v0-ing-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        command_email = client.post(
            "/command",
            json={
                "command_type": "email.ingest",
                "household_id": household_id,
                "payload": {
                    "email": {
                        "email_id": "email-contract-cmd",
                        "subject": "After school pickup",
                        "body": "Action: confirm pickup today.",
                        "from": "school@example.com",
                        "received_at": f"{scenario_date}T07:30:00Z",
                    }
                },
            },
        )
        assert command_email.status_code == 200, command_email.text

        _ingest_email(
            client,
            household_id=household_id,
            email={
                "email_id": "email-contract-direct",
                "subject": "Medication refill",
                "body": "Action: call pharmacy this week.",
                "from": "clinic@example.com",
                "received_at": f"{scenario_date}T10:00:00Z",
            },
        )
        _ingest_calendar(
            client,
            household_id=household_id,
            events=[
                {
                    "event_id": "cal-contract-1",
                    "title": "Doctor call",
                    "start_at": f"{scenario_date}T11:00:00Z",
                    "end_at": f"{scenario_date}T11:30:00Z",
                }
            ],
        )

        payload = _get_home(client, household_id=household_id, scenario_date=scenario_date)

    action_ids = {str(item.get("id") or "") for item in payload["actions"]}
    calendar_ids = {str(item.get("id") or "") for item in payload["calendar"]}

    assert "email-contract-cmd" in action_ids
    assert "email-contract-direct" in action_ids
    assert "cal-contract-1" in calendar_ids


def test_home_v0_ordering_rules_enforced() -> None:
    household_id = f"home-v0-order-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        _ingest_email(
            client,
            household_id=household_id,
            email={
                "email_id": "email-order-1",
                "subject": "Urgent callback",
                "body": "Action: call coordinator today.",
                "from": "ops@example.com",
                "received_at": f"{scenario_date}T07:00:00Z",
            },
        )
        _ingest_email(
            client,
            household_id=household_id,
            email={
                "email_id": "email-order-2",
                "subject": "Invoice review",
                "body": "Action: review monthly invoice and send notes next week.",
                "from": "billing@example.com",
                "received_at": f"{scenario_date}T09:00:00Z",
            },
        )
        _ingest_calendar(
            client,
            household_id=household_id,
            events=[
                {
                    "event_id": "cal-order-3",
                    "title": "Dinner prep",
                    "start_at": f"{scenario_date}T18:00:00Z",
                    "end_at": f"{scenario_date}T18:30:00Z",
                },
                {
                    "event_id": "cal-order-2",
                    "title": "Soccer pickup",
                    "start_at": f"{scenario_date}T15:20:00Z",
                    "end_at": f"{scenario_date}T16:00:00Z",
                },
                {
                    "event_id": "cal-order-1",
                    "title": "Parent call",
                    "start_at": f"{scenario_date}T15:00:00Z",
                    "end_at": f"{scenario_date}T15:45:00Z",
                },
            ],
        )

        payload = _get_home(client, household_id=household_id, scenario_date=scenario_date)

    priority_rank = {"high": 0, "medium": 1, "low": 2}

    assert 0 <= len(payload["actions"]) <= 5
    action_priority_ranks = [
        priority_rank.get(str(item.get("priority") or "low"), 3)
        for item in payload["actions"]
    ]
    assert action_priority_ranks == sorted(action_priority_ranks)
    assert all(str(item.get("title") or "").strip() for item in payload["actions"])
    assert all("?" not in str(item.get("title") or "") for item in payload["actions"])

    expected_calendar = sorted(
        payload["calendar"],
        key=lambda item: (
            str(item.get("start") or ""),
            str(item.get("end") or ""),
            str(item.get("id") or ""),
        ),
    )
    assert payload["calendar"] == expected_calendar
    assert 0 <= len(payload["calendar"]) <= 5

    assert 0 <= len(payload["needs_decision"]) <= 3
    decision_priority_ranks = [
        priority_rank.get(str(item.get("priority") or "low"), 3)
        for item in payload["needs_decision"]
    ]
    assert decision_priority_ranks == sorted(decision_priority_ranks)
    assert all(
        str(item.get("type") or "") in {"calendar_conflict", "time_constraint_violation", "promotion_decision"}
        for item in payload["needs_decision"]
    )


def test_runtime_single_instance_and_entrypoint_enforcement() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    reset_command_runtime_service()
    first_runtime = get_command_runtime_service()
    second_runtime = get_command_runtime_service()
    assert first_runtime is second_runtime

    app_main_source = (repo_root / "app" / "main.py").read_text(encoding="utf-8")
    assert app_main_source.count("app = create_app()") == 1

    archive_main_source = (repo_root / "archive" / "apps" / "api" / "main.py").read_text(encoding="utf-8")
    assert "_fastapi_app = create_app()" not in archive_main_source
    assert "app = AdmissionGateASGI(" not in archive_main_source

    backend_dockerfile = (repo_root / "Dockerfile.backend").read_text(encoding="utf-8")
    assert "app.main:app" in backend_dockerfile

    deployment_files = [
        repo_root / "docker-compose.yml",
        repo_root / "docker-compose.intelligence.yml",
        repo_root / "scripts" / "boot_smoke_test.py",
        repo_root / "scripts" / "runtime_stress_audit.py",
    ]
    for file_path in deployment_files:
        source = file_path.read_text(encoding="utf-8")
        assert "archive.apps.api.main:app" not in source, (
            "Deprecated archive runtime target must not appear in deployment/run paths: "
            f"{file_path}"
        )


def test_runtime_graph_has_no_archive_imports() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    runtime_sources = [repo_root / "main.py", *repo_root.joinpath("app").rglob("*.py")]

    offenders: list[str] = []
    for path in runtime_sources:
        source = path.read_text(encoding="utf-8")
        if re.search(r"\b(from|import)\s+archive\b", source):
            offenders.append(path.relative_to(repo_root).as_posix())

    assert offenders == [], f"Runtime graph must not import archive modules: {offenders}"


def test_home_has_no_shadow_surface_or_api_reshape() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    assert not (repo_root / "app" / "surfaces" / "household_home_screen_surface.py").exists()
    assert not (repo_root / "tests" / "test_household_home_screen_surface.py").exists()

    tasks_source = (repo_root / "app" / "api" / "tasks.py").read_text(encoding="utf-8")
    assert "freeze_home_v0_contract(" not in tasks_source
    assert "ordered_payload = orchestrator(email_items, calendar_items)" in tasks_source

    home_agent_source = (repo_root / "app" / "services" / "agents" / "v0.py").read_text(encoding="utf-8")
    assert "class PlanningAgent" not in home_agent_source
    assert "def build_home(" not in home_agent_source


def test_home_v0_restart_replay_equivalence() -> None:
    household_id = f"home-v0-replay-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    reset_command_runtime_service()
    with TestClient(app) as client:
        _ingest_email(
            client,
            household_id=household_id,
            email={
                "email_id": "email-replay-1",
                "subject": "School pickup",
                "body": "Action: confirm pickup today.",
                "from": "school@example.com",
                "received_at": f"{scenario_date}T08:10:00Z",
            },
        )
        _ingest_calendar(
            client,
            household_id=household_id,
            events=[
                {
                    "event_id": "cal-replay-1",
                    "title": "Parent call",
                    "start_at": f"{scenario_date}T15:00:00Z",
                    "end_at": f"{scenario_date}T15:45:00Z",
                },
                {
                    "event_id": "cal-replay-2",
                    "title": "Pickup",
                    "start_at": f"{scenario_date}T15:20:00Z",
                    "end_at": f"{scenario_date}T16:00:00Z",
                },
            ],
        )
        home_live_before_restart = _get_home(
            client,
            household_id=household_id,
            scenario_date=scenario_date,
        )

    runtime_before_restart = get_command_runtime_service()
    projection_live = _to_plain(runtime_before_restart.get_projection(household_id, force_replay=False))
    projection_replay = _to_plain(runtime_before_restart.get_projection(household_id, force_replay=True))
    assert projection_live == projection_replay

    reset_command_runtime_service()
    with TestClient(app) as client:
        home_live_after_restart = _get_home(
            client,
            household_id=household_id,
            scenario_date=scenario_date,
        )

    runtime_after_restart = get_command_runtime_service()
    projection_after_restart = _to_plain(runtime_after_restart.get_projection(household_id, force_replay=True))

    assert home_live_before_restart == home_live_after_restart
    assert projection_replay == projection_after_restart
