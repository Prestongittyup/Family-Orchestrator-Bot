from __future__ import annotations

from uuid import uuid4
from typing import Any

from fastapi.testclient import TestClient

from app.main import app


def _post_ingest_email(client: TestClient, *, household_id: str, email: dict[str, Any]) -> None:
    response = client.post(
        "/ingest/email",
        json={"household_id": household_id, "email": email},
    )
    assert response.status_code == 200, response.text


def _post_ingest_calendar(client: TestClient, *, household_id: str, events: list[dict[str, Any]]) -> None:
    response = client.post(
        "/ingest/calendar",
        json={"household_id": household_id, "events": events},
    )
    assert response.status_code == 200, response.text


def _post_ingest_message(
    client: TestClient,
    *,
    household_id: str,
    raw_content: str,
    source: str,
    created_at: str,
    member_id: str,
) -> dict[str, Any]:
    response = client.post(
        "/ingest/message",
        json={
            "household_id": household_id,
            "raw_content": raw_content,
            "source": source,
            "created_at": created_at,
            "member_id": member_id,
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body, dict)
    return body


def _get_home(client: TestClient, *, household_id: str, scenario_date: str) -> dict[str, Any]:
    response = client.get(
        "/home",
        params={
            "household_id": household_id,
            "date": scenario_date,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def _seed_overlap_household(client: TestClient, *, household_id: str, scenario_date: str) -> None:
    _post_ingest_email(
        client,
        household_id=household_id,
        email={
            "email_id": "email-v0-1",
            "subject": "School trip form",
            "body": "Action: confirm permission slip and reply by end of day.",
            "from": "school@example.com",
            "received_at": f"{scenario_date}T08:10:00Z",
        },
    )
    _post_ingest_calendar(
        client,
        household_id=household_id,
        events=[
            {
                "event_id": "calendar-v0-1",
                "title": "Parent call",
                "start_at": f"{scenario_date}T15:00:00Z",
                "end_at": f"{scenario_date}T15:45:00Z",
            },
            {
                "event_id": "calendar-v0-2",
                "title": "Soccer pickup",
                "start_at": f"{scenario_date}T15:20:00Z",
                "end_at": f"{scenario_date}T16:00:00Z",
            },
            {
                "event_id": "calendar-v0-3",
                "title": "Dinner prep",
                "start_at": f"{scenario_date}T18:00:00Z",
                "end_at": f"{scenario_date}T18:30:00Z",
            },
        ],
    )


def test_home_v0_baseline_contract_and_determinism() -> None:
    household_id = f"home-v0-base-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        _seed_overlap_household(client, household_id=household_id, scenario_date=scenario_date)
        first = _get_home(client, household_id=household_id, scenario_date=scenario_date)
        second = _get_home(client, household_id=household_id, scenario_date=scenario_date)

    assert list(first.keys()) == ["needs_decision", "actions", "calendar", "summary"]
    assert isinstance(first["needs_decision"], list)
    assert isinstance(first["actions"], list)
    assert isinstance(first["calendar"], list)
    assert isinstance(first["summary"], str) and first["summary"].strip()
    assert first == second


def test_home_v0_intelligence_priority_and_conflict_routing() -> None:
    household_id = f"home-v0-intel-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        _seed_overlap_household(client, household_id=household_id, scenario_date=scenario_date)
        _post_ingest_email(
            client,
            household_id=household_id,
            email={
                "email_id": "email-v0-2",
                "subject": "Invoice review",
                "body": "Action: review monthly invoice and send notes next week.",
                "from": "billing@example.com",
                "received_at": f"{scenario_date}T09:15:00Z",
            },
        )
        payload = _get_home(client, household_id=household_id, scenario_date=scenario_date)

    needs_decision = payload["needs_decision"]
    assert needs_decision
    assert all(item.get("type") == "calendar_conflict" for item in needs_decision)
    assert all(item.get("priority") == "high" for item in needs_decision)

    actions = payload["actions"]
    assert 1 <= len(actions) <= 5
    assert all(str(item.get("title") or "").strip() for item in actions)
    assert all(str(item.get("source") or "") in {"email", "task"} for item in actions)
    assert all("?" not in str(item.get("title") or "") for item in actions)
    assert isinstance(payload["summary"], str) and payload["summary"].strip()


def test_home_v0_ingestion_path_regression() -> None:
    household_id = f"home-v0-reg-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        command_email = client.post(
            "/command",
            json={
                "command_type": "email.ingest",
                "household_id": household_id,
                "payload": {
                    "email": {
                        "email_id": "email-cmd-1",
                        "subject": "After school pickup",
                        "body": "Action: confirm pickup today.",
                        "from": "school@example.com",
                        "received_at": f"{scenario_date}T07:30:00Z",
                    }
                },
            },
        )
        assert command_email.status_code == 200, command_email.text

        direct_email = client.post(
            "/ingest/email",
            json={
                "household_id": household_id,
                "email": {
                    "email_id": "email-ingest-1",
                    "subject": "Medication refill",
                    "body": "Action: call pharmacy this week.",
                    "from": "clinic@example.com",
                    "received_at": f"{scenario_date}T10:00:00Z",
                },
            },
        )
        assert direct_email.status_code == 200, direct_email.text

        _post_ingest_calendar(
            client,
            household_id=household_id,
            events=[
                {
                    "event_id": "calendar-reg-1",
                    "title": "Doctor call",
                    "start_at": f"{scenario_date}T11:00:00Z",
                    "end_at": f"{scenario_date}T11:30:00Z",
                }
            ],
        )

        payload = _get_home(client, household_id=household_id, scenario_date=scenario_date)

    action_ids = {str(item.get("id") or "") for item in payload["actions"]}
    assert "email-cmd-1" in action_ids
    assert "email-ingest-1" in action_ids


def test_home_v0_golden_household_overlap_ordering_stable() -> None:
    household_id = f"home-v0-golden-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        _seed_overlap_household(client, household_id=household_id, scenario_date=scenario_date)
        home_direct = _get_home(client, household_id=household_id, scenario_date=scenario_date)

        alias_response = client.get(
            f"/household/{household_id}/home",
            params={"date": scenario_date},
        )
        assert alias_response.status_code == 200, alias_response.text
        home_alias = alias_response.json()

    assert home_direct == home_alias
    assert any(item.get("type") == "calendar_conflict" for item in home_direct["needs_decision"])
    assert len(home_direct["actions"]) >= 1

    calendar_sorted = sorted(
        home_direct["calendar"],
        key=lambda item: (
            str(item.get("start") or ""),
            str(item.get("end") or ""),
            str(item.get("id") or ""),
        ),
    )
    assert home_direct["calendar"] == calendar_sorted


def test_home_v0_digest_summary_is_time_and_change_aware() -> None:
    household_id = f"home-v0-digest-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        _seed_overlap_household(client, household_id=household_id, scenario_date=scenario_date)
        _post_ingest_message(
            client,
            household_id=household_id,
            raw_content="Please buy groceries today and confirm dinner plan.",
            source="manual",
            created_at=f"{scenario_date}T08:40:00Z",
            member_id="member-digest",
        )

        payload = _get_home(client, household_id=household_id, scenario_date=scenario_date)

    summary = str(payload.get("summary") or "")
    assert summary.startswith("Command center ")
    assert "Execute next:" in summary
    assert "No new changes since last check." in summary
    assert "Latest change" in summary
    assert any(str(item.get("source") or "") == "task" for item in payload["actions"])


def test_home_v0_digest_summary_detects_projection_change() -> None:
    household_id = f"home-v0-digest-delta-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        _seed_overlap_household(client, household_id=household_id, scenario_date=scenario_date)

        baseline = _get_home(client, household_id=household_id, scenario_date=scenario_date)
        baseline_repeat = _get_home(client, household_id=household_id, scenario_date=scenario_date)
        assert baseline == baseline_repeat

        _post_ingest_email(
            client,
            household_id=household_id,
            email={
                "email_id": "email-v0-delta-1",
                "subject": "Urgent callback",
                "body": "Action: call coordinator today before noon.",
                "from": "ops@example.com",
                "received_at": f"{scenario_date}T11:30:00Z",
            },
        )

        changed = _get_home(client, household_id=household_id, scenario_date=scenario_date)
        changed_repeat = _get_home(client, household_id=household_id, scenario_date=scenario_date)

    baseline_summary = str(baseline.get("summary") or "")
    changed_summary = str(changed.get("summary") or "")
    assert "No new changes since last check." in baseline_summary
    assert "Updated since last check:" in changed_summary
    assert changed == changed_repeat


def test_household_message_promotion_persists_across_home_refresh() -> None:
    household_id = f"home-v0-msg-loop-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        promotion = _post_ingest_message(
            client,
            household_id=household_id,
            raw_content="Action: call the clinic today to confirm insurance details.",
            source="manual",
            created_at=f"{scenario_date}T09:05:00Z",
            member_id="member-loop",
        )
        assert promotion.get("status") == "accepted"

        first = _get_home(client, household_id=household_id, scenario_date=scenario_date)
        second = _get_home(client, household_id=household_id, scenario_date=scenario_date)

    assert first == second
    assert any(str(item.get("source") or "") == "task" for item in first["actions"])


def test_household_message_promotion_calendar_and_decision_paths() -> None:
    household_id = f"home-v0-msg-paths-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"
    tomorrow = "2026-05-06"

    with TestClient(app) as client:
        calendar_promotion = _post_ingest_message(
            client,
            household_id=household_id,
            raw_content="Book dentist appointment tomorrow morning.",
            source="manual",
            created_at=f"{scenario_date}T10:00:00Z",
            member_id="member-calendar",
        )
        decision_promotion = _post_ingest_message(
            client,
            household_id=household_id,
            raw_content="Should we move soccer practice to Friday?",
            source="test",
            created_at=f"{scenario_date}T11:00:00Z",
            member_id="member-decision",
        )

        assert calendar_promotion.get("status") == "accepted"
        assert decision_promotion.get("status") == "accepted"

        today_payload = _get_home(client, household_id=household_id, scenario_date=scenario_date)
        tomorrow_payload = _get_home(client, household_id=household_id, scenario_date=tomorrow)

    assert any(str(item.get("type") or "") == "promotion_decision" for item in today_payload["needs_decision"])
    assert any(str(item.get("id") or "").startswith("schedule-msg-") for item in tomorrow_payload["calendar"])
