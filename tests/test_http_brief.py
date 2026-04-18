from __future__ import annotations

from fastapi.testclient import TestClient

from app import app, manual_items


def setup_function() -> None:
    manual_items.clear()


def teardown_function() -> None:
    manual_items.clear()


def test_brief_endpoint_returns_200() -> None:
    client = TestClient(app)
    response = client.get("/brief")

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, dict)


def test_brief_render_human_includes_rendered() -> None:
    client = TestClient(app)
    response = client.get("/brief?render_human=true")

    assert response.status_code == 200
    payload = response.json()
    assert "brief" in payload
    assert "rendered" in payload
    assert isinstance(payload["rendered"], str)
    assert payload["rendered"].strip() != ""


def test_home_returns_html_with_readable_content() -> None:
    client = TestClient(app)
    response = client.get("/legacy-brief")

    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    body = response.text
    assert "Daily Plan" in body
    assert "Today's Plan" in body
    assert "submitForm(event)" in body


def test_add_endpoint_and_manual_injection() -> None:
    client = TestClient(app)

    add_scheduled = client.post(
        "/add",
        json={
            "title": "Do laundry",
            "type": "task",
            "time": "2026-04-16T18:00:00",
        },
    )
    assert add_scheduled.status_code == 200
    assert add_scheduled.json()["status"] == "ok"

    add_unscheduled = client.post(
        "/add",
        json={
            "title": "Call plumber",
            "type": "event",
        },
    )
    assert add_unscheduled.status_code == 200
    assert add_unscheduled.json()["status"] == "ok"
    assert add_unscheduled.json()["count"] == 2

    brief_response = client.get("/brief")
    assert brief_response.status_code == 200
    payload = brief_response.json()

    assert "scheduled_actions" in payload
    assert any(item.get("title") == "Do laundry" for item in payload["scheduled_actions"])


def test_time_normalization_normalizes_flexible_formats() -> None:
    """Test that time normalizer handles flexible input formats."""
    client = TestClient(app)

    # Test HH:MM format
    response_hhmm = client.post(
        "/add",
        json={"title": "Meeting", "type": "event", "time": "14:30"},
    )
    assert response_hhmm.status_code == 200

    # Test named time block
    response_morning = client.post(
        "/add",
        json={"title": "Breakfast", "type": "event", "time": "morning"},
    )
    assert response_morning.status_code == 200

    # Test natural alias
    response_after_lunch = client.post(
        "/add",
        json={"title": "Meditation", "type": "event", "time": "after lunch"},
    )
    assert response_after_lunch.status_code == 200

    brief_response = client.get("/brief")
    payload = brief_response.json()

    actions = payload.get("scheduled_actions", [])
    assert len(actions) == 3

    # Verify all items have normalized start_time
    for action in actions:
        assert "start_time" in action
        # start_time should be ISO format
        assert "T" in action["start_time"]
        assert ":" in action["start_time"]


def test_time_normalization_preserves_raw_input() -> None:
    """Test that raw_time_input field is preserved for traceability."""
    client = TestClient(app)

    # Add items with various time formats
    test_cases = [
        ("14:30", "Meeting"),
        ("morning", "Breakfast"),
        ("after lunch", "Meditation"),
        ("tonight", "Dinner"),
    ]

    for raw_time, title in test_cases:
        response = client.post(
            "/add",
            json={"title": title, "type": "event", "time": raw_time},
        )
        assert response.status_code == 200

    brief_response = client.get("/brief")
    payload = brief_response.json()

    actions = payload.get("scheduled_actions", [])
    assert len(actions) == 4

    # Verify raw_time_input is preserved
    for i, (expected_raw_time, expected_title) in enumerate(test_cases):
        matching = [a for a in actions if a.get("title") == expected_title]
        assert len(matching) == 1
        action = matching[0]
        assert action.get("raw_time_input") == expected_raw_time
        assert "start_time" in action

    rendered_response = client.get("/brief?render_human=true")
    assert rendered_response.status_code == 200
    rendered_payload = rendered_response.json()
    assert "rendered" in rendered_payload
    # Verify that manually added items appear in rendered output
    assert "Breakfast" in rendered_payload["rendered"]
    assert "Meeting" in rendered_payload["rendered"]
