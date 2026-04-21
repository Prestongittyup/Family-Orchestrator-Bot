from __future__ import annotations

import re

from fastapi.testclient import TestClient

from apps.api.main import app
from household_os.presentation.time_formatter import format_relative_datetime


def test_relative_time_formatting():
    phrase = format_relative_datetime(
        target="2026-04-19T06:00:00+00:00",
        reference="2026-04-19T08:00:00+00:00",
    )
    assert phrase == "tomorrow morning at 6:00 AM"


def test_no_raw_timestamps():
    client = TestClient(app)
    response = client.post(
        "/assistant/run",
        json={"message": "I need to start working out", "household_id": "humanize-no-raw-ts"},
    )
    assert response.status_code == 200
    payload = response.json()

    assert not re.search(r"\b\d{4}-\d{2}-\d{2}\b", payload["recommendation"])
    assert "tomorrow" in payload["recommendation"].lower()


def test_humanized_language():
    client = TestClient(app)
    response = client.post(
        "/assistant/run",
        json={"message": "I need to start working out", "household_id": "humanize-workout"},
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["recommendation"] == "Schedule a 45-minute workout tomorrow morning at 6:00 AM before your day fills up."
    assert payload["why"] == [
        "Your schedule is already pretty full",
        "You're trying to build consistency",
        "You haven't worked out recently",
    ]
    assert len(payload["why"]) <= 3
    assert all(len(item.split()) < 20 for item in payload["why"])
