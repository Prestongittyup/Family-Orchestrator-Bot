from __future__ import annotations

from apps.api.integration_core.normalization import normalize_provider_event, normalize_provider_events


def test_deterministic_event_id_generation() -> None:
    raw = {
        "id": "gmail-1",
        "title": "Inbox Review",
        "start": "2026-01-01T09:00:00",
        "meta": {"source": "gmail"},
    }

    first = normalize_provider_event(
        user_id="u-1",
        provider_name="gmail",
        raw_event=raw,
        event_type="email_event",
    )
    second = normalize_provider_event(
        user_id="u-1",
        provider_name="gmail",
        raw_event=raw,
        event_type="email_event",
    )

    assert first.event_id == second.event_id
    assert first.timestamp == "2026-01-01T09:00:00"
    assert first.provider_name == "gmail"


def test_normalization_consistency_across_providers() -> None:
    gmail_events = [
        {"id": "g1", "title": "Mail 1", "start": "2026-01-01T13:00:00"},
        {"id": "g2", "title": "Mail 2", "start": "2026-01-01T09:00:00"},
    ]
    calendar_events = [
        {"id": "c1", "title": "Calendar 1", "start": "2026-01-01T10:00:00"},
        {"id": "c2", "title": "Calendar 2", "start": "2026-01-01T15:00:00"},
    ]

    normalized_gmail = normalize_provider_events(
        user_id="u-2",
        provider_name="gmail",
        raw_events=gmail_events,
        event_type="email_event",
    )
    normalized_calendar = normalize_provider_events(
        user_id="u-2",
        provider_name="google_calendar",
        raw_events=calendar_events,
        event_type="calendar_event",
    )

    # Same model and stable ordering by timestamp then provider_name.
    combined = sorted(
        [*normalized_gmail, *normalized_calendar],
        key=lambda event: (event.timestamp, event.provider_name),
    )

    assert len(combined) == 4
    assert [row.timestamp for row in combined] == [
        "2026-01-01T09:00:00",
        "2026-01-01T10:00:00",
        "2026-01-01T13:00:00",
        "2026-01-01T15:00:00",
    ]
    assert combined[0].provider_name == "gmail"
    assert combined[1].provider_name == "google_calendar"


def test_recurring_event_collapse_deduplication() -> None:
    raw_events = [
        {
            "id": f"birthday-series-instance-{i}",
            "timestamp": f"20{20 + i}-04-01T00:00:00Z",
            "title": "Birthday",
            "recurringEventId": "birthday-series-42",
            "recurrence": ["RRULE:FREQ=YEARLY"],
            "_raw_google_event": {
                "id": f"birthday-series-instance-{i}",
                "recurringEventId": "birthday-series-42",
                "iCalUID": "birthday-series-ical-42",
                "recurrence": ["RRULE:FREQ=YEARLY"],
            },
        }
        for i in range(7)
    ]

    normalized = normalize_provider_events(
        user_id="u-rec-1",
        provider_name="google_calendar",
        raw_events=raw_events,
        event_type="calendar.event",
    )

    assert len(normalized) == 1
    assert normalized[0].payload["is_recurring"] is True
    assert normalized[0].payload["recurrence_type"] == "yearly"
    assert normalized[0].payload["recurrence_source_id"] == "birthday-series-42"


def test_no_duplicate_events_from_recurring_series() -> None:
    raw_events = [
        {
            "id": "series-a-instance-1",
            "timestamp": "2026-04-01T09:00:00Z",
            "title": "Family Lunch",
            "_raw_google_event": {
                "id": "series-a-instance-1",
                "iCalUID": "series-a-ical",
            },
        },
        {
            "id": "series-a-instance-2",
            "timestamp": "2026-04-08T09:00:00Z",
            "title": "Family Lunch",
            "_raw_google_event": {
                "id": "series-a-instance-2",
                "iCalUID": "series-a-ical",
            },
        },
        {
            "id": "series-b-instance-1",
            "timestamp": "2026-04-02T11:00:00Z",
            "title": "Practice",
            "_raw_google_event": {
                "id": "series-b-instance-1",
                "iCalUID": "series-b-ical",
            },
        },
        {
            "id": "series-b-instance-2",
            "timestamp": "2026-04-09T11:00:00Z",
            "title": "Practice",
            "_raw_google_event": {
                "id": "series-b-instance-2",
                "iCalUID": "series-b-ical",
            },
        },
    ]

    normalized = normalize_provider_events(
        user_id="u-rec-2",
        provider_name="google_calendar",
        raw_events=raw_events,
        event_type="calendar.event",
    )

    assert len(normalized) == 2
    assert all(event.payload["is_recurring"] is True for event in normalized)
    assert len({event.event_id for event in normalized}) == 2


def test_event_id_stability_for_recurring_series() -> None:
    first_batch = [
        {
            "id": "instance-2026",
            "timestamp": "2026-01-10T08:00:00Z",
            "recurringEventId": "stable-series-1",
            "recurrence": ["RRULE:FREQ=WEEKLY"],
        },
        {
            "id": "instance-2027",
            "timestamp": "2027-01-10T08:00:00Z",
            "recurringEventId": "stable-series-1",
            "recurrence": ["RRULE:FREQ=WEEKLY"],
        },
    ]
    second_batch = list(reversed(first_batch))

    first = normalize_provider_events(
        user_id="u-rec-3",
        provider_name="google_calendar",
        raw_events=first_batch,
        event_type="calendar.event",
    )
    second = normalize_provider_events(
        user_id="u-rec-3",
        provider_name="google_calendar",
        raw_events=second_batch,
        event_type="calendar.event",
    )

    assert len(first) == 1
    assert len(second) == 1
    assert first[0].event_id == second[0].event_id
    assert first[0].payload["recurrence_type"] == "weekly"
    assert first[0].payload["recurrence_source_id"] == "stable-series-1"


def test_mixed_single_and_recurring_event_sets() -> None:
    raw_events = [
        {
            "id": "single-event-1",
            "timestamp": "2026-02-01T09:30:00Z",
            "title": "One-off Appointment",
        },
        {
            "id": "series-m-instance-1",
            "timestamp": "2026-02-02T12:00:00Z",
            "title": "Monthly Bill",
            "recurringEventId": "series-monthly-9",
            "recurrence": ["RRULE:FREQ=MONTHLY"],
        },
        {
            "id": "series-m-instance-2",
            "timestamp": "2026-03-02T12:00:00Z",
            "title": "Monthly Bill",
            "recurringEventId": "series-monthly-9",
            "recurrence": ["RRULE:FREQ=MONTHLY"],
        },
    ]

    first_run = normalize_provider_events(
        user_id="u-rec-4",
        provider_name="google_calendar",
        raw_events=raw_events,
        event_type="calendar.event",
    )
    second_run = normalize_provider_events(
        user_id="u-rec-4",
        provider_name="google_calendar",
        raw_events=raw_events,
        event_type="calendar.event",
    )

    assert len(first_run) == 2
    assert [event.event_id for event in first_run] == [event.event_id for event in second_run]
    assert [event.timestamp for event in first_run] == [event.timestamp for event in second_run]

    recurring = [event for event in first_run if event.payload["is_recurring"]]
    singles = [event for event in first_run if not event.payload["is_recurring"]]

    assert len(recurring) == 1
    assert len(singles) == 1
    assert recurring[0].payload["recurrence_type"] == "monthly"
    assert singles[0].payload["recurrence_type"] == "none"
