from __future__ import annotations

from datetime import UTC, datetime, timedelta

from apps.api.integration_core.event_windowing import (
    OrchestrationView,
    filter_events_to_window,
    get_time_window,
    prune_stale_events,
)


def _event(*, event_id: str, start: datetime, end: datetime) -> dict[str, str]:
    return {
        "event_id": event_id,
        "timestamp": start.isoformat().replace("+00:00", "Z"),
        "start": start.isoformat().replace("+00:00", "Z"),
        "end_timestamp": end.isoformat().replace("+00:00", "Z"),
    }


def test_short_term_window_returns_at_most_seven_days():
    time_min, time_max = get_time_window(OrchestrationView.SHORT)
    assert time_max - time_min <= timedelta(days=8)


def test_mid_term_window_returns_at_most_thirty_days():
    time_min, time_max = get_time_window(OrchestrationView.MEDIUM)
    assert time_max - time_min <= timedelta(days=31)


def test_long_term_window_returns_at_most_ninety_days():
    time_min, time_max = get_time_window(OrchestrationView.LONG)
    assert time_max - time_min <= timedelta(days=91)


def test_filter_drops_events_older_than_twenty_four_hours_past():
    now = datetime(2026, 4, 16, 12, 0, tzinfo=UTC)
    kept = filter_events_to_window(
        [
            _event(
                event_id="stale",
                start=now - timedelta(hours=26),
                end=now - timedelta(hours=25),
            )
        ],
        now=now,
        time_min=now - timedelta(hours=24),
        time_max=now + timedelta(days=7),
    )
    assert kept == []


def test_filter_drops_events_beyond_ninety_days_future():
    now = datetime(2026, 4, 16, 12, 0, tzinfo=UTC)
    kept = filter_events_to_window(
        [
            _event(
                event_id="far-future",
                start=now + timedelta(days=91),
                end=now + timedelta(days=91, hours=1),
            )
        ],
        now=now,
        time_min=now - timedelta(hours=24),
        time_max=now + timedelta(days=90),
    )
    assert kept == []


def test_filter_preserves_ongoing_events():
    now = datetime(2026, 4, 16, 12, 0, tzinfo=UTC)
    ongoing = _event(
        event_id="ongoing",
        start=now - timedelta(hours=23),
        end=now + timedelta(hours=1),
    )
    kept = filter_events_to_window(
        [ongoing],
        now=now,
        time_min=now - timedelta(hours=24),
        time_max=now + timedelta(days=7),
    )
    assert [row["event_id"] for row in kept] == ["ongoing"]


def test_retention_pruning_removes_stale_events():
    now = datetime(2026, 4, 16, 12, 0, tzinfo=UTC)
    rows = [
        _event(
            event_id="keep",
            start=now + timedelta(days=1),
            end=now + timedelta(days=1, hours=1),
        ),
        _event(
            event_id="drop-old",
            start=now - timedelta(days=2),
            end=now - timedelta(hours=25),
        ),
        _event(
            event_id="drop-future",
            start=now + timedelta(days=91),
            end=now + timedelta(days=91, hours=1),
        ),
    ]
    kept = prune_stale_events(rows, now=now)
    assert [row["event_id"] for row in kept] == ["keep"]