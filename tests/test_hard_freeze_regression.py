from __future__ import annotations

import json
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from apps.api.services import decision_engine as de
from apps.api.services import synthesis_engine as se
from apps.api.services.decision_engine import run_decision_engine_v2
from apps.api.services.synthesis_engine import build_daily_brief


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "hard_freeze"


class _FrozenDateTime(datetime):
    @classmethod
    def utcnow(cls) -> datetime:
        return cls(2026, 4, 15, 9, 0, 0)


class _FrozenDate(_date):
    @classmethod
    def today(cls) -> _date:
        return cls(2026, 4, 15)


def _load_json(name: str) -> dict[str, Any]:
    with (FIXTURES_DIR / name).open("r", encoding="utf-8") as f:
        return json.load(f)


def _assert_shape_exact(expected: Any, actual: Any, path: str = "root") -> None:
    assert type(actual) is type(expected), f"Type drift at {path}: {type(expected)} != {type(actual)}"

    if isinstance(expected, dict):
        assert set(actual.keys()) == set(expected.keys()), f"Key drift at {path}"
        for key in expected:
            _assert_shape_exact(expected[key], actual[key], f"{path}.{key}")
    elif isinstance(expected, list):
        assert len(actual) == len(expected), f"List length drift at {path}"
        for idx, (exp_item, act_item) in enumerate(zip(expected, actual)):
            _assert_shape_exact(exp_item, act_item, f"{path}[{idx}]")


def _overlaps(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    start_a = datetime.fromisoformat(a_start.replace("Z", "+00:00"))
    end_a = datetime.fromisoformat(a_end.replace("Z", "+00:00"))
    start_b = datetime.fromisoformat(b_start.replace("Z", "+00:00"))
    end_b = datetime.fromisoformat(b_end.replace("Z", "+00:00"))
    return start_a < end_b and start_b < end_a


def _schedule_capacity_hours(payload: dict[str, Any], day: str) -> float:
    day_start = datetime.fromisoformat(f"{day}T08:00:00")
    day_end = datetime.fromisoformat(f"{day}T21:00:00")

    blocked = 0.0
    for event in payload.get("calendar_events", []):
        start = datetime.fromisoformat(str(event["start_time"]).replace("Z", "+00:00"))
        end = datetime.fromisoformat(str(event["end_time"]).replace("Z", "+00:00"))
        if end <= day_start or start >= day_end:
            continue
        clip_start = max(start, day_start)
        clip_end = min(end, day_end)
        if clip_start < clip_end:
            blocked += (clip_end - clip_start).total_seconds() / 3600.0

    return float((day_end - day_start).total_seconds() / 3600.0) - blocked


def _scheduled_hours(rows: list[dict[str, Any]]) -> float:
    return float(sum(int(row.get("duration_units", 1)) for row in rows))


def test_decision_v2_golden_output_is_bit_stable_over_5_runs(monkeypatch: Any) -> None:
    monkeypatch.setattr(de, "datetime", _FrozenDateTime)

    fixture = _load_json("decision_v2_golden.json")
    payload = fixture["input"]
    expected = fixture["expected"]

    runs = [run_decision_engine_v2(payload) for _ in range(5)]

    for run in runs:
        assert run == expected
        _assert_shape_exact(expected, run)

    assert runs[0] == runs[1] == runs[2] == runs[3] == runs[4]


def test_decision_v2_ordering_and_drift_guards(monkeypatch: Any) -> None:
    monkeypatch.setattr(de, "datetime", _FrozenDateTime)

    fixture = _load_json("decision_v2_golden.json")
    payload = fixture["input"]

    runs = [run_decision_engine_v2(payload) for _ in range(5)]

    scheduled_ids_runs = [[row["proposal_id"] for row in r["scheduled_actions"]] for r in runs]
    unscheduled_ids_runs = [[row["proposal_id"] for row in r["unscheduled_actions"]] for r in runs]
    backlog_ids_runs = [[row["proposal_id"] for row in r["backlog"]] for r in runs]

    first_scheduled = scheduled_ids_runs[0]
    first_unscheduled = unscheduled_ids_runs[0]
    first_backlog = backlog_ids_runs[0]

    for ids in scheduled_ids_runs[1:]:
        assert ids == first_scheduled
    for ids in unscheduled_ids_runs[1:]:
        assert ids == first_unscheduled
    for ids in backlog_ids_runs[1:]:
        assert ids == first_backlog

    for run in runs:
        for row in run["scheduled_actions"]:
            start_hour = datetime.fromisoformat(str(row["start_time"]).replace("Z", "+00:00")).hour
            expected_bucket = "morning" if start_hour < 12 else ("afternoon" if start_hour < 18 else "evening")
            assert row["bucket"] == expected_bucket
            assert row["preferred_bucket"] in {"morning", "afternoon", "evening"}


def test_decision_v2_calendar_overlap_and_capacity_guards(monkeypatch: Any) -> None:
    monkeypatch.setattr(de, "datetime", _FrozenDateTime)

    fixture = _load_json("decision_v2_golden.json")
    payload = fixture["input"]

    result = run_decision_engine_v2(payload)
    scheduled = result["scheduled_actions"]

    for row in scheduled:
        for event in payload["calendar_events"]:
            assert not _overlaps(
                str(row["start_time"]),
                str(row["end_time"]),
                str(event["start_time"]),
                str(event["end_time"]),
            )

    for i in range(len(scheduled)):
        for j in range(i + 1, len(scheduled)):
            assert not _overlaps(
                str(scheduled[i]["start_time"]),
                str(scheduled[i]["end_time"]),
                str(scheduled[j]["start_time"]),
                str(scheduled[j]["end_time"]),
            )

    if scheduled:
        day = str(scheduled[0]["start_time"])[:10]
    else:
        day = "2026-04-16"

    capacity = _schedule_capacity_hours(payload, day)
    used = _scheduled_hours(scheduled)
    assert used <= capacity


def test_brief_golden_alpha_is_bit_stable_over_5_runs(monkeypatch: Any) -> None:
    monkeypatch.setattr(de, "datetime", _FrozenDateTime)
    monkeypatch.setattr(se, "date", _FrozenDate)

    fixture = _load_json("brief_household_alpha_golden.json")

    household_id = str(fixture["household_id"])
    orchestrator_output = dict(fixture["orchestrator_output"])
    expected = fixture["expected"]

    runs = [build_daily_brief(household_id, orchestrator_output=orchestrator_output) for _ in range(5)]

    for run in runs:
        assert run == expected
        _assert_shape_exact(expected, run)

    assert runs[0] == runs[1] == runs[2] == runs[3] == runs[4]


def test_brief_golden_beta_is_bit_stable_over_5_runs(monkeypatch: Any) -> None:
    monkeypatch.setattr(de, "datetime", _FrozenDateTime)
    monkeypatch.setattr(se, "date", _FrozenDate)

    fixture = _load_json("brief_household_beta_golden.json")

    household_id = str(fixture["household_id"])
    orchestrator_output = dict(fixture["orchestrator_output"])
    expected = fixture["expected"]

    runs = [build_daily_brief(household_id, orchestrator_output=orchestrator_output) for _ in range(5)]

    for run in runs:
        assert run == expected
        _assert_shape_exact(expected, run)

    assert runs[0] == runs[1] == runs[2] == runs[3] == runs[4]

