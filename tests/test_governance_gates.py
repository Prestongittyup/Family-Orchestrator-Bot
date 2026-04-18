from __future__ import annotations

from datetime import date as _date
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

from apps.api.services import decision_engine as de
from apps.api.services import synthesis_engine as se
from apps.api.services.decision_engine import run_decision_engine_v2
from apps.api.services.synthesis_engine import build_daily_brief
TESTS_DIR = Path(__file__).parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from governance_utils import (
    GOVERNANCE_ROOT,
    all_tracks,
    classify_change,
    diff_summary,
    latest_version_dir,
    load_json,
    schema_hash,
    value_hash,
    write_json,
)


REPORTS_DIR = GOVERNANCE_ROOT / "reports"


class _FrozenDateTime(datetime):
    @classmethod
    def utcnow(cls) -> datetime:
        return cls(2026, 4, 15, 9, 0, 0)


class _FrozenDate(_date):
    @classmethod
    def today(cls) -> _date:
        return cls(2026, 4, 15)


def _run_track(track_id: str, input_payload: dict[str, Any]) -> dict[str, Any]:
    if track_id == "decision_engine_v2":
        return run_decision_engine_v2(input_payload)

    if track_id.startswith("brief/"):
        return build_daily_brief(
            str(input_payload["household_id"]),
            orchestrator_output=dict(input_payload["orchestrator_output"]),
        )

    raise ValueError(f"Unknown track id: {track_id}")


def test_governance_fixture_metadata_hashes_are_consistent() -> None:
    for track in all_tracks():
        latest = latest_version_dir(track.root)
        expected = load_json(latest / "expected.json")
        metadata = load_json(latest / "metadata.json")

        assert metadata["schema_hash"] == schema_hash(expected)
        assert metadata["value_hash"] == value_hash(expected)


def test_governance_classification_gate_is_safe_and_replay_stable(monkeypatch: Any) -> None:
    monkeypatch.setattr(de, "datetime", _FrozenDateTime)
    monkeypatch.setattr(se, "date", _FrozenDate)

    report_rows: list[dict[str, Any]] = []

    for track in all_tracks():
        latest = latest_version_dir(track.root)
        expected = load_json(latest / "expected.json")
        input_payload = load_json(latest / "input.json")

        runs = [_run_track(track.track_id, input_payload) for _ in range(5)]
        first = runs[0]

        for run in runs[1:]:
            assert run == first

        classification = classify_change(expected, first)
        summary = diff_summary(expected, first)

        report_rows.append(
            {
                "track_id": track.track_id,
                "version": latest.name,
                "classification": classification,
                "replay_deterministic": all(run == first for run in runs),
                "diff": summary,
            }
        )

        assert classification == "SAFE", (
            f"Governance gate blocked for {track.track_id}/{latest.name}: "
            f"classification={classification}"
        )

    report = {
        "report_type": "governance_diff_report",
        "status": "pass",
        "tracks": report_rows,
    }
    write_json(REPORTS_DIR / "latest_governance_report.json", report)

