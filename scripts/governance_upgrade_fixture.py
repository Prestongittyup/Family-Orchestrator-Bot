#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from datetime import UTC, date as _date
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
TESTS_DIR = ROOT / "tests"
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from apps.api.services import decision_engine as de
from apps.api.services import synthesis_engine as se
from apps.api.services.decision_engine import run_decision_engine_v2
from apps.api.services.synthesis_engine import build_daily_brief
from governance_utils import (
    GOVERNANCE_ROOT,
    all_tracks,
    classify_change,
    diff_summary,
    latest_version_dir,
    load_json,
    next_version_name,
    schema_hash,
    value_hash,
    write_json,
)


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


def _find_track(track_id: str) -> Path:
    for track in all_tracks():
        if track.track_id == track_id:
            return track.root
    raise ValueError(f"Unknown governance track: {track_id}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a new versioned governance fixture from current runtime output."
    )
    parser.add_argument(
        "--track",
        required=True,
        help="Track id, e.g. decision_engine_v2 or brief/household_alpha",
    )
    parser.add_argument(
        "--new-version",
        default=None,
        help="Optional explicit version name (e.g. v2). Defaults to next numeric version.",
    )
    parser.add_argument(
        "--allow-breaking",
        action="store_true",
        help="Allow creating a new version when classification is BREAKING/UNKNOWN.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    # Freeze time for deterministic candidate generation.
    de.datetime = _FrozenDateTime  # type: ignore[assignment]
    se.date = _FrozenDate  # type: ignore[assignment]

    track_root = _find_track(args.track)
    latest = latest_version_dir(track_root)

    input_payload = load_json(latest / "input.json")
    expected = load_json(latest / "expected.json")
    actual = _run_track(args.track, input_payload)

    classification = classify_change(expected, actual)
    if classification in {"BREAKING", "UNKNOWN"} and not args.allow_breaking:
        print(
            "Blocked: classification is "
            f"{classification}. Re-run with --allow-breaking to create a new version."
        )
        return 2

    target_version = args.new_version or next_version_name(track_root)
    target_dir = track_root / target_version
    if target_dir.exists() and (target_dir / "expected.json").exists():
        print(f"Blocked: target version already exists with expected.json at {target_dir}")
        return 3

    report = {
        "report_type": "governance_upgrade_report",
        "track_id": args.track,
        "from_version": latest.name,
        "to_version": target_version,
        "classification": classification,
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "diff": diff_summary(expected, actual),
    }

    write_json(target_dir / "input.json", input_payload)
    write_json(target_dir / "expected.json", actual)
    write_json(
        target_dir / "metadata.json",
        {
            "version": target_version,
            "created_at_utc": report["generated_at_utc"],
            "derived_from_version": latest.name,
            "source_track": args.track,
            "classification_from_previous": classification,
            "schema_hash": schema_hash(actual),
            "value_hash": value_hash(actual),
        },
    )

    reports_dir = GOVERNANCE_ROOT / "reports"
    safe_track_name = args.track.replace("/", "__")
    report_path = reports_dir / f"upgrade_{safe_track_name}_{latest.name}_to_{target_version}.json"
    write_json(report_path, report)

    print(f"Created governance fixture version: {target_dir}")
    print(f"Wrote diff report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

