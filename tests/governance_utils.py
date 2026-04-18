from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


GOVERNANCE_ROOT = Path(__file__).parent / "fixtures" / "governance"


class GovernanceError(RuntimeError):
    """Raised when governance fixtures are malformed."""


@dataclass(frozen=True)
class GovernanceTrack:
    track_id: str
    root: Path


def structure_signature(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            "type": "object",
            "keys": {k: structure_signature(obj[k]) for k in sorted(obj.keys())},
        }
    if isinstance(obj, list):
        item = structure_signature(obj[0]) if obj else {"type": "empty"}
        return {"type": "array", "item": item}
    if obj is None:
        t = "null"
    elif isinstance(obj, bool):
        t = "bool"
    elif isinstance(obj, int):
        t = "int"
    elif isinstance(obj, float):
        t = "float"
    elif isinstance(obj, str):
        t = "str"
    else:
        t = type(obj).__name__
    return {"type": t}


def schema_hash(obj: Any) -> str:
    payload = json.dumps(structure_signature(obj), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def value_hash(obj: Any) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _version_num(path: Path) -> int:
    return int(path.name[1:])


def _version_dirs(track_root: Path) -> list[Path]:
    versions = []
    for child in track_root.iterdir():
        if child.is_dir() and child.name.startswith("v") and child.name[1:].isdigit():
            versions.append(child)
    return sorted(versions, key=_version_num)


def latest_version_dir(track_root: Path) -> Path:
    versions = [v for v in _version_dirs(track_root) if (v / "expected.json").exists()]
    if not versions:
        raise GovernanceError(f"No version with expected.json found under {track_root}")
    return versions[-1]


def next_version_name(track_root: Path) -> str:
    versions = _version_dirs(track_root)
    if not versions:
        return "v1"
    return f"v{_version_num(versions[-1]) + 1}"


def all_tracks(root: Path = GOVERNANCE_ROOT) -> list[GovernanceTrack]:
    tracks: list[GovernanceTrack] = []

    decision_track = root / "decision_engine_v2"
    if decision_track.exists():
        tracks.append(GovernanceTrack("decision_engine_v2", decision_track))

    brief_root = root / "brief"
    if brief_root.exists():
        for household in sorted(brief_root.iterdir()):
            if household.is_dir():
                tracks.append(GovernanceTrack(f"brief/{household.name}", household))

    return tracks


def classify_change(expected: Any, actual: Any) -> str:
    try:
        if actual == expected:
            return "SAFE"
        if schema_hash(actual) == schema_hash(expected):
            return "BEHAVIORAL"
        return "BREAKING"
    except Exception:
        return "UNKNOWN"


def diff_summary(expected: Any, actual: Any) -> dict[str, Any]:
    expected_schema = schema_hash(expected)
    actual_schema = schema_hash(actual)
    return {
        "expected_schema_hash": expected_schema,
        "actual_schema_hash": actual_schema,
        "expected_value_hash": value_hash(expected),
        "actual_value_hash": value_hash(actual),
        "value_equal": actual == expected,
        "schema_equal": expected_schema == actual_schema,
        "expected_top_level_keys": sorted(expected.keys()) if isinstance(expected, dict) else [],
        "actual_top_level_keys": sorted(actual.keys()) if isinstance(actual, dict) else [],
    }


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")