from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from household_os.core.lifecycle_state import LifecycleState, parse_lifecycle_state

LIFECYCLE_KEYS = {
    "state",
    "status",
    "current_state",
    "from_state",
    "to_state",
    "lifecycle_state",
}

LEGACY_FORBIDDEN = {"executed", "ignored", "pending_approval"}
LIFECYCLE_PATH_HINTS = {
    "action_lifecycle",
    "transition_log",
    "behavior_feedback",
    "event_history",
    "lifecycle",
}


@dataclass
class RawStateValue:
    source: str
    value: str


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _candidate_data_roots() -> list[Path]:
    roots = [
        _repo_root() / "data",
        _repo_root() / "fixtures",
    ]
    return [root for root in roots if root.exists()]


def _path_is_lifecycle(path: str) -> bool:
    lowered = path.lower()
    return any(hint in lowered for hint in LIFECYCLE_PATH_HINTS)


def _collect_json_states(node: Any, *, path: str, out: list[RawStateValue]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            next_path = f"{path}.{key}"
            if key in LIFECYCLE_KEYS and isinstance(value, str) and _path_is_lifecycle(next_path):
                out.append(RawStateValue(source=next_path, value=value))
            _collect_json_states(value, path=next_path, out=out)
        return

    if isinstance(node, list):
        for idx, item in enumerate(node):
            _collect_json_states(item, path=f"{path}[{idx}]", out=out)


def _load_raw_state_values() -> list[RawStateValue]:
    found: list[RawStateValue] = []

    for root in _candidate_data_roots():
        for json_path in root.rglob("*.json"):
            try:
                payload = json.loads(json_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            _collect_json_states(payload, path=str(json_path.relative_to(_repo_root())), out=found)

        for db_path in root.rglob("*.db"):
            conn = sqlite3.connect(db_path)
            try:
                cursor = conn.cursor()
                tables = [row[0] for row in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")]
                for table in tables:
                    columns = [row[1] for row in cursor.execute(f"PRAGMA table_info({table})")]
                    target_columns = [c for c in columns if c in LIFECYCLE_KEYS or c == "payload"]
                    if not target_columns:
                        continue

                    rows = cursor.execute(f"SELECT rowid, {', '.join(target_columns)} FROM {table}").fetchall()
                    for row in rows:
                        rowid = row[0]
                        values = dict(zip(target_columns, row[1:]))
                        for column, raw in values.items():
                            if not isinstance(raw, str):
                                continue

                            source = f"{db_path.relative_to(_repo_root())}:{table}.{column}[rowid={rowid}]"
                            if column in LIFECYCLE_KEYS:
                                found.append(RawStateValue(source=source, value=raw))
                                continue

                            if column == "payload":
                                try:
                                    payload = json.loads(raw)
                                except Exception:
                                    continue
                                _collect_json_states(payload, path=source, out=found)
            finally:
                conn.close()

    return found


def test_all_persisted_states_are_canonical() -> None:
    raw_values = _load_raw_state_values()

    invalid: list[str] = []
    for item in raw_values:
        try:
            parsed = parse_lifecycle_state(item.value)
            assert isinstance(parsed, LifecycleState)
        except (TypeError, ValueError):
            invalid.append(f"{item.source}={item.value}")

    assert not invalid, "Non-canonical persisted lifecycle values found:\n" + "\n".join(invalid)


def test_no_legacy_state_strings_present() -> None:
    raw_values = _load_raw_state_values()

    found_legacy = [
        f"{item.source}={item.value}"
        for item in raw_values
        if isinstance(item.value, str) and item.value.strip().lower() in LEGACY_FORBIDDEN
    ]

    assert not found_legacy, "Legacy lifecycle values still present:\n" + "\n".join(found_legacy)
