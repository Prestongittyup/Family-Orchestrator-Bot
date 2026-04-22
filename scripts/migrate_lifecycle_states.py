from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from household_os.core.lifecycle_state import LifecycleState


MAPPING = {
    "executed": LifecycleState.COMMITTED.value,
    "ignored": LifecycleState.FAILED.value,
}

TARGET_KEYS = {
    "state",
    "status",
    "current_state",
    "from_state",
    "to_state",
    "lifecycle_state",
}

LIFECYCLE_PATH_HINTS = {
    "action_lifecycle",
    "transition_log",
    "behavior_feedback",
    "event_history",
    "lifecycle",
}


@dataclass
class MigrationStats:
    total_scanned: int = 0
    total_migrated: int = 0
    file_updates: int = 0
    db_updates: int = 0
    unknown_values: list[str] = field(default_factory=list)


def _valid_states() -> set[str]:
    return {state.value for state in LifecycleState}


def _is_lifecycle_path(path: str) -> bool:
    lowered = path.lower()
    return any(hint in lowered for hint in LIFECYCLE_PATH_HINTS)


def _walk_and_migrate(node: Any, *, stats: MigrationStats, path: str = "$") -> tuple[Any, bool]:
    changed = False
    valid = _valid_states()

    if isinstance(node, dict):
        out: dict[str, Any] = {}
        for key, value in node.items():
            node_path = f"{path}.{key}"
            if key in TARGET_KEYS and isinstance(value, str) and _is_lifecycle_path(node_path):
                stats.total_scanned += 1
                lowered = value.strip().lower()
                if lowered in MAPPING:
                    out[key] = MAPPING[lowered]
                    stats.total_migrated += 1
                    changed = True
                    continue
                if lowered in valid:
                    out[key] = lowered
                    if lowered != value:
                        changed = True
                    continue
                stats.unknown_values.append(f"{node_path}={value}")
                out[key] = value
                continue

            migrated_value, inner_changed = _walk_and_migrate(value, stats=stats, path=node_path)
            out[key] = migrated_value
            changed = changed or inner_changed
        return out, changed

    if isinstance(node, list):
        out_list: list[Any] = []
        for idx, item in enumerate(node):
            migrated_value, inner_changed = _walk_and_migrate(item, stats=stats, path=f"{path}[{idx}]")
            out_list.append(migrated_value)
            changed = changed or inner_changed
        return out_list, changed

    return node, False


def _process_json_file(path: Path, *, dry_run: bool, stats: MigrationStats) -> None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return

    migrated, changed = _walk_and_migrate(payload, stats=stats, path=f"{path.name}")
    if not changed:
        return

    stats.file_updates += 1
    if dry_run:
        return

    path.write_text(json.dumps(migrated, indent=2), encoding="utf-8")


def _process_sqlite_file(path: Path, *, dry_run: bool, stats: MigrationStats) -> None:
    valid = _valid_states()
    conn = sqlite3.connect(path)
    try:
        cursor = conn.cursor()
        tables = [row[0] for row in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")]

        for table in tables:
            table_is_lifecycle = any(
                hint in table.lower() for hint in ("lifecycle", "action", "event", "snapshot")
            )
            columns = [row[1] for row in cursor.execute(f"PRAGMA table_info({table})")]
            text_columns = [c for c in columns if c in TARGET_KEYS or c in {"payload"}]
            if not text_columns:
                continue

            rows = cursor.execute(f"SELECT rowid, {', '.join(text_columns)} FROM {table}").fetchall()
            for row in rows:
                rowid = row[0]
                values = dict(zip(text_columns, row[1:]))
                updates: dict[str, Any] = {}

                for col, raw in values.items():
                    if raw is None:
                        continue

                    if col in TARGET_KEYS and isinstance(raw, str) and table_is_lifecycle:
                        stats.total_scanned += 1
                        lowered = raw.strip().lower()
                        if lowered in MAPPING:
                            updates[col] = MAPPING[lowered]
                            stats.total_migrated += 1
                        elif lowered in valid:
                            if lowered != raw:
                                updates[col] = lowered
                        else:
                            stats.unknown_values.append(f"{path.name}:{table}.{col}[rowid={rowid}]={raw}")

                    if col == "payload" and isinstance(raw, str):
                        try:
                            payload = json.loads(raw)
                        except Exception:
                            continue
                        migrated_payload, changed = _walk_and_migrate(
                            payload,
                            stats=stats,
                            path=f"{path.name}:{table}.payload[rowid={rowid}]",
                        )
                        if changed:
                            updates[col] = json.dumps(migrated_payload, separators=(",", ":"))

                if updates:
                    stats.db_updates += 1
                    if dry_run:
                        continue
                    set_clause = ", ".join(f"{col}=?" for col in updates)
                    cursor.execute(
                        f"UPDATE {table} SET {set_clause} WHERE rowid=?",
                        [*updates.values(), rowid],
                    )

        if not dry_run:
            conn.commit()
    finally:
        conn.close()


def run(*, data_dir: Path, dry_run: bool) -> MigrationStats:
    stats = MigrationStats()

    for json_file in sorted(data_dir.rglob("*.json")):
        _process_json_file(json_file, dry_run=dry_run, stats=stats)

    for db_file in sorted(data_dir.rglob("*.db")):
        _process_sqlite_file(db_file, dry_run=dry_run, stats=stats)

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="One-time lifecycle state migration for persisted data")
    parser.add_argument("--data-dir", default=str(ROOT / "data"), help="Data directory to scan")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    dry_run = not args.apply

    stats = run(data_dir=data_dir, dry_run=dry_run)

    print("Lifecycle migration summary")
    print(f"mode: {'dry-run' if dry_run else 'apply'}")
    print(f"data_dir: {data_dir}")
    print(f"total records scanned: {stats.total_scanned}")
    print(f"total migrated: {stats.total_migrated}")
    print(f"json files updated: {stats.file_updates}")
    print(f"db rows updated: {stats.db_updates}")

    if stats.unknown_values:
        print("unknown values detected:")
        for row in stats.unknown_values:
            print(f" - {row}")
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
