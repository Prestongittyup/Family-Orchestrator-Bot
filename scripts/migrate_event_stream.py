from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from household_os.core.lifecycle_state import LifecycleState
from scripts.lifecycle_conversion import LEGACY_LIFECYCLE_MAP, normalize_lifecycle_literal


LEGACY_MAP = LEGACY_LIFECYCLE_MAP

LIFECYCLE_KEYS = {"state", "status", "current_state", "from_state", "to_state", "lifecycle_state"}
LIFECYCLE_PATH_HINTS = {"action_lifecycle", "transition_log", "behavior_feedback", "event_history", "lifecycle"}


@dataclass
class StreamMigrationReport:
    scanned: int = 0
    migrated: int = 0
    files_updated: int = 0
    unknown: list[str] = field(default_factory=list)


def _valid_states() -> set[str]:
    return {s.value for s in LifecycleState}


def _is_lifecycle_path(path: str) -> bool:
    lowered = path.lower()
    return any(hint in lowered for hint in LIFECYCLE_PATH_HINTS)


def _rewrite(node: Any, *, report: StreamMigrationReport, path: str) -> tuple[Any, bool]:
    changed = False
    valid = _valid_states()

    if isinstance(node, dict):
        out: dict[str, Any] = {}
        for key, value in node.items():
            next_path = f"{path}.{key}"
            if key in LIFECYCLE_KEYS and isinstance(value, str) and _is_lifecycle_path(next_path):
                report.scanned += 1
                lowered = normalize_lifecycle_literal(value)
                if lowered in LEGACY_MAP:
                    out[key] = LEGACY_MAP[lowered]
                    report.migrated += 1
                    changed = True
                    continue
                if lowered in valid:
                    out[key] = lowered
                    if lowered != value:
                        changed = True
                    continue
                report.unknown.append(f"{next_path}={value}")
                out[key] = value
                continue

            rewritten, sub_changed = _rewrite(value, report=report, path=next_path)
            out[key] = rewritten
            changed = changed or sub_changed
        return out, changed

    if isinstance(node, list):
        out_list: list[Any] = []
        for idx, item in enumerate(node):
            rewritten, sub_changed = _rewrite(item, report=report, path=f"{path}[{idx}]")
            out_list.append(rewritten)
            changed = changed or sub_changed
        return out_list, changed

    return node, False


def _migrate_file(path: Path, *, dry_run: bool, report: StreamMigrationReport) -> None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return

    rewritten, changed = _rewrite(payload, report=report, path=path.name)
    if not changed:
        return

    report.files_updated += 1
    if dry_run:
        return

    path.write_text(json.dumps(rewritten, indent=2), encoding="utf-8")


def run(*, data_dir: Path, dry_run: bool) -> StreamMigrationReport:
    report = StreamMigrationReport()
    for file_path in sorted(data_dir.rglob("*.json")):
        _migrate_file(file_path, dry_run=dry_run, report=report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Pre-replay migration for lifecycle values in persisted event streams")
    parser.add_argument("--data-dir", default=str(ROOT / "data"), help="Data directory to scan")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    args = parser.parse_args()

    dry_run = not args.apply
    report = run(data_dir=Path(args.data_dir), dry_run=dry_run)

    print("Event stream migration summary")
    print(f"mode: {'dry-run' if dry_run else 'apply'}")
    print(f"records scanned: {report.scanned}")
    print(f"records migrated: {report.migrated}")
    print(f"files updated: {report.files_updated}")

    if report.unknown:
        print("unknown values detected:")
        for item in report.unknown:
            print(f" - {item}")
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
