from __future__ import annotations

import argparse
import fnmatch
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ROOT_ARTIFACTS_DIR = ROOT / "verification_reports" / "root_artifacts"


@dataclass(frozen=True)
class MoveRule:
    name: str
    patterns: tuple[str, ...]
    destination_dir: Path


MOVE_RULES: tuple[MoveRule, ...] = (
    MoveRule(
        name="checkpoints",
        patterns=("*_checkpoint.json",),
        destination_dir=ROOT_ARTIFACTS_DIR / "checkpoints",
    ),
    MoveRule(
        name="reports",
        patterns=(
            "*_report.json",
            "*.report.json",
            "*.report.second.json",
            "*_results.json",
            "production_torture_report*.json",
            "sse_*_report*.json",
            "phase4_gate_validation*.json",
            "cleanup_analysis_*.json",
        ),
        destination_dir=ROOT_ARTIFACTS_DIR / "reports",
    ),
    MoveRule(
        name="logs",
        patterns=("*.log", "*.jsonl"),
        destination_dir=ROOT_ARTIFACTS_DIR / "logs",
    ),
)

RULE_BY_NAME: dict[str, MoveRule] = {rule.name: rule for rule in MOVE_RULES}

EXPLICIT_RULE_BY_FILE: dict[str, str] = {
    "assistant_core_report.json": "reports",
    "boot_smoke_report.json": "reports",
    "insight_report.json": "reports",
    "operational_mode_report.json": "reports",
    "policy_engine_report.json": "reports",
    "runtime_stress_report.json": "reports",
    "simulation_results.json": "reports",
    "evaluation_results.json": "reports",
    "calibration_log.jsonl": "logs",
}


def _iter_root_files() -> list[Path]:
    files: list[Path] = []
    for candidate in sorted(ROOT.iterdir(), key=lambda item: item.name.lower()):
        if candidate.is_file():
            files.append(candidate)
    return files


def _file_bytes_equal(left: Path, right: Path) -> bool:
    try:
        return left.read_bytes() == right.read_bytes()
    except OSError:
        return False


def _pattern_match(name: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatch(name, pattern) for pattern in patterns)


def _pick_rule(path: Path) -> MoveRule | None:
    explicit_name = EXPLICIT_RULE_BY_FILE.get(path.name)
    if explicit_name:
        return RULE_BY_NAME[explicit_name]

    for rule in MOVE_RULES:
        if _pattern_match(path.name, rule.patterns):
            return rule
    return None


def _next_conflict_destination(destination: Path) -> Path:
    stem = destination.stem
    suffix = destination.suffix
    index = 2

    while True:
        candidate = destination.with_name(f"{stem}.dup{index}{suffix}")
        if not candidate.exists():
            return candidate
        index += 1


def _to_relative(path: Path) -> str:
    return path.resolve().relative_to(ROOT.resolve()).as_posix()


def build_plan() -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []

    for source in _iter_root_files():
        rule = _pick_rule(source)
        if rule is None:
            continue

        destination = rule.destination_dir / source.name
        operation = "move"

        if destination.exists():
            if _file_bytes_equal(source, destination):
                operation = "delete_source_duplicate"
            else:
                destination = _next_conflict_destination(destination)

        plan.append(
            {
                "rule": rule.name,
                "operation": operation,
                "source": _to_relative(source),
                "destination": _to_relative(destination),
            }
        )

    return plan


def apply_plan(plan: list[dict[str, Any]], apply: bool) -> dict[str, int]:
    moved_count = 0
    dedup_deleted_count = 0

    if not apply:
        return {
            "moved_count": moved_count,
            "dedup_deleted_count": dedup_deleted_count,
        }

    for entry in plan:
        source = ROOT / str(entry["source"])
        destination = ROOT / str(entry["destination"])
        operation = str(entry["operation"])

        if not source.exists():
            continue

        if operation == "delete_source_duplicate":
            source.unlink()
            dedup_deleted_count += 1
            continue

        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))
        moved_count += 1

    return {
        "moved_count": moved_count,
        "dedup_deleted_count": dedup_deleted_count,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Organize root-level generated artifacts deterministically.")
    parser.add_argument("--apply", action="store_true", help="Apply file moves (default is dry-run).")
    parser.add_argument(
        "--report",
        default="verification_reports/root_artifacts/organize_root_artifacts_report.json",
        help="Output JSON report path relative to repository root.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    plan = build_plan()
    results = apply_plan(plan, apply=bool(args.apply))

    report_payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "apply": bool(args.apply),
        "planned_operations": len(plan),
        "moved_count": results["moved_count"],
        "dedup_deleted_count": results["dedup_deleted_count"],
        "operations": plan,
    }

    report_path = ROOT / str(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "apply": bool(args.apply),
                "planned_operations": len(plan),
                "moved_count": results["moved_count"],
                "dedup_deleted_count": results["dedup_deleted_count"],
                "report": _to_relative(report_path),
            },
            indent=2,
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
