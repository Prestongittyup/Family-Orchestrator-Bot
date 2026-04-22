from __future__ import annotations

import ast
import json
import os
import re
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from vulture import Vulture

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_ROOT = ROOT / "archive"

PYTHON_PATHS = [
    "apps",
    "household_os",
    "modules",
    "services",
    "shared",
    "workflows",
    "scripts",
    "tests",
    "legacy",
]

SKIP_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    "node_modules",
    "archive",
}

DO_NOT_TOUCH_PATTERNS = [
    re.compile(r"(^|/)household_os/core/lifecycle_state\.py$"),
    re.compile(r"(^|/)household_os/runtime/state_reducer\.py$"),
    re.compile(r"(^|/)household_os/runtime/event_store\.py$"),
    re.compile(r"(^|/)household_os/runtime/command_handler\.py$"),
    re.compile(r"(^|/)household_os/runtime/lifecycle_firewall\.py$"),
    re.compile(r"(^|/)household_os/runtime/lifecycle_migration\.py$"),
    re.compile(r"(^|/)apps/api/core/state_machine\.py$"),
    re.compile(r"(^|/)ci/"),
    re.compile(r"(^|/)\.github/workflows/"),
    re.compile(r"(^|/)tests/"),
]

LEGACY_HINTS = [
    "fsm",
    "legacy",
    "migration",
    "dual_write",
    "deprecated",
]

DYNAMIC_HINTS = [
    "importlib",
    "__import__(",
    "getattr(",
    "pkg_resources",
]


@dataclass
class FileRecord:
    path: str
    inbound_refs: int = 0
    test_refs: int = 0
    vulture_hits: int = 0
    dynamic_hint: bool = False
    category: str = "ACTIVE"
    reason: str = ""
    safety_locked: bool = False


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def is_python_file(path: Path) -> bool:
    if path.suffix != ".py":
        return False
    parts = set(path.parts)
    return not any(skip in parts for skip in SKIP_DIRS)


def module_name_for(path: Path) -> str:
    rp = rel(path)
    if rp.endswith("/__init__.py"):
        return rp[:-12].replace("/", ".")
    return rp[:-3].replace("/", ".")


def gather_python_files() -> list[Path]:
    files: list[Path] = []
    for base in PYTHON_PATHS:
        root = ROOT / base
        if not root.exists():
            continue
        for p in root.rglob("*.py"):
            if is_python_file(p):
                files.append(p)
    return sorted(set(files))


def is_do_not_touch(path: str) -> bool:
    return any(p.search(path) for p in DO_NOT_TOUCH_PATTERNS)


def parse_imports(path: Path) -> tuple[set[str], bool, bool]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    dynamic_hint = any(h in text for h in DYNAMIC_HINTS)
    is_test = "/tests/" in f"/{rel(path)}/" or rel(path).startswith("tests/")
    mods: set[str] = set()
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return mods, dynamic_hint, is_test

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                mods.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                mods.add(node.module)
    return mods, dynamic_hint, is_test


def run_vulture(paths: list[Path], min_confidence: int = 80) -> dict[str, int]:
    v = Vulture()
    v.scavenge([str(p) for p in paths])
    per_file: dict[str, int] = defaultdict(int)

    for item in v.get_unused_code(min_confidence=min_confidence):
        p = Path(item.filename)
        try:
            rp = rel(p)
        except Exception:
            continue
        per_file[rp] += 1
    return per_file


def classify(records: dict[str, FileRecord]) -> None:
    for rec in records.values():
        if rec.safety_locked:
            rec.category = "ACTIVE"
            rec.reason = "Safety-locked critical invariant/harness"
            continue

        lowered = rec.path.lower()
        is_legacy_named = any(h in lowered for h in LEGACY_HINTS)

        if rec.inbound_refs > 0 or rec.test_refs > 0:
            rec.category = "ACTIVE"
            rec.reason = "Referenced by runtime import graph and/or tests"
            continue

        if rec.dynamic_hint:
            rec.category = "INDIRECT"
            rec.reason = "Dynamic import/reflection hints detected"
            continue

        if is_legacy_named:
            rec.category = "LEGACY"
            rec.reason = "Legacy/FSM/migration naming hint with no inbound refs"
            continue

        if rec.vulture_hits > 0 and rec.inbound_refs == 0 and rec.test_refs == 0:
            rec.category = "UNUSED_CANDIDATE"
            rec.reason = "Vulture >=80 and no inbound/test references"
        else:
            rec.category = "INDIRECT"
            rec.reason = "Low-confidence orphan; manual review required"


def archive_target_for(path: str, category: str) -> Path:
    src = ROOT / path
    if category == "LEGACY":
        base = ARCHIVE_ROOT / "legacy_fsm"
    elif category == "UNUSED_CANDIDATE":
        base = ARCHIVE_ROOT / "unused_candidates"
    else:
        base = ARCHIVE_ROOT / "indirect_review"
    return base / src.relative_to(ROOT)


def move_files(records: dict[str, FileRecord], dry_run: bool) -> list[dict[str, str]]:
    moved: list[dict[str, str]] = []
    for rec in records.values():
        if rec.safety_locked:
            continue
        if rec.category not in {"LEGACY", "UNUSED_CANDIDATE", "INDIRECT"}:
            continue

        # Only auto-move high-confidence files to avoid runtime risk.
        if rec.category == "INDIRECT":
            continue
        if rec.category == "LEGACY" and rec.dynamic_hint:
            continue

        src = ROOT / rec.path
        if not src.exists():
            continue
        dst = archive_target_for(rec.path, rec.category)
        moved.append({"from": rec.path, "to": rel(dst), "category": rec.category})
        if dry_run:
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
    return moved


def rollback_moves(moves: list[dict[str, str]]) -> None:
    for m in reversed(moves):
        src = ROOT / m["to"]
        dst = ROOT / m["from"]
        if not src.exists():
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))


def run(dry_run: bool) -> dict[str, Any]:
    py_files = gather_python_files()
    module_to_file = {module_name_for(p): rel(p) for p in py_files}

    records = {
        rel(p): FileRecord(path=rel(p), safety_locked=is_do_not_touch(rel(p)))
        for p in py_files
    }

    for p in py_files:
        imports, dynamic_hint, is_test = parse_imports(p)
        caller = rel(p)
        records[caller].dynamic_hint = records[caller].dynamic_hint or dynamic_hint

        for mod in imports:
            target = module_to_file.get(mod)
            if not target:
                continue
            if is_test:
                records[target].test_refs += 1
            else:
                records[target].inbound_refs += 1

    vulture_map = run_vulture(py_files, min_confidence=80)
    for path, hits in vulture_map.items():
        if path in records:
            records[path].vulture_hits = hits

    classify(records)
    moved = move_files(records, dry_run=dry_run)

    by_category: dict[str, int] = defaultdict(int)
    for r in records.values():
        by_category[r.category] += 1

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "total_files_scanned": len(records),
        "category_breakdown": dict(sorted(by_category.items())),
        "moved": moved,
        "records": [r.__dict__ for r in sorted(records.values(), key=lambda x: x.path)],
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Safe repo cleanup analyzer/mover")
    parser.add_argument("--apply", action="store_true", help="Apply archive moves")
    parser.add_argument("--report", default="cleanup_analysis.json", help="Report output path")
    args = parser.parse_args()

    result = run(dry_run=not args.apply)
    out = ROOT / args.report
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({
        "total_files_scanned": result["total_files_scanned"],
        "category_breakdown": result["category_breakdown"],
        "moved_count": len(result["moved"]),
        "report": rel(out),
    }, indent=2))
