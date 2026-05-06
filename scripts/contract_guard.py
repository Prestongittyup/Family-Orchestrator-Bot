from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SCAN_EXTENSIONS = {".py", ".yml", ".yaml", ".md"}
EXCLUDED_DIR_NAMES = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    ".mypy_cache",
    ".pytest_cache",
    "node_modules",
}
EXCLUDED_RELATIVE_DIRS = {
    Path("archive/unused_candidates"),
    Path("archive/_quarantined"),
}
EXCLUDED_RELATIVE_FILES = {
    Path("scripts/contract_guard.py"),
}

COMMAND_BLOCK_LANGS = {
    "",
    "bash",
    "sh",
    "shell",
    "zsh",
    "powershell",
    "ps1",
    "cmd",
    "bat",
    "dockerfile",
    "yaml",
    "yml",
}

ACTIVE_COMMAND_PREFIXES = (
    "$",
    "python ",
    "python3 ",
    "uvicorn ",
    "gunicorn ",
    "docker ",
    "docker-compose ",
    "docker compose ",
    "curl ",
    "cmd [",
)

RULES: list[tuple[str, re.Pattern[str]]] = [
    # Match route-like /health usages while avoiding filesystem path fragments.
    ("non_canonical_health", re.compile(r"(?:^|[\s\"'`(=])(/health)(?!z)\b")),
    ("legacy_ready", re.compile(r"/ready\b")),
    ("legacy_system", re.compile(r"/v1/system/")),
    ("legacy_v1_health", re.compile(r"/v1/health\b")),
    ("legacy_archive_entrypoint", re.compile(r"\barchive\.apps\.api\.main\b")),
    ("legacy_apps_entrypoint", re.compile(r"\bapps\.api\.main\b")),
]


def _is_excluded_path(rel_path: Path) -> bool:
    rel_posix = rel_path.as_posix()
    if rel_path in EXCLUDED_RELATIVE_FILES:
        return True
    if any(part in EXCLUDED_DIR_NAMES for part in rel_path.parts):
        return True
    for excluded in EXCLUDED_RELATIVE_DIRS:
        excluded_posix = excluded.as_posix()
        if rel_posix == excluded_posix or rel_posix.startswith(excluded_posix + "/"):
            return True
    return False


def _is_active_markdown_line(line: str, *, in_command_block: bool) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if in_command_block:
        return True
    lowered = stripped.lower()
    return lowered.startswith(ACTIVE_COMMAND_PREFIXES)


def _iter_text_files() -> list[Path]:
    files: list[Path] = []
    for root, dirs, filenames in os.walk(ROOT):
        rel_root = Path(root).resolve().relative_to(ROOT)
        dirs[:] = [
            d for d in dirs if not _is_excluded_path(rel_root / d)
        ]

        for name in filenames:
            file_path = (Path(root) / name).resolve()
            rel_file = file_path.relative_to(ROOT)
            if _is_excluded_path(rel_file):
                continue
            if file_path.suffix.lower() not in SCAN_EXTENSIONS:
                continue
            files.append(file_path)
    return files


def _scan_file(path: Path) -> list[tuple[int, str, str]]:
    violations: list[tuple[int, str, str]] = []
    rel_path = path.relative_to(ROOT)

    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return violations

    if path.suffix.lower() != ".md":
        for idx, line in enumerate(lines, start=1):
            for rule_name, pattern in RULES:
                if pattern.search(line):
                    violations.append((idx, line, rule_name))
        return violations

    in_fence = False
    command_fence = False

    for idx, line in enumerate(lines, start=1):
        stripped = line.strip()
        if stripped.startswith("```"):
            if not in_fence:
                lang = stripped[3:].strip().lower()
                in_fence = True
                command_fence = lang in COMMAND_BLOCK_LANGS
            else:
                in_fence = False
                command_fence = False
            continue

        if not _is_active_markdown_line(line, in_command_block=command_fence):
            continue

        for rule_name, pattern in RULES:
            if pattern.search(line):
                violations.append((idx, line, rule_name))

    return violations


def main() -> int:
    violations_found = False

    for file_path in sorted(_iter_text_files(), key=lambda p: p.as_posix()):
        rel_path = file_path.relative_to(ROOT)
        violations = _scan_file(file_path)
        for line_no, line, _rule_name in violations:
            violations_found = True
            print(f"{rel_path.as_posix()}:{line_no}: {line}")

    if violations_found:
        return 1

    print("CONTRACT_GUARD_PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
