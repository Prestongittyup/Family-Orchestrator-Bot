from __future__ import annotations

import ast
from pathlib import Path

LEGACY_LIFECYCLE_TERMS = {"executed", "ignored"}
LIFECYCLE_KEYS = {"state", "status", "current_state", "from_state", "to_state", "lifecycle_state"}
RUNTIME_ROOTS = [Path("apps"), Path("household_os"), Path("assistant"), Path("services")]
ALLOWED_DIRS = {"tests", "docs", "migrations"}
ALLOWED_PATH_SUFFIXES = {
    Path("apps") / "api" / "assistant_runtime_router.py",
}


def _is_excluded_path(path: Path) -> bool:
    parts = set(path.parts)
    if parts.intersection(ALLOWED_DIRS):
        return True
    if "archive" in parts or ".venv" in parts:
        return True
    return False


def _is_state_like(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return "state" in node.id.lower() or "status" in node.id.lower()
    if isinstance(node, ast.Attribute):
        return "state" in node.attr.lower() or "status" in node.attr.lower()
    if isinstance(node, ast.Subscript):
        target = node.slice
        if isinstance(target, ast.Constant) and isinstance(target.value, str):
            return target.value.lower() in LIFECYCLE_KEYS
    return False


def _legacy_literal(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        lowered = node.value.strip().lower()
        if lowered in LEGACY_LIFECYCLE_TERMS:
            return lowered
    return None


def test_no_legacy_lifecycle_literals_in_runtime_code() -> None:
    findings: list[str] = []

    for root in RUNTIME_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if _is_excluded_path(path):
                continue
            if any(path == suffix or path.as_posix().endswith(suffix.as_posix()) for suffix in ALLOWED_PATH_SUFFIXES):
                continue

            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

            for node in ast.walk(tree):
                if isinstance(node, ast.Compare):
                    targets = [node.left, *node.comparators]
                    if not any(_is_state_like(target) for target in targets):
                        continue
                    for target in targets:
                        legacy = _legacy_literal(target)
                        if legacy is not None:
                            findings.append(f"{path}:{getattr(target, 'lineno', '?')}:{legacy}")

                if isinstance(node, ast.Dict):
                    for key_node, value_node in zip(node.keys, node.values):
                        if not (isinstance(key_node, ast.Constant) and isinstance(key_node.value, str)):
                            continue
                        if key_node.value.lower() not in LIFECYCLE_KEYS:
                            continue
                        legacy = _legacy_literal(value_node)
                        if legacy is not None:
                            findings.append(f"{path}:{getattr(value_node, 'lineno', '?')}:{legacy}")

                if isinstance(node, ast.Call):
                    for arg in node.args:
                        legacy = _legacy_literal(arg)
                        if legacy is None:
                            continue
                        if isinstance(node.func, ast.Name) and node.func.id in {
                            "parse_lifecycle_state",
                            "normalize_state",
                            "enforce_boundary_state",
                        }:
                            findings.append(f"{path}:{getattr(arg, 'lineno', '?')}:{legacy}")

    assert not findings, "Legacy lifecycle literals found in runtime code:\n" + "\n".join(sorted(findings))
