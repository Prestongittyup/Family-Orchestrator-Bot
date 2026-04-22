from __future__ import annotations

import ast
from pathlib import Path

LEGACY_FORBIDDEN = {
    "executed",
    "ignored",
    "pending_approval",
}

RAW_FORBIDDEN = {
    "approved",
    "committed",
    "failed",
    "rejected",
    "proposed",
}


def _roots() -> list[Path]:
    candidates = [
        Path("src"),
        Path("tests"),
        Path("fixtures"),
        Path("scripts"),
        Path("household_os"),
        Path("apps"),
    ]
    return [p for p in candidates if p.exists()]


def _is_state_like(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return "state" in node.id.lower()
    if isinstance(node, ast.Attribute):
        return "state" in node.attr.lower()
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        return node.func.id in {"parse_lifecycle_state", "assert_lifecycle_state", "reduce_state", "replay_events"}
    return False


def test_no_legacy_lifecycle_strings_in_python_files() -> None:
    findings: list[str] = []

    for root in _roots():
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

            for node in ast.walk(tree):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    value = node.value.strip().lower()
                    if value in LEGACY_FORBIDDEN:
                        findings.append(f"{path}:{node.lineno}:{value}")

                if isinstance(node, ast.Compare):
                    targets = [node.left, *node.comparators]
                    if not any(_is_state_like(t) for t in targets):
                        continue
                    for target in targets:
                        if isinstance(target, ast.Constant) and isinstance(target.value, str):
                            value = target.value.strip().lower()
                            if value in RAW_FORBIDDEN or value in LEGACY_FORBIDDEN:
                                findings.append(f"{path}:{target.lineno}:{value}")

    assert not findings, "Forbidden lifecycle strings found:\n" + "\n".join(findings)


def test_no_legacy_lifecycle_strings_in_fixtures_and_data() -> None:
    findings: list[str] = []
    text_roots = [Path("fixtures"), Path("data")]

    for root in text_roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in {".json", ".yaml", ".yml", ".txt"}:
                continue

            content = path.read_text(encoding="utf-8", errors="ignore").lower()
            for forbidden in LEGACY_FORBIDDEN:
                if forbidden in content:
                    findings.append(f"{path}:{forbidden}")

    assert not findings, "Legacy lifecycle strings found in fixtures/data:\n" + "\n".join(findings)
