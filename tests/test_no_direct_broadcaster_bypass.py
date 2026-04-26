from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCAN_ROOTS = [
    ROOT / "apps" / "api" / "services",
    ROOT / "apps" / "api" / "identity",
    ROOT / "apps" / "api" / "hpal",
]
ALLOWED_IMPORTER = (ROOT / "apps" / "api" / "services" / "canonical_event_router.py").resolve()


def _is_direct_broadcaster_import(path: Path, source: str) -> bool:
    """Return True when a file directly imports broadcaster via import syntax."""
    tree = ast.parse(source, filename=str(path))

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module.split(".")[-1] == "broadcaster":
                return True

        if isinstance(node, ast.Import):
            for alias in node.names:
                imported = alias.name
                if imported == "broadcaster" or imported.split(".")[-1] == "broadcaster":
                    return True

    return False


def test_no_direct_broadcaster_bypass() -> None:
    """
    Enforce architectural boundary:
    - No file under services/identity/hpal may directly import broadcaster.
    - Only canonical_event_router.py is allowed to import broadcaster.
    - Explicitly fail on text patterns: 'from broadcaster import' or 'import broadcaster'.
    """
    violations: list[str] = []

    for scan_root in SCAN_ROOTS:
        if not scan_root.exists():
            continue

        for file_path in scan_root.rglob("*.py"):
            resolved = file_path.resolve()
            source = file_path.read_text(encoding="utf-8")

            # Pattern-level guard requested by requirement.
            has_forbidden_pattern = (
                "from broadcaster import" in source
                or "import broadcaster" in source
            )

            # AST-level guard for direct module imports (including qualified imports).
            has_direct_import = _is_direct_broadcaster_import(file_path, source)

            if not (has_forbidden_pattern or has_direct_import):
                continue

            if resolved != ALLOWED_IMPORTER:
                rel = file_path.relative_to(ROOT).as_posix()
                reasons: list[str] = []
                if has_forbidden_pattern:
                    reasons.append("forbidden text pattern")
                if has_direct_import:
                    reasons.append("direct broadcaster import")
                violations.append(f"{rel} ({', '.join(reasons)})")

    assert not violations, (
        "Direct broadcaster bypass detected. Only apps/api/services/canonical_event_router.py "
        "may import broadcaster.\n"
        + "\n".join(f"- {v}" for v in violations)
    )
