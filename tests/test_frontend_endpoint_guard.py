from __future__ import annotations

from pathlib import Path

import pytest

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate]


ROOT = Path(__file__).resolve().parents[1]
FRONTEND_SRC = ROOT / "hpal-frontend" / "src"

FORBIDDEN_ENDPOINT_PATTERNS: tuple[str, ...] = (
    "/v1/ui/",
    "/v1/calendar/",
    "/integrations/",
)

SHADOW_GUARD_MARKERS: tuple[str, ...] = (
    "SHADOW_ENDPOINTS_ENABLED = false",
    "SHADOW_INTEGRATION_ENDPOINTS_ENABLED = false",
)

SHADOW_TODO_MARKER = "TODO: REMOVE_SHADOW_ENDPOINT"


def _frontend_sources() -> list[Path]:
    candidates = list(FRONTEND_SRC.rglob("*.ts")) + list(FRONTEND_SRC.rglob("*.tsx"))
    return sorted(path for path in candidates if path.is_file())


def _is_explicitly_gated_shadow_reference(
    source_text: str,
    source_lines: list[str],
    line_index_zero_based: int,
) -> bool:
    if not any(marker in source_text for marker in SHADOW_GUARD_MARKERS):
        return False

    window_start = max(0, line_index_zero_based - 30)
    window_end = min(len(source_lines), line_index_zero_based + 1)
    window = "\n".join(source_lines[window_start:window_end])
    return SHADOW_TODO_MARKER in window


@pytest.mark.integration
def test_frontend_forbidden_endpoints_are_absent_or_explicitly_shadow_gated() -> None:
    assert FRONTEND_SRC.exists(), f"Missing frontend source directory: {FRONTEND_SRC}"

    offenders: list[str] = []
    for file_path in _frontend_sources():
        source_text = file_path.read_text(encoding="utf-8")
        source_lines = source_text.splitlines()

        for index, line in enumerate(source_lines):
            for pattern in FORBIDDEN_ENDPOINT_PATTERNS:
                if pattern not in line:
                    continue

                if _is_explicitly_gated_shadow_reference(source_text, source_lines, index):
                    continue

                relative = file_path.relative_to(ROOT)
                offenders.append(f"{relative}:{index + 1}: {pattern} -> {line.strip()}")

    assert offenders == [], (
        "Frontend contains non-canonical endpoint references without explicit shadow disable guards. "
        "Allowed shadow references must include both a local disabled guard constant and "
        f"a nearby '{SHADOW_TODO_MARKER}' marker.\n"
        + "\n".join(offenders)
    )
