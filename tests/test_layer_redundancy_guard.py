from __future__ import annotations
import pytest

import os
from pathlib import Path

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate]



ROOT = Path(__file__).resolve().parents[1]

CANONICAL_LAYER_DIRS = (
    ROOT / "apps" / "api" / "endpoints",
    ROOT / "apps" / "api" / "conversation_orchestration",
    ROOT / "apps" / "api" / "intent_contract",
    ROOT / "apps" / "api" / "policy_engine",
    ROOT / "apps" / "api" / "integration_core",
    ROOT / "household_os" / "runtime",
)

INTEGRATION_CORE_FACADE_EXPECTED = {
    "__init__.py": "from apps.api.integration_core import *",
    "orchestrator.py": "from apps.api.integration_core.orchestrator import *",
    "providers.py": "from apps.api.integration_core.providers import *",
    "state_builder.py": "from apps.api.integration_core.state_builder import *",
    "brief_builder.py": "from apps.api.integration_core.brief_builder import *",
}


@pytest.mark.integration
def test_canonical_layer_directories_exist() -> None:
    for directory in CANONICAL_LAYER_DIRS:
        assert directory.exists(), f"Missing canonical layer folder: {directory}"
        assert directory.is_dir(), f"Expected folder but found non-directory path: {directory}"


@pytest.mark.integration
def test_integration_core_facade_is_thin_reexport_only() -> None:
    facade_root = ROOT / "integration_core"

    for file_name, expected_import in INTEGRATION_CORE_FACADE_EXPECTED.items():
        file_path = facade_root / file_name
        assert file_path.exists(), f"Missing compatibility facade file: {file_path}"

        lines = [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        assert lines == [expected_import], (
            "Compatibility facade must remain a one-line re-export to avoid duplicate logic. "
            f"Unexpected content in {file_path}: {lines}"
        )


@pytest.mark.integration
def test_integration_core_canonical_files_not_duplicated_in_other_integration_core_folders() -> None:
    canonical_files = {
        "orchestrator.py",
        "providers.py",
        "state_builder.py",
        "brief_builder.py",
    }

    allowed = {
        (ROOT / "apps" / "api" / "integration_core" / name).resolve()
        for name in canonical_files
    } | {
        (ROOT / "integration_core" / name).resolve()
        for name in canonical_files
    }

    skip_dirs = {
        ".git",
        ".venv",
        "venv",
        "__pycache__",
        ".pytest_cache",
        "node_modules",
        "archive",
    }

    duplicates: list[Path] = []

    for current_root, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in skip_dirs]

        root_path = Path(current_root)
        if root_path.name != "integration_core":
            continue

        for file_name in files:
            if file_name not in canonical_files:
                continue
            candidate = (root_path / file_name).resolve()
            if candidate not in allowed:
                duplicates.append(candidate)

    assert not duplicates, (
        "Unexpected integration_core duplicates found outside canonical+facade paths: "
        + ", ".join(str(path.relative_to(ROOT)) for path in sorted(duplicates))
    )