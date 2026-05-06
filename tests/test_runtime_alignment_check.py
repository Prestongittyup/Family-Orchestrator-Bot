from __future__ import annotations

from pathlib import Path

import pytest

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate]


ROOT = Path(__file__).resolve().parents[1]
CANONICAL_TARGET = "app.main:app"


@pytest.mark.integration
def test_runtime_and_containers_target_canonical_app_main() -> None:
    root_main = (ROOT / "main.py").read_text(encoding="utf-8")
    docker_backend = (ROOT / "Dockerfile.backend").read_text(encoding="utf-8")
    docker_intelligence = (ROOT / "Dockerfile.intelligence").read_text(encoding="utf-8")

    assert 'CANONICAL_ASGI_TARGET = "app.main:app"' in root_main
    assert 'CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]' in docker_backend
    assert 'CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]' in docker_intelligence


@pytest.mark.integration
def test_app_main_router_mounts_stay_on_canonical_surface() -> None:
    app_main = (ROOT / "app" / "main.py").read_text(encoding="utf-8")

    required_router_mounts = (
        "app.include_router(command_router)",
        "app.include_router(ingest_router)",
        "app.include_router(tasks_router)",
        "app.include_router(schedule_router)",
        "app.include_router(reminders_router)",
        "app.include_router(notifications_router)",
    )
    missing_mounts = [mount for mount in required_router_mounts if mount not in app_main]
    assert missing_mounts == [], f"Canonical router mounts missing from app.main: {missing_mounts}"

    forbidden_shadow_router_fragments = (
        "integrations_router",
        "app.include_router(integrations",
        "from app.api.integrations",
        "from app.api.v1",
        "/v1/ui",
    )
    present_shadow_fragments = [fragment for fragment in forbidden_shadow_router_fragments if fragment in app_main]
    assert present_shadow_fragments == [], (
        "app.main must remain free of shadow router mounts and deprecated v1 ui paths. "
        f"Found fragments: {present_shadow_fragments}"
    )
