from __future__ import annotations

import pytest

from apps.api.integration_core.architecture_guard import (
    IntegrationCoreBoundaryViolation,
    assert_allowed_import,
    guarded_import,
)


def test_forbidden_imports_fail_at_runtime_guard() -> None:
    forbidden = [
        "apps.api.ingestion.service",
        "apps.api.services.decision_engine",
        "apps.api.endpoints.brief_renderer_v1",
    ]

    for module_name in forbidden:
        with pytest.raises(IntegrationCoreBoundaryViolation):
            guarded_import(module_name)


def test_allowed_import_passes_runtime_guard() -> None:
    # Should not raise for an integration-core module.
    assert_allowed_import("apps.api.integration_core.providers")


def test_forbidden_prefix_check_is_deterministic() -> None:
    module_name = "apps.api.services.decision_engine"

    with pytest.raises(IntegrationCoreBoundaryViolation):
        assert_allowed_import(module_name)

    with pytest.raises(IntegrationCoreBoundaryViolation):
        assert_allowed_import(module_name)

