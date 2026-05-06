from __future__ import annotations

import pytest

from archive.apps.api.integration_core.architecture_guard import (

    IntegrationCoreBoundaryViolation,
    assert_allowed_import,
    guarded_import,
)

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



@pytest.mark.unit
@pytest.mark.legacy
def test_forbidden_imports_fail_at_runtime_guard() -> None:
    forbidden = [
        "apps.api.ingestion.service",
        "apps.api.services.decision_engine",
        "apps.api.endpoints.brief_renderer_v1",
    ]

    for module_name in forbidden:
        with pytest.raises(IntegrationCoreBoundaryViolation):
            guarded_import(module_name)


@pytest.mark.unit
@pytest.mark.legacy
def test_allowed_import_passes_runtime_guard() -> None:
    # Should not raise for an integration-core module.
    assert_allowed_import("apps.api.integration_core.providers")


@pytest.mark.unit
@pytest.mark.legacy
def test_forbidden_prefix_check_is_deterministic() -> None:
    module_name = "apps.api.services.decision_engine"

    with pytest.raises(IntegrationCoreBoundaryViolation):
        assert_allowed_import(module_name)

    with pytest.raises(IntegrationCoreBoundaryViolation):
        assert_allowed_import(module_name)
