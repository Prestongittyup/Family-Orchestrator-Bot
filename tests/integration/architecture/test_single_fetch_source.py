import pytest
import inspect
import importlib

import integration_core.orchestrator as orch
import integration_core.state_builder as sb

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate]



@pytest.mark.integration
def test_only_state_builder_has_fetch_events():
    sb_src = inspect.getsource(importlib.import_module("apps.api.integration_core.state_builder"))
    orch_src = inspect.getsource(importlib.import_module("apps.api.integration_core.orchestrator"))

    assert "fetch_events" in sb_src
    assert "fetch_events" not in orch_src