import inspect
import importlib

import integration_core.orchestrator as orch
import integration_core.state_builder as sb


def test_only_state_builder_has_fetch_events():
    sb_src = inspect.getsource(importlib.import_module("apps.api.integration_core.state_builder"))
    orch_src = inspect.getsource(importlib.import_module("apps.api.integration_core.orchestrator"))

    assert "fetch_events" in sb_src
    assert "fetch_events" not in orch_src
