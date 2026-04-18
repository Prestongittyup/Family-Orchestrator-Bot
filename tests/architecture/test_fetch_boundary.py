import inspect
import importlib

import integration_core.orchestrator as orchestrator
import integration_core.state_builder as state_builder


def test_only_state_builder_handles_fetch():
    """Only StateBuilder may interact with external providers."""
    source = inspect.getsource(importlib.import_module("apps.api.integration_core.state_builder"))
    assert "fetch_events" in source, "StateBuilder must be the fetch boundary"


def test_orchestrator_does_not_fetch():
    source = inspect.getsource(importlib.import_module("apps.api.integration_core.orchestrator"))
    assert "fetch_events" not in source


def test_orchestrator_is_pure_coordinator():
    """
    Orchestrator must NOT:
    - access env vars
    - construct providers
    - perform IO
    """
    source = inspect.getsource(orchestrator.Orchestrator)
    forbidden = [
        "os.getenv",
        "provider",
        "GoogleCalendar",
        "fetch_events",
    ]

    for marker in forbidden:
        assert marker not in source
