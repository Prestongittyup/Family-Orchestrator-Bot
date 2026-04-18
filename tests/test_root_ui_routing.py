from __future__ import annotations

from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from apps.api import main


def test_only_one_root_route_exists() -> None:
    root_routes = [route for route in main.app.routes if getattr(route, "path", None) == "/"]
    assert len(root_routes) == 1
    assert isinstance(root_routes[0], APIRoute)
    assert root_routes[0].name == "ui_home"


def test_root_renders_integration_control_panel() -> None:
    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)
    response = client.get("/")
    assert response.status_code == 200
    assert "Integration Control Panel" in response.text
    assert "Connect Google Calendar" in response.text
    assert "View Calendar Debug Data" in response.text
    assert "Refresh Brief" in response.text
