from __future__ import annotations

from copy import deepcopy

from fastapi import FastAPI
from fastapi.testclient import TestClient

import archive.apps.api.endpoints.pantry_router as pantry_router
from archive.apps.api.hpal.projection_builder import ProjectionBuilder


class _InMemoryPantryAdapter:
    def __init__(self, graph: dict):
        self.graph = deepcopy(graph)

    def load_graph(self, family_id: str) -> dict:
        if family_id != self.graph.get("household_id"):
            raise ValueError("family not found")
        return deepcopy(self.graph)

    def save_hpal_state(
        self,
        *,
        family_id: str,
        graph: dict,
        expected_state_version: int | None = None,
    ) -> dict:
        if family_id != self.graph.get("household_id"):
            raise ValueError("family not found")
        current_version = int(self.graph.get("state_version", 0))
        if expected_state_version is not None and expected_state_version != current_version:
            raise ValueError("concurrent state update detected")

        out = deepcopy(graph)
        out["state_version"] = current_version + 1
        self.graph = deepcopy(out)
        return deepcopy(out)


def _base_graph() -> dict:
    return {
        "household_id": "family-pantry-1",
        "state_version": 7,
        "grocery_inventory": {
            "eggs": 4,
            "spinach": 2,
            "salmon": 1,
            "brown rice": 1,
            "broccoli": 1,
            "olive oil": 1,
        },
        "meal_history": [],
        "event_history": [],
    }


def _build_test_client() -> TestClient:
    test_app = FastAPI()
    test_app.include_router(pantry_router.router)
    return TestClient(test_app, raise_server_exceptions=True, follow_redirects=False)


def test_adjust_inventory_updates_counts(monkeypatch) -> None:
    adapter = _InMemoryPantryAdapter(_base_graph())
    monkeypatch.setattr(pantry_router, "_adapter", adapter)

    payload = {
        "updates": [
            {"item": "eggs", "delta": 2},
            {"item": "spinach", "delta": -1},
            {"item": "milk", "delta": 1},
        ],
        "note": "manual correction",
    }

    with _build_test_client() as client:
        response = client.post("/v1/pantry/family-pantry-1/adjust", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "updated"
    assert body["inventory"]["eggs"] == 6
    assert body["inventory"]["spinach"] == 1
    assert body["inventory"]["milk"] == 1


def test_adjust_inventory_supports_units_and_decimal_quantities(monkeypatch) -> None:
    adapter = _InMemoryPantryAdapter(_base_graph())
    monkeypatch.setattr(pantry_router, "_adapter", adapter)

    payload = {
        "updates": [
            {"item": "shrimp", "delta": 1.5, "unit": "lb"},
            {"item": "olive oil", "delta": 12, "unit": "fl oz"},
        ],
        "note": "manual inventory add with units",
    }

    with _build_test_client() as client:
        response = client.post("/v1/pantry/family-pantry-1/adjust", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["inventory"]["shrimp"] == 1.5
    assert body["inventory"]["olive oil"] == 13

    stored_inventory = adapter.graph["inventory"]
    assert stored_inventory["shrimp"]["quantity"] == 1.5
    assert stored_inventory["shrimp"]["unit"] == "lb"
    assert stored_inventory["olive oil"]["unit"] == "fl_oz"


def test_cook_recipe_decrements_inventory_and_records_history(monkeypatch) -> None:
    adapter = _InMemoryPantryAdapter(_base_graph())
    monkeypatch.setattr(pantry_router, "_adapter", adapter)

    with _build_test_client() as client:
        response = client.post(
            "/v1/pantry/family-pantry-1/cook",
            json={
                "recipe_name": "Salmon Rice Plate",
                "servings": 1,
                "consumed_at": "2026-04-27",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "updated"
    assert body["inventory"]["salmon"] == 0
    assert body["inventory"]["brown rice"] == 0
    assert body["inventory"]["broccoli"] == 0
    assert body["inventory"]["olive oil"] == 0

    assert adapter.graph["meal_history"][-1]["recipe_name"] == "Salmon Rice Plate"
    assert adapter.graph["meal_history"][-1]["served_on"] == "2026-04-27"


def test_receipt_ingestion_detects_and_applies_text_items(monkeypatch) -> None:
    adapter = _InMemoryPantryAdapter(_base_graph())
    monkeypatch.setattr(pantry_router, "_adapter", adapter)

    files = {
        "file": (
            "receipt.txt",
            "Eggs x2\nSpinach 1\nTOTAL 8.42\n",
            "text/plain",
        )
    }

    with _build_test_client() as client:
        dry_run = client.post(
            "/v1/pantry/family-pantry-1/ingest-receipt",
            files=files,
            data={"dry_run": "true"},
        )
        applied = client.post(
            "/v1/pantry/family-pantry-1/ingest-receipt",
            files=files,
            data={"dry_run": "false"},
        )

    assert dry_run.status_code == 200
    dry_body = dry_run.json()
    assert dry_body["status"] == "dry_run"
    assert {row["item"] for row in dry_body["detected_items"]} == {"eggs", "spinach"}

    assert applied.status_code == 200
    apply_body = applied.json()
    assert apply_body["status"] == "applied"
    assert apply_body["inventory"]["eggs"] == 6
    assert apply_body["inventory"]["spinach"] == 3


def test_adjust_inventory_refreshes_projection_watermark(monkeypatch) -> None:
    adapter = _InMemoryPantryAdapter(_base_graph())
    monkeypatch.setattr(pantry_router, "_adapter", adapter)

    with _build_test_client() as client:
        response = client.post(
            "/v1/pantry/family-pantry-1/adjust",
            json={
                "updates": [{"item": "eggs", "delta": 1}],
                "note": "watermark-check",
            },
        )

    assert response.status_code == 200

    graph = adapter.graph
    watermark = graph.get("hpal", {}).get("projection_watermark", {})
    assert isinstance(watermark, dict)
    assert int(watermark.get("event_count", -1)) == len(graph.get("event_history", []))
    assert int(watermark.get("transition_count", -1)) == len(
        graph.get("action_lifecycle", {}).get("transition_log", [])
    )

    family = ProjectionBuilder().build_family(family_id="family-pantry-1", graph=graph)
    assert family.system_state_summary["stale_projection"] is False
