from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api import main
from apps.api.endpoints import brief_endpoint


def test_brief_endpoint_success():
    brief_endpoint._clear_brief_cache()
    client = TestClient(main.app)

    response = client.get("/brief/hh-step6")
    body = response.json()

    assert response.status_code == 200
    assert body["status"] == "success"
    assert "brief" in body
    assert "generated_at" in body


def test_brief_cache_hit_behavior(monkeypatch):
    brief_endpoint._clear_brief_cache()
    client = TestClient(main.app)

    calls = {"count": 0}

    def _fake_build_daily_brief(household_id: str):
        calls["count"] += 1
        return {
            "household_id": household_id,
            "date": "2026-04-14",
            "schedule": [],
            "personal_agendas": {},
            "suggestions": [],
            "financial": {},
            "meals": {},
            "interrupts": [],
            "meta": {"decision_count": 0, "interrupt_count": 0, "suggestion_count": 0},
        }

    monkeypatch.setattr(brief_endpoint, "build_daily_brief", _fake_build_daily_brief)

    first = client.get("/brief/hh-cache")
    second = client.get("/brief/hh-cache")

    assert first.status_code == 200
    assert second.status_code == 200
    assert calls["count"] == 1
    assert first.json()["brief"] == second.json()["brief"]


def test_brief_failure_fallback(monkeypatch):
    brief_endpoint._clear_brief_cache()
    client = TestClient(main.app)

    seed = client.get("/brief/hh-fallback")
    assert seed.status_code == 200
    seed_brief = seed.json()["brief"]

    brief_endpoint._clear_brief_cache(clear_last_known_good=False)

    def _boom(_: str):
        raise RuntimeError("upstream pipeline failed")

    monkeypatch.setattr(brief_endpoint, "build_daily_brief", _boom)

    response = client.get("/brief/hh-fallback")
    body = response.json()

    assert response.status_code == 200
    assert body["status"] == "partial_failure"
    assert "upstream pipeline failed" in body["error"]
    assert body["brief"] == seed_brief


def test_no_recomputation_within_ttl(monkeypatch):
    brief_endpoint._clear_brief_cache()
    client = TestClient(main.app)

    calls = {"count": 0}

    def _fake_build_daily_brief(household_id: str):
        calls["count"] += 1
        return {
            "household_id": household_id,
            "date": "2026-04-14",
            "schedule": [],
            "personal_agendas": {},
            "suggestions": [],
            "financial": {},
            "meals": {},
            "interrupts": [],
            "meta": {"decision_count": 0, "interrupt_count": 0, "suggestion_count": 0},
        }

    monkeypatch.setattr(brief_endpoint, "build_daily_brief", _fake_build_daily_brief)

    for _ in range(4):
        res = client.get("/brief/hh-ttl")
        assert res.status_code == 200

    assert calls["count"] == 1


def test_output_shape_consistency():
    brief_endpoint._clear_brief_cache()
    client = TestClient(main.app)

    response = client.get("/brief/hh-shape")
    body = response.json()

    assert response.status_code == 200
    assert set(body.keys()) == {"status", "brief", "generated_at"}

    brief = body["brief"]
    assert set(brief.keys()) == {
        "household_id",
        "date",
        "schedule",
        "personal_agendas",
        "suggestions",
        "financial",
        "meals",
        "interrupts",
        "meta",
    }
