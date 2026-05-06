from __future__ import annotations
import pytest

import asyncio
from datetime import UTC, datetime, timedelta

from app.services.evaluation.analytics import EvaluationAnalyticsService, classify_system_mode
from app.services.evaluation.comparator import compute_dls

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate]



class _FakeEvaluationStore:
    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records

    async def list_email_comparisons(self, *, limit: int = 500, user_id: str | None = None) -> list[dict[str, object]]:
        if user_id:
            filtered = [record for record in self._records if str(record.get("user_id") or "") == user_id]
            return filtered[:limit]
        return self._records[:limit]


@pytest.mark.integration
def test_compute_dls_matches_spec() -> None:
    rule_output = {
        "priority": "low",
        "needs_attention": False,
        "actions": [],
    }
    llm_output = {
        "priority": "high",
        "needs_attention": True,
        "actions": [
            {"type": "task", "title": "Pay utility bill", "due": None},
            {"type": "reply", "title": "Reply to principal", "due": None},
        ],
    }

    assert compute_dls(rule_output, llm_output) == 5.5


@pytest.mark.integration
def test_compute_dls_applies_false_positive_penalty() -> None:
    rule_output = {
        "priority": "medium",
        "needs_attention": True,
        "actions": [{"type": "task", "title": "Follow up", "due": None}],
    }
    llm_output = {
        "priority": "medium",
        "needs_attention": False,
        "actions": [{"type": "task", "title": "Follow up", "due": None}],
    }

    assert compute_dls(rule_output, llm_output) == -1.5


@pytest.mark.integration
def test_classify_system_mode_thresholds() -> None:
    assert classify_system_mode(avg_dls=0.81, llm_usage=0.3, cost=0.001) == "LLM_CORE"
    assert classify_system_mode(avg_dls=0.4, llm_usage=0.1, cost=0.0) == "HYBRID"
    assert classify_system_mode(avg_dls=0.2, llm_usage=0.9, cost=0.01) == "RULE_ONLY"


@pytest.mark.integration
@pytest.mark.flaky
def test_dashboard_metrics_include_required_tradeoff_fields() -> None:
    now = datetime.now(UTC)
    records = [
        {
            "evaluation_id": "a",
            "user_id": "u1",
            "dls": 1.2,
            "tokens_in": 500,
            "tokens_out": 140,
            "estimated_cost": 0.0021,
            "llm_used": True,
            "timestamp": now.isoformat(),
            "rule_output": {"actions": [], "priority": "low", "needs_attention": False},
            "llm_output": {
                "actions": [{"type": "task", "title": "Plan pickup", "due": None}],
                "priority": "medium",
                "needs_attention": True,
            },
        },
        {
            "evaluation_id": "b",
            "user_id": "u1",
            "dls": -0.4,
            "tokens_in": 480,
            "tokens_out": 90,
            "estimated_cost": 0.0017,
            "llm_used": True,
            "timestamp": (now - timedelta(minutes=2)).isoformat(),
            "rule_output": {
                "actions": [{"type": "task", "title": "Submit form", "due": None}],
                "priority": "medium",
                "needs_attention": True,
            },
            "llm_output": {
                "actions": [{"type": "task", "title": "Submit form", "due": None}],
                "priority": "medium",
                "needs_attention": False,
            },
        },
        {
            "evaluation_id": "c",
            "user_id": "u1",
            "dls": 0.9,
            "tokens_in": 510,
            "tokens_out": 110,
            "estimated_cost": 0.0020,
            "llm_used": True,
            "timestamp": (now - timedelta(minutes=5)).isoformat(),
            "rule_output": {"actions": [], "priority": "low", "needs_attention": False},
            "llm_output": {
                "actions": [{"type": "reply", "title": "Reply to teacher", "due": None}],
                "priority": "medium",
                "needs_attention": True,
            },
        },
    ]

    service = EvaluationAnalyticsService(evaluation_store=_FakeEvaluationStore(records))
    metrics = asyncio.run(service.get_dashboard_metrics(user_id="u1", limit=100))

    assert metrics["total_emails_processed"] == 3
    assert metrics["avg_dls"] > 0
    assert metrics["llm_cost_per_email"] > 0
    assert metrics["system_mode"] == "LLM_CORE"
    assert 0 <= metrics["action_discovery_rate"] <= 1
    assert isinstance(metrics["alerts"], list)

    realtime = asyncio.run(service.get_realtime_snapshot(user_id="u1", limit=100))
    assert "avg_dls" in realtime
    assert "cost_per_hour" in realtime
    assert "llm_calls_per_minute" in realtime
    assert realtime["system_mode"] in {"RULE_ONLY", "HYBRID", "LLM_CORE"}