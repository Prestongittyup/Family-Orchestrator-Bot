from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from app.services.evaluation.store import EvaluationStore


def classify_system_mode(avg_dls: float, llm_usage: float, cost: float) -> str:
    if avg_dls >= 0.8 and llm_usage > 0.25:
        return "LLM_CORE"

    # High-adoption, positive-lift usage can still be considered LLM core
    # even when average lift is below the strict 0.8 threshold.
    if avg_dls >= 0.5 and llm_usage >= 0.9 and cost <= 0.003:
        return "LLM_CORE"

    if avg_dls >= 0.3:
        return "HYBRID"

    return "RULE_ONLY"


class EvaluationAnalyticsService:
    def __init__(self, *, evaluation_store: EvaluationStore) -> None:
        self._store = evaluation_store

    async def get_dashboard_metrics(self, *, user_id: str | None = None, limit: int = 500) -> dict[str, Any]:
        records = await self._store.list_email_comparisons(limit=limit, user_id=user_id)
        return self._build_metrics(records)

    async def get_realtime_snapshot(self, *, user_id: str | None = None, limit: int = 500) -> dict[str, Any]:
        records = await self._store.list_email_comparisons(limit=limit, user_id=user_id)
        metrics = self._build_metrics(records)

        now = datetime.now(UTC)
        one_hour_ago = now - timedelta(hours=1)
        one_minute_ago = now - timedelta(minutes=1)

        cost_per_hour = 0.0
        llm_calls_per_minute = 0
        for record in records:
            timestamp = _parse_timestamp(record.get("timestamp"))
            if timestamp is None:
                continue
            if timestamp >= one_hour_ago:
                cost_per_hour += _record_cost(record)
            if timestamp >= one_minute_ago and _record_llm_used(record):
                llm_calls_per_minute += 1

        return {
            "timestamp": now.isoformat(),
            "avg_dls": metrics["avg_dls"],
            "cost_per_hour": round(cost_per_hour, 6),
            "llm_calls_per_minute": float(llm_calls_per_minute),
            "system_mode": metrics["system_mode"],
            "efficiency_score": metrics["efficiency_score"],
            "alerts": metrics["alerts"],
        }

    def _build_metrics(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        total = len(records)
        if total == 0:
            return {
                "avg_dls": 0.0,
                "positive_lift_ratio": 0.0,
                "negative_lift_ratio": 0.0,
                "llm_cost_per_email": 0.0,
                "rule_only_cost": 0.0,
                "llm_usage_percentage": 0.0,
                "total_emails_processed": 0,
                "rolling_avg_dls": 0.0,
                "rolling_cost_per_email": 0.0,
                "lift_to_cost": 0.0,
                "efficiency_score": 0.0,
                "action_discovery_rate": 0.0,
                "system_mode": "RULE_ONLY",
                "alerts": ["No evaluation data yet"],
            }

        dls_values = [_record_dls(record) for record in records]
        total_cost = sum(_record_cost(record) for record in records)

        llm_used_count = sum(1 for record in records if _record_llm_used(record))
        positive_lift_count = sum(1 for value in dls_values if value > 0)
        negative_lift_count = sum(1 for value in dls_values if value < 0)
        action_discovery_count = sum(1 for record in records if _record_action_discovery(record))

        avg_dls = _round(sum(dls_values) / max(total, 1))
        llm_cost_per_email = _round(total_cost / max(total, 1), places=6)
        llm_usage_percentage = _round(llm_used_count / max(total, 1))

        rolling_window = records[:50]
        rolling_avg_dls = _round(
            sum(_record_dls(record) for record in rolling_window) / max(len(rolling_window), 1)
        )
        rolling_cost_per_email = _round(
            sum(_record_cost(record) for record in rolling_window) / max(len(rolling_window), 1),
            places=6,
        )

        lift_to_cost = _round(avg_dls / max(llm_cost_per_email, 0.0001), places=6)
        efficiency_score = lift_to_cost

        system_mode = classify_system_mode(
            avg_dls=avg_dls,
            llm_usage=llm_usage_percentage,
            cost=llm_cost_per_email,
        )

        alerts = self._build_alerts(records, avg_dls=avg_dls)

        return {
            "avg_dls": avg_dls,
            "positive_lift_ratio": _round(positive_lift_count / max(total, 1)),
            "negative_lift_ratio": _round(negative_lift_count / max(total, 1)),
            "llm_cost_per_email": llm_cost_per_email,
            "rule_only_cost": 0.0,
            "llm_usage_percentage": llm_usage_percentage,
            "total_emails_processed": total,
            "rolling_avg_dls": rolling_avg_dls,
            "rolling_cost_per_email": rolling_cost_per_email,
            "lift_to_cost": lift_to_cost,
            "efficiency_score": efficiency_score,
            "action_discovery_rate": _round(action_discovery_count / max(total, 1)),
            "system_mode": system_mode,
            "alerts": alerts,
        }

    def _build_alerts(self, records: list[dict[str, Any]], *, avg_dls: float) -> list[str]:
        alerts: list[str] = []

        if avg_dls < 0.3:
            alerts.append("LLM not worth using")

        recent, previous = _split_recent_windows(records, window=25)
        if recent and previous:
            recent_cost = _avg(_record_cost(record) for record in recent)
            previous_cost = _avg(_record_cost(record) for record in previous)
            recent_dls = _avg(_record_dls(record) for record in recent)
            previous_dls = _avg(_record_dls(record) for record in previous)

            if avg_dls > 0.8 and recent_cost > previous_cost * 1.1:
                alerts.append("Optimize usage caps")

            if recent_cost > previous_cost * 1.25 and recent_dls <= previous_dls + 0.05:
                alerts.append("LLM waste detected")

        if not alerts:
            alerts.append("LLM operating within expected value/cost range")

        return alerts


def _record_dls(record: dict[str, Any]) -> float:
    raw = record.get("dls", record.get("delta_score", 0.0))
    return _as_float(raw)


def _record_cost(record: dict[str, Any]) -> float:
    return max(0.0, _as_float(record.get("estimated_cost", 0.0)))


def _record_llm_used(record: dict[str, Any]) -> bool:
    if isinstance(record.get("llm_used"), bool):
        return bool(record.get("llm_used"))

    tokens_in = int(_as_float(record.get("tokens_in", 0)))
    tokens_out = int(_as_float(record.get("tokens_out", 0)))
    return tokens_in > 0 or tokens_out > 0


def _record_action_discovery(record: dict[str, Any]) -> bool:
    rule_output = record.get("rule_output")
    llm_output = record.get("llm_output")

    if not isinstance(rule_output, dict) or not isinstance(llm_output, dict):
        return False

    rule_actions = rule_output.get("actions")
    llm_actions = llm_output.get("actions")

    rule_count = len(rule_actions) if isinstance(rule_actions, list) else 0
    llm_count = len(llm_actions) if isinstance(llm_actions, list) else 0
    return llm_count > rule_count


def _parse_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _as_float(value: Any) -> float:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return 0.0


def _round(value: float, *, places: int = 4) -> float:
    return round(float(value), places)


def _avg(values: Any) -> float:
    collected = [float(item) for item in values]
    if not collected:
        return 0.0
    return sum(collected) / len(collected)


def _split_recent_windows(records: list[dict[str, Any]], *, window: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if window <= 0:
        return [], []

    recent = records[:window]
    previous = records[window : window * 2]
    return recent, previous
