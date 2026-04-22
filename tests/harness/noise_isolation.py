from __future__ import annotations

from typing import Any


TRANSPORT_STATUS_CODES = {0, 599}


def _metric_ts(metric: dict[str, Any]) -> float:
    if "ts" in metric:
        return float(metric.get("ts", 0.0))
    return float(metric.get("timestamp", 0.0))


def _is_retry_artifact(metric: dict[str, Any]) -> bool:
    if bool(metric.get("retry_amplified", False)):
        return True
    if int(metric.get("retry_attempt", 0) or 0) > 0:
        return True
    return bool(metric.get("retried", False))


def _is_transport_artifact(metric: dict[str, Any]) -> bool:
    if bool(metric.get("transport_artifact", False)):
        return True
    return int(metric.get("status", 0) or 0) in TRANSPORT_STATUS_CODES


def isolate(metrics: list[dict[str, Any]], warmup_seconds: int = 10) -> list[dict[str, Any]]:
    if not metrics:
        return []

    start_ts = min(_metric_ts(m) for m in metrics)
    clean: list[dict[str, Any]] = []
    for m in metrics:
        ts = _metric_ts(m)

        if ts - start_ts < warmup_seconds:
            continue
        if bool(m.get("cold_start_anomaly", False)):
            continue
        if _is_transport_artifact(m):
            continue
        if _is_retry_artifact(m):
            continue
        clean.append(dict(m))
    return clean


def classify_noise(metrics: list[dict[str, Any]], warmup_seconds: int = 10) -> dict[str, float]:
    total = float(len(metrics))
    if total == 0:
        return {
            "client_noise_ratio": 0.0,
            "warmup_impact": 0.0,
            "retry_inflation_factor": 1.0,
            "cold_start_ratio": 0.0,
            "removed_ratio": 0.0,
        }

    start_ts = min(_metric_ts(m) for m in metrics)
    client_noise = sum(1 for m in metrics if _is_transport_artifact(m))
    warmup_count = sum(1 for m in metrics if _metric_ts(m) - start_ts < warmup_seconds)
    retries = sum(1 for m in metrics if _is_retry_artifact(m))
    cold_start = sum(1 for m in metrics if bool(m.get("cold_start_anomaly", False)))
    clean_total = len(isolate(metrics, warmup_seconds=warmup_seconds))

    client_noise_ratio = client_noise / total
    warmup_impact = warmup_count / total
    retry_inflation_factor = (total + retries) / total
    cold_start_ratio = cold_start / total
    removed_ratio = (total - float(clean_total)) / total

    return {
        "client_noise_ratio": round(client_noise_ratio, 6),
        "warmup_impact": round(warmup_impact, 6),
        "retry_inflation_factor": round(retry_inflation_factor, 6),
        "cold_start_ratio": round(cold_start_ratio, 6),
        "removed_ratio": round(removed_ratio, 6),
    }
