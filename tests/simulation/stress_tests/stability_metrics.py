from __future__ import annotations

from typing import Any


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 4)


def compute_stability_metrics(evolution: list[dict[str, Any]]) -> dict[str, Any]:
    if not evolution:
        return {
            "decision_drift_score": 0.0,
            "priority_flip_rate": 0.0,
            "brief_instability_index": 0.0,
            "recovery_time_steps": 0,
            "stability_score": 1.0,
        }

    drift_sum = 0
    flip_count = 0

    prev_titles: list[str] | None = None
    stable_streak = 0
    recovery_time = len(evolution)

    for idx, item in enumerate(evolution):
        titles = list(item.get("top_event_titles", []))
        if prev_titles is not None:
            changed = titles != prev_titles
            if changed:
                flip_count += 1
                drift_sum += abs(len(set(titles) - set(prev_titles))) + abs(len(set(prev_titles) - set(titles)))
                stable_streak = 0
            else:
                stable_streak += 1
                if stable_streak >= 2 and recovery_time == len(evolution):
                    recovery_time = idx
        prev_titles = titles

    steps = max(1, len(evolution) - 1)
    decision_drift_score = round(drift_sum / steps, 4)
    priority_flip_rate = _safe_div(flip_count, steps)
    brief_instability_index = round((decision_drift_score * 0.6) + (priority_flip_rate * 0.4), 4)
    stability_score = round(max(0.0, 1.0 - min(1.0, brief_instability_index / 3.0)), 4)

    return {
        "decision_drift_score": decision_drift_score,
        "priority_flip_rate": priority_flip_rate,
        "brief_instability_index": brief_instability_index,
        "recovery_time_steps": recovery_time,
        "stability_score": stability_score,
    }
