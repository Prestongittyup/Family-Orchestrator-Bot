from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tests.simulation.live_orchestration.brief_tracker import (
    build_brief_evolution,
    evaluate_live_assertions,
)
from tests.simulation.live_orchestration.ingestion_simulator import run_ingestion_sequence
from tests.simulation.live_orchestration.timeline_engine import build_timeline
from tests.simulation.stress_tests.stability_metrics import compute_stability_metrics


def run_live_simulation(
    *,
    seed: int = 42,
    household_size: int = 4,
    chaos_level: str = "medium",
    event_density: int = 18,
    scenario_preset: str = "school_work_balance",
    timeline_override: list[dict[str, Any]] | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    timeline = timeline_override if timeline_override is not None else build_timeline(
        seed=seed,
        household_size=household_size,
        event_density=event_density,
        chaos_level=chaos_level,
        scenario_preset=scenario_preset,
    )

    household_id = f"sim-household-{seed}"
    user_id = f"sim-user-{seed}"

    snapshots = run_ingestion_sequence(
        timeline_events=timeline,
        household_id=household_id,
        user_id=user_id,
    )

    evolution = build_brief_evolution(snapshots)
    assertions = evaluate_live_assertions(snapshots, evolution)
    stability_metrics = compute_stability_metrics(evolution)

    failures = []
    if not assertions.get("priority_stability_under_change", False):
        failures.append("priority_instability")
    if not assertions.get("correct_reordering_of_urgent_events", False):
        failures.append("urgent_reordering_failure")
    if not assertions.get("no_stale_event_persistence", False):
        failures.append("stale_event_persistence")

    result: dict[str, Any] = {
        "simulation_id": f"sim-{seed}-{chaos_level}",
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "config": {
            "seed": seed,
            "household_size": household_size,
            "chaos_level": chaos_level,
            "event_density": event_density,
            "scenario_preset": scenario_preset,
        },
        "event_timeline": timeline,
        "brief_outputs_over_time": [
            {
                "step": snap.get("step"),
                "event_id": snap.get("event", {}).get("event_id"),
                "brief": snap.get("brief"),
            }
            for snap in snapshots
        ],
        "brief_evolution": evolution,
        "decision_drift_metrics": {
            "decision_drift_score": stability_metrics["decision_drift_score"],
            "priority_flip_rate": stability_metrics["priority_flip_rate"],
            "brief_instability_index": stability_metrics["brief_instability_index"],
        },
        "stability_scores": {
            "stability_score": stability_metrics["stability_score"],
        },
        "system_recovery_metrics": {
            "recovery_time_steps": stability_metrics["recovery_time_steps"],
        },
        "failure_patterns": failures,
        "assertions": assertions,
        "system_health": {
            "pipeline_latency_ms": len(timeline) * 3,
            "decision_distribution": {
                "stable": max(0, len(timeline) - assertions.get("priority_flip_count", 0)),
                "changed": assertions.get("priority_flip_count", 0),
            },
            "conflict_frequency": sum(item.get("conflict_count", 0) for item in evolution),
            "stability_trend": [row.get("conflict_count", 0) for row in evolution],
        },
    }

    if persist:
        Path("simulation_results.json").write_text(
            json.dumps(result, indent=2),
            encoding="utf-8",
        )

    return result
