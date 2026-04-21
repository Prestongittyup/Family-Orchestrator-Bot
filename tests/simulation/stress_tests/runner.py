from __future__ import annotations

from typing import Any

from tests.simulation.live_orchestration.engine import run_live_simulation
from tests.simulation.stress_tests.mutation_engine import mutate_timeline
from tests.simulation.stress_tests.stability_metrics import compute_stability_metrics


def run_stress_scenarios(seed: int = 42) -> dict[str, Any]:
    configs = [
        {"name": "low_noise", "chaos_level": "low", "event_density": 12},
        {"name": "moderate_chaos", "chaos_level": "medium", "event_density": 20},
        {"name": "high_chaos", "chaos_level": "high", "event_density": 30},
    ]

    scenarios: list[dict[str, Any]] = []

    for idx, cfg in enumerate(configs):
        base = run_live_simulation(
            seed=seed + idx,
            household_size=4,
            chaos_level=cfg["chaos_level"],
            event_density=cfg["event_density"],
            scenario_preset=cfg["name"],
            persist=False,
        )

        mutated_timeline = mutate_timeline(
            timeline_events=base.get("event_timeline", []),
            seed=seed + idx,
            chaos_level=cfg["chaos_level"],
        )

        rerun = run_live_simulation(
            seed=seed + idx,
            household_size=4,
            chaos_level=cfg["chaos_level"],
            event_density=len(mutated_timeline),
            scenario_preset=cfg["name"],
            timeline_override=mutated_timeline,
            persist=False,
        )

        metrics = compute_stability_metrics(rerun.get("brief_evolution", []))

        scenarios.append(
            {
                "scenario": cfg["name"],
                "chaos_level": cfg["chaos_level"],
                "event_count": len(mutated_timeline),
                "metrics": metrics,
                "assertions": rerun.get("assertions", {}),
            }
        )

    return {
        "seed": seed,
        "stress_scenarios": scenarios,
    }
