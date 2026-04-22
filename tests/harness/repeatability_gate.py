from __future__ import annotations

from dataclasses import dataclass
import statistics
from typing import Any


@dataclass(frozen=True)
class RepeatabilityConfig:
    n_runs: int = 5
    success_rate_variance_max: float = 0.01
    rejection_rate_variance_max: float = 0.01
    completion_ratio_variance_max: float = 0.02


class RepeatabilityGate:
    @staticmethod
    def _variance(values: list[float]) -> float:
        if len(values) <= 1:
            return 0.0
        return float(statistics.pvariance(values))

    @staticmethod
    def _outliers(values: list[float]) -> list[int]:
        if len(values) <= 2:
            return []
        mean = statistics.fmean(values)
        std = statistics.pstdev(values)
        if std == 0.0:
            return []
        threshold = 2.0 * std
        return [i for i, v in enumerate(values) if abs(v - mean) > threshold]

    @staticmethod
    def _breakdown(values: list[float], threshold: float) -> dict[str, Any]:
        variance = 0.0
        stddev = 0.0
        mean = 0.0
        value_range = 0.0
        if values:
            mean = float(statistics.fmean(values))
            value_range = float(max(values) - min(values)) if len(values) > 1 else 0.0
        if len(values) > 1:
            variance = float(statistics.pvariance(values))
            stddev = float(statistics.pstdev(values))
        return {
            "values": [round(v, 8) for v in values],
            "mean": round(mean, 8),
            "variance": round(variance, 8),
            "stddev": round(stddev, 8),
            "range": round(value_range, 8),
            "threshold": threshold,
            "pass": variance < threshold,
        }

    @classmethod
    def evaluate(cls, runs: list[dict[str, Any]], config: RepeatabilityConfig | None = None) -> dict[str, Any]:
        cfg = config or RepeatabilityConfig()
        if not runs:
            return {
                "status": "FAIL",
                "variance": {},
                "outliers": [],
                "reason": "no_runs_provided",
            }

        success = [float(r.get("success_rate", 0.0)) for r in runs]
        rejection = [float(r.get("rejection_rate", 0.0)) for r in runs]
        completion = [float(r.get("completion_ratio", 0.0)) for r in runs]
        p95 = [float(r.get("p95_latency", 0.0)) for r in runs]

        variance_breakdown = {
            "success_rate": cls._breakdown(success, cfg.success_rate_variance_max),
            "rejection_rate": cls._breakdown(rejection, cfg.rejection_rate_variance_max),
            "completion_ratio": cls._breakdown(completion, cfg.completion_ratio_variance_max),
            "p95_latency": cls._breakdown(p95, float("inf")),
        }
        variance = {name: details["variance"] for name, details in variance_breakdown.items()}

        pass_gate = (
            variance_breakdown["success_rate"]["pass"]
            and variance_breakdown["rejection_rate"]["pass"]
            and variance_breakdown["completion_ratio"]["pass"]
        )

        outlier_idxs = sorted(
            set(cls._outliers(success) + cls._outliers(rejection) + cls._outliers(completion) + cls._outliers(p95))
        )

        return {
            "status": "PASS" if pass_gate else "FAIL",
            "variance": variance,
            "variance_breakdown": variance_breakdown,
            "outliers": outlier_idxs,
            "outlier_run_indices": outlier_idxs,
            "runs_evaluated": len(runs),
        }
