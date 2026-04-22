from __future__ import annotations

from typing import Any


class ProductionReadinessClassifier:
    @classmethod
    def classify(cls, report: dict[str, Any]) -> dict[str, Any]:
        soak = report.get("soak_results", {}) if isinstance(report, dict) else {}
        breakpoint_results = report.get("breakpoint_results", {}) if isinstance(report, dict) else {}
        chaos = report.get("chaos_results", {}) if isinstance(report, dict) else {}
        repeatability = report.get("repeatability", {}) if isinstance(report, dict) else {}

        sustained_error_rate = float(soak.get("error_rate", 1.0) or 0.0)
        soak_completion = float(soak.get("completion_ratio", 0.0) or 0.0)
        chaos_completion = float(chaos.get("completion_ratio", soak_completion) or 0.0)
        completion_ratio = min(soak_completion, chaos_completion)
        sse_lag_growth_slope = max(
            float(soak.get("sse_lag_growth_slope", 0.0) or 0.0),
            float(breakpoint_results.get("sse_lag_growth_slope", 0.0) or 0.0),
            float(chaos.get("sse_lag_growth_slope", 0.0) or 0.0),
        )
        timeout_count = float(breakpoint_results.get("timeout_count", 0.0) or 0.0) + float(
            chaos.get("timeout_count", 0.0) or 0.0
        )
        rejection_count = float(breakpoint_results.get("rejections_429", 0.0) or 0.0) + float(
            chaos.get("rejections_429", 0.0) or 0.0
        )
        timeout_imbalance = timeout_count > max(5.0, rejection_count * 1.5)
        inflight_recovery_ratio = min(
            float(soak.get("inflight_recovery_ratio", 1.0) or 0.0),
            float(breakpoint_results.get("inflight_recovery_ratio", 1.0) or 0.0),
            float(chaos.get("inflight_recovery_ratio", 1.0) or 0.0),
        )
        inflight_saturation_without_recovery = inflight_recovery_ratio < 0.35
        memory_growth_slope = max(
            float(soak.get("memory_growth_slope", 0.0) or 0.0),
            float(chaos.get("memory_growth_slope", 0.0) or 0.0),
            float(breakpoint_results.get("memory_growth_slope", 0.0) or 0.0),
        )
        repeatability_failed = str(repeatability.get("status", "FAIL")).upper() != "PASS"
        max_stable_concurrency = int(breakpoint_results.get("max_stable_concurrency", 0) or 0)
        stage_target_peak = int(breakpoint_results.get("target_peak", 0) or 0)

        reasons: list[str] = []
        severity = 0

        if sustained_error_rate > 0.05:
            reasons.append(f"sustained error rate {sustained_error_rate:.3f} exceeds 0.05")
            severity += 2
        if completion_ratio < 0.8:
            reasons.append(f"completion ratio {completion_ratio:.3f} is below 0.8")
            severity += 2
        if sse_lag_growth_slope > 25.0:
            reasons.append(f"SSE lag growth slope {sse_lag_growth_slope:.3f} indicates degradation")
            severity += 1
        if timeout_imbalance:
            reasons.append(
                f"timeout distribution dominates 429 handling (timeouts={int(timeout_count)}, rejections={int(rejection_count)})"
            )
            severity += 1
        if inflight_saturation_without_recovery:
            reasons.append(f"inflight recovery ratio {inflight_recovery_ratio:.3f} indicates saturation without recovery")
            severity += 2
        if memory_growth_slope > 1.0:
            reasons.append(f"memory growth slope {memory_growth_slope:.3f} MB/sample exceeds stability threshold")
            severity += 1
        if repeatability_failed:
            reasons.append("repeatability gate failed")
            severity += 2

        if sustained_error_rate > 0.2 or completion_ratio < 0.5:
            classification = "FAILURE_DOMINATED"
        elif severity >= 5:
            classification = "NOT_READY"
        elif sustained_error_rate > 0.05 or completion_ratio < 0.8 or timeout_imbalance or sse_lag_growth_slope > 25.0:
            classification = "DEGRADING_UNDER_LOAD"
        elif stage_target_peak > 0 and max_stable_concurrency < stage_target_peak:
            reasons.append(
                f"max stable concurrency {max_stable_concurrency} is below target peak {stage_target_peak}"
            )
            classification = "STABLE_BUT_LIMITED"
        else:
            classification = "PRODUCTION_READY"

        if not reasons:
            reasons.append("soak, breakpoint, chaos, and repeatability signals remain within readiness thresholds")

        confidence = min(0.99, 0.55 + (0.08 * max(1, len(reasons))))
        if classification == "PRODUCTION_READY":
            confidence = max(0.75, confidence)

        return {
            "classification": classification,
            "confidence": round(confidence, 3),
            "reasons": reasons,
        }