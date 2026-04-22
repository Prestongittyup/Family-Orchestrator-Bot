from __future__ import annotations

from typing import Any


class RuntimeSaturationClassifier:
    @staticmethod
    def _num(metrics: dict[str, Any], key: str, default: float = 0.0) -> float:
        value = metrics.get(key, default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @classmethod
    def classify(cls, metrics: dict[str, Any]) -> dict[str, Any]:
        accepted_total = cls._num(metrics, "accepted_total", cls._num(metrics, "ADMISSION_ACCEPTED_COUNT", 0.0))
        rejected_total = cls._num(metrics, "rejected_total", cls._num(metrics, "ADMISSION_REJECTED_COUNT", 0.0))
        completed_total = cls._num(metrics, "completed_total", cls._num(metrics, "COMPLETED_REQUESTS_COUNT", 0.0))
        failed_total = cls._num(metrics, "failed_total", cls._num(metrics, "FAILED_REQUESTS_COUNT", 0.0))
        inflight_current = cls._num(metrics, "inflight_current", cls._num(metrics, "INFLIGHT_CURRENT", 0.0))
        completion_ratio = cls._num(metrics, "completion_ratio", 0.0)
        asgi_entry_received = cls._num(metrics, "ASGI_ENTRY_RECEIVED_COUNT", 0.0)
        client_timeout_count = cls._num(metrics, "CLIENT_TIMEOUT_COUNT", 0.0)
        max_inflight_observed = cls._num(metrics, "MAX_INFLIGHT_OBSERVED", 0.0)
        max_inflight_cap = cls._num(metrics, "MAX_INFLIGHT_CAP", 0.0)

        retry_rate = cls._num(metrics, "retry_rate", -1.0)
        p95_latency = cls._num(
            metrics,
            "p95_latency",
            cls._num(metrics, "p95_latency_ms", cls._num(metrics, "p95_ms", -1.0)),
        )

        # Derived helpers
        saturation_ratio = (inflight_current / max_inflight_cap) if max_inflight_cap > 0 else 0.0
        rejection_share = (rejected_total / asgi_entry_received) if asgi_entry_received > 0 else 0.0
        timeout_share = (client_timeout_count / accepted_total) if accepted_total > 0 else 0.0
        failed_share = (failed_total / accepted_total) if accepted_total > 0 else 0.0

        # A) CLIENT_ARTIFACT
        # Dominant timeout signal with otherwise healthy completion.
        if completion_ratio >= 0.85 and (
            timeout_share >= 0.2
            or (client_timeout_count > failed_total and retry_rate >= 0.1 and accepted_total >= completed_total)
        ):
            return {
                "classification": "CLIENT_ARTIFACT",
                "confidence": 0.9,
                "signals": {
                    "primary_driver": "client_timeout_dominance",
                    "supporting_evidence": [
                        f"completion_ratio={completion_ratio:.3f}",
                        f"client_timeout_count={int(client_timeout_count)}",
                        f"timeout_share={timeout_share:.3f}",
                    ],
                },
            }

        # B) ACCEPT_SATURATION
        # Rejections visible while accepted execution still completes reasonably.
        if (
            rejected_total > 0
            and completion_ratio > 0.7
            and saturation_ratio >= 0.9
            and rejection_share >= 0.15
            and max_inflight_observed >= max_inflight_cap
        ):
            return {
                "classification": "ACCEPT_SATURATION",
                "confidence": 0.9,
                "signals": {
                    "primary_driver": "admission_cap_pressure",
                    "supporting_evidence": [
                        f"rejected_total={int(rejected_total)}",
                        f"rejection_share={rejection_share:.3f}",
                        f"inflight_current={int(inflight_current)}",
                        f"max_inflight_cap={int(max_inflight_cap)}",
                    ],
                },
            }

        # C) EVENT_LOOP_STARVATION
        # Many accepted requests with low completion and growing in-flight pressure.
        if (
            accepted_total >= 50
            and rejected_total <= max(5.0, accepted_total * 0.1)
            and completion_ratio < 0.7
            and inflight_current >= max(5.0, accepted_total * 0.15)
            and (p95_latency < 0 or p95_latency >= 1000.0)
        ):
            return {
                "classification": "EVENT_LOOP_STARVATION",
                "confidence": 0.9,
                "signals": {
                    "primary_driver": "accepted_without_completion",
                    "supporting_evidence": [
                        f"accepted_total={int(accepted_total)}",
                        f"completion_ratio={completion_ratio:.3f}",
                        f"inflight_current={int(inflight_current)}",
                        f"p95_latency={p95_latency:.3f}",
                    ],
                },
            }

        # D) DOWNSTREAM_BOTTLENECK
        # Accepted traffic accumulates with failures/inflight growth and degraded completion.
        if (
            accepted_total >= 20
            and completion_ratio < 0.85
            and (failed_share >= 0.1 or inflight_current >= max(5.0, accepted_total * 0.1))
            and (p95_latency < 0 or p95_latency >= 700.0)
        ):
            return {
                "classification": "DOWNSTREAM_BOTTLENECK",
                "confidence": 0.9,
                "signals": {
                    "primary_driver": "completion_degradation_under_load",
                    "supporting_evidence": [
                        f"accepted_total={int(accepted_total)}",
                        f"failed_total={int(failed_total)}",
                        f"inflight_current={int(inflight_current)}",
                        f"completion_ratio={completion_ratio:.3f}",
                    ],
                },
            }

        # E) STABLE
        if (
            completion_ratio >= 0.9
            and (max_inflight_cap <= 0 or inflight_current <= max_inflight_cap)
            and failed_total <= max(1.0, accepted_total * 0.02)
        ):
            return {
                "classification": "STABLE",
                "confidence": 0.95,
                "signals": {
                    "primary_driver": "healthy_completion",
                    "supporting_evidence": [
                        f"completion_ratio={completion_ratio:.3f}",
                        f"failed_total={int(failed_total)}",
                        f"inflight_current={int(inflight_current)}",
                    ],
                },
            }

        # Deterministic fallback when none of the strict conditions match.
        return {
            "classification": "DOWNSTREAM_BOTTLENECK",
            "confidence": 0.9,
            "signals": {
                "primary_driver": "fallback_non_stable_pattern",
                "supporting_evidence": [
                    f"accepted_total={int(accepted_total)}",
                    f"rejected_total={int(rejected_total)}",
                    f"completion_ratio={completion_ratio:.3f}",
                ],
            },
        }
