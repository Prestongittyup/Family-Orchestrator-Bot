"""Application service for system diagnostics and runtime health snapshots."""

from __future__ import annotations

from datetime import datetime, timezone

from apps.api.core.asgi_admission import get_runtime_metrics_snapshot
from apps.api.core.boot_diagnostics import run_boot_probe
from apps.api.core.runtime_classifier import RuntimeSaturationClassifier
from apps.api.realtime.broadcaster import broadcaster
from apps.api.runtime.backpressure_controller import backpressure
from apps.api.runtime.event_loop_guard import event_loop_guard
from apps.api.runtime.execution_fairness import fairness_gate
from apps.api.runtime.sse_pressure_guard import sse_guard


class SystemDiagnosticsService:
    def get_boot_status(self) -> dict:
        probe = run_boot_probe()
        probe["checked_at"] = datetime.now(timezone.utc).isoformat()
        return probe

    def get_boot_probe(self) -> dict:
        probe = run_boot_probe()
        probe["checked_at"] = datetime.now(timezone.utc).isoformat()
        return probe

    def get_health(self) -> dict:
        probe = run_boot_probe()
        if probe.get("overall") == "ok":
            return {"status": "healthy"}
        return {"status": "unhealthy", "issues": probe}

    def get_runtime_metrics(self) -> dict:
        metrics = get_runtime_metrics_snapshot()
        enriched = dict(metrics)
        enriched["runtime_classification"] = RuntimeSaturationClassifier.classify(enriched)
        enriched.update(sse_guard.snapshot())
        enriched.update(fairness_gate.snapshot())
        enriched.update(backpressure.snapshot())
        enriched.update(event_loop_guard.snapshot())
        enriched.update(broadcaster.diagnostics_snapshot())
        return enriched
