"""
System diagnostics and health check endpoints.

Exposes boot status, health checks, and system information for monitoring
and debugging purposes.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from apps.api.core.boot_diagnostics import run_boot_probe
from apps.api.core.asgi_admission import get_runtime_metrics_snapshot
from apps.api.core.runtime_classifier import RuntimeSaturationClassifier
from apps.api.runtime.sse_pressure_guard import sse_guard
from apps.api.runtime.execution_fairness import fairness_gate
from apps.api.runtime.backpressure_controller import backpressure
from apps.api.runtime.event_loop_guard import event_loop_guard
from apps.api.realtime.broadcaster import broadcaster


router = APIRouter(prefix="/v1/system", tags=["system"])


@router.get("/boot-status")
def get_boot_status() -> dict:
    """
    Get current boot diagnostics status.
    
    Returns status of all critical components:
    - database: DB connectivity and table existence
    - identity_repo: Identity repository instantiation
    - household_repo: Household repository operations
    - token_service: JWT token service
    - auth_middleware: Bearer token validation
    - broadcaster: Event broadcaster
    
    All must be "ok" for the system to be operational.
    """
    probe = run_boot_probe()
    probe["checked_at"] = datetime.now(timezone.utc).isoformat()
    return probe


@router.get("/boot-probe")
def get_boot_probe() -> dict:
    """Live externalized boot probe with fresh DB/auth/repository/SSE checks."""
    probe = run_boot_probe()
    probe["checked_at"] = datetime.now(timezone.utc).isoformat()
    return probe


@router.get("/health")
def get_health() -> dict:
    """
    Quick health check (no diagnostics).
    
    Returns 200 if app is responsive, 503 if any critical component is down.
    """
    probe = run_boot_probe()
    if probe.get("overall") == "ok":
        return {"status": "healthy"}
    else:
        return {"status": "unhealthy", "issues": probe}


@router.get("/runtime-metrics")
def get_runtime_metrics() -> dict:
    """Runtime edge-admission counters for transport vs app-layer diagnosis."""
    metrics = get_runtime_metrics_snapshot()
    enriched = dict(metrics)
    enriched["runtime_classification"] = RuntimeSaturationClassifier.classify(enriched)
    enriched.update(sse_guard.snapshot())
    enriched.update(fairness_gate.snapshot())
    enriched.update(backpressure.snapshot())
    enriched.update(event_loop_guard.snapshot())
    enriched.update(broadcaster.diagnostics_snapshot())
    return enriched
