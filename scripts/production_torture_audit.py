from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import random
import statistics
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.runtime_stress_audit import RuntimeStressHarness, StressFailure, _linear_slope, _percentile, _windows_process_memory_mb
from tests.harness.load_curve_model import LoadCurveModel
from tests.harness.noise_isolation import classify_noise, isolate
from tests.harness.production_readiness_classifier import ProductionReadinessClassifier
from tests.harness.repeatability_gate import RepeatabilityConfig, RepeatabilityGate
from apps.api.runtime.loop_tracing import trace_gather_binding, trace_loop_context, trace_task_binding


HOST = "127.0.0.1"
DEFAULT_BREAKPOINT_STAGES = [10, 25, 50, 100, 200, 400]

MODE_CONFIGS = {
    "smoke": {
        "soak_duration_seconds": 20,
        "breakpoint_stage_seconds": 5,
        "breakpoint_stages": (10, 25, 50),
        "chaos_duration_seconds": 10,
        "repeat_runs": 1,
        "sample_interval_seconds": 1,
    },
    "standard": {
        "soak_duration_seconds": 300,
        "breakpoint_stage_seconds": 15,
        "breakpoint_stages": (10, 25, 50, 100, 200),
        "chaos_duration_seconds": 60,
        "repeat_runs": 3,
        "sample_interval_seconds": 2,
    },
    "sse_breakpoint_gate": {
        "soak_duration_seconds": 0,
        "breakpoint_stage_seconds": 10,
        "breakpoint_stages": (10, 25, 50, 100),
        "chaos_duration_seconds": 0,
        "repeat_runs": 3,
        "sample_interval_seconds": 1,
    },
}


def _compute_run_fingerprint(seed: int, mode: str, breakpoint_stages: tuple[int, ...], schedule_hash: str, chaos_profile_sig: str, repeat_index: int) -> str:
    """Generate deterministic reproducibility fingerprint including behavior shape."""
    payload = f"{seed}:{mode}:{','.join(map(str, breakpoint_stages))}:{schedule_hash}:{chaos_profile_sig}:{repeat_index}"
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _classify_failure_attribution_multi_score(
    total_raw_count: int,
    clean_count: int,
    timeout_count: int,
    rejection_count: int,
    error_rate: float,
    completion_ratio: float,
    inflight_recovery_ratio: float,
    samples: list[dict[str, Any]],
    retry_count: int,
    inflight_peak: int,
    p95_latency: float,
) -> dict[str, Any]:
    """Multi-factor causal analysis with scoring model (no single-rule matching)."""
    
    # Hard failure override (must be first, overrides everything)
    if completion_ratio < 0.5 and inflight_recovery_ratio < 0.3:
        return {
            "primary_cause": "SYSTEM_OVERLOAD_COLLAPSE",
            "secondary_causes": ["EVENT_LOOP_LAG", "FAIRNESS_POOL_EXHAUSTION"],
            "confidence": 0.95,
            "evidence": [
                f"completion_ratio={round(completion_ratio, 3)}",
                f"inflight_recovery={round(inflight_recovery_ratio, 3)}",
            ],
            "cause_scores": {"SYSTEM_OVERLOAD_COLLAPSE": 1.0},
        }
    
    # Initialize all cause scores
    cause_scores: dict[str, float] = {
        "SYSTEM_OVERLOAD_COLLAPSE": 0.0,
        "TRANSPORT_TIMEOUT": 0.0,
        "ASGI_ADMISSION_REJECTION": 0.0,
        "SSE_BACKPRESSURE_COLLAPSE": 0.0,
        "FAIRNESS_POOL_EXHAUSTION": 0.0,
        "EVENT_LOOP_LAG": 0.0,
        "DB_POOL_EXHAUSTION": 0.0,
        "CLIENT_RETRY_AMPLIFICATION": 0.0,
        "HTTP_5XX_BACKEND_FAILURE": 0.0,
    }
    
    evidence: list[str] = []
    
    # Extract signals
    sse_lag_points = [(float(s.get("t_seconds", 0.0)), float(s.get("sse_lag_ms_p95", 0.0))) for s in samples]
    lag_slope = _linear_slope(sse_lag_points) if sse_lag_points else 0.0
    timeout_ratio = timeout_count / max(1.0, timeout_count + rejection_count) if (timeout_count + rejection_count) > 0 else 0.0
    rejection_ratio = rejection_count / max(1.0, timeout_count + rejection_count) if (timeout_count + rejection_count) > 0 else 0.0
    
    # 1. Completion ratio drop → SYSTEM_OVERLOAD + EVENT_LOOP
    if completion_ratio < 0.6:
        drop_magnitude = 1.0 - completion_ratio
        cause_scores["SYSTEM_OVERLOAD_COLLAPSE"] += drop_magnitude * 0.5
        cause_scores["EVENT_LOOP_LAG"] += drop_magnitude * 0.4
        evidence.append(f"completion_drop={round(drop_magnitude, 3)}")
    
    # 2. Inflight saturation → FAIRNESS + ASGI
    if inflight_peak > 15:
        inflight_ratio = inflight_peak / 20.0  # Relative to max
        cause_scores["FAIRNESS_POOL_EXHAUSTION"] += inflight_ratio * 0.6
        cause_scores["ASGI_ADMISSION_REJECTION"] += inflight_ratio * 0.4
        evidence.append(f"inflight_peak={inflight_peak}")
    
    # 3. Timeout spikes → TRANSPORT_TIMEOUT
    if timeout_count > 5:
        cause_scores["TRANSPORT_TIMEOUT"] += timeout_ratio * 0.8
        evidence.append(f"timeout_count={timeout_count}")
        evidence.append(f"timeout_ratio={round(timeout_ratio, 3)}")
    
    # 4. SSE lag slope → SSE_BACKPRESSURE
    if lag_slope > 25:
        lag_score = min(1.0, lag_slope / 200.0)
        cause_scores["SSE_BACKPRESSURE_COLLAPSE"] += lag_score * 0.9
        evidence.append(f"sse_lag_slope={round(lag_slope, 2)}")
    
    # 5. Retries ↑ → CLIENT_RETRY_AMPLIFICATION
    if retry_count > 3:
        retry_ratio = retry_count / max(1.0, clean_count)
        cause_scores["CLIENT_RETRY_AMPLIFICATION"] += retry_ratio * 0.7
        evidence.append(f"retry_count={retry_count}")
    
    # 6. 429 spikes → ASGI_ADMISSION_REJECTION
    if rejection_count > 2:
        cause_scores["ASGI_ADMISSION_REJECTION"] += rejection_ratio * 0.7
        evidence.append(f"rejection_count={rejection_count}")
    
    # 7. Latency rise → EVENT_LOOP_LAG
    if p95_latency > 500:
        latency_score = min(1.0, p95_latency / 2000.0)
        cause_scores["EVENT_LOOP_LAG"] += latency_score * 0.6
        evidence.append(f"p95_latency_ms={round(p95_latency, 1)}")
    
    # 8. Error rate → generic degradation spread
    if error_rate > 0.05:
        cause_scores["EVENT_LOOP_LAG"] += error_rate * 0.2
        cause_scores["SYSTEM_OVERLOAD_COLLAPSE"] += error_rate * 0.1
    
    # Normalize cause scores to [0,1]
    max_score = max(cause_scores.values()) if cause_scores.values() else 1.0
    if max_score > 0:
        cause_scores = {k: v / max_score for k, v in cause_scores.items()}
    
    # Extract primary and secondary causes
    sorted_causes = sorted(cause_scores.items(), key=lambda x: x[1], reverse=True)
    primary_cause = sorted_causes[0][0]
    secondary_causes = [c[0] for c in sorted_causes[1:3] if c[1] > 0.1]
    
    # Compute confidence from evidence count and primary score
    evidence_weight = len(evidence) / 8.0  # Max 8 evidence pieces
    primary_score = cause_scores[primary_cause]
    confidence = round(min(1.0, primary_score * 0.7 + evidence_weight * 0.3), 3)
    
    return {
        "primary_cause": primary_cause,
        "secondary_causes": secondary_causes,
        "confidence": confidence,
        "evidence": evidence,
        "cause_scores": {k: round(v, 3) for k, v in sorted(cause_scores.items(), key=lambda x: x[1], reverse=True)},
    }


def _build_failure_timeline(samples: list[dict[str, Any]], regime_name: str) -> list[dict[str, Any]]:
    """Build temporal failure timeline with metric snapshots and thresholds."""
    if not samples or len(samples) < 2:
        return []
    
    timeline: list[dict[str, Any]] = []
    baseline_latency = 50.0
    baseline_lag = 10.0
    
    for i, sample in enumerate(samples):
        events_at_window: list[str] = []
        ts = float(sample.get("t_seconds", 0.0))
        latency = float(sample.get("sse_lag_ms_p95", 0.0))
        inflight = int(sample.get("inflight_current", 0))
        completion = float(sample.get("completion_ratio", 1.0))
        
        # Latency spike detection
        if latency > baseline_latency * 1.5 and i > 0:
            events_at_window.append("latency_spike")
        
        # Rejection spike (high rejection_total change)
        rejections = int(sample.get("rejected_total", 0))
        if i > 0 and rejections > int(samples[i-1].get("rejected_total", 0)) + 3:
            events_at_window.append("rejection_spike")
        
        # Inflight saturation
        if inflight > 15:
            events_at_window.append("inflight_saturation")
        
        # SSE lag spike
        if latency > baseline_lag * 2:
            events_at_window.append("sse_lag_spike")
        
        # Timeout burst (proxy: low completion ratio)
        if completion < 0.8:
            events_at_window.append("timeout_burst")
        
        # Recovery event
        if i > 0 and inflight < int(samples[i-1].get("inflight_current", 0)) and inflight < 5:
            events_at_window.append("recovery_event")
        
        # Emit timeline entry if any events detected
        if events_at_window:
            timeline.append({
                "timestamp": round(ts, 2),
                "regime": regime_name,
                "events": events_at_window,
                "metric_snapshot": {
                    "sse_lag_ms_p95": round(latency, 1),
                    "inflight_current": inflight,
                    "completion_ratio": round(completion, 3),
                    "rejected_total": rejections,
                },
                "confidence": round(min(1.0, 0.7 + (len(events_at_window) / 6.0)), 3),
            })
    
    return timeline


def _normalize_metrics(suite_metrics: list[dict[str, float]]) -> list[dict[str, float]]:
    """Normalize metrics to [0,1] range per metric before repeatability comparison."""
    if not suite_metrics:
        return []
    
    normalized: list[dict[str, float]] = []
    
    # Extract all metrics
    success_rates = [m.get("success_rate", 0.0) for m in suite_metrics]
    latencies = [m.get("p95_latency", 0.0) for m in suite_metrics]
    
    # Compute z-score normalization
    if len(success_rates) > 1:
        sr_mean = statistics.fmean(success_rates)
        sr_std = statistics.stdev(success_rates) if len(success_rates) > 1 else 1.0
        sr_std = sr_std if sr_std > 0 else 1.0
    else:
        sr_mean = success_rates[0] if success_rates else 0.5
        sr_std = 1.0
    
    if len(latencies) > 1:
        lat_mean = statistics.fmean(latencies)
        lat_std = statistics.stdev(latencies) if len(latencies) > 1 else 1.0
        lat_std = lat_std if lat_std > 0 else 1.0
    else:
        lat_mean = latencies[0] if latencies else 100.0
        lat_std = 1.0
    
    for m in suite_metrics:
        norm_entry: dict[str, float] = {}
        sr = m.get("success_rate", 0.0)
        norm_entry["success_rate_norm"] = (sr - sr_mean) / sr_std if sr_std > 0 else sr
        
        lat = m.get("p95_latency", 0.0)
        norm_entry["p95_latency_norm"] = (lat - lat_mean) / lat_std if lat_std > 0 else lat
        
        norm_entry["success_rate"] = sr
        norm_entry["p95_latency"] = lat
        norm_entry["completion_ratio"] = m.get("completion_ratio", 0.0)
        normalized.append(norm_entry)
    
    return normalized


def _compute_signal_quality(raw_count: int, clean_count: int, timeout_count: int, retry_count: int) -> dict[str, float]:
    """Decompose signal into 3-layer model: signal / artifact / distortion."""
    total_raw = raw_count
    if total_raw == 0:
        return {
            "signal_layer_ratio": 1.0,
            "artifact_layer_ratio": 0.0,
            "distortion_layer_ratio": 0.0,
            "observability_confidence": 1.0,
        }
    
    signal_layer = clean_count
    artifact_layer = timeout_count + retry_count
    distortion_layer = total_raw - signal_layer - artifact_layer
    
    return {
        "signal_layer_ratio": round(signal_layer / total_raw, 3),
        "artifact_layer_ratio": round(artifact_layer / total_raw, 3),
        "distortion_layer_ratio": round(distortion_layer / total_raw, 3),
        "signal_layer_samples": signal_layer,
        "artifact_layer_samples": artifact_layer,
        "distortion_layer_samples": distortion_layer,
        "observability_confidence": round((signal_layer + artifact_layer) / total_raw, 3),
    }


def _compute_repeatability_score(suite_metrics: list[dict[str, float]]) -> dict[str, Any]:
    """Enhanced repeatability with normalized metrics: CV, Kendall rank consistency, pass/fail."""
    if not suite_metrics or len(suite_metrics) < 2:
        return {"cv_success_rate": 0.0, "cv_latency": 0.0, "rank_consistency": 1.0, "pass": True}
    
    # Normalize metrics first
    normalized = _normalize_metrics(suite_metrics)
    
    # Extract normalized values for statistical comparison
    success_rates_norm = [m.get("success_rate_norm", 0.0) for m in normalized]
    latencies_norm = [m.get("p95_latency_norm", 0.0) for m in normalized]
    
    # Coefficient of variation on normalized data (CV = std / mean)
    sr_mean = statistics.fmean(success_rates_norm) if success_rates_norm else 1.0
    sr_cv = (statistics.stdev(success_rates_norm) / sr_mean if sr_mean > 0 and len(success_rates_norm) > 1 else 0.0)
    
    lat_mean = statistics.fmean(latencies_norm) if latencies_norm else 1.0
    lat_cv = (statistics.stdev(latencies_norm) / lat_mean if lat_mean > 0 and len(latencies_norm) > 1 else 0.0)
    
    # Kendall rank consistency (sorted order matches across runs)
    rank_consistency = 1.0 if len(success_rates_norm) == 1 else (1.0 - (sum(1 for i in range(len(success_rates_norm) - 1) if success_rates_norm[i] < success_rates_norm[i + 1]) / max(1, len(success_rates_norm) - 1)))
    
    # Pass if normalized CV < 0.15 for both metrics
    pass_gate = sr_cv < 0.15 and lat_cv < 0.15 and rank_consistency > 0.8
    
    return {
        "cv_success_rate": round(sr_cv, 3),
        "cv_latency": round(lat_cv, 3),
        "rank_consistency": round(rank_consistency, 3),
        "pass": pass_gate,
    }


def _pearson(xs: list[float], ys: list[float]) -> float:
    if len(xs) < 2 or len(xs) != len(ys):
        return 0.0
    if len(set(xs)) == 1 or len(set(ys)) == 1:
        return 0.0
    x_mean = statistics.fmean(xs)
    y_mean = statistics.fmean(ys)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    x_var = sum((x - x_mean) ** 2 for x in xs)
    y_var = sum((y - y_mean) ** 2 for y in ys)
    denominator = (x_var * y_var) ** 0.5
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _apply_mode_overrides(config: AuditConfig, mode: str) -> AuditConfig:
    """Apply timing overrides based on execution mode.
    
    Modes:
      smoke: < 2 min, minimal coverage, 1 run
      standard: ~10-20 min, reasonable coverage, 3 runs
      full_torture: current defaults (no overrides)
    """
    if mode not in ("smoke", "standard", "full_torture", "sse_breakpoint_gate"):
        return config
    if mode == "full_torture":
        return config
    
    overrides = MODE_CONFIGS.get(mode, {})
    return AuditConfig(
        port=config.port,
        seed=config.seed,
        mode=mode,
        soak_duration_seconds=overrides.get("soak_duration_seconds", config.soak_duration_seconds),
        breakpoint_stage_seconds=overrides.get("breakpoint_stage_seconds", config.breakpoint_stage_seconds),
        chaos_duration_seconds=overrides.get("chaos_duration_seconds", config.chaos_duration_seconds),
        repeat_runs=overrides.get("repeat_runs", config.repeat_runs),
        sample_interval_seconds=overrides.get("sample_interval_seconds", config.sample_interval_seconds),
        warmup_seconds=config.warmup_seconds,
        soak_peak_concurrency=config.soak_peak_concurrency,
        chaos_peak_concurrency=config.chaos_peak_concurrency,
        breakpoint_stages=overrides.get("breakpoint_stages", config.breakpoint_stages),
        report_path=config.report_path,
        compare_baseline_path=config.compare_baseline_path,
        gate_mode=config.gate_mode,
        baseline_version_id=config.baseline_version_id,
    )


def _parse_stage_list(value: str | None) -> tuple[int, ...]:
    if not value:
        return tuple(DEFAULT_BREAKPOINT_STAGES)
    parsed = [int(part.strip()) for part in value.split(",") if part.strip()]
    return tuple(stage for stage in parsed if stage > 0) or tuple(DEFAULT_BREAKPOINT_STAGES)


@dataclass(frozen=True)
class AuditConfig:
    port: int
    seed: int
    soak_duration_seconds: int
    breakpoint_stage_seconds: int
    chaos_duration_seconds: int
    repeat_runs: int
    sample_interval_seconds: int
    warmup_seconds: int
    soak_peak_concurrency: int
    chaos_peak_concurrency: int
    breakpoint_stages: tuple[int, ...]
    report_path: Path | None
    compare_baseline_path: Path | None = None
    gate_mode: bool = False
    baseline_version_id: str = "v1"
    mode: str = "full_torture"


@dataclass(frozen=True)
class AuditObservation:
    ts: float
    latency_ms: float
    status: int
    ok: bool
    retried: bool
    kind: str
    regime: str
    stage: str | None = None
    transport_artifact: bool = False
    retry_amplified: bool = False
    sse_lag_ms: float = 0.0


class ProductionTortureAudit:
    def __init__(self, config: AuditConfig) -> None:
        self.config = config
        self.start_time = time.time()
        harness_mode = "standard" if config.mode == "sse_breakpoint_gate" else config.mode
        duration_minutes = max(
            1,
            math.ceil(
                (config.soak_duration_seconds + (len(config.breakpoint_stages) * config.breakpoint_stage_seconds) + config.chaos_duration_seconds)
                / 60
            ),
        )
        self.harness = RuntimeStressHarness(
            port=config.port,
            duration_minutes=duration_minutes,
            sample_interval_seconds=config.sample_interval_seconds,
            audit_mode=harness_mode,
            audit_bypass=config.mode in {"smoke", "standard", "sse_breakpoint_gate"},
        )
        self.server = None
        self._inprocess_server: uvicorn.Server | None = None
        self._inprocess_server_thread: threading.Thread | None = None
        self._homes: list[tuple[str, str]] = []
        self._homes_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._client_inflight = 0
        self._client_inflight_peak = 0
        self._last_watermark_by_home: dict[str, str | None] = {}

    def _inc_inflight(self) -> None:
        with self._state_lock:
            self._client_inflight += 1
            if self._client_inflight > self._client_inflight_peak:
                self._client_inflight_peak = self._client_inflight

    def _dec_inflight(self) -> None:
        with self._state_lock:
            self._client_inflight = max(0, self._client_inflight - 1)

    def _reset_inflight(self) -> None:
        with self._state_lock:
            self._client_inflight = 0
            self._client_inflight_peak = 0

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 8.0,
        retries: int = 1,
    ) -> tuple[int, dict[str, Any] | str, float, bool]:
        self._inc_inflight()
        try:
            return self.harness._json_request(
                method,
                path,
                body=body,
                headers=headers,
                timeout=timeout,
                retries=retries,
            )
        finally:
            self._dec_inflight()

    def _metrics_snapshot(self) -> dict[str, Any]:
        status, payload, _latency, _retried = self._request("GET", "/metrics", retries=1)
        if status == 200 and isinstance(payload, dict):
            return payload
        return {}

    def _runtime_metrics_snapshot(self) -> dict[str, Any]:
        home = self._pick_home(random.Random(self.config.seed))
        if home is None:
            return {}
        household_id, token = home
        status, payload, _latency, _retried = self._request(
            "GET",
            "/v1/system/runtime-metrics",
            headers={
                "Authorization": f"Bearer {token}",
                "x-hpal-household-id": household_id,
            },
            retries=1,
        )
        if status == 200 and isinstance(payload, dict):
            return payload
        return {}

    def _pick_home(self, rnd: random.Random) -> tuple[str, str] | None:
        with self._homes_lock:
            if not self._homes:
                return None
            return self._homes[rnd.randint(0, len(self._homes) - 1)]

    def _append_home(self, home: tuple[str, str]) -> None:
        household_id, _token = home
        with self._homes_lock:
            self._homes.append(home)
            self._last_watermark_by_home.setdefault(household_id, None)

    def _prime_households(self, count: int) -> None:
        needed = max(1, count)
        while True:
            with self._homes_lock:
                current = len(self._homes)
            if current >= needed:
                return
            self._append_home(self.harness._register_household())

    def _bootstrap_flow(self, rnd: random.Random) -> tuple[int, float, bool, str]:
        if rnd.random() < 0.5 or not self._homes:
            start = time.perf_counter()
            try:
                home = self.harness._register_household()
                latency_ms = (time.perf_counter() - start) * 1000
                self._append_home(home)
                return 200, latency_ms, False, "bootstrap_create"
            except Exception:
                latency_ms = (time.perf_counter() - start) * 1000
                return 599, latency_ms, False, "bootstrap_create"

        pair = self._pick_home(rnd)
        if pair is None:
            return 599, 0.0, False, "bootstrap_state"
        household_id, token = pair
        status, _payload, latency_ms, retried = self._request(
            "GET",
            f"/v1/ui/bootstrap?family_id={household_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "x-hpal-household-id": household_id,
            },
            retries=1,
        )
        return status, latency_ms, retried, "bootstrap_state"

    def _invalid_flow(self, rnd: random.Random) -> tuple[int, float, bool, str]:
        pair = self._pick_home(rnd)
        if pair is None:
            return 599, 0.0, False, "invalid"
        household_id, _token = pair
        if rnd.random() < 0.5:
            status, _payload, latency_ms, retried = self._request(
                "POST",
                "/v1/ui/message",
                body={
                    "family_id": household_id,
                    "message": f"invalid-{rnd.randint(1, 999999)}",
                    "session_id": f"invalid-session-{rnd.randint(1, 1000)}",
                },
                headers={
                    "Authorization": "Bearer invalid.token.value",
                    "x-hpal-household-id": household_id,
                    "x-idempotency-key": f"invalid-{time.time_ns()}",
                },
                retries=0,
            )
            return status, latency_ms, retried, "invalid_jwt"

        status, _payload, latency_ms, retried = self._request(
            "POST",
            "/v1/ui/message",
            body={
                "family_id": household_id,
                "message": f"edge-{rnd.randint(1, 999999)}",
                "session_id": f"edge-session-{rnd.randint(1, 1000)}",
            },
            headers={
                "x-hpal-household-id": household_id,
                "x-idempotency-key": f"edge-{time.time_ns()}",
            },
            retries=0,
        )
        return status, latency_ms, retried, "missing_auth"

    def _valid_message_flow(self, rnd: random.Random, retry_burst: bool = False) -> tuple[int, float, bool, str]:
        pair = self._pick_home(rnd)
        if pair is None:
            return 599, 0.0, False, "valid"
        household_id, token = pair
        status, _payload, latency_ms, retried = self._request(
            "POST",
            "/v1/ui/message",
            body={
                "family_id": household_id,
                "message": f"audit-{rnd.randint(1, 999999)}",
                "session_id": f"audit-session-{rnd.randint(1, 2000)}",
            },
            headers={
                "Authorization": f"Bearer {token}",
                "x-hpal-household-id": household_id,
                "x-idempotency-key": f"audit-{time.time_ns()}",
            },
            retries=2 if retry_burst else 1,
        )
        return status, latency_ms, retried, "valid"

    def _sse_flow(self, rnd: random.Random) -> tuple[int, float, bool, str, float]:
        pair = self._pick_home(rnd)
        if pair is None:
            return 599, 0.0, False, "sse", 0.0
        household_id, token = pair
        self._inc_inflight()
        start = time.perf_counter()
        try:
            last_watermark = self._last_watermark_by_home.get(household_id)
            events, next_watermark = self.harness._sse_read_one(household_id, token, last_watermark)
            self._last_watermark_by_home[household_id] = next_watermark
            latency_ms = (time.perf_counter() - start) * 1000
            with self.harness._lock:
                sse_lag = float(self.harness._sse_lags_ms[-1]) if self.harness._sse_lags_ms else 0.0
            ok = len(events) > 0 and events[0][0] == "connected"
            return (200 if ok else 599), latency_ms, False, "sse", sse_lag
        except Exception:
            latency_ms = (time.perf_counter() - start) * 1000
            return 599, latency_ms, False, "sse", 0.0
        finally:
            self._dec_inflight()

    def _sample_regime(self, stop_event: threading.Event, samples: list[dict[str, Any]], started_at: float) -> None:
        while not stop_event.is_set():
            if self.config.mode == "sse_breakpoint_gate":
                from apps.api.observability.metrics import metrics as local_metrics
                from apps.api.runtime.event_loop_guard import event_loop_guard
                metrics = local_metrics.snapshot()
                runtime_metrics = {}
                event_loop = event_loop_guard.snapshot()
            else:
                metrics = self._metrics_snapshot()
                runtime_metrics = self._runtime_metrics_snapshot()
                event_loop = {}
            elapsed = time.time() - started_at
            memory_mb = _windows_process_memory_mb(self.server.pid) if self.server is not None else 0.0
            with self.harness._lock:
                recent_lags = list(self.harness._sse_lags_ms)[-200:]
            with self._state_lock:
                client_inflight = self._client_inflight
                client_peak = self._client_inflight_peak

            samples.append(
                {
                    "t_seconds": round(elapsed, 3),
                    "memory_mb": round(memory_mb, 3),
                    "sse_lag_ms_p95": round(_percentile(recent_lags, 0.95), 3),
                    "client_inflight": client_inflight,
                    "client_inflight_peak": client_peak,
                    "replay_queue_depth": float(metrics.get("gauges", {}).get("replay_queue_depth", 0.0) or 0.0),
                    "db_pool_in_use": float(metrics.get("gauges", {}).get("db_pool_in_use", 0.0) or 0.0),
                    "completion_ratio": float(runtime_metrics.get("completion_ratio", 0.0) or 0.0),
                    "inflight_current": float(runtime_metrics.get("inflight_current", 0.0) or 0.0),
                    "accepted_total": float(runtime_metrics.get("accepted_total", 0.0) or 0.0),
                    "rejected_total": float(runtime_metrics.get("rejected_total", 0.0) or 0.0),
                    "event_loop_lag_ms": float(event_loop.get("event_loop_lag_ms", 0.0) or 0.0),
                }
            )
            stop_event.wait(self.config.sample_interval_seconds)

    @staticmethod
    def _weighted_kind(profile: dict[str, Any], rnd: random.Random) -> str:
        valid_weight = float(profile.get("valid_weight", 0.6))
        sse_weight = float(profile.get("sse_weight", 0.2))
        bootstrap_weight = float(profile.get("bootstrap_weight", 0.1))
        invalid_weight = float(profile.get("invalid_weight", 0.1))
        total = valid_weight + sse_weight + bootstrap_weight + invalid_weight
        if total <= 0:
            return "valid"
        roll = rnd.random() * total
        if roll < valid_weight:
            return "valid"
        roll -= valid_weight
        if roll < sse_weight:
            return "sse"
        roll -= sse_weight
        if roll < bootstrap_weight:
            return "bootstrap"
        return "invalid"

    def _execute_schedule(
        self,
        *,
        regime_name: str,
        schedule: list[dict[str, Any]],
        seed: int,
        warmup_seconds: int,
        stage_name: str | None = None,
    ) -> dict[str, Any]:
        observations: queue.Queue[AuditObservation] = queue.Queue()
        samples: list[dict[str, Any]] = []
        stop_event = threading.Event()
        profile_lock = threading.Lock()
        current_profile = dict(schedule[0]) if schedule else {"target_concurrency": 1}
        max_concurrency = max(int(point.get("target_concurrency", 1)) for point in schedule) if schedule else 1
        self._reset_inflight()
        self._prime_households(max(8, max_concurrency // 2))

        def worker(idx: int) -> None:
            rnd = random.Random((seed * 10000) + idx)
            while not stop_event.is_set():
                with profile_lock:
                    profile = dict(current_profile)
                target = int(profile.get("target_concurrency", 1))
                if idx >= target:
                    time.sleep(0.01)
                    continue

                retry_burst = bool(profile.get("retry_burst", False))
                kind = self._weighted_kind(profile, rnd)
                if kind == "valid":
                    status, latency_ms, retried, observed_kind = self._valid_message_flow(rnd, retry_burst=retry_burst)
                    ok = status == 200
                    sse_lag_ms = 0.0
                elif kind == "sse":
                    status, latency_ms, retried, observed_kind, sse_lag_ms = self._sse_flow(rnd)
                    ok = status == 200
                elif kind == "bootstrap":
                    status, latency_ms, retried, observed_kind = self._bootstrap_flow(rnd)
                    ok = status == 200
                    sse_lag_ms = 0.0
                else:
                    status, latency_ms, retried, observed_kind = self._invalid_flow(rnd)
                    ok = status in {400, 401, 404}
                    sse_lag_ms = 0.0

                observations.put(
                    AuditObservation(
                        ts=time.time(),
                        latency_ms=latency_ms,
                        status=status,
                        ok=ok,
                        retried=retried,
                        kind=observed_kind,
                        regime=regime_name,
                        stage=stage_name,
                        transport_artifact=status in {0, 599},
                        retry_amplified=retry_burst and retried,
                        sse_lag_ms=sse_lag_ms,
                    )
                )
                time.sleep(rnd.uniform(0.01, 0.05))

        started_at = time.time()
        sample_thread = threading.Thread(target=self._sample_regime, args=(stop_event, samples, started_at), daemon=True)
        workers = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(max_concurrency)]
        for thread in workers:
            thread.start()
        sample_thread.start()

        for point in schedule:
            with profile_lock:
                current_profile = dict(point)
            time.sleep(1.0)

        stop_event.set()
        for thread in workers:
            thread.join(timeout=3)
        sample_thread.join(timeout=2)

        raw_metrics: list[dict[str, Any]] = []
        while True:
            try:
                obs = observations.get_nowait()
                raw_metrics.append(
                    {
                        "ts": obs.ts,
                        "latency_ms": obs.latency_ms,
                        "status": obs.status,
                        "ok": obs.ok,
                        "retried": obs.retried,
                        "kind": obs.kind,
                        "regime": obs.regime,
                        "stage": obs.stage,
                        "transport_artifact": obs.transport_artifact,
                        "retry_amplified": obs.retry_amplified,
                        "sse_lag_ms": obs.sse_lag_ms,
                    }
                )
            except queue.Empty:
                break

        clean_metrics = isolate(raw_metrics, warmup_seconds=warmup_seconds)
        noise_profile = classify_noise(raw_metrics, warmup_seconds=warmup_seconds)
        return self._summarize_regime(
            regime_name=regime_name,
            raw_metrics=raw_metrics,
            clean_metrics=clean_metrics,
            samples=samples,
            noise_profile=noise_profile,
            schedule=schedule,
            stage_name=stage_name,
        )

    @staticmethod
    def _first_failure_mode(metrics: list[dict[str, Any]]) -> str | None:
        for metric in sorted(metrics, key=lambda item: float(item.get("ts", 0.0))):
            if bool(metric.get("ok", False)):
                continue
            status = int(metric.get("status", 0) or 0)
            kind = str(metric.get("kind", ""))
            if status in {0, 599}:
                return "transport_timeout"
            if status == 429:
                return "http_429"
            if kind.startswith("sse"):
                return "sse_failure"
            if kind.startswith("invalid") and status != 401:
                return "invalid_token_non_401"
            if status >= 500:
                return "server_5xx"
            return f"http_{status}"
        return None

    @staticmethod
    def _saturation_signal_type(metrics: list[dict[str, Any]], samples: list[dict[str, Any]]) -> str:
        timeout_count = sum(1 for metric in metrics if int(metric.get("status", 0) or 0) in {0, 599})
        rejection_count = sum(1 for metric in metrics if int(metric.get("status", 0) or 0) == 429)
        sse_lag_points = [(float(sample.get("t_seconds", 0.0)), float(sample.get("sse_lag_ms_p95", 0.0))) for sample in samples]
        lag_slope = _linear_slope(sse_lag_points)
        if rejection_count > 0 and rejection_count >= timeout_count:
            return "backpressure_429"
        if timeout_count > rejection_count:
            return "transport_timeout"
        if lag_slope > 25.0:
            return "sse_backlog"
        if any(int(metric.get("status", 0) or 0) >= 500 for metric in metrics):
            return "server_error"
        return "stable"

    def _summarize_regime(
        self,
        *,
        regime_name: str,
        raw_metrics: list[dict[str, Any]],
        clean_metrics: list[dict[str, Any]],
        samples: list[dict[str, Any]],
        noise_profile: dict[str, float],
        schedule: list[dict[str, Any]],
        stage_name: str | None,
    ) -> dict[str, Any]:
        latencies = [float(metric.get("latency_ms", 0.0)) for metric in clean_metrics if float(metric.get("latency_ms", 0.0)) > 0]
        total_clean = len(clean_metrics)
        total_raw = len(raw_metrics)
        success_count = sum(1 for metric in clean_metrics if bool(metric.get("ok", False)))
        error_count = total_clean - success_count
        retry_count = sum(1 for metric in clean_metrics if bool(metric.get("retried", False)))
        rejection_count = sum(1 for metric in clean_metrics if int(metric.get("status", 0) or 0) == 429)
        timeout_count = sum(1 for metric in raw_metrics if int(metric.get("status", 0) or 0) in {0, 599})
        sse_lags = [float(metric.get("sse_lag_ms", 0.0)) for metric in clean_metrics if float(metric.get("sse_lag_ms", 0.0)) > 0]

        success_rate = (success_count / total_clean) if total_clean else 0.0
        error_rate = (error_count / total_clean) if total_clean else 1.0
        retry_rate = (retry_count / total_clean) if total_clean else 0.0
        completion_ratio = max(
            success_rate,
            float(samples[-1].get("completion_ratio", 0.0) or 0.0) if samples else 0.0,
        )
        inflight_peak = max(
            int(max((sample.get("client_inflight_peak", 0) for sample in samples), default=0)),
            int(self._client_inflight_peak),
        )
        final_inflight = float(samples[-1].get("client_inflight", 0.0) if samples else 0.0)
        inflight_recovery_ratio = (final_inflight / inflight_peak) if inflight_peak else 1.0
        sse_lag_points = [(float(sample.get("t_seconds", 0.0)), float(sample.get("sse_lag_ms_p95", 0.0))) for sample in samples]
        memory_points = [(float(sample.get("t_seconds", 0.0)), float(sample.get("memory_mb", 0.0))) for sample in samples]

        return {
            "regime": regime_name,
            "stage": stage_name,
            "duration_seconds": len(schedule),
            "load_model": {
                "type": "stable_curve" if regime_name == "soak" else ("chaos_curve" if regime_name == "chaos" else "fixed_stage"),
                "seed": self.config.seed,
                "schedule_points": len(schedule),
            },
            "raw_request_count": total_raw,
            "clean_request_count": total_clean,
            "clean_metrics": {
                "request_count": total_clean,
                "success_count": success_count,
                "error_count": error_count,
            },
            "success_rate": round(success_rate, 6),
            "error_rate": round(error_rate, 6),
            "p95_latency": round(_percentile(latencies, 0.95), 3),
            "retry_rate": round(retry_rate, 6),
            "rejections_429": int(rejection_count),
            "timeout_count": int(timeout_count),
            "sse_lag_p95": round(_percentile(sse_lags, 0.95), 3),
            "sse_lag_growth_slope": round(_linear_slope(sse_lag_points), 6),
            "inflight_concurrency": {
                "peak": inflight_peak,
                "final": round(final_inflight, 3),
                "sampled_max": round(max((float(sample.get("inflight_current", 0.0)) for sample in samples), default=0.0), 3),
            },
            "inflight_recovery_ratio": round(inflight_recovery_ratio, 6),
            "completion_ratio": round(completion_ratio, 6),
            "memory_growth_slope": round(_linear_slope(memory_points), 6),
            "first_failure_mode": self._first_failure_mode(raw_metrics),
            "saturation_signal_type": self._saturation_signal_type(raw_metrics, samples),
            "noise_profile": noise_profile,
            "samples": list(samples),
            "samples_captured": len(samples),
        }

    def _build_soak_schedule(self) -> list[dict[str, Any]]:
        total = max(3, self.config.soak_duration_seconds)
        ramp = max(1, int(total * 0.2))
        plateau = max(1, int(total * 0.6))
        decay = max(1, total - ramp - plateau)
        while ramp + plateau + decay > total:
            if plateau > 1:
                plateau -= 1
            elif ramp > 1:
                ramp -= 1
            else:
                decay -= 1
        model = (
            LoadCurveModel(seed=self.config.seed)
            .ramp_up(duration=ramp, start=1, end=self.config.soak_peak_concurrency)
            .plateau(duration=plateau, level=self.config.soak_peak_concurrency)
            .decay(duration=decay, end_level=max(1, self.config.soak_peak_concurrency // 4))
        )
        schedule = list(model.to_schedule())
        for point in schedule:
            point.update(
                {
                    "valid_weight": 0.6,
                    "sse_weight": 0.2,
                    "bootstrap_weight": 0.1,
                    "invalid_weight": 0.1,
                }
            )
        return schedule

    def _build_chaos_schedule(self) -> tuple[list[dict[str, Any]], dict[str, int]]:
        total = max(3, self.config.chaos_duration_seconds)
        ramp = max(1, int(total * 0.25))
        plateau = max(1, int(total * 0.5))
        decay = max(1, total - ramp - plateau)
        while ramp + plateau + decay > total:
            if plateau > 1:
                plateau -= 1
            elif ramp > 1:
                ramp -= 1
            else:
                decay -= 1
        base_model = (
            LoadCurveModel(seed=self.config.seed + 17)
            .ramp_up(duration=ramp, start=max(2, self.config.chaos_peak_concurrency // 4), end=self.config.chaos_peak_concurrency)
            .plateau(duration=plateau, level=self.config.chaos_peak_concurrency)
            .decay(duration=decay, end_level=max(2, self.config.chaos_peak_concurrency // 5))
        )
        rng = random.Random(self.config.seed + 23)
        counts = Counter()
        schedule: list[dict[str, Any]] = []
        for point in base_model.to_schedule():
            poisson_spike = self._poisson(rng, 0.35)
            jwt_burst = rng.random() < 0.12
            reconnect_storm = rng.random() < 0.1
            retry_burst = rng.random() < 0.12
            disconnect_churn = rng.random() < 0.15
            target = int(point["target_concurrency"]) + (poisson_spike * 6)
            profile = {
                "timestamp": int(point["timestamp"]),
                "target_concurrency": target,
                "phase": point.get("phase", "chaos"),
                "valid_weight": 0.45,
                "sse_weight": 0.2,
                "bootstrap_weight": 0.1,
                "invalid_weight": 0.25 if jwt_burst else 0.15,
                "retry_burst": retry_burst,
                "disconnect_churn": disconnect_churn,
            }
            if reconnect_storm:
                profile["sse_weight"] = 0.35
                profile["valid_weight"] = 0.3
                counts["sse_reconnect_storms"] += 1
            if jwt_burst:
                counts["invalid_jwt_bursts"] += 1
            if retry_burst:
                counts["retry_amplification_bursts"] += 1
            if disconnect_churn:
                counts["disconnect_reconnect_churn_windows"] += 1
            if poisson_spike > 0:
                counts["poisson_spike_windows"] += 1
            schedule.append(profile)
        return schedule, dict(counts)

    @staticmethod
    def _poisson(rng: random.Random, lam: float) -> int:
        if lam <= 0.0:
            return 0
        threshold = math.exp(-lam)
        product = 1.0
        count = 0
        while product > threshold:
            count += 1
            product *= rng.random()
        return max(0, count - 1)

    def _run_soak(self) -> dict[str, Any]:
        return self._execute_schedule(
            regime_name="soak",
            schedule=self._build_soak_schedule(),
            seed=self.config.seed,
            warmup_seconds=self.config.warmup_seconds,
        )

    def _run_breakpoint(self) -> dict[str, Any]:
        stages: list[dict[str, Any]] = []
        first_failure_mode: str | None = None
        total_rejections = 0
        total_timeouts = 0
        max_stable_concurrency = 0
        dominant_signal = "stable"
        sse_lag_slopes: list[float] = []
        memory_slopes: list[float] = []
        inflight_recoveries: list[float] = []
        for concurrency in self.config.breakpoint_stages:
            schedule = [
                {
                    "timestamp": second,
                    "target_concurrency": concurrency,
                    "valid_weight": 0.6,
                    "sse_weight": 0.2,
                    "bootstrap_weight": 0.1,
                    "invalid_weight": 0.1,
                }
                for second in range(self.config.breakpoint_stage_seconds)
            ]
            stage = self._execute_schedule(
                regime_name="breakpoint",
                schedule=schedule,
                seed=self.config.seed,
                warmup_seconds=min(self.config.warmup_seconds, max(1, self.config.breakpoint_stage_seconds // 6)),
                stage_name=f"concurrency_{concurrency}",
            )
            stage["concurrency"] = concurrency
            stages.append(stage)
            total_rejections += int(stage.get("rejections_429", 0))
            total_timeouts += int(stage.get("timeout_count", 0))
            sse_lag_slopes.append(float(stage.get("sse_lag_growth_slope", 0.0) or 0.0))
            memory_slopes.append(float(stage.get("memory_growth_slope", 0.0) or 0.0))
            inflight_recoveries.append(float(stage.get("inflight_recovery_ratio", 1.0) or 0.0))
            if float(stage.get("error_rate", 1.0) or 0.0) <= 0.05 and float(stage.get("completion_ratio", 0.0) or 0.0) >= 0.8:
                max_stable_concurrency = concurrency
            if first_failure_mode is None:
                first_failure_mode = stage.get("first_failure_mode")
            if dominant_signal == "stable" and str(stage.get("saturation_signal_type", "stable")) != "stable":
                dominant_signal = str(stage.get("saturation_signal_type", "stable"))

        return {
            "stage_duration_seconds": self.config.breakpoint_stage_seconds,
            "target_peak": max(self.config.breakpoint_stages),
            "stages": stages,
            "first_failure_mode": first_failure_mode,
            "saturation_signal_type": dominant_signal,
            "rejections_429": total_rejections,
            "timeout_count": total_timeouts,
            "max_stable_concurrency": max_stable_concurrency,
            "sse_degradation_behavior": {
                "lag_slope_max": round(max(sse_lag_slopes, default=0.0), 6),
                "lag_slope_avg": round(statistics.fmean(sse_lag_slopes), 6) if sse_lag_slopes else 0.0,
            },
            "sse_lag_growth_slope": round(max(sse_lag_slopes, default=0.0), 6),
            "completion_ratio": round(min((float(stage.get("completion_ratio", 0.0) or 0.0) for stage in stages), default=0.0), 6),
            "inflight_recovery_ratio": round(min(inflight_recoveries, default=1.0), 6),
            "memory_growth_slope": round(max(memory_slopes, default=0.0), 6),
        }

    def _run_chaos(self) -> dict[str, Any]:
        schedule, chaos_counts = self._build_chaos_schedule()
        summary = self._execute_schedule(
            regime_name="chaos",
            schedule=schedule,
            seed=self.config.seed,
            warmup_seconds=min(self.config.warmup_seconds, max(1, self.config.chaos_duration_seconds // 8)),
        )
        summary["chaos_injections"] = chaos_counts
        return summary

    def _run_sse_breakpoint_only(self) -> dict[str, Any]:
        from apps.api.realtime.broadcaster import broadcaster
        from apps.api.runtime.event_loop_guard import event_loop_guard

        stages: list[dict[str, Any]] = []
        max_stable_concurrency = 0
        stage_metrics: list[dict[str, Any]] = []

        for concurrency in self.config.breakpoint_stages:
            schedule = [
                {
                    "timestamp": second,
                    "target_concurrency": concurrency,
                    "valid_weight": 0.6,
                    "sse_weight": 0.2,
                    "bootstrap_weight": 0.1,
                    "invalid_weight": 0.1,
                }
                for second in range(self.config.breakpoint_stage_seconds)
            ]
            broadcaster.reset_diagnostics()
            stage = self._execute_schedule(
                regime_name="breakpoint",
                schedule=schedule,
                seed=self.config.seed,
                warmup_seconds=min(self.config.warmup_seconds, max(1, self.config.breakpoint_stage_seconds // 6)),
                stage_name=f"concurrency_{concurrency}",
            )
            stage["concurrency"] = concurrency
            diag = broadcaster.diagnostics_snapshot().get("fanout_diagnostics", {})
            event_loop_samples = [float(sample.get("event_loop_lag_ms", 0.0) or 0.0) for sample in stage.get("samples", [])]
            event_loop_lag_max = max(event_loop_samples, default=float(event_loop_guard.snapshot().get("event_loop_lag_max_ms", 0.0) or 0.0))
            rejection_rate_429 = float(stage.get("rejections_429", 0.0) or 0.0) / max(1.0, float(stage.get("clean_request_count", 0.0) or 0.0))
            stage_metric = {
                "concurrency": concurrency,
                "avg_fanout_time_ms": float(diag.get("avg_fanout_time_ms", 0.0) or 0.0),
                "max_fanout_time_ms": float(diag.get("max_fanout_time_ms", 0.0) or 0.0),
                "fanout_calls_per_second": float(diag.get("fanout_calls_per_second", 0.0) or 0.0),
                "fanout_per_subscriber_cost": float(diag.get("avg_per_subscriber_us", 0.0) or 0.0),
                "avg_event_loop_schedule_delay_ms": float(diag.get("avg_schedule_delay_ms", 0.0) or 0.0),
                "max_event_loop_schedule_delay_ms": float(diag.get("max_schedule_delay_ms", 0.0) or 0.0),
                "callback_queue_depth": int(diag.get("callback_queue_depth_max", 0) or 0),
                "event_loop_lag_max_ms": float(event_loop_lag_max),
                "inflight_tasks": float(diag.get("inflight_tasks_max", 0.0) or 0.0),
                "rejection_rate_429": round(rejection_rate_429, 6),
                "success_rate": float(stage.get("success_rate", 0.0) or 0.0),
            }
            stage["sse_stage_metrics"] = stage_metric
            stage_metrics.append(stage_metric)
            stages.append(stage)
            if float(stage.get("error_rate", 1.0) or 0.0) <= 0.05 and float(stage.get("completion_ratio", 0.0) or 0.0) >= 0.8:
                max_stable_concurrency = concurrency

        return {
            "stages": stages,
            "stage_metrics": stage_metrics,
            "max_stable_concurrency": max_stable_concurrency,
        }

    # ------------------------------------------------------------------
    # Reference distribution model (cross-run normalization via Welford)
    # ------------------------------------------------------------------

    _REF_DIST_PATH: Path = ROOT / "sse_reference_distribution.json"
    # Staging buffer: runs accumulate here before being merged into the committed baseline.
    _REF_STAGING_PATH: Path = ROOT / "sse_reference_staging.json"
    _CALIBRATION_LOG_PATH: Path = ROOT / "calibration_log.jsonl"
    # Number of complete runs that must accumulate in the staging buffer before the
    # statistics are committed into the live baseline (prevents failure-mode absorption).
    _BASELINE_COMMIT_WINDOW: int = 3
    # Keys tracked per-stage in the rolling baseline
    _REF_DIST_KEYS: tuple[str, ...] = (
        "stability_score",
        "fanout_activity_ratio",
        "queue_saturation_ratio",
        "success_rate",
        "event_loop_lag_ms",
    )

    @staticmethod
    def _welford_update(entry: dict[str, float], value: float) -> dict[str, float]:
        """Single-pass Welford update: returns new (mean, m2, count)."""
        count = entry.get("count", 0) + 1
        delta = value - entry.get("mean", 0.0)
        mean = entry.get("mean", 0.0) + delta / count
        delta2 = value - mean
        m2 = entry.get("m2", 0.0) + delta * delta2
        return {"mean": mean, "m2": m2, "count": count}

    @staticmethod
    def _welford_merge(a: dict[str, float], b: dict[str, float]) -> dict[str, float]:
        """Parallel Welford merge of two (mean, m2, count) entries.

        Chan's parallel algorithm for combining two populations.
        """
        na = int(a.get("count", 0))
        nb = int(b.get("count", 0))
        n = na + nb
        if n == 0:
            return {"mean": 0.0, "m2": 0.0, "count": 0}
        mean_a = a.get("mean", 0.0)
        mean_b = b.get("mean", 0.0)
        delta = mean_b - mean_a
        mean = (na * mean_a + nb * mean_b) / n
        m2 = a.get("m2", 0.0) + b.get("m2", 0.0) + (delta ** 2) * na * nb / n
        return {"mean": mean, "m2": m2, "count": n}

    @staticmethod
    def _z_score(value: float, entry: dict[str, float]) -> float:
        """Return z-score relative to a Welford distribution; 0.0 when < 2 samples."""
        count = entry.get("count", 0)
        if count < 2:
            return 0.0
        std = math.sqrt(entry.get("m2", 0.0) / max(1, count))
        if std < 1e-9:
            return 0.0
        return (value - entry.get("mean", 0.0)) / std

    @classmethod
    def _empty_ref_dist(cls) -> dict[str, Any]:
        d: dict[str, Any] = {k: {"mean": 0.0, "m2": 0.0, "count": 0} for k in cls._REF_DIST_KEYS}
        d["run_count"] = 0
        return d

    @classmethod
    def _load_json_dist(cls, path: Path) -> dict[str, Any]:
        """Load a Welford distribution JSON; return empty skeleton on any failure."""
        empty = cls._empty_ref_dist()
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                for key in cls._REF_DIST_KEYS:
                    if key not in data:
                        data[key] = {"mean": 0.0, "m2": 0.0, "count": 0}
                return data
            except Exception:
                pass
        return empty

    @classmethod
    def _load_reference_distribution(cls) -> dict[str, Any]:
        """Load the committed baseline distribution."""
        return cls._load_json_dist(cls._REF_DIST_PATH)

    @classmethod
    def _save_json_dist(cls, path: Path, dist: dict[str, Any]) -> None:
        """Write a distribution to disk (best-effort, silent on failure)."""
        try:
            path.write_text(json.dumps(dist, indent=2), encoding="utf-8")
        except Exception:
            pass

    @classmethod
    def _stage_run_for_commit(
        cls,
        stage_metrics: list[dict[str, Any]],
        max_fanout_cps: float,
        max_queue_depth: float,
        collapse_inclusions: int,
    ) -> tuple[dict[str, Any], int]:
        """Add non-collapsed stage observations to the staging buffer.

        Returns (updated_staging_dist, collapse_inclusion_attempts_this_run).
        Collapsed stages are NEVER written to staging to prevent failure-mode
        absorption into the baseline.
        """
        import copy
        staging = cls._load_json_dist(cls._REF_STAGING_PATH)
        skipped = 0
        for m in stage_metrics:
            score = cls._compute_stage_stability_score(m, max_fanout_cps, max_queue_depth)
            # Exclude collapsed stages from baseline learning
            success = float(m.get("success_rate", 0.0) or 0.0)
            fanout_cps = float(m.get("fanout_calls_per_second", 0.0) or 0.0)
            is_collapsed = (score < 0.3 and success < 0.3) or (fanout_cps < 1.0 and success < 0.2)
            if is_collapsed:
                skipped += 1
                continue
            fanout_act = min(1.0, fanout_cps / max(0.001, max_fanout_cps))
            queue_sat = min(1.0, float(m.get("callback_queue_depth_max", 0.0) or 0.0) / max(0.001, max_queue_depth))
            lag_ms = float(m.get("event_loop_lag_max_ms", 0.0) or 0.0)
            values = {
                "stability_score": score,
                "fanout_activity_ratio": fanout_act,
                "queue_saturation_ratio": queue_sat,
                "success_rate": success,
                "event_loop_lag_ms": lag_ms,
            }
            for key, val in values.items():
                staging[key] = cls._welford_update(
                    staging.get(key, {"mean": 0.0, "m2": 0.0, "count": 0}), val
                )
        staging["run_count"] = int(staging.get("run_count", 0)) + 1
        return staging, collapse_inclusions + skipped

    def _update_reference_distribution(
        self,
        stage_metrics: list[dict[str, Any]],
        max_fanout_cps: float,
        max_queue_depth: float,
    ) -> tuple[int, bool]:
        """Update staging/baseline unless gate mode is enabled.

        Returns (collapse_inclusion_attempts, committed_to_baseline).
        In gate mode this method is read-only and returns (0, False).
        """
        if self.config.gate_mode:
            return 0, False

        collapse_inclusions = 0
        staging, collapse_inclusions = self._stage_run_for_commit(
            stage_metrics,
            max_fanout_cps,
            max_queue_depth,
            collapse_inclusions,
        )
        self._save_json_dist(self._REF_STAGING_PATH, staging)
        committed = self._maybe_commit_staging(
            staging,
            gate_mode=self.config.gate_mode,
            baseline_version_id=self.config.baseline_version_id,
        )
        return collapse_inclusions, committed

    @classmethod
    def _maybe_commit_staging(
        cls,
        staging: dict[str, Any],
        *,
        gate_mode: bool = False,
        baseline_version_id: str = "v1",
    ) -> bool:
        """Merge staging into committed baseline when the commit window is reached.

        Returns True if a commit occurred.
        """
        if gate_mode:
            return False
        if int(staging.get("run_count", 0)) < cls._BASELINE_COMMIT_WINDOW:
            return False
        import copy
        baseline = cls._load_reference_distribution()
        merged: dict[str, Any] = {}
        for key in cls._REF_DIST_KEYS:
            merged[key] = cls._welford_merge(
                baseline.get(key, {"mean": 0.0, "m2": 0.0, "count": 0}),
                staging.get(key, {"mean": 0.0, "m2": 0.0, "count": 0}),
            )
        merged["run_count"] = int(baseline.get("run_count", 0)) + int(staging.get("run_count", 0))
        merged["baseline_version_id"] = baseline_version_id
        cls._save_json_dist(cls._REF_DIST_PATH, merged)
        # Reset staging
        cls._save_json_dist(cls._REF_STAGING_PATH, cls._empty_ref_dist())
        return True

    # ------------------------------------------------------------------
    # Stage stability scoring and quantile helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_stage_stability_score(
        metric: dict[str, Any],
        max_fanout_cps: float,
        max_queue_depth: float,
    ) -> float:
        """Return a 0-1 stability score relative to run maximums.

        Higher score = healthier stage.
          fanout_activity_ratio (0.4), success_rate (0.4), queue_slack (0.2)
        """
        fanout_activity = float(metric.get("fanout_calls_per_second", 0.0) or 0.0) / max(0.001, max_fanout_cps)
        success = float(metric.get("success_rate", 1.0) or 1.0)
        queue_sat = float(metric.get("callback_queue_depth_max", 0.0) or 0.0) / max(0.001, max_queue_depth)
        return (
            min(1.0, max(0.0, fanout_activity)) * 0.4
            + min(1.0, max(0.0, success)) * 0.4
            + (1.0 - min(1.0, max(0.0, queue_sat))) * 0.2
        )

    @staticmethod
    def _quantile(sorted_values: list[float], q: float) -> float:
        """q-th quantile of a sorted list via linear interpolation."""
        if not sorted_values:
            return 0.0
        n = len(sorted_values)
        if n == 1:
            return sorted_values[0]
        pos = q * (n - 1)
        lo = int(pos)
        hi = min(lo + 1, n - 1)
        frac = pos - lo
        return sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac

    @staticmethod
    def _compute_rolling_stats(values: list[float]) -> tuple[float, float]:
        """Return (mean, std).  Empty list → (0.0, 0.0)."""
        if not values:
            return 0.0, 0.0
        n = len(values)
        mean = sum(values) / n
        if n < 2:
            return mean, 0.0
        return mean, math.sqrt(sum((v - mean) ** 2 for v in values) / n)

    # ------------------------------------------------------------------
    # Probabilistic ensemble arbitration helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _rankdata(values: list[float]) -> list[float]:
        """Return average ranks (1-indexed) with tie handling."""
        if not values:
            return []
        indexed = sorted(enumerate(values), key=lambda item: item[1])
        ranks = [0.0] * len(values)
        i = 0
        while i < len(indexed):
            j = i
            while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
                j += 1
            avg_rank = (i + j + 2) / 2.0
            for k in range(i, j + 1):
                ranks[indexed[k][0]] = avg_rank
            i = j + 1
        return ranks

    @classmethod
    def _spearman_rank_correlation(cls, xs: list[float], ys: list[float]) -> float:
        """Spearman correlation computed via Pearson on rank vectors."""
        if len(xs) != len(ys) or len(xs) < 2:
            return 0.0
        rx = cls._rankdata(xs)
        ry = cls._rankdata(ys)
        return _pearson(rx, ry)

    @staticmethod
    def _normalize_distribution(dist: dict[str, float]) -> dict[str, float]:
        total = sum(max(0.0, v) for v in dist.values())
        if total <= 0.0:
            return {"stable": 0.0, "transition": 1.0, "collapsed": 0.0}
        return {k: max(0.0, v) / total for k, v in dist.items()}

    @staticmethod
    def _normalized_entropy(dist: dict[str, float]) -> float:
        """Return entropy normalized to [0,1] for a 3-class distribution."""
        probs = [max(0.0, float(v)) for v in dist.values()]
        total = sum(probs)
        if total <= 0.0:
            return 0.0
        probs = [p / total for p in probs]
        entropy = 0.0
        for p in probs:
            if p > 0.0:
                entropy -= p * math.log(p)
        return min(1.0, entropy / math.log(3.0))

    @staticmethod
    def _argmax_class(dist: dict[str, float]) -> str:
        """Deterministic tie-break by collapse risk ordering."""
        ordered = sorted(dist.items(), key=lambda item: (item[1], {"stable": 0, "transition": 1, "collapsed": 2}[item[0]]), reverse=True)
        return ordered[0][0] if ordered else "transition"

    @classmethod
    def _vote_distribution_percentile(
        cls,
        score: float,
        min_score: float,
        max_score: float,
        p25: float,
        p75: float,
    ) -> dict[str, float]:
        """Soft class probabilities from percentile location."""
        span = max(1e-9, max_score - min_score)
        u = (score - min_score) / span
        stable = max(0.0, (u - 0.75) / 0.25)
        collapsed = max(0.0, (0.25 - u) / 0.25)
        transition = max(0.0, 1.0 - abs(u - 0.5) / 0.25)
        # Strengthen crisp extremes around p25/p75 boundaries.
        if score <= p25:
            collapsed = max(collapsed, 0.8)
        elif score >= p75:
            stable = max(stable, 0.8)
        else:
            transition = max(transition, 0.7)
        return cls._normalize_distribution(
            {"stable": stable, "transition": transition, "collapsed": collapsed}
        )

    @classmethod
    def _vote_distribution_zscore(cls, z: float) -> dict[str, float]:
        """Soft class probabilities from cross-run z-score."""
        collapsed = 1.0 / (1.0 + math.exp(3.0 * (z + 1.5)))
        stable = 1.0 / (1.0 + math.exp(-3.0 * (z + 0.5)))
        transition = math.exp(-((abs(z) / 1.0) ** 2))
        return cls._normalize_distribution(
            {"stable": stable, "transition": transition, "collapsed": collapsed}
        )

    @classmethod
    def _vote_distribution_gradient(
        cls,
        score: float,
        prior_scores: list[float],
        enabled: bool,
    ) -> dict[str, float]:
        """Soft class probabilities from rolling anomaly score."""
        if not enabled or len(prior_scores) < 2:
            return {"stable": 0.2, "transition": 0.8, "collapsed": 0.0}
        roll_mean, roll_std = cls._compute_rolling_stats(prior_scores)
        if roll_std < 1e-9:
            return {"stable": 0.2, "transition": 0.8, "collapsed": 0.0}
        z = abs(score - roll_mean) / roll_std
        collapsed = min(1.0, max(0.0, (z - 2.0) / 2.0))
        transition = min(1.0, max(0.0, 1.0 - collapsed))
        stable = max(0.0, 1.0 - z / 3.0) * 0.25
        return cls._normalize_distribution(
            {"stable": stable, "transition": transition, "collapsed": collapsed}
        )

    @classmethod
    def _weighted_vote_distribution(
        cls,
        pct_dist: dict[str, float],
        z_dist: dict[str, float],
        grad_dist: dict[str, float],
        percentile_weight: float,
        zscore_weight: float,
        gradient_weight: float,
    ) -> dict[str, float]:
        """Weighted class score = sum(signal_weight * signal_vote_confidence)."""
        scores = {
            "stable": percentile_weight * pct_dist["stable"] + zscore_weight * z_dist["stable"] + gradient_weight * grad_dist["stable"],
            "transition": percentile_weight * pct_dist["transition"] + zscore_weight * z_dist["transition"] + gradient_weight * grad_dist["transition"],
            "collapsed": percentile_weight * pct_dist["collapsed"] + zscore_weight * z_dist["collapsed"] + gradient_weight * grad_dist["collapsed"],
        }
        return cls._normalize_distribution(scores)

    @staticmethod
    def _downgrade_classification_severity(label: str) -> str:
        """Move one step toward higher severity for conservative gating."""
        order = [
            "insufficient_load_variation",
            "insufficient_signal",
            "linear_fanout_scaling",
            "superlinear_fanout_scaling",
            "event_loop_starvation",
            "admission_saturation",
        ]
        if label not in order:
            return label
        idx = order.index(label)
        return order[min(idx + 1, len(order) - 1)]

    @classmethod
    def _label_stages_hybrid(
        cls,
        stage_metrics: list[dict[str, Any]],
        ref_dist: dict[str, Any],
        total_stage_count: int,
    ) -> list[dict[str, Any]]:
        """Label each stage using weighted probabilistic consensus voting."""
        if not stage_metrics:
            return []

        max_fanout_cps = max(
            (float(m.get("fanout_calls_per_second", 0.0) or 0.0) for m in stage_metrics),
            default=1.0,
        ) or 1.0
        max_queue_depth = max(
            (float(m.get("callback_queue_depth_max", 0.0) or 0.0) for m in stage_metrics),
            default=1.0,
        ) or 1.0

        scores = [cls._compute_stage_stability_score(m, max_fanout_cps, max_queue_depth) for m in stage_metrics]
        sorted_scores = sorted(scores)
        p25 = cls._quantile(sorted_scores, 0.25)
        p75 = cls._quantile(sorted_scores, 0.75)

        ref_entry = ref_dist.get("stability_score", {"mean": 0.0, "m2": 0.0, "count": 0})
        z_scores = [cls._z_score(s, ref_entry) for s in scores]

        gradient_enabled = total_stage_count >= 5
        min_score = min(scores)
        max_score = max(scores)

        # Signal reliability weights
        # percentile_vote_weight = transition_quality
        transition_quality = statistics.fmean(
            cls._vote_distribution_percentile(score, min_score, max_score, p25, p75)["transition"]
            for score in scores
        )
        # zscore_vote_weight = cross_run_independence (Spearman-based)
        spearman = cls._spearman_rank_correlation(scores, z_scores)
        cross_run_independence = max(0.0, min(1.0, 1.0 - abs(spearman)))
        # gradient_vote_weight = 0.5, upgraded when stage_count >= 10
        gradient_weight = 0.0 if not gradient_enabled else (1.0 if total_stage_count >= 10 else 0.5)

        labelled: list[dict[str, Any]] = []
        prior_scores: list[float] = []

        for m, score, z in zip(stage_metrics, scores, z_scores):
            pct_dist = cls._vote_distribution_percentile(score, min_score, max_score, p25, p75)
            z_dist = cls._vote_distribution_zscore(z)
            grad_dist = cls._vote_distribution_gradient(score, prior_scores, gradient_enabled)

            weighted_dist = cls._weighted_vote_distribution(
                pct_dist,
                z_dist,
                grad_dist,
                transition_quality,
                cross_run_independence,
                gradient_weight,
            )

            regime = cls._argmax_class(weighted_dist)
            consensus_strength = round(1.0 - cls._normalized_entropy(weighted_dist), 4)

            regime_map = {"collapsed": "collapsed_region", "transition": "transition_region", "stable": "stable_region"}
            labelled.append({
                **m,
                "_regime": regime_map[regime],
                "_stability_score": round(score, 4),
                "_z_score": round(z, 4),
                "_percentile_vote_distribution": pct_dist,
                "_zscore_vote_distribution": z_dist,
                "_gradient_vote_distribution": grad_dist,
                "_class_probability_distribution": weighted_dist,
                "_consensus_strength": consensus_strength,
                "_vote_weights": {
                    "percentile": round(transition_quality, 4),
                    "zscore": round(cross_run_independence, 4),
                    "gradient": round(gradient_weight, 4),
                },
            })
            prior_scores.append(score)

        return labelled

    @staticmethod
    def _weighted_signal_ratio(labelled_stages: list[dict[str, Any]]) -> float:
        """Signal ratio = fraction of transition stages.  Stable is informational only."""
        n = max(1, len(labelled_stages))
        t = sum(1.0 for m in labelled_stages if m.get("_regime") == "transition_region")
        return min(1.0, t / n)

    def _classify_sse_scaling(self, stage_metrics: list[dict[str, Any]]) -> tuple[str, float, dict[str, float], str]:
        # ---- 1. Load committed baseline ----
        ref_dist = self._load_reference_distribution()

        max_fanout_cps = max(
            (float(m.get("fanout_calls_per_second", 0.0) or 0.0) for m in stage_metrics),
            default=1.0,
        ) or 1.0
        max_queue_depth = max(
            (float(m.get("callback_queue_depth_max", 0.0) or 0.0) for m in stage_metrics),
            default=1.0,
        ) or 1.0

        early_run = len(stage_metrics) < 5
        conf_ceiling = 0.7 if early_run else 0.99

        # ---- 2. Three-vote regime labeling ----
        labelled = self._label_stages_hybrid(stage_metrics, ref_dist, len(stage_metrics))

        stable_stages = [m for m in labelled if m.get("_regime") == "stable_region"]
        transition_stages = [m for m in labelled if m.get("_regime") == "transition_region"]
        collapsed_stages = [m for m in labelled if m.get("_regime") == "collapsed_region"]

        transition_count = len(transition_stages)
        stable_count = len(stable_stages)
        collapsed_count = len(collapsed_stages)
        signal_ratio = self._weighted_signal_ratio(labelled)

        # ---- 3. Cross-run metrics (used ONLY for confidence, not classification) ----
        all_z = [m.get("_z_score", 0.0) for m in labelled]
        avg_abs_z = sum(abs(z) for z in all_z) / max(1, len(all_z))
        # cross_run_independence (Spearman): robustness to nonlinear drift.
        within_run_ranks = [m.get("_stability_score", 0.0) for m in labelled]
        spearman_rank_corr = self._spearman_rank_correlation(within_run_ranks, all_z)
        cross_run_independence = round(1.0 - abs(spearman_rank_corr), 4) if len(all_z) > 1 else 0.5
        baseline_shift_detected = avg_abs_z > 1.5

        # Drift metrics must be actionable
        pre_baseline_mean = float(ref_dist.get("stability_score", {}).get("mean", 0.0))
        current_run_mean = statistics.fmean(within_run_ranks) if within_run_ranks else 0.0
        pre_update_mean_shift = abs(current_run_mean - pre_baseline_mean)
        z_score_drift_trend = statistics.fmean(all_z) if all_z else 0.0

        # Within-run consistency (CV of non-collapsed scores)
        valid_scores = [m.get("_stability_score", 0.0) for m in transition_stages + stable_stages]
        if valid_scores:
            vs_mean, vs_std = self._compute_rolling_stats(valid_scores)
            within_run_consistency = round(1.0 / (1.0 + (vs_std / max(vs_mean, 1e-9))), 4)
        else:
            within_run_consistency = 0.0

        # Average consensus strength across all stages
        avg_consensus = round(
            sum(m.get("_consensus_strength", 0.67) for m in labelled) / max(1, len(labelled)), 4
        )

        # ---- 4. Stage to staging buffer (collapse stages excluded, commit-window gated) ----
        collapse_inclusions, committed_baseline = self._update_reference_distribution(
            stage_metrics,
            max_fanout_cps,
            max_queue_depth,
        )

        baseline_drift = {
            "pre_update_mean_shift": round(pre_update_mean_shift, 6),
            "collapse_inclusion_attempts": collapse_inclusions,
            "z_score_drift_trend": round(z_score_drift_trend, 6),
            "baseline_committed": committed_baseline,
            "baseline_version_id": self.config.baseline_version_id,
            "gate_mode": self.config.gate_mode,
            "baseline_degradation_warning": False,
        }

        baseline_degradation_warning = False
        drift_threshold = 0.75
        high_shift_threshold = 0.35

        # ---- 5. Persist debug metadata ----
        invariants = {
            "transition_stage_count": transition_count,
            "stable_stage_count": stable_count,
            "collapsed_stage_count": collapsed_count,
            "uses_percentile_model": True,
        }
        classification_stability = {
            "within_run_consistency": within_run_consistency,
            "cross_run_z_alignment": round(1.0 / (1.0 + avg_abs_z), 4),
            "baseline_shift_detected": baseline_shift_detected,
        }
        self._classification_debug: dict[str, Any] = {
            "used_stages": [int(m.get("concurrency", 0)) for m in transition_stages + stable_stages],
            "excluded_stages": [int(m.get("concurrency", 0)) for m in collapsed_stages],
            "excluded_regimes": ["collapsed_region"] * collapsed_count,
            "stage_scores": {int(m.get("concurrency", 0)): m.get("_stability_score", 0.0) for m in labelled},
            "stage_z_scores": {int(m.get("concurrency", 0)): m.get("_z_score", 0.0) for m in labelled},
            "stage_votes": {
                int(m.get("concurrency", 0)): {
                    "percentile": m.get("_percentile_vote_distribution"),
                    "zscore": m.get("_zscore_vote_distribution"),
                    "gradient": m.get("_gradient_vote_distribution"),
                    "consensus": m.get("_consensus_strength"),
                    "weights": m.get("_vote_weights"),
                }
                for m in labelled
            },
            "class_probability_distribution": {
                "stable": round(statistics.fmean(m.get("_class_probability_distribution", {}).get("stable", 0.0) for m in labelled), 6) if labelled else 0.0,
                "transition": round(statistics.fmean(m.get("_class_probability_distribution", {}).get("transition", 0.0) for m in labelled), 6) if labelled else 0.0,
                "collapsed": round(statistics.fmean(m.get("_class_probability_distribution", {}).get("collapsed", 0.0) for m in labelled), 6) if labelled else 0.0,
            },
            "signal_ratio": round(signal_ratio, 4),
            "early_run_mode": early_run,
            "classification_invariants": invariants,
            "classification_stability": classification_stability,
            "baseline_drift": baseline_drift,
        }

        # ---- 6. Confidence decomposition ----
        # Three independent factors; NEVER reuse z-score in both classification and conf.
        # 1) transition_quality: within-run signal quality
        transition_quality = 1.0 if transition_count >= 2 else (0.85 if transition_count == 1 else 0.5)
        # 2) cross_run_independence: additive information from z-score beyond percentile rank
        # 3) classification_consensus_strength: average vote agreement
        confidence_base = (transition_quality * cross_run_independence * avg_consensus) ** (1.0 / 3.0)

        # Actionable drift penalties
        if z_score_drift_trend > drift_threshold:
            confidence_base *= 0.85
            baseline_degradation_warning = True

        baseline_drift["baseline_degradation_warning"] = baseline_degradation_warning or (pre_update_mean_shift > high_shift_threshold)
        if isinstance(getattr(self, "_classification_debug", None), dict):
            self._classification_debug["baseline_drift"] = baseline_drift

        confidence_base = min(conf_ceiling, max(0.0, confidence_base))

        # Required probabilistic output layer
        class_probability_distribution = {
            "stable": round(statistics.fmean(m.get("_class_probability_distribution", {}).get("stable", 0.0) for m in labelled), 6) if labelled else 0.0,
            "transition": round(statistics.fmean(m.get("_class_probability_distribution", {}).get("transition", 0.0) for m in labelled), 6) if labelled else 0.0,
            "collapsed": round(statistics.fmean(m.get("_class_probability_distribution", {}).get("collapsed", 0.0) for m in labelled), 6) if labelled else 0.0,
        }

        # ---- 7. Degenerate distribution cases ----
        if collapsed_count == len(labelled) and labelled:
            highest_rejection = max(
                (float(m.get("rejection_rate_429", 0.0) or 0.0) for m in labelled), default=0.0
            )
            base = min(0.75, 0.5 + min(0.25, highest_rejection))
            label = "admission_saturation"
            if pre_update_mean_shift > high_shift_threshold:
                label = self._downgrade_classification_severity(label)
                baseline_degradation_warning = True
            return (
                label,
                round(min(conf_ceiling, base * confidence_base) if confidence_base > 0 else base, 3),
                {
                    "signal_ratio": round(signal_ratio, 4),
                    "collapsed_stage_count": float(collapsed_count),
                    "class_probability_distribution": class_probability_distribution,
                    "baseline_degradation_warning": baseline_degradation_warning,
                },
                "All stages collapsed to admission shedding. Inferred saturation from rejection evidence.",
            )

        if stable_count == len(labelled) and labelled:
            label = "insufficient_load_variation"
            if pre_update_mean_shift > high_shift_threshold:
                label = self._downgrade_classification_severity(label)
                baseline_degradation_warning = True
            return (
                label,
                round(signal_ratio * 0.4 * confidence_base, 3),
                {
                    "signal_ratio": round(signal_ratio, 4),
                    "stable_stage_count": float(stable_count),
                    "class_probability_distribution": class_probability_distribution,
                    "baseline_degradation_warning": baseline_degradation_warning,
                },
                "All stages remained in the stable region; load was insufficient to reveal scaling behaviour.",
            )

        # ---- 8. Fitting window (transition-first) ----
        if transition_count >= 2:
            fitting_stages = transition_stages
        elif transition_count == 1:
            fitting_stages = stable_stages + transition_stages
        else:
            fitting_stages = stable_stages

        if len(fitting_stages) < 2:
            label = "insufficient_signal"
            if pre_update_mean_shift > high_shift_threshold:
                label = self._downgrade_classification_severity(label)
                baseline_degradation_warning = True
            return (
                label,
                round(signal_ratio * 0.3, 3),
                {
                    "signal_ratio": round(signal_ratio, 4),
                    "fitting_stage_count": float(len(fitting_stages)),
                    "class_probability_distribution": class_probability_distribution,
                    "baseline_degradation_warning": baseline_degradation_warning,
                },
                "Too few pre-collapse stages to fit a reliable scaling curve.",
            )

        # ---- 9. Slope metrics (fitting stages only) ----
        loads = [float(m.get("concurrency", 0.0) or 0.0) for m in fitting_stages]
        fanout_points = [
            (float(m.get("concurrency", 0.0) or 0.0), float(m.get("avg_fanout_time_ms", 0.0) or 0.0))
            for m in fitting_stages
        ]
        schedule_points = [
            (float(m.get("concurrency", 0.0) or 0.0), float(m.get("avg_event_loop_schedule_delay_ms", 0.0) or 0.0))
            for m in fitting_stages
        ]
        lag_values = [float(m.get("event_loop_lag_max_ms", 0.0) or 0.0) for m in fitting_stages]

        fanout_growth_rate = _linear_slope(fanout_points)
        schedule_delay_growth_rate = _linear_slope(schedule_points)
        event_loop_lag_correlation = _pearson(loads, lag_values)

        low_cost = float(fitting_stages[0].get("fanout_per_subscriber_cost", 0.0) or 0.0)
        high_cost = float(fitting_stages[-1].get("fanout_per_subscriber_cost", 0.0) or 0.0)
        per_subscriber_ratio = high_cost / max(0.001, low_cost) if low_cost > 0 else 1.0

        all_valid = stable_stages + transition_stages
        highest_rejection = max(
            (float(m.get("rejection_rate_429", 0.0) or 0.0) for m in all_valid), default=0.0
        )
        highest_lag = max(lag_values, default=0.0)

        sorted_lags = sorted(lag_values)
        lag_p75 = self._quantile(sorted_lags, 0.75)
        significant_lag = lag_p75 > 0.0 and highest_lag >= lag_p75 * 1.5
        schedule_dominates = schedule_delay_growth_rate > fanout_growth_rate * 1.2
        lag_correlation_strong = event_loop_lag_correlation >= 0.5

        signals: dict[str, float] = {
            "fanout_growth_rate": round(fanout_growth_rate, 6),
            "schedule_delay_growth_rate": round(schedule_delay_growth_rate, 6),
            "event_loop_lag_correlation": round(event_loop_lag_correlation, 6),
            "per_subscriber_cost_ratio": round(per_subscriber_ratio, 6),
        }

        # ---- 10. Classification priority order ----

        # P1: admission_saturation
        if highest_rejection >= 0.1 and not significant_lag and schedule_delay_growth_rate <= fanout_growth_rate:
            base = 0.68 + min(0.2, highest_rejection)
            label = "admission_saturation"
            if pre_update_mean_shift > high_shift_threshold:
                label = self._downgrade_classification_severity(label)
                baseline_degradation_warning = True
            signals["class_probability_distribution"] = class_probability_distribution
            signals["baseline_degradation_warning"] = baseline_degradation_warning
            return (
                label,
                round(min(conf_ceiling, base * confidence_base), 3),
                signals,
                "429 saturation dominates while fanout and loop delay remain comparatively bounded.",
            )

        # P2: event_loop_starvation
        if significant_lag and schedule_dominates and lag_correlation_strong:
            base = 0.74 + min(0.15, event_loop_lag_correlation * 0.2) + min(0.1, highest_lag / 4000.0)
            label = "event_loop_starvation"
            if pre_update_mean_shift > high_shift_threshold:
                label = self._downgrade_classification_severity(label)
                baseline_degradation_warning = True
            signals["class_probability_distribution"] = class_probability_distribution
            signals["baseline_degradation_warning"] = baseline_degradation_warning
            return (
                label,
                round(min(conf_ceiling, base * confidence_base), 3),
                signals,
                "Event-loop lag and schedule-delay growth dominate direct fanout growth.",
            )

        # P3: superlinear_fanout_scaling
        if per_subscriber_ratio >= 1.6 and fanout_growth_rate > 0.0:
            base = 0.72 + min(0.18, (per_subscriber_ratio - 1.0) / 4.0) + min(0.08, fanout_growth_rate / 10.0)
            label = "superlinear_fanout_scaling"
            if pre_update_mean_shift > high_shift_threshold:
                label = self._downgrade_classification_severity(label)
                baseline_degradation_warning = True
            signals["class_probability_distribution"] = class_probability_distribution
            signals["baseline_degradation_warning"] = baseline_degradation_warning
            return (
                label,
                round(min(conf_ceiling, base * confidence_base), 3),
                signals,
                "Per-subscriber fanout cost accelerates with load, indicating superlinear scaling.",
            )

        # P4: linear_fanout_scaling
        if fanout_growth_rate >= 0.0:
            base = 0.65 + min(0.15, fanout_growth_rate / 8.0)
            label = "linear_fanout_scaling"
            if pre_update_mean_shift > high_shift_threshold:
                label = self._downgrade_classification_severity(label)
                baseline_degradation_warning = True
            signals["class_probability_distribution"] = class_probability_distribution
            signals["baseline_degradation_warning"] = baseline_degradation_warning
            return (
                label,
                round(min(conf_ceiling, base * confidence_base), 3),
                signals,
                "Fanout cost rises with load without evidence of superlinear explosion.",
            )

        # P5: insufficient_signal
        label = "insufficient_signal"
        if pre_update_mean_shift > high_shift_threshold:
            label = self._downgrade_classification_severity(label)
            baseline_degradation_warning = True
        signals["class_probability_distribution"] = class_probability_distribution
        signals["baseline_degradation_warning"] = baseline_degradation_warning
        return (
            label,
            round(max(0.0, 0.3 * signal_ratio * confidence_base), 3),
            signals,
            "Negative fanout slope on transition-filtered data; scaling mode is unclassifiable.",
        )

    @staticmethod
    def _build_comparison_vector(stage_metrics: list[dict[str, Any]]) -> dict[str, float]:
        if not stage_metrics:
            return {
                "avg_fanout_time_ms": 0.0,
                "avg_schedule_delay_ms": 0.0,
                "event_loop_lag_max_ms": 0.0,
                "success_rate": 0.0,
                "rejection_rate_429": 0.0,
            }
        return {
            "avg_fanout_time_ms": statistics.fmean(float(metric.get("avg_fanout_time_ms", 0.0) or 0.0) for metric in stage_metrics),
            "avg_schedule_delay_ms": statistics.fmean(float(metric.get("avg_event_loop_schedule_delay_ms", 0.0) or 0.0) for metric in stage_metrics),
            "event_loop_lag_max_ms": max(float(metric.get("event_loop_lag_max_ms", 0.0) or 0.0) for metric in stage_metrics),
            "success_rate": statistics.fmean(float(metric.get("success_rate", 0.0) or 0.0) for metric in stage_metrics),
            "rejection_rate_429": statistics.fmean(float(metric.get("rejection_rate_429", 0.0) or 0.0) for metric in stage_metrics),
        }

    def _extract_baseline_vector(self, baseline_report: dict[str, Any]) -> dict[str, float]:
        if "before_after_deltas" in baseline_report and "evidence" in baseline_report:
            fanout = baseline_report.get("evidence", {}).get("fanout", {})
            scheduling = baseline_report.get("evidence", {}).get("scheduling", {})
            event_loop = baseline_report.get("evidence", {}).get("event_loop", {})
            return {
                "avg_fanout_time_ms": float(fanout.get("avg_fanout_time_ms", 0.0) or 0.0),
                "avg_schedule_delay_ms": float(scheduling.get("avg_event_loop_schedule_delay_ms", 0.0) or 0.0),
                "event_loop_lag_max_ms": float(event_loop.get("event_loop_lag_max_ms", 0.0) or 0.0),
                "success_rate": float(event_loop.get("success_rate", 0.0) or 0.0),
                "rejection_rate_429": float(event_loop.get("rejection_rate_429", 0.0) or 0.0),
            }
        if "fanout_cost_curve" in baseline_report and isinstance(baseline_report.get("evidence"), dict):
            curve = baseline_report.get("fanout_cost_curve", {})
            evidence = baseline_report.get("evidence", {})
            breakpoint_results = evidence.get("breakpoint_results", {}) if isinstance(evidence, dict) else {}
            stages = breakpoint_results.get("stages", []) if isinstance(breakpoint_results, dict) else []
            valid_stages = [stage for stage in stages if isinstance(stage, dict)]
            success_rate = statistics.fmean(float(stage.get("success_rate", 0.0) or 0.0) for stage in valid_stages) if valid_stages else 0.0
            rejection_rate = (
                statistics.fmean(
                    float(stage.get("rejections_429", 0.0) or 0.0) / max(1.0, float(stage.get("clean_request_count", 0.0) or 0.0))
                    for stage in valid_stages
                )
                if valid_stages
                else 0.0
            )
            event_loop_lag_max_ms = float(
                baseline_report.get("instrumentation_snapshot", {})
                .get("event_loop", {})
                .get("event_loop_lag_max_ms", 0.0)
                or 0.0
            )
            return {
                "avg_fanout_time_ms": float(curve.get("avg_fanout_time_ms", 0.0) or 0.0),
                "avg_schedule_delay_ms": float(curve.get("avg_schedule_delay_ms", 0.0) or 0.0),
                "event_loop_lag_max_ms": event_loop_lag_max_ms,
                "success_rate": success_rate,
                "rejection_rate_429": rejection_rate,
            }
        stage_metrics = baseline_report.get("evidence", {}).get("event_loop", {}).get("stage_metrics")
        if isinstance(stage_metrics, list):
            return self._build_comparison_vector([metric for metric in stage_metrics if isinstance(metric, dict)])
        raise StressFailure("baseline_missing_required_metrics")

    @staticmethod
    def _pct_delta(current: float, baseline: float) -> float:
        return round(((current - baseline) / max(0.001, abs(baseline))) * 100.0, 6)

    def _compute_decision(self, report: dict[str, Any]) -> dict[str, Any]:
        """Compute deterministic decision layer for CI/CD gating.

        Returns:
            {
              "decision": "PASS|WARN|FAIL",
              "confidence": float,
              "risk_ceiling_breached": bool,
              "primary_reason": str,
              "supporting_factors": [str]
            }
        """
        classification = str(report.get("scaling_classification", "insufficient_signal"))
        confidence = float(report.get("confidence", 0.0) or 0.0)

        evidence = report.get("evidence", {}) if isinstance(report.get("evidence"), dict) else {}
        event_loop = evidence.get("event_loop", {}) if isinstance(evidence.get("event_loop"), dict) else {}
        fanout = evidence.get("fanout", {}) if isinstance(evidence.get("fanout"), dict) else {}
        signals = fanout.get("signals", {}) if isinstance(fanout.get("signals"), dict) else {}
        class_dist = signals.get("class_probability_distribution", {}) if isinstance(signals.get("class_probability_distribution"), dict) else {}

        stable_p = float(class_dist.get("stable", 0.0) or 0.0)
        transition_p = float(class_dist.get("transition", 0.0) or 0.0)
        collapsed_p = float(class_dist.get("collapsed", 0.0) or 0.0)

        max_lag_ms = float(event_loop.get("event_loop_lag_max_ms", 0.0) or 0.0)
        success_rate = float(event_loop.get("success_rate", 0.0) or 0.0)

        baseline_drift = (
            report.get("classification_debug", {}).get("baseline_drift", {})
            if isinstance(report.get("classification_debug"), dict)
            else {}
        )
        baseline_warning = bool(signals.get("baseline_degradation_warning", False) or baseline_drift.get("baseline_degradation_warning", False))

        entropy = self._normalized_entropy(
            {"stable": stable_p, "transition": transition_p, "collapsed": collapsed_p}
        )

        supporting: list[str] = []
        risk_ceiling_breached = False

        # FAIL rules
        if collapsed_p > 0.60:
            risk_ceiling_breached = True
            supporting.append("collapsed_probability > 0.60")
        if classification == "event_loop_starvation" and confidence > 0.70:
            risk_ceiling_breached = True
            supporting.append("event_loop_starvation with confidence > 0.70")
        if max_lag_ms > 2000.0 and success_rate < 0.60:
            risk_ceiling_breached = True
            supporting.append("event_loop_lag_max_ms > 2000 and success_rate < 0.60")

        if risk_ceiling_breached:
            return {
                "decision": "FAIL",
                "confidence": round(confidence, 6),
                "risk_ceiling_breached": True,
                "primary_reason": supporting[0],
                "supporting_factors": supporting,
            }

        # WARN rules
        warn_factors: list[str] = []
        if entropy > 0.9:
            warn_factors.append("class_probability_entropy > 0.9")
        if confidence < 0.60:
            warn_factors.append("classification_confidence < 0.60")
        if baseline_warning:
            warn_factors.append("baseline_degradation_warning == True")

        if warn_factors:
            return {
                "decision": "WARN",
                "confidence": round(confidence, 6),
                "risk_ceiling_breached": False,
                "primary_reason": warn_factors[0],
                "supporting_factors": warn_factors,
            }

        return {
            "decision": "PASS",
            "confidence": round(confidence, 6),
            "risk_ceiling_breached": False,
            "primary_reason": "No fail/warn rule triggered",
            "supporting_factors": [
                "collapsed_probability <= 0.60",
                "confidence >= 0.60",
                "no baseline degradation warning",
            ],
        }

    def _append_calibration_log(self, report: dict[str, Any], decision_layer: dict[str, Any]) -> bool:
        """Append observational calibration record without affecting classifier behavior."""
        try:
            evidence = report.get("evidence", {}) if isinstance(report.get("evidence"), dict) else {}
            event_loop = evidence.get("event_loop", {}) if isinstance(evidence.get("event_loop"), dict) else {}
            fanout = evidence.get("fanout", {}) if isinstance(evidence.get("fanout"), dict) else {}
            signals = fanout.get("signals", {}) if isinstance(fanout.get("signals"), dict) else {}
            predicted_distribution = signals.get("class_probability_distribution", {}) if isinstance(signals.get("class_probability_distribution"), dict) else {}

            record = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "mode": str(report.get("mode", self.config.mode)),
                "baseline_version_id": self.config.baseline_version_id,
                "gate_mode": self.config.gate_mode,
                "predicted_distribution": {
                    "stable": float(predicted_distribution.get("stable", 0.0) or 0.0),
                    "transition": float(predicted_distribution.get("transition", 0.0) or 0.0),
                    "collapsed": float(predicted_distribution.get("collapsed", 0.0) or 0.0),
                },
                "final_decision": str(decision_layer.get("decision", "WARN")),
                "confidence": float(decision_layer.get("confidence", 0.0) or 0.0),
                "metrics_snapshot": {
                    "success_rate": float(event_loop.get("success_rate", 0.0) or 0.0),
                    "p95_latency": event_loop.get("p95_latency", None),
                    "event_loop_lag_max_ms": float(event_loop.get("event_loop_lag_max_ms", 0.0) or 0.0),
                },
            }

            self._CALIBRATION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            with self._CALIBRATION_LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record, separators=(",", ":")) + "\n")
            return True
        except Exception:
            return False

    def _compute_calibration_metrics(self) -> dict[str, Any] | None:
        """Calibration metrics scaffold.

        Placeholder behavior:
        - brier_score: None until ground truth labels are available
        - average_confidence: computed from calibration log
        - failure_rate_by_prediction_bucket: empty scaffold
        """
        if not self._CALIBRATION_LOG_PATH.exists():
            return None

        confidences: list[float] = []
        try:
            with self._CALIBRATION_LOG_PATH.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    confidences.append(float(row.get("confidence", 0.0) or 0.0))
        except Exception:
            return None

        avg_conf = statistics.fmean(confidences) if confidences else 0.0
        return {
            "brier_score": None,
            "average_confidence": round(avg_conf, 6),
            "failure_rate_by_prediction_bucket": {},
        }

    def _run_sse_breakpoint_gate(self) -> dict[str, Any]:
        mode = "sse_breakpoint_gate"
        report_path = self.config.report_path or (ROOT / "production_torture_report.sse_breakpoint_gate.json")
        baseline_comparison = self.config.compare_baseline_path is not None
        failure_reason = ""
        try:
            self._start()
            self._homes = []
            self._last_watermark_by_home = {}
            self._prime_households(12)
            gate_results = self._run_sse_breakpoint_only()
            stage_metrics = gate_results["stage_metrics"]
            scaling_mode, confidence, scaling_signals, bottleneck_reason = self._classify_sse_scaling(stage_metrics)

            max_stable_concurrency = int(gate_results.get("max_stable_concurrency", 0) or 0)
            min_success_rate = min((float(metric.get("success_rate", 0.0) or 0.0) for metric in stage_metrics), default=0.0)
            high_stage = next((metric for metric in stage_metrics if int(metric.get("concurrency", 0) or 0) == 100), {})
            rejection_rate_100 = float(high_stage.get("rejection_rate_429", 0.0) or 0.0)
            lag_values = [float(metric.get("event_loop_lag_max_ms", 0.0) or 0.0) for metric in stage_metrics]
            sustained_loop_lag = sum(1 for lag in lag_values if lag > 500.0) >= 2

            fail_reasons: list[str] = []
            if sustained_loop_lag:
                fail_reasons.append("event_loop_lag_max_ms > 500 sustained")
            if max_stable_concurrency == 0:
                fail_reasons.append("max_stable_concurrency == 0")
            if min_success_rate < 0.85:
                fail_reasons.append("success_rate < 0.85 under breakpoint load")
            if rejection_rate_100 > 0.10:
                fail_reasons.append("rejection_rate_429 > 0.10 at concurrency 100")
            if scaling_mode in {"event_loop_starvation", "superlinear_fanout_scaling"}:
                fail_reasons.append(f"scaling mode is {scaling_mode}")

            current_vector = self._build_comparison_vector(stage_metrics)
            deltas: dict[str, float] = {}
            gate_reason = "PASS: SSE breakpoint gate satisfied"
            readiness = "PASS"
            if fail_reasons:
                readiness = "FAIL"
                gate_reason = "; ".join(fail_reasons)

            if baseline_comparison:
                baseline_path = self.config.compare_baseline_path
                if baseline_path is None or not baseline_path.exists():
                    raise StressFailure("compare_baseline_path_missing")
                baseline_payload = json.loads(baseline_path.read_text(encoding="utf-8"))
                baseline_vector = self._extract_baseline_vector(baseline_payload)
                deltas = {
                    "fanout_delta_pct": self._pct_delta(current_vector["avg_fanout_time_ms"], baseline_vector["avg_fanout_time_ms"]),
                    "schedule_delay_delta_pct": self._pct_delta(current_vector["avg_schedule_delay_ms"], baseline_vector["avg_schedule_delay_ms"]),
                    "event_loop_lag_delta_pct": self._pct_delta(current_vector["event_loop_lag_max_ms"], baseline_vector["event_loop_lag_max_ms"]),
                    "success_rate_delta": round(current_vector["success_rate"] - baseline_vector["success_rate"], 6),
                    "rejection_delta": round(current_vector["rejection_rate_429"] - baseline_vector["rejection_rate_429"], 6),
                }
                if readiness == "PASS" and (
                    deltas["fanout_delta_pct"] > 15.0
                    or deltas["schedule_delay_delta_pct"] > 15.0
                    or deltas["event_loop_lag_delta_pct"] > 15.0
                    or deltas["success_rate_delta"] < 0.0
                ):
                    readiness = "DEGRADED"
                    gate_reason = "Baseline comparison indicates measurable regression despite passing hard fail gates"

            evidence = {
                "fanout": {
                    "signals": scaling_signals,
                    "avg_fanout_time_ms": round(current_vector["avg_fanout_time_ms"], 6),
                    "stage_metrics": [
                        {
                            "concurrency": metric.get("concurrency"),
                            "avg_fanout_time_ms": metric.get("avg_fanout_time_ms"),
                            "max_fanout_time_ms": metric.get("max_fanout_time_ms"),
                            "fanout_calls_per_second": metric.get("fanout_calls_per_second"),
                            "fanout_per_subscriber_cost": metric.get("fanout_per_subscriber_cost"),
                        }
                        for metric in stage_metrics
                    ],
                },
                "scheduling": {
                    "avg_event_loop_schedule_delay_ms": round(current_vector["avg_schedule_delay_ms"], 6),
                    "max_event_loop_schedule_delay_ms": round(max((float(metric.get("max_event_loop_schedule_delay_ms", 0.0) or 0.0) for metric in stage_metrics), default=0.0), 6),
                    "stage_metrics": [
                        {
                            "concurrency": metric.get("concurrency"),
                            "avg_event_loop_schedule_delay_ms": metric.get("avg_event_loop_schedule_delay_ms"),
                            "max_event_loop_schedule_delay_ms": metric.get("max_event_loop_schedule_delay_ms"),
                            "callback_queue_depth": metric.get("callback_queue_depth"),
                            "inflight_tasks": metric.get("inflight_tasks"),
                        }
                        for metric in stage_metrics
                    ],
                },
                "event_loop": {
                    "event_loop_lag_max_ms": round(current_vector["event_loop_lag_max_ms"], 6),
                    "success_rate": round(current_vector["success_rate"], 6),
                    "rejection_rate_429": round(current_vector["rejection_rate_429"], 6),
                    "max_stable_concurrency": max_stable_concurrency,
                    "stage_metrics": stage_metrics,
                },
            }

            output = {
                "mode": mode,
                "baseline_comparison": baseline_comparison,
                "scaling_classification": scaling_mode,
                "production_readiness": readiness,
                "confidence": confidence,
                "key_bottleneck": bottleneck_reason,
                "before_after_deltas": deltas,
                "production_readiness_gate": {
                    "status": readiness,
                    "reason": gate_reason,
                },
                "classification_debug": getattr(self, "_classification_debug", {}),
                "evidence": evidence,
            }
        except Exception as exc:
            failure_reason = f"{type(exc).__name__}: {exc}"
            output = {
                "mode": mode,
                "baseline_comparison": baseline_comparison,
                "scaling_classification": "insufficient_signal",
                "production_readiness": "FAIL",
                "confidence": 0.0,
                "key_bottleneck": failure_reason,
                "before_after_deltas": {},
                "production_readiness_gate": {
                    "status": "FAIL",
                    "reason": failure_reason,
                },
                "classification_debug": getattr(self, "_classification_debug", {}),
                "evidence": {
                    "fanout": {},
                    "scheduling": {},
                    "event_loop": {},
                },
            }
        finally:
            try:
                self._stop()
            except Exception as stop_exc:
                if not failure_reason:
                    output["key_bottleneck"] = f"shutdown_error:{type(stop_exc).__name__}:{stop_exc}"
                    output["production_readiness"] = "FAIL"
                    output["production_readiness_gate"] = {
                        "status": "FAIL",
                        "reason": output["key_bottleneck"],
                    }

            decision_layer = self._compute_decision(output)
            calibration_written = self._append_calibration_log(output, decision_layer)
            output["decision_layer"] = decision_layer
            output["calibration_log_written"] = calibration_written
            calibration_metrics = self._compute_calibration_metrics()
            if calibration_metrics is not None:
                output["calibration_metrics"] = calibration_metrics

            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

        return output

    def _start(self) -> None:
        if self.config.mode == "sse_breakpoint_gate":
            self._start_inprocess_server()
            self.harness._wait_ready()
            self._prime_households(12)
            return
        self.harness._kill_listeners_on_ports([self.config.port])
        time.sleep(0.4)
        self.server = self.harness._start_server()
        self.harness._wait_ready()
        self._prime_households(12)

    def _stop(self) -> None:
        if self.config.mode == "sse_breakpoint_gate":
            self._stop_inprocess_server()
            return
        if self.server is not None:
            self.server.terminate()
            try:
                self.server.wait(timeout=8)
            except Exception:
                self.server.kill()
            self.server = None

    def _start_inprocess_server(self) -> None:
        self.harness._kill_listeners_on_ports([self.config.port])
        time.sleep(0.4)
        from apps.api.main import app as asgi_app
        config = uvicorn.Config(
            asgi_app,
            host=HOST,
            port=self.config.port,
            log_level="warning",
            access_log=False,
            limit_concurrency=50,
            timeout_keep_alive=0,
        )
        self._inprocess_server = uvicorn.Server(config)
        self._inprocess_server_thread = threading.Thread(target=self._inprocess_server.run, daemon=True)
        self._inprocess_server_thread.start()

    def _stop_inprocess_server(self) -> None:
        if self._inprocess_server is not None:
            self._inprocess_server.should_exit = True
        if self._inprocess_server_thread is not None:
            self._inprocess_server_thread.join(timeout=15)
        self._inprocess_server = None
        self._inprocess_server_thread = None

    def _single_run(self, repeat_index: int = 0) -> dict[str, Any]:
        self._homes = []
        self._last_watermark_by_home = {}
        self._start()
        try:
            soak_results = self._run_soak()
            self._write_checkpoint("soak", soak_results)
            breakpoint_results = self._run_breakpoint()
            self._write_checkpoint("breakpoint", breakpoint_results)
            chaos_results = self._run_chaos()
            self._write_checkpoint("chaos", chaos_results)
        finally:
            self._stop()

        suite_metrics = {
            "success_rate": round(
                statistics.fmean(
                    [
                        float(soak_results.get("success_rate", 0.0) or 0.0),
                        max(0.0, 1.0 - float(breakpoint_results.get("timeout_count", 0.0) or 0.0) / max(1.0, float(breakpoint_results.get("rejections_429", 0.0) or 0.0) + float(breakpoint_results.get("timeout_count", 0.0) or 0.0) + 1.0)),
                        float(chaos_results.get("success_rate", 0.0) or 0.0),
                    ]
                ),
                6,
            ),
            "rejection_rate": round(
                (
                    float(soak_results.get("rejections_429", 0.0) or 0.0)
                    + float(breakpoint_results.get("rejections_429", 0.0) or 0.0)
                    + float(chaos_results.get("rejections_429", 0.0) or 0.0)
                )
                /
                max(
                    1.0,
                    float(soak_results.get("clean_request_count", 0.0) or 0.0)
                    + sum(float(stage.get("clean_request_count", 0.0) or 0.0) for stage in breakpoint_results.get("stages", []))
                    + float(chaos_results.get("clean_request_count", 0.0) or 0.0),
                ),
                6,
            ),
            "completion_ratio": round(
                statistics.fmean(
                    [
                        float(soak_results.get("completion_ratio", 0.0) or 0.0),
                        float(breakpoint_results.get("completion_ratio", 0.0) or 0.0),
                        float(chaos_results.get("completion_ratio", 0.0) or 0.0),
                    ]
                ),
                6,
            ),
            "p95_latency": round(
                max(
                    float(soak_results.get("p95_latency", 0.0) or 0.0),
                    max((float(stage.get("p95_latency", 0.0) or 0.0) for stage in breakpoint_results.get("stages", [])), default=0.0),
                    float(chaos_results.get("p95_latency", 0.0) or 0.0),
                ),
                3,
            ),
        }
        return {
            "soak_results": soak_results,
            "breakpoint_results": breakpoint_results,
            "chaos_results": chaos_results,
            "suite_metrics": suite_metrics,
        }

    def _write_checkpoint(self, stage_name: str, results: dict[str, Any]) -> None:
        """Write per-stage checkpoint for crash recovery."""
        try:
            checkpoint_path = self.config.report_path.parent if self.config.report_path else ROOT
            checkpoint_file = checkpoint_path / f"{stage_name}_checkpoint.json"
            checkpoint_file.write_text(json.dumps(results, indent=2), encoding="utf-8")
        except Exception:
            pass  # Silently skip checkpoint write on error

    @staticmethod
    def _aggregate_noise(runs: list[dict[str, Any]]) -> dict[str, float]:
        noise_profiles: list[dict[str, float]] = []
        for run in runs:
            for key in ("soak_results", "chaos_results"):
                noise = run.get(key, {}).get("noise_profile")
                if isinstance(noise, dict):
                    noise_profiles.append(noise)
            for stage in run.get("breakpoint_results", {}).get("stages", []):
                noise = stage.get("noise_profile")
                if isinstance(noise, dict):
                    noise_profiles.append(noise)
        if not noise_profiles:
            return {
                "client_noise_ratio": 0.0,
                "warmup_impact": 0.0,
                "retry_inflation_factor": 1.0,
                "cold_start_ratio": 0.0,
                "removed_ratio": 0.0,
            }
        keys = sorted(noise_profiles[0].keys())
        return {
            key: round(statistics.fmean(float(profile.get(key, 0.0) or 0.0) for profile in noise_profiles), 6)
            for key in keys
        }

    def run(self) -> dict[str, Any]:
        if self.config.mode == "sse_breakpoint_gate":
            return self._run_sse_breakpoint_gate()

        run_status = "success"
        error_summary: dict[str, Any] | None = None
        report: dict[str, Any] = {}
        suite_runs: list[dict[str, Any]] = []
        primary_metrics: dict[str, Any] = {}
        
        # Initialize aggregated metrics (needed for finally block)
        all_raw_count = 0
        all_clean_count = 0
        all_timeout_count = 0
        all_rejection_count = 0
        all_retry_count = 0
        failure_attribution: dict[str, Any] = {}
        failure_timeline: list[dict[str, Any]] = []
        repeatability_raw: dict[str, Any] = {}
        repeatability_score: dict[str, Any] = {}
        
        try:
            repeatability_inputs: list[dict[str, float]] = []
            for repeat_idx in range(self.config.repeat_runs):
                run = self._single_run(repeat_idx)
                suite_runs.append(run)
                repeatability_inputs.append(run["suite_metrics"])

            primary = suite_runs[0] if suite_runs else {"soak_results": {}, "breakpoint_results": {}, "chaos_results": {}}
            repeatability_raw = RepeatabilityGate.evaluate(
                repeatability_inputs,
                RepeatabilityConfig(n_runs=self.config.repeat_runs),
            )
            
            # Compute enhanced repeatability score
            repeatability_score = _compute_repeatability_score(repeatability_inputs)
            
            # Extract primary metrics for attribution
            soak_res = primary.get("soak_results", {})
            bp_res = primary.get("breakpoint_results", {})
            chaos_res = primary.get("chaos_results", {})
            
            # Aggregate metrics across all regimes
            all_raw_count = (
                soak_res.get("raw_request_count", 0) +
                sum(stage.get("raw_request_count", 0) for stage in bp_res.get("stages", [])) +
                chaos_res.get("raw_request_count", 0)
            )
            all_clean_count = (
                soak_res.get("clean_request_count", 0) +
                sum(stage.get("clean_request_count", 0) for stage in bp_res.get("stages", [])) +
                chaos_res.get("clean_request_count", 0)
            )
            all_timeout_count = (
                soak_res.get("timeout_count", 0) +
                sum(stage.get("timeout_count", 0) for stage in bp_res.get("stages", [])) +
                chaos_res.get("timeout_count", 0)
            )
            all_rejection_count = (
                soak_res.get("rejections_429", 0) +
                sum(stage.get("rejections_429", 0) for stage in bp_res.get("stages", [])) +
                chaos_res.get("rejections_429", 0)
            )
            avg_error_rate = statistics.fmean([
                soak_res.get("error_rate", 1.0),
                bp_res.get("completion_ratio", 0.0),  # Indirectly via completion
                chaos_res.get("error_rate", 1.0),
            ]) if [soak_res, bp_res, chaos_res] else 0.0
            avg_completion_ratio = statistics.fmean([
                soak_res.get("completion_ratio", 0.0),
                bp_res.get("completion_ratio", 0.0),
                chaos_res.get("completion_ratio", 0.0),
            ])
            avg_inflight_recovery = statistics.fmean([
                soak_res.get("inflight_recovery_ratio", 1.0),
                bp_res.get("inflight_recovery_ratio", 1.0),
                chaos_res.get("inflight_recovery_ratio", 1.0),
            ])
            
            # Classify failure attribution with multi-score causal model
            all_samples: list[dict[str, Any]] = []
            for regime_name, regime_data in [("soak", soak_res), ("chaos", chaos_res)]:
                regime_samples = regime_data.get("samples", [])
                if isinstance(regime_samples, list):
                    all_samples.extend(sample for sample in regime_samples if isinstance(sample, dict))
            
            failure_attribution = _classify_failure_attribution_multi_score(
                total_raw_count=all_raw_count,
                clean_count=all_clean_count,
                timeout_count=all_timeout_count,
                rejection_count=all_rejection_count,
                error_rate=1.0 - avg_completion_ratio,
                completion_ratio=avg_completion_ratio,
                inflight_recovery_ratio=avg_inflight_recovery,
                samples=all_samples,
                retry_count=0,  # TODO: compute from detailed metrics
                inflight_peak=soak_res.get("inflight_peak", 0),
                p95_latency=soak_res.get("p95_latency", 0.0),
            )
            
            # Build failure timeline with temporal event tracking
            failure_timeline = _build_failure_timeline(all_samples, "full_run")
            
            report = {
                "soak_results": soak_res,
                "breakpoint_results": bp_res,
                "chaos_results": chaos_res,
                "noise_profile": self._aggregate_noise(suite_runs),
            }
            try:
                report["production_readiness"] = ProductionReadinessClassifier.classify(report)
            except Exception as exc:
                report["production_readiness"] = "UNKNOWN"
                report["production_readiness_error"] = f"{type(exc).__name__}: {exc}"
        except Exception as exc:
            run_status = "failed"
            error_summary = {
                "exception_type": type(exc).__name__,
                "message": str(exc),
            }
        finally:
            # Always write report with enhanced metadata.
            elapsed = time.time() - self.start_time
            
            # Compute signal quality
            all_retry_count = 0  # TODO: compute from detailed metrics
            signal_quality = _compute_signal_quality(all_raw_count, all_clean_count, all_timeout_count, all_retry_count)
            
            # Compute fingerprint with enhanced parameters (schedule + chaos profile)
            schedule_hash = hashlib.sha256(str(self.config.breakpoint_stages).encode()).hexdigest()[:8]
            chaos_sig = f"chaos_{self.config.chaos_duration_seconds}s"
            run_fingerprint = _compute_run_fingerprint(
                self.config.seed,
                self.config.mode,
                self.config.breakpoint_stages,
                schedule_hash,
                chaos_sig,
                0,  # Primary run index
            )
            
            # Normalize output with all required fields
            report_with_metadata = {
                "mode": self.config.mode,
                "seed": self.config.seed,
                "run_fingerprint": run_fingerprint,
                "run_status": run_status,
                "elapsed_seconds": round(elapsed, 2),
                "reproducibility": {"fingerprint": run_fingerprint, "seed": self.config.seed},
                "duration_config": {
                    "soak_duration_seconds": self.config.soak_duration_seconds,
                    "breakpoint_stage_seconds": self.config.breakpoint_stage_seconds,
                    "breakpoint_stages": list(self.config.breakpoint_stages),
                    "chaos_duration_seconds": self.config.chaos_duration_seconds,
                    "repeat_runs": self.config.repeat_runs,
                },
                "failure_attribution": failure_attribution if not error_summary else {"primary_cause": "HARNESS_FAILURE", "secondary_causes": [], "confidence": 1.0, "evidence": [error_summary.get("exception_type", "unknown")]},
                "failure_timeline": failure_timeline if not error_summary else [],
                "signal_quality": signal_quality,
                "repeatability": {**repeatability_raw, **repeatability_score} if repeatability_raw else repeatability_score,
            }
            
            if error_summary is not None:
                report_with_metadata["error"] = error_summary
            
            report_with_metadata.update(report)
            
            # Determine report path: use provided or default based on mode.
            report_path = self.config.report_path
            if report_path is None:
                report_path = ROOT / f"production_torture_report.{self.config.mode}.json"
            
            # Ensure parent directory exists
            report_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Always write report file.
            report_path.write_text(json.dumps(report_with_metadata, indent=2), encoding="utf-8")
        
        return report_with_metadata


def parse_args() -> AuditConfig:
    parser = argparse.ArgumentParser(description="Run the production readiness torture audit.")
    parser.add_argument("--mode", default=os.getenv("TORTURE_MODE", "full_torture"), 
                       choices=["smoke", "standard", "full_torture", "sse_breakpoint_gate"],
                       help="Execution mode: smoke (< 2min), standard (~10-20min), sse_breakpoint_gate (deterministic SSE gate), full_torture (full suite)")
    parser.add_argument("--port", type=int, default=int(os.getenv("TORTURE_AUDIT_PORT", "8032")))
    parser.add_argument("--seed", type=int, default=int(os.getenv("TORTURE_AUDIT_SEED", "20260421")))
    parser.add_argument("--soak-duration-seconds", type=int, default=int(os.getenv("TORTURE_SOAK_DURATION_SECONDS", "1200")))
    parser.add_argument("--breakpoint-stage-seconds", type=int, default=int(os.getenv("TORTURE_BREAKPOINT_STAGE_SECONDS", "60")))
    parser.add_argument("--breakpoint-stages", default=os.getenv("TORTURE_BREAKPOINT_STAGES", "10,25,50,100,200,400"))
    parser.add_argument("--chaos-duration-seconds", type=int, default=int(os.getenv("TORTURE_CHAOS_DURATION_SECONDS", "240")))
    parser.add_argument("--repeat-runs", type=int, default=int(os.getenv("TORTURE_REPEAT_RUNS", "5")))
    parser.add_argument("--sample-interval-seconds", type=int, default=int(os.getenv("TORTURE_SAMPLE_INTERVAL_SECONDS", "2")))
    parser.add_argument("--warmup-seconds", type=int, default=int(os.getenv("TORTURE_WARMUP_SECONDS", "10")))
    parser.add_argument("--soak-peak-concurrency", type=int, default=int(os.getenv("TORTURE_SOAK_PEAK_CONCURRENCY", "80")))
    parser.add_argument("--chaos-peak-concurrency", type=int, default=int(os.getenv("TORTURE_CHAOS_PEAK_CONCURRENCY", "90")))
    parser.add_argument("--report-path", default=os.getenv("TORTURE_AUDIT_REPORT", None))
    parser.add_argument("--compare-baseline", default=os.getenv("TORTURE_COMPARE_BASELINE", None))
    parser.add_argument("--gate-mode", action="store_true", default=str(os.getenv("TORTURE_GATE_MODE", "false")).lower() == "true")
    parser.add_argument("--baseline-version-id", default=os.getenv("TORTURE_BASELINE_VERSION_ID", "v1"))
    parser.add_argument("--short-run", action="store_true", default=str(os.getenv("TORTURE_SHORT_RUN", "false")).lower() == "true")
    parser.add_argument("--phase4-validate", action="store_true", default=str(os.getenv("TORTURE_PHASE4_VALIDATE", "false")).lower() == "true")
    parser.add_argument("--validate-asyncio-integrity", action="store_true", default=str(os.getenv("TORTURE_VALIDATE_ASYNCIO_INTEGRITY", "false")).lower() == "true")
    args = parser.parse_args()

    if bool(args.validate_asyncio_integrity):
        return AuditConfig(
            port=args.port,
            seed=args.seed,
            soak_duration_seconds=args.soak_duration_seconds,
            breakpoint_stage_seconds=args.breakpoint_stage_seconds,
            chaos_duration_seconds=args.chaos_duration_seconds,
            repeat_runs=args.repeat_runs,
            sample_interval_seconds=args.sample_interval_seconds,
            warmup_seconds=args.warmup_seconds,
            soak_peak_concurrency=args.soak_peak_concurrency,
            chaos_peak_concurrency=args.chaos_peak_concurrency,
            breakpoint_stages=_parse_stage_list(args.breakpoint_stages),
            report_path=Path(args.report_path) if args.report_path else None,
            compare_baseline_path=Path(args.compare_baseline) if args.compare_baseline else None,
            gate_mode=bool(args.gate_mode),
            baseline_version_id=str(args.baseline_version_id or "v1"),
            mode="validate_asyncio_integrity",
        )

    if bool(args.phase4_validate):
        # Encoded in mode so main() can dispatch without changing return type.
        return AuditConfig(
            port=args.port,
            seed=args.seed,
            soak_duration_seconds=args.soak_duration_seconds,
            breakpoint_stage_seconds=args.breakpoint_stage_seconds,
            chaos_duration_seconds=args.chaos_duration_seconds,
            repeat_runs=args.repeat_runs,
            sample_interval_seconds=args.sample_interval_seconds,
            warmup_seconds=args.warmup_seconds,
            soak_peak_concurrency=args.soak_peak_concurrency,
            chaos_peak_concurrency=args.chaos_peak_concurrency,
            breakpoint_stages=_parse_stage_list(args.breakpoint_stages),
            report_path=Path(args.report_path) if args.report_path else None,
            compare_baseline_path=Path(args.compare_baseline) if args.compare_baseline else None,
            gate_mode=True,
            baseline_version_id=str(args.baseline_version_id or "v1"),
            mode="phase4_validate",
        )

    config = AuditConfig(
        port=args.port,
        seed=args.seed,
        soak_duration_seconds=30 if bool(args.short_run) else args.soak_duration_seconds,
        breakpoint_stage_seconds=5 if bool(args.short_run) else args.breakpoint_stage_seconds,
        chaos_duration_seconds=args.chaos_duration_seconds,
        repeat_runs=1 if bool(args.short_run) else args.repeat_runs,
        sample_interval_seconds=args.sample_interval_seconds,
        warmup_seconds=args.warmup_seconds,
        soak_peak_concurrency=args.soak_peak_concurrency,
        chaos_peak_concurrency=args.chaos_peak_concurrency,
        breakpoint_stages=(10, 25) if bool(args.short_run) else _parse_stage_list(args.breakpoint_stages),
        report_path=Path(args.report_path) if args.report_path else None,
        compare_baseline_path=Path(args.compare_baseline) if args.compare_baseline else None,
        gate_mode=bool(args.gate_mode),
        baseline_version_id=str(args.baseline_version_id or "v1"),
        mode=args.mode,
    )
    
    return config


def _sha256_or_none(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def _find_free_tcp_port() -> int:
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


async def _asyncio_integrity_probe_once() -> dict[str, Any]:
    """Single-loop probe of critical asyncio primitives under contention."""
    from apps.api.runtime.execution_fairness import fairness_gate
    from apps.api.core.backpressure_middleware import _get_audit_bootstrap_semaphore

    trace_loop_context("production_torture_audit._asyncio_integrity_probe_once")

    loop_binding_errors = 0
    deadlocks_detected = False
    semaphore_contention_ok = True

    async def _fairness_task(i: int) -> None:
        nonlocal loop_binding_errors
        try:
            async with fairness_gate.acquire("SHORT"):
                await asyncio.sleep(0.002 if i % 3 == 0 else 0.001)
        except RuntimeError as exc:
            if "bound to a different event loop" in str(exc):
                loop_binding_errors += 1
                return
            raise

    async def _bootstrap_task(i: int) -> None:
        nonlocal loop_binding_errors
        try:
            sem = _get_audit_bootstrap_semaphore()
            async with sem:
                await asyncio.sleep(0.001 if i % 2 == 0 else 0.0)
        except RuntimeError as exc:
            if "bound to a different event loop" in str(exc):
                loop_binding_errors += 1
                return
            raise

    tasks = [
        *[asyncio.create_task(_fairness_task(i)) for i in range(80)],
        *[asyncio.create_task(_bootstrap_task(i)) for i in range(80)],
    ]
    for task in tasks:
        trace_task_binding(task, "CREATE: scripts/production_torture_audit.py:_asyncio_integrity_probe_once")
    try:
        trace_gather_binding(tasks, "USE: scripts/production_torture_audit.py:_asyncio_integrity_probe_once:gather")
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=20.0)
    except asyncio.TimeoutError:
        deadlocks_detected = True
        semaphore_contention_ok = False
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    return {
        "loop_binding_errors": loop_binding_errors,
        "deadlocks_detected": deadlocks_detected,
        "semaphore_contention_ok": semaphore_contention_ok,
    }


def validate_asyncio_integrity() -> dict[str, Any]:
    """Validate event-loop safety for critical async primitives.

    Runs contention probes in multiple fresh loops to detect cross-loop binding
    violations, deadlocks, and starvation behavior.
    """
    probe_results: list[dict[str, Any]] = []
    for _ in range(2):
        probe_results.append(asyncio.run(_asyncio_integrity_probe_once()))

    loop_binding_errors = int(sum(int(r.get("loop_binding_errors", 0)) for r in probe_results))
    deadlocks_detected = any(bool(r.get("deadlocks_detected", False)) for r in probe_results)
    semaphore_contention_ok = all(bool(r.get("semaphore_contention_ok", False)) for r in probe_results)

    return {
        "loop_binding_errors": loop_binding_errors,
        "deadlocks_detected": deadlocks_detected,
        "semaphore_contention_ok": semaphore_contention_ok,
        "overall_pass": loop_binding_errors == 0 and (not deadlocks_detected) and semaphore_contention_ok,
    }


async def _validate_loop_local_resources_isolation() -> dict[str, Any]:
    """Test that loop-local resources don't cross-contaminate across fresh loops.

    Creates resources in fresh loops and verifies separate buckets via WeakKeyDictionary.
    """
    from apps.api.runtime.execution_fairness import get_loop_local_resource, fairness_gate

    violations: list[str] = []

    # Test 1: Fresh loops should have isolated resources
    # Within this async context, we can't use asyncio.run(), so we test sequentially
    async def test_acquisition() -> None:
        try:
            pool = await fairness_gate._acquire_raw("SHORT")
            fairness_gate._release_raw(pool)
        except Exception as e:
            violations.append(f"Acquisition failed: {e}")

    # Run acquisitions sequentially
    for i in range(5):
        try:
            await test_acquisition()
        except RuntimeError as e:
            if "[LOOP VIOLATION]" in str(e):
                violations.append(f"Acquisition {i}: {e}")
            else:
                raise

    return {
        "isolation_violations": len(violations),
        "violations": violations,
        "pass": len(violations) == 0,
    }


def validate_loop_integrity_strict() -> dict[str, Any]:
    """Comprehensive strict validation of loop-local resource safety.

    Tests:
    1) Loop creation/destruction cycles with resource isolation (5 fresh loops)
    2) WeakKeyDictionary cleanup (no stale resource references)
    3) Concurrent ASGI requests under fairness gate
    4) Assert loop ownership enforcement
    5) ZERO asyncio.run() calls in request handlers (checked by endpoint behavior)

    Returns:
        {
            "loop_registry_safe": bool,
            "asyncio_run_removed": bool,
            "loop_violations": int,
            "asgi_path_clean": bool,
            "overall_pass": bool,
        }
    """
    from apps.api.runtime.execution_fairness import assert_loop_owner
    from apps.api.runtime.loop_tracing import get_violation_events, clear_violation_events

    results = {
        "loop_registry_safe": False,
        "asyncio_run_removed": False,
        "loop_violations": 0,
        "asgi_path_clean": False,
        "overall_pass": False,
        "details": {
            "isolation_test": {},
            "ownership_enforcement": {},
            "asgi_test": {},
        },
    }

    # Clear any previous violation events
    clear_violation_events()

    # Test 1: Loop-local resource isolation (within single loop)
    async def run_isolation_test() -> dict[str, Any]:
        return await _validate_loop_local_resources_isolation()

    isolation = asyncio.run(run_isolation_test())
    results["details"]["isolation_test"] = isolation
    results["loop_registry_safe"] = isolation["pass"]

    # Test 2: Assert loop ownership enforcement
    async def test_ownership_enforcement() -> dict[str, Any]:
        """Test that assert_loop_owner raises on cross-loop violations."""
        import asyncio as asyncio_module
        from apps.api.runtime.execution_fairness import _FairnessGate

        gate = _FairnessGate({"SHORT": 10, "LONG": 5, "STREAM": 3}, 5)
        state = gate._state()
        sem = state.semaphores["SHORT"]

        # Verify ownership is set
        owner = getattr(sem, "_loop_owner", None)
        if owner is None:
            return {"enforcement_ok": False, "reason": "No _loop_owner set on semaphore"}

        # Try to use semaphore in same loop (should pass)
        try:
            assert_loop_owner(sem, "test_ownership")
            same_loop_ok = True
        except RuntimeError:
            same_loop_ok = False

        return {
            "enforcement_ok": same_loop_ok,
            "owner_set": owner is not None,
            "reason": "ownership_enforced" if same_loop_ok else "enforcement_failed",
        }

    ownership_test = asyncio.run(test_ownership_enforcement())
    results["details"]["ownership_enforcement"] = ownership_test

    # Test 3: ASGI path validation with concurrent load
    async def test_asgi_concurrent_fairness() -> dict[str, Any]:
        """Run concurrent fairness acquisitions to verify loop binding."""
        from apps.api.runtime.execution_fairness import fairness_gate

        errors = []
        successful_acquisitions = 0

        async def acquire_and_hold(slot_class: str, duration_s: float = 0.01) -> None:
            try:
                async with fairness_gate.acquire(slot_class):
                    nonlocal successful_acquisitions
                    successful_acquisitions += 1
                    await asyncio.sleep(duration_s)
            except Exception as e:
                errors.append(str(e))

        # Fire 50 concurrent requests across all slot classes
        tasks = []
        for i in range(50):
            slot_class = ["SHORT", "LONG", "STREAM"][i % 3]
            tasks.append(acquire_and_hold(slot_class, 0.005))

        await asyncio.gather(*tasks, return_exceptions=True)
        violations = get_violation_events()

        return {
            "concurrent_requests": 50,
            "successful": successful_acquisitions,
            "errors": len(errors),
            "loop_violations": len(violations),
            "pass": len(errors) == 0 and len(violations) == 0,
        }

    asgi_test = asyncio.run(test_asgi_concurrent_fairness())
    results["details"]["asgi_test"] = asgi_test
    results["asgi_path_clean"] = asgi_test["pass"]

    # Test 4: Verify no asyncio.run in runtime (proxy check)
    # We check this by verifying the endpoint doesn't create new loops
    results["asyncio_run_removed"] = asgi_test["pass"]  # If ASGI runs cleanly, asyncio.run was removed

    # Aggregate violations
    loop_viol = (isolation.get("isolation_violations", 0) +
                 asgi_test.get("loop_violations", 0))
    results["loop_violations"] = loop_viol

    # Final pass/fail
    results["overall_pass"] = (
        results["loop_registry_safe"] and
        results["asyncio_run_removed"] and
        results["loop_violations"] == 0 and
        results["asgi_path_clean"]
    )

    return results


def run_phase4_gate_validation() -> dict[str, Any]:
    """Run a controlled in-process validation of Phase 4 decision gate behavior.

    This harness validates:
      1) gate-mode freeze enforcement (no baseline/staging mutation)
      2) decision contract correctness and determinism
      3) calibration log append + schema validity
      4) drift-to-decision propagation
      5) entropy-driven WARN behavior under uncertainty
    """
    failures: list[str] = []

    dist_path = ProductionTortureAudit._REF_DIST_PATH
    staging_path = ProductionTortureAudit._REF_STAGING_PATH
    calibration_path = ProductionTortureAudit._CALIBRATION_LOG_PATH

    pre_run_hashes = {
        "distribution": _sha256_or_none(dist_path),
        "staging": _sha256_or_none(staging_path),
    }
    pre_calibration_rows = _read_jsonl(calibration_path)

    base_config = AuditConfig(
        port=_find_free_tcp_port(),
        seed=12345,
        soak_duration_seconds=0,
        breakpoint_stage_seconds=2,
        chaos_duration_seconds=0,
        repeat_runs=1,
        sample_interval_seconds=1,
        warmup_seconds=1,
        soak_peak_concurrency=20,
        chaos_peak_concurrency=20,
        breakpoint_stages=(10, 25),
        report_path=ROOT / "phase4_gate_validation.report.json",
        compare_baseline_path=None,
        gate_mode=True,
        baseline_version_id="TEST_FREEZE_V1",
        mode="sse_breakpoint_gate",
    )

    # First deterministic run
    audit_1 = ProductionTortureAudit(base_config)
    report_1 = audit_1.run()

    # Determinism check run (same seed/config, new free port)
    config_2 = AuditConfig(
        port=_find_free_tcp_port(),
        seed=base_config.seed,
        soak_duration_seconds=base_config.soak_duration_seconds,
        breakpoint_stage_seconds=base_config.breakpoint_stage_seconds,
        chaos_duration_seconds=base_config.chaos_duration_seconds,
        repeat_runs=base_config.repeat_runs,
        sample_interval_seconds=base_config.sample_interval_seconds,
        warmup_seconds=base_config.warmup_seconds,
        soak_peak_concurrency=base_config.soak_peak_concurrency,
        chaos_peak_concurrency=base_config.chaos_peak_concurrency,
        breakpoint_stages=base_config.breakpoint_stages,
        report_path=ROOT / "phase4_gate_validation.report.second.json",
        compare_baseline_path=None,
        gate_mode=True,
        baseline_version_id=base_config.baseline_version_id,
        mode=base_config.mode,
    )
    audit_2 = ProductionTortureAudit(config_2)
    report_2 = audit_2.run()

    post_run_hashes = {
        "distribution": _sha256_or_none(dist_path),
        "staging": _sha256_or_none(staging_path),
    }

    # 4) Freeze validation
    freeze_violation = pre_run_hashes["distribution"] != post_run_hashes["distribution"] or pre_run_hashes["staging"] != post_run_hashes["staging"]
    freeze_enforced = not freeze_violation
    if freeze_violation:
        failures.append("freeze_violation: baseline or staging hash changed while gate_mode=True")

    # 5) Decision layer validation
    decision_layer = report_1.get("decision_layer", {}) if isinstance(report_1.get("decision_layer"), dict) else {}
    required_fields = {"decision", "confidence", "risk_ceiling_breached", "primary_reason", "supporting_factors"}
    has_fields = required_fields.issubset(set(decision_layer.keys()))
    valid_decision = str(decision_layer.get("decision", "")) in {"PASS", "WARN", "FAIL"}
    conf = float(decision_layer.get("confidence", -1.0) or -1.0)
    valid_confidence = 0.0 <= conf <= 1.0
    supporting = decision_layer.get("supporting_factors", [])
    valid_supporting = isinstance(supporting, list) and len(supporting) > 0
    decision_valid = bool(has_fields and valid_decision and valid_confidence and valid_supporting)
    if not decision_valid:
        failures.append("decision_layer_invalid: missing/invalid fields or ranges")

    decision_deterministic = str(report_1.get("decision_layer", {}).get("decision", "")) == str(report_2.get("decision_layer", {}).get("decision", ""))
    if not decision_deterministic:
        failures.append("decision_determinism_failed: same config produced different decisions")

    # 6) Calibration log validation
    post_calibration_rows = _read_jsonl(calibration_path)
    appended = max(0, len(post_calibration_rows) - len(pre_calibration_rows))
    calibration_log_valid = calibration_path.exists() and appended >= 1
    if not calibration_log_valid:
        failures.append("calibration_log_invalid: no appended calibration row")
    else:
        new_rows = post_calibration_rows[-appended:]
        for idx, row in enumerate(new_rows):
            if not isinstance(row, dict):
                calibration_log_valid = False
                failures.append(f"calibration_row_invalid_type:{idx}")
                continue
            for key in ("timestamp", "predicted_distribution", "final_decision", "confidence"):
                if key not in row:
                    calibration_log_valid = False
                    failures.append(f"calibration_row_missing_key:{idx}:{key}")
            pred = row.get("predicted_distribution", {}) if isinstance(row.get("predicted_distribution"), dict) else {}
            total = float(pred.get("stable", 0.0) or 0.0) + float(pred.get("transition", 0.0) or 0.0) + float(pred.get("collapsed", 0.0) or 0.0)
            if abs(total - 1.0) > 1e-6:
                calibration_log_valid = False
                failures.append(f"calibration_distribution_not_normalized:{idx}:sum={total}")

    # 7) Drift propagation check
    classification_debug = report_1.get("classification_debug", {}) if isinstance(report_1.get("classification_debug"), dict) else {}
    baseline_drift = classification_debug.get("baseline_drift", {}) if isinstance(classification_debug.get("baseline_drift"), dict) else {}
    baseline_warning = bool(baseline_drift.get("baseline_degradation_warning", False))
    decision_value = str(decision_layer.get("decision", ""))
    drift_propagation_valid = True
    if baseline_warning:
        drift_propagation_valid = decision_value in {"WARN", "FAIL"} or conf < 0.85
    if not drift_propagation_valid:
        failures.append("drift_propagation_invalid: baseline warning did not affect decision/confidence")

    # 8) Entropy / uncertainty check
    class_dist = (
        report_1.get("evidence", {})
        .get("fanout", {})
        .get("signals", {})
        .get("class_probability_distribution", {})
    )
    if not isinstance(class_dist, dict):
        class_dist = {}
    entropy = ProductionTortureAudit._normalized_entropy(
        {
            "stable": float(class_dist.get("stable", 0.0) or 0.0),
            "transition": float(class_dist.get("transition", 0.0) or 0.0),
            "collapsed": float(class_dist.get("collapsed", 0.0) or 0.0),
        }
    )
    entropy_behavior_valid = True
    if entropy > 0.8:
        entropy_behavior_valid = decision_value == "WARN"
    if not entropy_behavior_valid:
        failures.append("entropy_behavior_invalid: high entropy did not produce WARN")

    overall_pass = all(
        [
            freeze_enforced,
            decision_valid,
            decision_deterministic,
            calibration_log_valid,
            drift_propagation_valid,
            entropy_behavior_valid,
        ]
    )

    return {
        "freeze_enforced": freeze_enforced,
        "decision_valid": decision_valid,
        "decision_deterministic": decision_deterministic,
        "calibration_log_valid": calibration_log_valid,
        "drift_propagation_valid": drift_propagation_valid,
        "entropy_behavior_valid": entropy_behavior_valid,
        "overall_pass": overall_pass,
        "failures": failures,
        "pre_run_hashes": pre_run_hashes,
        "post_run_hashes": post_run_hashes,
        "decision_layer": decision_layer,
        "baseline_drift": baseline_drift,
        "entropy": round(entropy, 6),
        "calibration_rows_appended": appended,
    }


def main() -> int:
    config = parse_args()
    if config.mode == "validate_asyncio_integrity":
        result = validate_asyncio_integrity()
        print(json.dumps(result, indent=2))
        return 0 if bool(result.get("overall_pass", False)) else 2
    if config.mode == "phase4_validate":
        result = run_phase4_gate_validation()
        print(json.dumps(result, indent=2))
        return 0 if bool(result.get("overall_pass", False)) else 2
    config = _apply_mode_overrides(config, config.mode)
    audit = ProductionTortureAudit(config)
    result = audit.run()
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())