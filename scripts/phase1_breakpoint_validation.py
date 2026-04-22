from __future__ import annotations

import json
import math
import os
from pathlib import Path
import queue
import random
import socket
import statistics
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, build_opener, ProxyHandler

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.runtime_stress_audit import RuntimeStressHarness
from apps.api.core.runtime_classifier import RuntimeSaturationClassifier
from tests.harness.load_curve_model import LoadCurveModel
from tests.harness.noise_isolation import classify_noise, isolate
from tests.harness.repeatability_gate import RepeatabilityConfig, RepeatabilityGate


BASELINE_RETRY_RATE = 0.03773
HOST = "127.0.0.1"
PORT = 8013


@dataclass
class ReqObs:
    ts: float
    latency_ms: float
    status: int
    ok: bool
    retried: bool
    kind: str


class Runner:
    def __init__(self) -> None:
        self.harness = RuntimeStressHarness(port=PORT, duration_minutes=15, sample_interval_seconds=5)
        self.server = None
        self._lock = threading.Lock()
        self._inflight = 0
        self._inflight_peak = 0

    def _inc_inflight(self) -> None:
        with self._lock:
            self._inflight += 1
            if self._inflight > self._inflight_peak:
                self._inflight_peak = self._inflight

    def _dec_inflight(self) -> None:
        with self._lock:
            self._inflight = max(0, self._inflight - 1)

    def _json_request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 8.0,
        retries: int = 1,
    ) -> tuple[int, dict[str, Any] | str, float, bool]:
        payload = None
        request_headers = {"Content-Type": "application/json"}
        if headers:
            request_headers.update(headers)
        if body is not None:
            payload = json.dumps(body).encode("utf-8")

        req = Request(f"http://{HOST}:{PORT}{path}", data=payload, method=method, headers=request_headers)
        opener = build_opener(ProxyHandler({}))
        attempt = 0
        retried = False
        while True:
            self._inc_inflight()
            start = time.perf_counter()
            try:
                with opener.open(req, timeout=timeout) as resp:
                    status = int(resp.getcode())
                    text = resp.read().decode("utf-8")
                    latency_ms = (time.perf_counter() - start) * 1000
                    try:
                        return status, json.loads(text), latency_ms, retried
                    except json.JSONDecodeError:
                        return status, text, latency_ms, retried
            except HTTPError as exc:
                latency_ms = (time.perf_counter() - start) * 1000
                try:
                    body_text = exc.read().decode("utf-8") if exc.fp else ""
                except Exception:
                    body_text = ""
                try:
                    parsed = json.loads(body_text) if body_text else {}
                except json.JSONDecodeError:
                    parsed = body_text
                return int(exc.code), parsed, latency_ms, retried
            except (URLError, socket.timeout, TimeoutError, ConnectionAbortedError, ConnectionResetError, OSError):
                latency_ms = (time.perf_counter() - start) * 1000
                if attempt < retries:
                    attempt += 1
                    retried = True
                    continue
                return 0, {}, latency_ms, retried
            finally:
                self._dec_inflight()

    def _sse_connect_once(self, household_id: str, token: str, timeout: float = 6.0) -> tuple[bool, float, bool]:
        q = urlencode({"household_id": household_id})
        req = Request(
            f"http://{HOST}:{PORT}/v1/realtime/stream?{q}",
            method="GET",
            headers={
                "Authorization": f"Bearer {token}",
                "x-hpal-household-id": household_id,
                "Accept": "text/event-stream",
            },
        )
        opener = build_opener(ProxyHandler({}))
        retried = False
        for attempt in range(2):
            self._inc_inflight()
            start = time.perf_counter()
            try:
                with opener.open(req, timeout=timeout) as resp:
                    data = resp.read(256)
                    latency_ms = (time.perf_counter() - start) * 1000
                    ok = resp.getcode() == 200 and bool(data)
                    return ok, latency_ms, retried
            except Exception:
                latency_ms = (time.perf_counter() - start) * 1000
                if attempt == 0:
                    retried = True
                    continue
                return False, latency_ms, retried
            finally:
                self._dec_inflight()
        return False, 0.0, retried

    def _collect_metrics_snapshot(self) -> dict[str, Any]:
        status, payload, _lat, _retried = self._json_request("GET", "/metrics", retries=1)
        if status != 200 or not isinstance(payload, dict):
            return {}
        return payload

    @staticmethod
    def _counter(snapshot: dict[str, Any], name: str) -> float:
        return float(snapshot.get("counters", {}).get(name, 0.0))

    @staticmethod
    def _gauge(snapshot: dict[str, Any], name: str) -> float:
        return float(snapshot.get("gauges", {}).get(name, 0.0))

    @staticmethod
    def _summarize_records(records: list[dict[str, Any]]) -> dict[str, float]:
        total = len(records)
        if total == 0:
            return {
                "total_requests": 0.0,
                "success_rate": 0.0,
                "error_rate": 1.0,
                "retry_rate": 0.0,
                "rejections_429": 0.0,
                "p50_ms": 0.0,
                "p95_ms": 0.0,
            }

        success_count = sum(1 for r in records if bool(r.get("ok", False)))
        retry_count = sum(1 for r in records if bool(r.get("retried", False)))
        reject_429 = sum(1 for r in records if int(r.get("status", 0)) == 429)
        latencies = [float(r.get("latency_ms", 0.0)) for r in records if float(r.get("latency_ms", 0.0)) > 0]

        p50 = statistics.median(latencies) if latencies else 0.0
        p95 = statistics.quantiles(latencies, n=100, method="inclusive")[94] if len(latencies) >= 2 else p50

        return {
            "total_requests": float(total),
            "success_rate": float(success_count) / float(total),
            "error_rate": float(total - success_count) / float(total),
            "retry_rate": float(retry_count) / float(total),
            "rejections_429": float(reject_429),
            "p50_ms": float(p50),
            "p95_ms": float(p95),
        }

    def _collect_runtime_metrics_snapshot(self, household_id: str, token: str) -> dict[str, Any]:
        for _ in range(5):
            status, payload, _lat, _retried = self._json_request(
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
            time.sleep(0.2)
        return {}

    def run_stable_curve_once(self, schedule: list[dict[str, int]], seed: int, warmup_seconds: int = 10) -> dict[str, Any]:
        observations: "queue.Queue[ReqObs]" = queue.Queue()
        sse_success = 0
        sse_attempts = 0
        sse_reconnects = 0
        invalid_401 = 0
        invalid_non_401 = 0
        invalid_statuses: Counter[int] = Counter()
        valid_token_failures = 0
        auth_system_failures = 0
        total_invalid = 0
        lag_flag = False

        with self._lock:
            self._inflight_peak = 0

        baseline_metrics = self._collect_metrics_snapshot()
        db_pool_max_in_use = self._gauge(baseline_metrics, "db_pool_in_use")

        target_by_second = {int(p["timestamp"]): int(p["target_concurrency"]) for p in schedule}
        total_seconds = max(target_by_second.keys()) + 1 if target_by_second else 1
        max_concurrency = max(target_by_second.values()) if target_by_second else 1

        stop_event = threading.Event()
        active_lock = threading.Lock()
        active_target = target_by_second.get(0, 1)

        homes: list[tuple[str, str]] = []
        for _ in range(max(5, max_concurrency // 2)):
            homes.append(self.harness._register_household())

        def pick_home(rnd: random.Random) -> tuple[str, str]:
            return homes[rnd.randint(0, len(homes) - 1)]

        def worker(idx: int) -> None:
            nonlocal sse_success, sse_attempts, sse_reconnects
            nonlocal invalid_401, invalid_non_401, total_invalid
            nonlocal valid_token_failures, auth_system_failures
            rnd = random.Random(seed * 1000 + idx)
            msg_seq = 0
            while not stop_event.is_set():
                with active_lock:
                    target = active_target
                if idx >= target:
                    time.sleep(0.01)
                    continue

                hh, token = pick_home(rnd)
                r = rnd.random()
                msg_seq += 1

                if r < 0.72:
                    status, _payload, latency, retried = self._json_request(
                        "POST",
                        "/v1/ui/message",
                        body={
                            "family_id": hh,
                            "message": f"curve-valid-{seed}-{idx}-{msg_seq}",
                            "session_id": f"curve-s-{seed}-{idx}",
                        },
                        headers={
                            "Authorization": f"Bearer {token}",
                            "x-hpal-household-id": hh,
                            "x-idempotency-key": f"curve-valid-{seed}-{idx}-{msg_seq}",
                        },
                        timeout=8,
                        retries=1,
                    )
                    ok = status == 200
                    if status in {401, 403, 503}:
                        valid_token_failures += 1
                    if status == 503:
                        auth_system_failures += 1
                    observations.put(ReqObs(time.time(), latency, status, ok, retried, "valid"))

                elif r < 0.87:
                    total_invalid += 1
                    status, _payload, latency, retried = self._json_request(
                        "POST",
                        "/v1/ui/message",
                        body={
                            "family_id": hh,
                            "message": f"curve-invalid-{seed}-{idx}-{msg_seq}",
                            "session_id": f"curve-iv-{seed}-{idx}",
                        },
                        headers={
                            "Authorization": "Bearer invalid.token.value",
                            "x-hpal-household-id": hh,
                            "x-idempotency-key": f"curve-invalid-{seed}-{idx}-{msg_seq}",
                        },
                        timeout=8,
                        retries=1,
                    )
                    if status == 401:
                        invalid_401 += 1
                    else:
                        invalid_non_401 += 1
                        invalid_statuses[status] += 1
                        if status == 503:
                            auth_system_failures += 1
                    observations.put(ReqObs(time.time(), latency, status, status == 401, retried, "invalid"))

                else:
                    sse_attempts += 1
                    sse_reconnects += 1
                    ok, latency, retried = self._sse_connect_once(hh, token)
                    if ok:
                        sse_success += 1
                    observations.put(ReqObs(time.time(), latency, 200 if ok else 0, ok, retried, "sse"))

                time.sleep(rnd.uniform(0.01, 0.06))

        def sampler() -> None:
            nonlocal db_pool_max_in_use, lag_flag
            while not stop_event.is_set():
                snap = self._collect_metrics_snapshot()
                if snap:
                    db_pool_max_in_use = max(db_pool_max_in_use, self._gauge(snap, "db_pool_in_use"))
                    replay_depth = self._gauge(snap, "replay_queue_depth")
                    if replay_depth > 200:
                        lag_flag = True
                time.sleep(1.0)

        threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(max_concurrency)]
        sample_thread = threading.Thread(target=sampler, daemon=True)
        for t in threads:
            t.start()
        sample_thread.start()

        for sec in range(total_seconds):
            with active_lock:
                active_target = target_by_second.get(sec, active_target)
            time.sleep(1.0)

        stop_event.set()
        for t in threads:
            t.join(timeout=3)
        sample_thread.join(timeout=2)

        final_metrics = self._collect_metrics_snapshot()
        runtime_metrics = self._collect_runtime_metrics_snapshot(homes[0][0], homes[0][1])

        raw_records: list[dict[str, Any]] = []
        while True:
            try:
                obs = observations.get_nowait()
                raw_records.append(
                    {
                        "ts": obs.ts,
                        "latency_ms": obs.latency_ms,
                        "status": obs.status,
                        "ok": obs.ok,
                        "retried": obs.retried,
                        "kind": obs.kind,
                    }
                )
            except queue.Empty:
                break

        clean_records = isolate(raw_records, warmup_seconds=warmup_seconds)
        noise_profile = classify_noise(raw_records, warmup_seconds=warmup_seconds)

        raw_summary = self._summarize_records(raw_records)
        clean_summary = self._summarize_records(clean_records)

        db_rejections = self._counter(final_metrics, "db_pool_rejection_count") - self._counter(
            baseline_metrics, "db_pool_rejection_count"
        )

        fallback_metrics = {
            "accepted_total": clean_summary["total_requests"],
            "rejected_total": clean_summary["rejections_429"],
            "completed_total": clean_summary["total_requests"] * clean_summary["success_rate"],
            "failed_total": clean_summary["total_requests"] * clean_summary["error_rate"],
            "inflight_current": float(self._inflight),
            "completion_ratio": clean_summary["success_rate"],
            "ASGI_ENTRY_RECEIVED_COUNT": raw_summary["total_requests"],
            "ADMISSION_ACCEPTED_COUNT": clean_summary["total_requests"],
            "ADMISSION_REJECTED_COUNT": clean_summary["rejections_429"],
            "CLIENT_TIMEOUT_COUNT": noise_profile["client_noise_ratio"] * raw_summary["total_requests"],
            "MAX_INFLIGHT_OBSERVED": float(self._inflight_peak),
            "MAX_INFLIGHT_CAP": 20.0,
            "retry_rate": clean_summary["retry_rate"],
            "p95_latency": clean_summary["p95_ms"],
        }

        completion_ratio = float(runtime_metrics.get("completion_ratio", fallback_metrics["completion_ratio"])) if runtime_metrics else float(fallback_metrics["completion_ratio"])
        classification = (
            runtime_metrics.get("runtime_classification")
            if runtime_metrics and isinstance(runtime_metrics.get("runtime_classification"), dict)
            else RuntimeSaturationClassifier.classify(fallback_metrics)
        )

        notes_parts: list[str] = [
            f"raw_total={int(raw_summary['total_requests'])}",
            f"clean_total={int(clean_summary['total_requests'])}",
            f"noise_client_ratio={noise_profile['client_noise_ratio']:.4f}",
            f"sse_success_rate={(sse_success / sse_attempts) if sse_attempts else 1.0:.4f}",
            f"reconnect_rate={(sse_reconnects / total_seconds) if total_seconds else 0.0:.3f}/s",
            f"major_lag_detected={'yes' if lag_flag else 'no'}",
        ]
        if invalid_non_401 > 0:
            top_invalid = ",".join(f"{code}:{count}" for code, count in invalid_statuses.most_common(3))
            notes_parts.append(f"invalid_non_401_statuses={top_invalid}")

        return {
            "seed": seed,
            "duration_seconds": total_seconds,
            "success_rate": round(clean_summary["success_rate"], 5),
            "error_rate": round(clean_summary["error_rate"], 5),
            "auth": {
                "valid_token_failures": int(valid_token_failures),
                "invalid_token_responses": int(invalid_401),
                "system_failures": int(auth_system_failures),
            },
            "backpressure": {
                "inflight_peak": int(self._inflight_peak),
                "rejections_429": int(clean_summary["rejections_429"]),
                "retry_rate": round(clean_summary["retry_rate"], 5),
                "rejection_rate": round(
                    (clean_summary["rejections_429"] / clean_summary["total_requests"])
                    if clean_summary["total_requests"] > 0
                    else 0.0,
                    5,
                ),
            },
            "db": {
                "pool_max_in_use": int(round(db_pool_max_in_use)),
                "rejections": int(round(db_rejections)),
            },
            "latency": {
                "p50_ms": round(clean_summary["p50_ms"], 3),
                "p95_ms": round(clean_summary["p95_ms"], 3),
            },
            "noise_profile": noise_profile,
            "runtime_metrics": runtime_metrics,
            "classification": classification,
            "completion_ratio": round(completion_ratio, 6),
            "notes": "; ".join(notes_parts),
            "_internal": {
                "raw_total": int(raw_summary["total_requests"]),
                "clean_total": int(clean_summary["total_requests"]),
                "lag": lag_flag,
                "total_invalid": total_invalid,
            },
        }

    def run_tier(self, concurrency: int, seconds: int) -> dict[str, Any]:
        observations: "queue.Queue[ReqObs]" = queue.Queue()
        sse_success = 0
        sse_attempts = 0
        sse_reconnects = 0
        invalid_401 = 0
        invalid_non_401 = 0
        invalid_statuses: Counter[int] = Counter()
        valid_token_failures = 0
        auth_system_failures = 0
        total_invalid = 0
        lag_flag = False

        with self._lock:
            self._inflight_peak = 0

        baseline_metrics = self._collect_metrics_snapshot()
        db_pool_max_in_use = self._gauge(baseline_metrics, "db_pool_in_use")

        stop_event = threading.Event()
        homes: list[tuple[str, str]] = []
        for _ in range(max(5, concurrency // 2)):
            homes.append(self.harness._register_household())

        def pick_home() -> tuple[str, str]:
            return homes[random.randint(0, len(homes) - 1)]

        def worker(idx: int) -> None:
            nonlocal sse_success, sse_attempts, sse_reconnects
            nonlocal invalid_401, invalid_non_401, total_invalid
            nonlocal valid_token_failures, auth_system_failures
            rnd = random.Random(1000 + idx)
            while not stop_event.is_set():
                hh, token = pick_home()
                r = rnd.random()

                if r < 0.72:
                    status, payload, latency, retried = self._json_request(
                        "POST",
                        "/v1/ui/message",
                        body={
                            "family_id": hh,
                            "message": f"phase1-valid-{idx}",
                            "session_id": f"phase1-s-{idx}",
                        },
                        headers={
                            "Authorization": f"Bearer {token}",
                            "x-hpal-household-id": hh,
                            "x-idempotency-key": f"phase1-{idx}-{time.time_ns()}",
                        },
                        timeout=8,
                        retries=1,
                    )
                    ok = status == 200
                    if status in {401, 403, 503}:
                        valid_token_failures += 1
                    if status == 503:
                        auth_system_failures += 1
                    observations.put(ReqObs(time.time(), latency, status, ok, retried, "valid"))

                elif r < 0.87:
                    total_invalid += 1
                    status, payload, latency, retried = self._json_request(
                        "POST",
                        "/v1/ui/message",
                        body={
                            "family_id": hh,
                            "message": f"phase1-invalid-{idx}",
                            "session_id": f"phase1-iv-{idx}",
                        },
                        headers={
                            "Authorization": "Bearer invalid.token.value",
                            "x-hpal-household-id": hh,
                            "x-idempotency-key": f"phase1-invalid-{idx}-{time.time_ns()}",
                        },
                        timeout=8,
                        retries=1,
                    )
                    if status == 401:
                        invalid_401 += 1
                    else:
                        invalid_non_401 += 1
                        invalid_statuses[status] += 1
                        if status == 503:
                            auth_system_failures += 1
                    ok = status == 401
                    observations.put(ReqObs(time.time(), latency, status, ok, retried, "invalid"))

                else:
                    sse_attempts += 1
                    sse_reconnects += 1
                    ok, latency, retried = self._sse_connect_once(hh, token)
                    if ok:
                        sse_success += 1
                    observations.put(ReqObs(time.time(), latency, 200 if ok else 0, ok, retried, "sse"))

                time.sleep(rnd.uniform(0.01, 0.06))

        # metrics sampler for db_pool_in_use and lag
        def sampler() -> None:
            nonlocal db_pool_max_in_use, lag_flag
            while not stop_event.is_set():
                snap = self._collect_metrics_snapshot()
                if snap:
                    db_pool_max_in_use = max(db_pool_max_in_use, self._gauge(snap, "db_pool_in_use"))
                    replay_depth = self._gauge(snap, "replay_queue_depth")
                    if replay_depth > 200:
                        lag_flag = True
                time.sleep(1.0)

        threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
        sample_thread = threading.Thread(target=sampler, daemon=True)
        for t in threads:
            t.start()
        sample_thread.start()

        time.sleep(seconds)
        stop_event.set()
        for t in threads:
            t.join(timeout=3)
        sample_thread.join(timeout=2)

        final_metrics = self._collect_metrics_snapshot()

        obs: list[ReqObs] = []
        while True:
            try:
                obs.append(observations.get_nowait())
            except queue.Empty:
                break

        total_requests = len(obs)
        successes = [x for x in obs if x.ok]
        errors = [x for x in obs if not x.ok]
        retries = [x for x in obs if x.retried]
        latencies = [x.latency_ms for x in obs if x.latency_ms > 0]

        success_rate = (len(successes) / total_requests) if total_requests else 0.0
        error_rate = (len(errors) / total_requests) if total_requests else 1.0
        retry_rate = (len(retries) / total_requests) if total_requests else 0.0

        p50 = statistics.median(latencies) if latencies else 0.0
        p95 = statistics.quantiles(latencies, n=100, method="inclusive")[94] if len(latencies) >= 2 else p50

        rejections_429 = sum(1 for x in obs if x.status == 429)

        db_rejections = self._counter(final_metrics, "db_pool_rejection_count") - self._counter(
            baseline_metrics, "db_pool_rejection_count"
        )

        # error trend within tier window
        trend = "flat"
        if total_requests >= 20:
            half = total_requests // 2
            first = obs[:half]
            second = obs[half:]
            e1 = (sum(1 for x in first if not x.ok) / len(first)) if first else 0.0
            e2 = (sum(1 for x in second if not x.ok) / len(second)) if second else 0.0
            if e2 > e1 + 0.03:
                trend = "increasing"
            elif e1 > e2 + 0.03:
                trend = "decreasing"

        sse_connection_success_rate = (sse_success / sse_attempts) if sse_attempts else 1.0
        reconnect_rate = (sse_reconnects / seconds) if seconds else 0.0

        notes_parts: list[str] = []
        if invalid_non_401 > 0:
            notes_parts.append(f"invalid_token_non_401={invalid_non_401}")
            top_invalid = ",".join(f"{code}:{count}" for code, count in invalid_statuses.most_common(3))
            notes_parts.append(f"invalid_non_401_statuses={top_invalid}")
        notes_parts.append(
            f"sse_success_rate={sse_connection_success_rate:.4f}, reconnect_rate={reconnect_rate:.3f}/s, major_lag_detected={'yes' if lag_flag else 'no'}"
        )
        notes_parts.append(f"error_trend={trend}")

        return {
            "concurrency": concurrency,
            "success_rate": round(success_rate, 5),
            "error_rate": round(error_rate, 5),
            "auth": {
                "valid_token_failures": int(valid_token_failures),
                "invalid_token_responses": int(invalid_401),
                "system_failures": int(auth_system_failures),
            },
            "backpressure": {
                "inflight_peak": int(self._inflight_peak),
                "rejections_429": int(rejections_429),
                "retry_rate": round(retry_rate, 5),
            },
            "db": {
                "pool_max_in_use": int(round(db_pool_max_in_use)),
                "rejections": int(round(db_rejections)),
            },
            "latency": {
                "p50_ms": round(p50, 3),
                "p95_ms": round(p95, 3),
            },
            "notes": "; ".join(notes_parts),
            "_internal": {
                "total_requests": total_requests,
                "trend": trend,
                "sse_connection_success_rate": sse_connection_success_rate,
                "invalid_non_401": invalid_non_401,
                "lag": lag_flag,
                "total_invalid": total_invalid,
            },
        }

    def run(self) -> dict[str, Any]:
        self.harness._kill_listeners_on_ports([PORT])
        time.sleep(0.4)
        self.server = self.harness._start_server()
        self.harness._wait_ready()

        base_seed = int(os.getenv("PHASE1_LOAD_SEED", "20260421"))
        repeat_runs = int(os.getenv("PHASE1_REPEATABILITY_RUNS", "5"))

        load_curve = (
            LoadCurveModel(seed=base_seed)
            .ramp_up(duration=40, start=10, end=50)
            .plateau(duration=40, level=50)
            .burst(duration=20, spikes=8, amplitude=50)
            .decay(duration=30, end_level=20)
        )
        schedule = list(load_curve.to_schedule())

        curve_runs: list[dict[str, Any]] = []
        repeatability_inputs: list[dict[str, float]] = []
        noise_profiles: list[dict[str, float]] = []

        for run_idx in range(repeat_runs):
            run_seed = base_seed + run_idx
            run_summary = self.run_stable_curve_once(schedule=schedule, seed=run_seed, warmup_seconds=10)
            curve_runs.append({k: v for k, v in run_summary.items() if k not in {"_internal", "runtime_metrics"}})
            noise_profiles.append(run_summary["noise_profile"])
            repeatability_inputs.append(
                {
                    "success_rate": float(run_summary["success_rate"]),
                    "rejection_rate": float(run_summary["backpressure"]["rejection_rate"]),
                    "completion_ratio": float(run_summary.get("completion_ratio", 0.0)),
                    "p95_latency": float(run_summary["latency"]["p95_ms"]),
                }
            )

        repeatability = RepeatabilityGate.evaluate(
            repeatability_inputs,
            RepeatabilityConfig(n_runs=repeat_runs),
        )

        if noise_profiles:
            noise_profile = {
                "client_noise_ratio": round(statistics.fmean(n["client_noise_ratio"] for n in noise_profiles), 6),
                "warmup_impact": round(statistics.fmean(n["warmup_impact"] for n in noise_profiles), 6),
                "retry_inflation_factor": round(statistics.fmean(n["retry_inflation_factor"] for n in noise_profiles), 6),
            }
        else:
            noise_profile = {
                "client_noise_ratio": 0.0,
                "warmup_impact": 0.0,
                "retry_inflation_factor": 1.0,
            }

        return {
            "repeatability": repeatability,
            "noise_profile": noise_profile,
            "load_model": {
                "type": "stable_curve",
                "seed": base_seed,
                "duration_seconds": load_curve.duration_seconds,
                "schedule_points": len(schedule),
            },
            "curve_runs": curve_runs,
        }

    def close(self) -> None:
        if self.server is not None:
            self.server.terminate()
            try:
                self.server.wait(timeout=8)
            except Exception:
                self.server.kill()


def main() -> int:
    runner = Runner()
    try:
        result = runner.run()
        print(json.dumps(result, indent=2))
        return 0
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())
