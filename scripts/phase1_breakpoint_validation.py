from __future__ import annotations

import json
import math
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

        tiers = [
            self.run_tier(10, 70),
            self.run_tier(50, 75),
            self.run_tier(100, 60),
        ]

        # pass/fail
        tier10 = tiers[0]
        tier50 = tiers[1]

        no_cascade = all(t["_internal"]["trend"] != "increasing" for t in tiers)
        no_error_growth = no_cascade

        tier10_pass = (
            math.isclose(float(tier10["success_rate"]), 1.0, rel_tol=0.0, abs_tol=0.0)
            and int(tier10["auth"]["valid_token_failures"]) == 0
            and int(tier10["auth"]["system_failures"]) == 0
        )

        tier50_retry = float(tier50["backpressure"]["retry_rate"])
        retry_reduction = tier50_retry <= (BASELINE_RETRY_RATE * 0.5)

        tier50_pass = (
            float(tier50["success_rate"]) >= 0.95
            and int(tier50["auth"]["valid_token_failures"]) == 0
            and int(tier50["db"]["rejections"]) == 0
            and retry_reduction
        )

        phase_passed = tier10_pass and tier50_pass and no_cascade and no_error_growth

        primary_failure_subsystem = "unknown"
        if not phase_passed:
            if int(tier10["auth"]["valid_token_failures"]) > 0 or int(tier50["auth"]["valid_token_failures"]) > 0 or int(tier50["auth"]["system_failures"]) > 0:
                primary_failure_subsystem = "auth"
            elif int(tier50["db"]["rejections"]) > 0:
                primary_failure_subsystem = "db"
            elif int(tier50["backpressure"]["rejections_429"]) > 0 or not retry_reduction:
                primary_failure_subsystem = "backpressure"

        auth_improvement = (
            int(tier10["auth"]["valid_token_failures"]) == 0
            and int(tier50["auth"]["valid_token_failures"]) == 0
            and int(tier10["auth"]["system_failures"]) == 0
            and int(tier50["auth"]["system_failures"]) == 0
        )

        stability_gain = no_cascade and no_error_growth and float(tier50["success_rate"]) >= 0.95

        output_tiers: list[dict[str, Any]] = []
        for t in tiers:
            clean = {k: v for k, v in t.items() if k != "_internal"}
            output_tiers.append(clean)

        return {
            "tier_results": output_tiers,
            "phase1_status": "PASSED" if phase_passed else "FAILED",
            "primary_failure_subsystem": primary_failure_subsystem,
            "regression_vs_baseline": {
                "auth_improvement": "yes" if auth_improvement else "no",
                "retry_reduction": "yes" if retry_reduction else "no",
                "stability_gain": "yes" if stability_gain else "no",
            },
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
