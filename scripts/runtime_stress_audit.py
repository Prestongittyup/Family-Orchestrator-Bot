from __future__ import annotations

import argparse
import csv
import http.client
import json
import os
import queue
import random
import re
import socket
import statistics
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

BASE_DIR = Path(__file__).resolve().parents[1]
HOST = "127.0.0.1"
BASE_PORT = 8021
REPORT_PATH = BASE_DIR / "runtime_stress_report.json"


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    k = (len(values) - 1) * p
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] + (values[c] - values[f]) * (k - f)


def _linear_slope(points: list[tuple[float, float]]) -> float:
    if len(points) < 2:
        return 0.0
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    mean_x = statistics.mean(xs)
    mean_y = statistics.mean(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in points)
    den = sum((x - mean_x) ** 2 for x in xs)
    if den == 0:
        return 0.0
    return num / den


def _parse_pool_status(pool_status: str | None) -> dict[str, int | None]:
    if not pool_status:
        return {
            "pool_size": None,
            "in_pool": None,
            "overflow": None,
            "checked_out": None,
        }
    patterns = {
        "pool_size": r"Pool size:\s*(-?\d+)",
        "in_pool": r"Connections in pool:\s*(-?\d+)",
        "overflow": r"Current Overflow:\s*(-?\d+)",
        "checked_out": r"Checked out connections:\s*(-?\d+)",
    }
    result: dict[str, int | None] = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, pool_status)
        result[key] = int(match.group(1)) if match else None
    return result


def _windows_process_memory_mb(pid: int) -> float:
    if os.name != "nt":
        return 0.0
    proc = subprocess.run(
        ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
        capture_output=True,
        text=True,
        check=False,
    )
    line = proc.stdout.strip()
    if not line or line.startswith("INFO:"):
        return 0.0

    try:
        row = next(csv.reader([line]))
        # row[4] like "32,144 K"
        mem_text = row[4].replace("\"", "").replace(" K", "").replace(",", "").strip()
        kb = float(mem_text)
        return round(kb / 1024.0, 2)
    except Exception:
        return 0.0


@dataclass
class RequestObservation:
    category: str
    latency_ms: float
    status_code: int
    ok: bool
    retried: bool
    timestamp: float


@dataclass
class SamplePoint:
    t_seconds: float
    process_memory_mb: float
    p95_latency_ms: float
    error_rate: float
    sse_lag_ms_p95: float
    retry_rate: float
    pool_checked_out: int | None
    pool_size: int | None
    replay_queue_depth: float


@dataclass
class BreakpointStepResult:
    users: int
    total_requests: int
    success_rate: float
    error_rate: float
    p95_latency_ms: float
    avg_latency_ms: float
    dominant_error: str | None
    stable: bool


@dataclass
class SaturationResult:
    db_exhaustion: dict[str, Any]
    sse_flood: dict[str, Any]
    token_storm: dict[str, Any]
    graceful_degradation: bool


class StressFailure(Exception):
    pass


class RuntimeStressHarness:
    def __init__(
        self,
        *,
        port: int,
        duration_minutes: int,
        sample_interval_seconds: int,
    ) -> None:
        self.port = port
        self.duration_minutes = duration_minutes
        self.sample_interval_seconds = sample_interval_seconds
        self.base_url = f"http://{HOST}:{self.port}"

        self._lock = threading.Lock()
        self._households: dict[str, str] = {}  # household_id -> token
        self._stop_event = threading.Event()
        self._observations: list[RequestObservation] = []
        self._sse_lags_ms: deque[float] = deque(maxlen=5000)
        self._sample_points: list[SamplePoint] = []
        self._subsystem_errors: Counter[str] = Counter()

    def _idem(self) -> str:
        return f"runtime-audit-{uuid.uuid4().hex}"

    def _json_request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 10.0,
        retries: int = 1,
    ) -> tuple[int, dict[str, Any] | str, float, bool]:
        payload = None
        request_headers = {"Content-Type": "application/json"}
        if headers:
            request_headers.update(headers)
        if body is not None:
            payload = json.dumps(body).encode("utf-8")

        req = Request(f"{self.base_url}{path}", data=payload, method=method, headers=request_headers)
        opener = build_opener(ProxyHandler({}))
        attempt = 0
        retried = False
        while True:
            start = time.perf_counter()
            try:
                with opener.open(req, timeout=timeout) as resp:
                    status = resp.getcode()
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
                return exc.code, parsed, latency_ms, retried
            except (
                URLError,
                socket.timeout,
                TimeoutError,
                ConnectionAbortedError,
                ConnectionResetError,
                OSError,
            ):
                latency_ms = (time.perf_counter() - start) * 1000
                if attempt < retries:
                    attempt += 1
                    retried = True
                    time.sleep(0.05)
                    continue
                return 599, {"detail": "network_error"}, latency_ms, retried

    def _record_obs(self, obs: RequestObservation) -> None:
        with self._lock:
            self._observations.append(obs)

    def _pick_household(self) -> tuple[str, str] | None:
        with self._lock:
            if not self._households:
                return None
            hh = random.choice(list(self._households.keys()))
            return hh, self._households[hh]

    def _register_household(self) -> tuple[str, str]:
        founder_email = f"runtime-{uuid.uuid4().hex[:10]}@example.com"
        body = {
            "name": f"Runtime Audit {uuid.uuid4().hex[:6]}",
            "timezone": "UTC",
            "founder_user_name": "Runtime Founder",
            "founder_email": founder_email,
        }
        status, payload, latency, retried = self._json_request(
            "POST",
            "/v1/identity/household/create",
            body=body,
            headers={"x-idempotency-key": self._idem()},
            retries=2,
        )
        ok = status == 200 and isinstance(payload, dict)
        self._record_obs(RequestObservation("household_create", latency, status, ok, retried, time.time()))
        if not ok:
            self._subsystem_errors["identity"] += 1
            raise StressFailure(f"household_create_failed:{status}:{payload}")

        household_id = payload["household"]["household_id"]
        b_status, b_payload, b_latency, b_retried = self._json_request(
            "POST",
            "/v1/identity/bootstrap",
            body={"household_id": household_id},
            retries=2,
        )
        token = b_payload.get("session_token") if isinstance(b_payload, dict) else ""
        is_jwt = isinstance(token, str) and token.count(".") == 2
        b_ok = b_status == 200 and is_jwt
        self._record_obs(RequestObservation("bootstrap", b_latency, b_status, b_ok, b_retried, time.time()))
        if not b_ok:
            self._subsystem_errors["auth"] += 1
            raise StressFailure(f"bootstrap_failed:{b_status}:{b_payload}")

        with self._lock:
            self._households[household_id] = token
        return household_id, token

    def _wait_ready(self, timeout_seconds: int = 60) -> dict[str, Any]:
        deadline = time.time() + timeout_seconds
        last_error = ""
        while time.time() < deadline:
            status, payload, latency, retried = self._json_request("GET", "/v1/system/boot-probe", retries=1)
            self._record_obs(RequestObservation("boot_probe", latency, status, status == 200, retried, time.time()))
            if status == 200 and isinstance(payload, dict) and payload.get("overall") == "ok":
                return payload
            last_error = f"status={status} payload={payload}"
            time.sleep(1.0)
        raise StressFailure(f"readiness_timeout:{last_error}")

    def _start_server(self) -> subprocess.Popen[str]:
        env = os.environ.copy()
        env["PORT"] = str(self.port)
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "apps.api.main:app",
                "--host",
                HOST,
                "--port",
                str(self.port),
                "--workers",
                "2",
                "--timeout-keep-alive",
                "2",
            ],
            cwd=str(BASE_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )

    def _kill_listeners_on_ports(self, ports: list[int]) -> None:
        if os.name != "nt":
            return

        netstat = subprocess.run(["netstat", "-ano", "-p", "tcp"], capture_output=True, text=True, check=False)
        pids: set[str] = set()
        for line in netstat.stdout.splitlines():
            if "LISTENING" not in line.upper():
                continue
            parts = line.split()
            if len(parts) < 5:
                continue
            local_addr = parts[1]
            pid = parts[-1]
            for port in ports:
                if local_addr.endswith(f":{port}"):
                    pids.add(pid)
        for pid in sorted(pids):
            subprocess.run(["taskkill", "/PID", pid, "/F"], capture_output=True, text=True, check=False)

    def _household_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                if random.random() < 0.35 or not self._households:
                    self._register_household()
                else:
                    pair = self._pick_household()
                    if pair is not None:
                        hh, _token = pair
                        status, payload, latency, retried = self._json_request(
                            "GET", f"/v1/ui/bootstrap?family_id={hh}", retries=1
                        )
                        ok = status == 200
                        self._record_obs(RequestObservation("bootstrap_state", latency, status, ok, retried, time.time()))
                        if not ok:
                            self._subsystem_errors["ui"] += 1
            except Exception:
                self._subsystem_errors["identity"] += 1
            self._stop_event.wait(2.0)

    def _ui_message_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                pair = self._pick_household()
                if pair is None:
                    self._stop_event.wait(0.2)
                    continue

                household_id, token = pair
                session_id = f"runtime-session-{random.randint(1, 2000)}"
                key = self._idem()
                body = {
                    "family_id": household_id,
                    "message": f"runtime ping {uuid.uuid4().hex[:8]}",
                    "session_id": session_id,
                }
                status, payload, latency, retried = self._json_request(
                    "POST",
                    "/v1/ui/message",
                    body=body,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "x-hpal-household-id": household_id,
                        "x-idempotency-key": key,
                    },
                    retries=2,
                )
                ok = status == 200
                self._record_obs(RequestObservation("ui_message", latency, status, ok, retried, time.time()))
                if not ok:
                    self._subsystem_errors["ui"] += 1

                # Duplicate idempotency probe to track lookup/duplicate path latency.
                d_status, _d_payload, d_latency, d_retried = self._json_request(
                    "POST",
                    "/v1/ui/message",
                    body=body,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "x-hpal-household-id": household_id,
                        "x-idempotency-key": key,
                    },
                    retries=1,
                )
                d_ok = d_status == 409
                self._record_obs(RequestObservation("idempotency_duplicate", d_latency, d_status, d_ok, d_retried, time.time()))
                if not d_ok:
                    self._subsystem_errors["idempotency"] += 1
            except Exception:
                self._subsystem_errors["ui"] += 1
            self._stop_event.wait(0.15)

    def _unauth_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                pair = self._pick_household()
                if pair is None:
                    self._stop_event.wait(0.2)
                    continue
                household_id, _token = pair
                body = {
                    "family_id": household_id,
                    "message": "unauth probe",
                    "session_id": "unauth-probe",
                }
                status, _payload, latency, retried = self._json_request(
                    "POST",
                    "/v1/ui/message",
                    body=body,
                    headers={"x-idempotency-key": self._idem()},
                    retries=0,
                )
                ok = status == 401
                self._record_obs(RequestObservation("unauth_message", latency, status, ok, retried, time.time()))
                if not ok:
                    self._subsystem_errors["auth"] += 1
            except Exception:
                self._subsystem_errors["auth"] += 1
            self._stop_event.wait(0.25)

    def _sse_read_one(self, household_id: str, token: str, last_watermark: str | None) -> tuple[list[tuple[str | None, dict[str, Any] | None]], str | None]:
        path = f"/v1/realtime/stream?household_id={household_id}"
        if last_watermark:
            path += f"&last_watermark={last_watermark}"

        headers = {
            "Authorization": f"Bearer {token}",
            "x-hpal-household-id": household_id,
        }

        conn = http.client.HTTPConnection(HOST, self.port, timeout=6.0)
        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()
        if resp.status != 200:
            conn.close()
            raise StressFailure(f"sse_http_{resp.status}")

        events: list[tuple[str | None, dict[str, Any] | None]] = []
        for _ in range(3):
            event_type = None
            data_obj = None
            for _ in range(120):
                try:
                    raw = resp.readline()
                except socket.timeout:
                    break
                if not raw:
                    break
                line = raw.decode("utf-8").strip()
                if not line:
                    break
                if line.startswith("event:"):
                    event_type = line.split(":", 1)[1].strip()
                elif line.startswith("data:"):
                    payload = line.split(":", 1)[1].strip()
                    try:
                        data_obj = json.loads(payload)
                    except json.JSONDecodeError:
                        data_obj = {"raw": payload}
            events.append((event_type, data_obj))

        conn.close()

        next_watermark = last_watermark
        now_ms = int(time.time() * 1000)
        for event_type, payload in events:
            if event_type == "update" and isinstance(payload, dict):
                wm = payload.get("watermark")
                if isinstance(wm, str) and "-" in wm:
                    next_watermark = wm
                    try:
                        produced_ms = int(wm.split("-", 1)[0])
                        lag = max(0, now_ms - produced_ms)
                        with self._lock:
                            self._sse_lags_ms.append(float(lag))
                    except ValueError:
                        pass

        return events, next_watermark

    def _sse_worker(self) -> None:
        last_watermark_by_household: dict[str, str | None] = defaultdict(lambda: None)
        while not self._stop_event.is_set():
            pair = self._pick_household()
            if pair is None:
                self._stop_event.wait(0.4)
                continue

            hh, token = pair
            start = time.perf_counter()
            try:
                events, next_wm = self._sse_read_one(hh, token, last_watermark_by_household.get(hh))
                last_watermark_by_household[hh] = next_wm
                ok = len(events) > 0 and events[0][0] == "connected"
                latency_ms = (time.perf_counter() - start) * 1000
                self._record_obs(RequestObservation("sse_cycle", latency_ms, 200, ok, False, time.time()))
                if not ok:
                    self._subsystem_errors["stream"] += 1
            except Exception:
                latency_ms = (time.perf_counter() - start) * 1000
                self._record_obs(RequestObservation("sse_cycle", latency_ms, 599, False, False, time.time()))
                self._subsystem_errors["stream"] += 1
            self._stop_event.wait(0.1)

    def _token_validation_storm_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                pair = self._pick_household()
                if pair is None:
                    self._stop_event.wait(0.2)
                    continue
                hh, token = pair
                body = {"family_id": hh, "message": "token-storm", "session_id": "token-storm"}

                for _ in range(4):
                    status, _payload, latency, retried = self._json_request(
                        "POST",
                        "/v1/ui/message",
                        body=body,
                        headers={
                            "Authorization": f"Bearer {token}",
                            "x-hpal-household-id": hh,
                            "x-idempotency-key": self._idem(),
                        },
                        retries=1,
                    )
                    ok = status == 200
                    self._record_obs(RequestObservation("token_storm_valid", latency, status, ok, retried, time.time()))
                    if not ok:
                        self._subsystem_errors["auth"] += 1

                status, _payload, latency, retried = self._json_request(
                    "POST",
                    "/v1/ui/message",
                    body=body,
                    headers={
                        "Authorization": "Bearer invalid",
                        "x-hpal-household-id": hh,
                        "x-idempotency-key": self._idem(),
                    },
                    retries=0,
                )
                ok = status == 401
                self._record_obs(RequestObservation("token_storm_invalid", latency, status, ok, retried, time.time()))
                if not ok:
                    self._subsystem_errors["auth"] += 1
            except Exception:
                self._subsystem_errors["auth"] += 1
            self._stop_event.wait(0.3)

    def _sample_worker(self, server_pid: int, soak_start: float) -> None:
        while not self._stop_event.is_set():
            sample_t = time.time() - soak_start

            status, probe, probe_latency, probe_retried = self._json_request(
                "GET", "/v1/system/boot-probe", retries=1
            )
            ok_probe = status == 200 and isinstance(probe, dict)
            self._record_obs(RequestObservation("probe_sample", probe_latency, status, ok_probe, probe_retried, time.time()))

            m_status, metrics_body, metrics_latency, metrics_retried = self._json_request("GET", "/metrics", retries=1)
            ok_metrics = m_status == 200 and isinstance(metrics_body, dict)
            self._record_obs(RequestObservation("metrics_sample", metrics_latency, m_status, ok_metrics, metrics_retried, time.time()))

            with self._lock:
                cutoff = time.time() - 60
                recent = [o for o in self._observations if o.timestamp >= cutoff]
                recent_lat = [o.latency_ms for o in recent]
                recent_err = [o for o in recent if not o.ok]
                recent_retry = [o for o in recent if o.retried]
                recent_sse_lags = list(self._sse_lags_ms)[-200:]

            pool_size = None
            checked_out = None
            replay_queue_depth = 0.0
            if ok_probe and isinstance(probe, dict):
                parsed_pool = _parse_pool_status(probe.get("pool_status"))
                pool_size = parsed_pool.get("pool_size")
                checked_out = parsed_pool.get("checked_out")

            if ok_metrics and isinstance(metrics_body, dict):
                gauges = metrics_body.get("gauges", {})
                if isinstance(gauges, dict):
                    replay_queue_depth = float(gauges.get("replay_queue_depth", 0.0) or 0.0)

            point = SamplePoint(
                t_seconds=round(sample_t, 3),
                process_memory_mb=_windows_process_memory_mb(server_pid),
                p95_latency_ms=round(_percentile(recent_lat, 0.95), 3),
                error_rate=round((len(recent_err) / len(recent)) if recent else 0.0, 5),
                sse_lag_ms_p95=round(_percentile(recent_sse_lags, 0.95), 3),
                retry_rate=round((len(recent_retry) / len(recent)) if recent else 0.0, 5),
                pool_checked_out=int(checked_out) if isinstance(checked_out, int) else None,
                pool_size=int(pool_size) if isinstance(pool_size, int) else None,
                replay_queue_depth=round(replay_queue_depth, 3),
            )
            with self._lock:
                self._sample_points.append(point)

            self._stop_event.wait(self.sample_interval_seconds)

    def _run_breakpoint_step(self, users: int, duration_seconds: int) -> BreakpointStepResult:
        stop_step = threading.Event()
        obs_queue: queue.Queue[RequestObservation] = queue.Queue()
        err_counter: Counter[str] = Counter()

        def _client(idx: int) -> None:
            while not stop_step.is_set():
                pair = self._pick_household()
                if pair is None:
                    time.sleep(0.05)
                    continue
                hh, token = pair
                status, payload, latency, retried = self._json_request(
                    "POST",
                    "/v1/ui/message",
                    body={
                        "family_id": hh,
                        "message": f"breakpoint-{users}-{idx}",
                        "session_id": f"break-{idx}",
                    },
                    headers={
                        "Authorization": f"Bearer {token}",
                        "x-hpal-household-id": hh,
                        "x-idempotency-key": self._idem(),
                    },
                    timeout=8,
                    retries=1,
                )
                ok = status == 200
                if not ok:
                    detail = payload.get("detail") if isinstance(payload, dict) else str(payload)
                    err_counter[str(detail)[:120]] += 1
                obs_queue.put(RequestObservation("breakpoint", latency, status, ok, retried, time.time()))
                time.sleep(0.05)

        threads = [threading.Thread(target=_client, args=(i,), daemon=True) for i in range(users)]
        for t in threads:
            t.start()

        time.sleep(duration_seconds)
        stop_step.set()
        for t in threads:
            t.join(timeout=3)

        observations: list[RequestObservation] = []
        while True:
            try:
                observations.append(obs_queue.get_nowait())
            except queue.Empty:
                break

        total = len(observations)
        if total == 0:
            return BreakpointStepResult(
                users=users,
                total_requests=0,
                success_rate=0.0,
                error_rate=1.0,
                p95_latency_ms=0.0,
                avg_latency_ms=0.0,
                dominant_error="no_requests_completed",
                stable=False,
            )

        success = [o for o in observations if o.ok]
        failures = [o for o in observations if not o.ok]
        latencies = [o.latency_ms for o in observations]
        success_rate = len(success) / total
        error_rate = len(failures) / total
        p95 = _percentile(latencies, 0.95)
        avg = statistics.mean(latencies)
        dominant_error = err_counter.most_common(1)[0][0] if err_counter else None
        stable = success_rate >= 0.98 and p95 < 1500 and error_rate < 0.02

        with self._lock:
            self._observations.extend(observations)

        return BreakpointStepResult(
            users=users,
            total_requests=total,
            success_rate=round(success_rate, 5),
            error_rate=round(error_rate, 5),
            p95_latency_ms=round(p95, 3),
            avg_latency_ms=round(avg, 3),
            dominant_error=dominant_error,
            stable=stable,
        )

    def _run_saturation_tests(self, saturate_seconds: int) -> SaturationResult:
        db_result: dict[str, Any] = {}
        sse_result: dict[str, Any] = {}
        token_result: dict[str, Any] = {}

        # 1) DB exhaustion simulation: heavy concurrent probe + bootstrap reads
        db_stop = threading.Event()
        max_checked_out = 0
        pool_size_seen = 0
        db_errors = 0

        def _db_hammer() -> None:
            nonlocal max_checked_out, pool_size_seen, db_errors
            while not db_stop.is_set():
                status, probe, latency, retried = self._json_request("GET", "/v1/system/boot-probe", retries=1, timeout=4)
                ok = status == 200 and isinstance(probe, dict)
                self._record_obs(RequestObservation("db_saturation_probe", latency, status, ok, retried, time.time()))
                if not ok:
                    db_errors += 1
                    continue
                parsed = _parse_pool_status(probe.get("pool_status") if isinstance(probe, dict) else None)
                checked = parsed.get("checked_out")
                psize = parsed.get("pool_size")
                if isinstance(checked, int):
                    max_checked_out = max(max_checked_out, checked)
                if isinstance(psize, int):
                    pool_size_seen = max(pool_size_seen, psize)

        db_threads = [threading.Thread(target=_db_hammer, daemon=True) for _ in range(120)]
        for t in db_threads:
            t.start()
        time.sleep(saturate_seconds)
        db_stop.set()
        for t in db_threads:
            t.join(timeout=2)

        db_result = {
            "max_checked_out": max_checked_out,
            "pool_size_seen": pool_size_seen,
            "errors": db_errors,
            "appears_exhausted": bool(pool_size_seen and max_checked_out >= pool_size_seen),
        }

        # 2) SSE broadcast flood: many message writers while SSE cycles run
        sse_stop = threading.Event()
        flood_err = 0
        with self._lock:
            lag_before = list(self._sse_lags_ms)

        def _flood_writer() -> None:
            nonlocal flood_err
            while not sse_stop.is_set():
                pair = self._pick_household()
                if pair is None:
                    time.sleep(0.02)
                    continue
                hh, token = pair
                status, _payload, latency, retried = self._json_request(
                    "POST",
                    "/v1/ui/message",
                    body={"family_id": hh, "message": "sse-flood", "session_id": "sse-flood"},
                    headers={
                        "Authorization": f"Bearer {token}",
                        "x-hpal-household-id": hh,
                        "x-idempotency-key": self._idem(),
                    },
                    retries=1,
                    timeout=6,
                )
                ok = status == 200
                self._record_obs(RequestObservation("sse_flood_write", latency, status, ok, retried, time.time()))
                if not ok:
                    flood_err += 1

        flood_threads = [threading.Thread(target=_flood_writer, daemon=True) for _ in range(40)]
        sse_threads = [threading.Thread(target=self._sse_worker, daemon=True) for _ in range(20)]
        for t in flood_threads + sse_threads:
            t.start()
        time.sleep(saturate_seconds)
        sse_stop.set()
        self._stop_event.set()
        # temporarily pause workers started by this function only; global stop will be reset below
        for t in flood_threads + sse_threads:
            t.join(timeout=2)
        self._stop_event.clear()

        with self._lock:
            lag_after = list(self._sse_lags_ms)
            recent = lag_after[len(lag_before):] if len(lag_after) > len(lag_before) else lag_after
        sse_result = {
            "flood_errors": flood_err,
            "lag_p95_ms": round(_percentile(recent, 0.95), 3),
            "lag_max_ms": round(max(recent), 3) if recent else 0.0,
            "backlog_indicator": round(_percentile(recent, 0.95), 3) > 5000,
        }

        # 3) Token validation storm
        token_stop = threading.Event()
        token_failures = 0
        invalid_failures = 0

        def _token_storm() -> None:
            nonlocal token_failures, invalid_failures
            while not token_stop.is_set():
                pair = self._pick_household()
                if pair is None:
                    time.sleep(0.02)
                    continue
                hh, token = pair
                status, _payload, latency, retried = self._json_request(
                    "POST",
                    "/v1/ui/message",
                    body={"family_id": hh, "message": "token-storm", "session_id": "token-storm"},
                    headers={
                        "Authorization": f"Bearer {token}",
                        "x-hpal-household-id": hh,
                        "x-idempotency-key": self._idem(),
                    },
                    retries=1,
                    timeout=6,
                )
                ok = status == 200
                self._record_obs(RequestObservation("token_saturation_valid", latency, status, ok, retried, time.time()))
                if not ok:
                    token_failures += 1

                i_status, _i_payload, i_latency, i_retried = self._json_request(
                    "POST",
                    "/v1/ui/message",
                    body={"family_id": hh, "message": "token-storm-invalid", "session_id": "token-storm-invalid"},
                    headers={
                        "Authorization": "Bearer invalid",
                        "x-hpal-household-id": hh,
                        "x-idempotency-key": self._idem(),
                    },
                    retries=0,
                    timeout=6,
                )
                i_ok = i_status == 401
                self._record_obs(RequestObservation("token_saturation_invalid", i_latency, i_status, i_ok, i_retried, time.time()))
                if not i_ok:
                    invalid_failures += 1

        token_threads = [threading.Thread(target=_token_storm, daemon=True) for _ in range(80)]
        for t in token_threads:
            t.start()
        time.sleep(saturate_seconds)
        token_stop.set()
        for t in token_threads:
            t.join(timeout=2)

        token_result = {
            "valid_token_failures": token_failures,
            "invalid_token_unexpected": invalid_failures,
            "storm_auth_stable": token_failures < 10 and invalid_failures == 0,
        }

        graceful = (
            (db_result["errors"] < 200)
            and (sse_result["flood_errors"] < 200)
            and token_result["storm_auth_stable"]
        )

        return SaturationResult(
            db_exhaustion=db_result,
            sse_flood=sse_result,
            token_storm=token_result,
            graceful_degradation=graceful,
        )

    def execute(
        self,
        *,
        breakpoint_levels: list[int],
        breakpoint_step_seconds: int,
        saturation_seconds: int,
    ) -> dict[str, Any]:
        server: subprocess.Popen[str] | None = None
        soak_start = time.time()
        soak_duration_seconds = self.duration_minutes * 60

        try:
            self._kill_listeners_on_ports([8000, 8010, self.port])
            time.sleep(0.4)
            server = self._start_server()
            probe = self._wait_ready()
            _ = probe

            # Seed baseline households/tokens before load loops.
            for _ in range(5):
                self._register_household()

            threads = [
                threading.Thread(target=self._household_worker, daemon=True),
                threading.Thread(target=self._ui_message_worker, daemon=True),
                threading.Thread(target=self._ui_message_worker, daemon=True),
                threading.Thread(target=self._ui_message_worker, daemon=True),
                threading.Thread(target=self._unauth_worker, daemon=True),
                threading.Thread(target=self._sse_worker, daemon=True),
                threading.Thread(target=self._sse_worker, daemon=True),
                threading.Thread(target=self._token_validation_storm_worker, daemon=True),
                threading.Thread(target=self._sample_worker, args=(server.pid, soak_start), daemon=True),
            ]

            for t in threads:
                t.start()

            end_time = soak_start + soak_duration_seconds
            while time.time() < end_time:
                time.sleep(1)

            self._stop_event.set()
            for t in threads:
                t.join(timeout=5)

            with self._lock:
                observations = list(self._observations)
                sample_points = list(self._sample_points)
                sse_lags = list(self._sse_lags_ms)
                subsystem_errors = dict(self._subsystem_errors)

            # Breakpoint phase
            breakpoint_results = []
            for level in breakpoint_levels:
                result = self._run_breakpoint_step(level, breakpoint_step_seconds)
                breakpoint_results.append(result)

            # Saturation phase
            saturation = self._run_saturation_tests(saturation_seconds)

            # Post checks
            h_status, h_payload, h_latency, h_retried = self._json_request("GET", "/v1/system/health", retries=1)
            health_ok = h_status == 200 and isinstance(h_payload, dict) and h_payload.get("status") == "healthy"
            self._record_obs(RequestObservation("post_health", h_latency, h_status, health_ok, h_retried, time.time()))

            return self._build_report(
                observations=observations,
                sample_points=sample_points,
                sse_lags=sse_lags,
                breakpoint_results=breakpoint_results,
                saturation=saturation,
                subsystem_errors=subsystem_errors,
                health_ok=health_ok,
                soak_duration_seconds=soak_duration_seconds,
            )
        finally:
            self._stop_event.set()
            if server is not None:
                server.terminate()
                try:
                    server.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    server.kill()

    def _build_report(
        self,
        *,
        observations: list[RequestObservation],
        sample_points: list[SamplePoint],
        sse_lags: list[float],
        breakpoint_results: list[BreakpointStepResult],
        saturation: SaturationResult,
        subsystem_errors: dict[str, int],
        health_ok: bool,
        soak_duration_seconds: int,
    ) -> dict[str, Any]:
        total = len(observations)
        failures = [o for o in observations if not o.ok]
        retries = [o for o in observations if o.retried]
        latencies = [o.latency_ms for o in observations]

        grouped: dict[str, list[RequestObservation]] = defaultdict(list)
        for obs in observations:
            grouped[obs.category].append(obs)

        idemp_lat = [o.latency_ms for o in grouped.get("idempotency_duplicate", [])]
        idemp_series: list[tuple[float, float]] = []
        if grouped.get("idempotency_duplicate"):
            t0 = grouped["idempotency_duplicate"][0].timestamp
            bins: dict[int, list[float]] = defaultdict(list)
            for o in grouped["idempotency_duplicate"]:
                minute = int((o.timestamp - t0) // 60)
                bins[minute].append(o.latency_ms)
            for minute, vals in sorted(bins.items()):
                idemp_series.append((float(minute), statistics.mean(vals)))

        latency_series = [(p.t_seconds / 60.0, p.p95_latency_ms) for p in sample_points]
        memory_series = [(p.t_seconds / 60.0, p.process_memory_mb) for p in sample_points]
        retry_series = [(p.t_seconds / 60.0, p.retry_rate) for p in sample_points]
        sse_lag_series = [(p.t_seconds / 60.0, p.sse_lag_ms_p95) for p in sample_points]

        latency_slope = _linear_slope(latency_series)
        memory_slope = _linear_slope(memory_series)
        retry_slope = _linear_slope(retry_series)
        sse_lag_slope = _linear_slope(sse_lag_series)
        idemp_slope = _linear_slope(idemp_series)

        max_stable = 0
        first_failing_subsystem = None
        for step in breakpoint_results:
            if step.stable:
                max_stable = max(max_stable, step.users)
            elif first_failing_subsystem is None:
                err = (step.dominant_error or "").lower()
                if "token" in err or "bearer" in err or "expired" in err:
                    first_failing_subsystem = "auth"
                elif "watermark" in err or "stream" in err or "sse" in err:
                    first_failing_subsystem = "stream"
                elif "database" in err or "sql" in err or "locked" in err:
                    first_failing_subsystem = "database"
                elif "idempotency" in err:
                    first_failing_subsystem = "idempotency"
                else:
                    first_failing_subsystem = "ui_or_gateway"

        if first_failing_subsystem is None:
            first_failing_subsystem = "none"

        # Scoring model for runtime-only behavior.
        score = 100.0
        error_rate = (len(failures) / total) if total else 1.0
        p95_latency = _percentile(latencies, 0.95)
        sse_lag_p95 = _percentile(sse_lags, 0.95)

        score -= min(35.0, error_rate * 220)
        score -= min(15.0, max(0.0, latency_slope) * 4.0)
        score -= min(15.0, max(0.0, memory_slope) * 0.8)
        score -= min(10.0, max(0.0, retry_slope) * 500)
        score -= min(10.0, max(0.0, sse_lag_slope) * 0.02)
        score -= min(8.0, max(0.0, idemp_slope) * 0.4)
        if max_stable < 100:
            score -= 12.0
        if not saturation.graceful_degradation:
            score -= 10.0
        if not health_ok:
            score -= 20.0

        score = max(0.0, round(score, 2))

        hard_fail = (
            (error_rate >= 0.10)
            or (max_stable < 50)
            or (not health_ok)
            or (not saturation.graceful_degradation)
        )

        if hard_fail or score < 60:
            classification = "NOT_READY"
        elif score >= 80 and max_stable >= 100 and saturation.graceful_degradation:
            classification = "PRODUCTION_READY"
        else:
            classification = "CONDITIONALLY_READY"

        dominant_subsystem = "none"
        if subsystem_errors:
            dominant_subsystem = sorted(subsystem_errors.items(), key=lambda kv: kv[1], reverse=True)[0][0]

        predicted_failure_mode = "none"
        if max_stable < 100:
            predicted_failure_mode = "concurrency_collapse_before_100_clients"
        elif memory_slope > 8.0:
            predicted_failure_mode = "memory_growth_indicates_leak_over_multi_hour_runtime"
        elif sse_lag_slope > 50:
            predicted_failure_mode = "sse_backlog_growth_leads_to_stale_ui_and_forced_resync"
        elif latency_slope > 80:
            predicted_failure_mode = "latency_runaway_under_sustained_load"
        elif idemp_slope > 20:
            predicted_failure_mode = "idempotency_lookup_path_degrades_with_key_growth"

        drift = {
            "latency_degradation_slope_ms_per_min": round(latency_slope, 4),
            "memory_growth_slope_mb_per_min": round(memory_slope, 4),
            "retry_growth_slope_ratio_per_min": round(retry_slope, 6),
            "sse_lag_slope_ms_per_min": round(sse_lag_slope, 4),
            "idempotency_duplicate_latency_slope_ms_per_min": round(idemp_slope, 4),
            "memory_leak_indicator": memory_slope > 4.0,
            "sse_backlog_indicator": sse_lag_slope > 30.0,
        }

        report = {
            "timestamp": _utc_now(),
            "mode": "runtime_stability_production_readiness_audit",
            "duration_minutes": self.duration_minutes,
            "soak_duration_seconds": soak_duration_seconds,
            "stability_score": score,
            "classification": classification,
            "primary_bottleneck_subsystem": dominant_subsystem,
            "first_failing_subsystem": first_failing_subsystem,
            "predicted_failure_mode_under_real_usage": predicted_failure_mode,
            "runtime_summary": {
                "total_observations": total,
                "error_rate": round(error_rate, 5),
                "retry_rate": round((len(retries) / total) if total else 0.0, 5),
                "p95_latency_ms": round(p95_latency, 3),
                "avg_latency_ms": round(statistics.mean(latencies), 3) if latencies else 0.0,
                "sse_event_lag_p95_ms": round(sse_lag_p95, 3),
                "idempotency_duplicate_p95_ms": round(_percentile(idemp_lat, 0.95), 3),
            },
            "failure_drift_detection": drift,
            "breakpoint_test": {
                "levels": [asdict(x) for x in breakpoint_results],
                "max_stable_concurrency": max_stable,
            },
            "resource_saturation": asdict(saturation),
            "subsystem_error_counts": subsystem_errors,
            "sample_points": [asdict(p) for p in sample_points],
            "observations_by_category": {
                key: {
                    "count": len(vals),
                    "error_rate": round((len([v for v in vals if not v.ok]) / len(vals)) if vals else 0.0, 5),
                    "p95_latency_ms": round(_percentile([v.latency_ms for v in vals], 0.95), 3),
                }
                for key, vals in grouped.items()
            },
            "scoring_basis": {
                "runtime_only": True,
                "boot_determinism_excluded": True,
            },
        }
        return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime stability and production readiness audit")
    parser.add_argument("--duration-minutes", type=int, default=15, help="Long-run soak duration (15-30 recommended)")
    parser.add_argument("--sample-interval-seconds", type=int, default=5, help="Telemetry sample interval")
    parser.add_argument("--breakpoint-levels", type=str, default="10,50,100,250", help="Comma-separated concurrency levels")
    parser.add_argument("--breakpoint-step-seconds", type=int, default=45, help="Duration per breakpoint level")
    parser.add_argument("--saturation-seconds", type=int, default=45, help="Duration per saturation sub-test")
    parser.add_argument("--port", type=int, default=BASE_PORT, help="API port for harness-owned server")
    args = parser.parse_args()

    levels = [int(x.strip()) for x in args.breakpoint_levels.split(",") if x.strip()]
    if args.duration_minutes < 15:
        raise SystemExit("duration-minutes must be >= 15 for production audit")

    harness = RuntimeStressHarness(
        port=args.port,
        duration_minutes=args.duration_minutes,
        sample_interval_seconds=args.sample_interval_seconds,
    )
    report = harness.execute(
        breakpoint_levels=levels,
        breakpoint_step_seconds=args.breakpoint_step_seconds,
        saturation_seconds=args.saturation_seconds,
    )
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))

    cls = str(report.get("classification"))
    return 0 if cls in {"PRODUCTION_READY", "CONDITIONALLY_READY"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
