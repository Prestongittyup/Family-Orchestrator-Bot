from __future__ import annotations

import argparse
import json
import os
import random
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener


BASE_DIR = Path(__file__).resolve().parents[1]
HOST = "127.0.0.1"
BASE_PORT = 8011
REPORT_PATH = BASE_DIR / "boot_smoke_report.json"

FAILURE_CLASSES = [
    "BOOT_FAILURE",
    "CONTRACT_FAILURE",
    "STATE_FAILURE",
    "AUTH_FAILURE",
    "STREAM_FAILURE",
]


@dataclass
class CheckResult:
    name: str
    passed: bool
    status_code: int | None = None
    detail: str | None = None


@dataclass
class RunResult:
    run_index: int
    port: int
    classification: str
    reason: str
    passed: bool
    boot_probe: dict[str, Any] | None
    checks: list[CheckResult]


class SmokeFailure(Exception):
    def __init__(self, category: str, message: str) -> None:
        super().__init__(message)
        self.category = category
        self.message = message


class BootSmokeHarness:
    def __init__(self, *, port: int) -> None:
        self.port = port
        self.base_url = f"http://{HOST}:{self.port}"

    def _idem(self) -> str:
        return f"boot-smoke-{uuid.uuid4().hex}"

    def _json_request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 10.0,
    ) -> tuple[int, dict[str, Any] | str]:
        payload = None
        req_headers = {"Content-Type": "application/json"}
        if headers:
            req_headers.update(headers)
        if body is not None:
            payload = json.dumps(body).encode("utf-8")

        req = Request(f"{self.base_url}{path}", data=payload, method=method, headers=req_headers)
        opener = build_opener(ProxyHandler({}))
        try:
            with opener.open(req, timeout=timeout) as resp:
                status = resp.getcode()
                text = resp.read().decode("utf-8")
                try:
                    return status, json.loads(text)
                except json.JSONDecodeError:
                    return status, text
        except HTTPError as exc:
            text = exc.read().decode("utf-8") if exc.fp else ""
            try:
                parsed = json.loads(text) if text else {}
            except json.JSONDecodeError:
                parsed = text
            return exc.code, parsed
        except URLError as exc:
            raise SmokeFailure("BOOT_FAILURE", f"network_error:{exc}") from exc

    def _kill_listeners_on_ports(self, ports: list[int]) -> None:
        if os.name != "nt":
            return

        netstat = subprocess.run(["netstat", "-ano", "-p", "tcp"], capture_output=True, text=True, check=False)
        pids: set[str] = set()
        for line in netstat.stdout.splitlines():
            upper = line.upper()
            if "LISTENING" not in upper:
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

    def _start_server(self) -> subprocess.Popen[str]:
        env = os.environ.copy()
        env["PORT"] = str(self.port)
        return subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "apps.api.main:app", "--host", HOST, "--port", str(self.port)],
            cwd=str(BASE_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )

    def _wait_ready(self, timeout_seconds: int = 45) -> dict[str, Any]:
        deadline = time.time() + timeout_seconds
        last_error = ""
        while time.time() < deadline:
            try:
                status, body = self._json_request("GET", "/v1/system/boot-probe")
                if status == 200 and isinstance(body, dict):
                    return body
                last_error = f"status={status} body={body}"
            except Exception as exc:
                last_error = str(exc)
            time.sleep(1)
        raise SmokeFailure("BOOT_FAILURE", f"readiness_timeout:{last_error}")

    def _read_sse_events(
        self,
        path: str,
        headers: dict[str, str],
        count: int = 1,
        timeout: float = 3.0,
        inter_line_delay: float = 0.0,
    ) -> list[tuple[str | None, dict[str, Any] | None]]:
        import http.client

        conn = http.client.HTTPConnection(HOST, self.port, timeout=timeout)
        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()
        if resp.status != 200:
            raise SmokeFailure("STREAM_FAILURE", f"sse_http_{resp.status}")

        events: list[tuple[str | None, dict[str, Any] | None]] = []
        for _ in range(count):
            event_type = None
            data_obj = None
            for _line_idx in range(120):
                try:
                    raw = resp.readline()
                except socket.timeout:
                    break
                if not raw:
                    break
                if inter_line_delay > 0:
                    time.sleep(inter_line_delay)
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
        return events

    def _add_result(
        self,
        results: list[CheckResult],
        name: str,
        passed: bool,
        status_code: int | None = None,
        detail: str | None = None,
    ) -> None:
        results.append(CheckResult(name=name, passed=passed, status_code=status_code, detail=detail))

    def _classify(self, results: list[CheckResult]) -> tuple[str, str]:
        failed = [r for r in results if not r.passed]
        if not failed:
            return "NONE", "all checks passed"

        if any(r.name.startswith("boot_") for r in failed):
            return "BOOT_FAILURE", failed[0].detail or "boot failure"
        if any(r.name.startswith("state_") for r in failed):
            return "STATE_FAILURE", failed[0].detail or "state failure"
        if any(r.name.startswith("auth_") for r in failed):
            return "AUTH_FAILURE", failed[0].detail or "auth failure"
        if any(r.name.startswith("stream_") for r in failed):
            return "STREAM_FAILURE", failed[0].detail or "stream failure"
        return "CONTRACT_FAILURE", failed[0].detail or "contract failure"

    def _run_sse_adversarial_test(self, *, household_id: str, token: str) -> tuple[bool, str]:
        headers = {"Authorization": f"Bearer {token}", "x-hpal-household-id": household_id}
        started = time.time()

        class _Collector:
            def __init__(self, name: str) -> None:
                self.name = name
                self.connected = 0
                self.update_watermarks: list[str] = []
                self.error: str | None = None

        collectors = [_Collector(f"client_{idx}") for idx in range(3)]
        slow = _Collector("slow_consumer")
        reconnect = _Collector("reconnect_storm")

        def _consume_stream(target: _Collector, slow_mode: bool = False) -> None:
            try:
                events = self._read_sse_events(
                    f"/v1/realtime/stream?household_id={household_id}",
                    headers=headers,
                    count=6,
                    timeout=4.0,
                    inter_line_delay=0.05 if slow_mode else 0.0,
                )
                for evt, payload in events:
                    if evt == "connected":
                        target.connected += 1
                    elif evt == "update" and isinstance(payload, dict):
                        wm = payload.get("watermark")
                        if isinstance(wm, str):
                            target.update_watermarks.append(wm)
            except Exception as exc:
                target.error = str(exc)

        threads: list[threading.Thread] = []
        for c in collectors:
            t = threading.Thread(target=_consume_stream, args=(c,), daemon=True)
            t.start()
            threads.append(t)

        slow_thread = threading.Thread(target=_consume_stream, args=(slow, True), daemon=True)
        slow_thread.start()
        threads.append(slow_thread)

        for _ in range(10):
            try:
                events = self._read_sse_events(
                    f"/v1/realtime/stream?household_id={household_id}",
                    headers=headers,
                    count=1,
                    timeout=2.0,
                )
                if events and events[0][0] == "connected":
                    reconnect.connected += 1
            except Exception as exc:
                reconnect.error = str(exc)
                break

        for i in range(4):
            body = {
                "family_id": household_id,
                "message": f"sse adversarial ping {i}",
                "session_id": f"sse-stress-{i}",
            }
            self._json_request(
                "POST",
                "/v1/ui/message",
                body=body,
                headers={
                    "Authorization": f"Bearer {token}",
                    "x-hpal-household-id": household_id,
                    "x-idempotency-key": self._idem(),
                },
                timeout=8.0,
            )

        for t in threads:
            t.join(timeout=5.0)

        if time.time() - started > 10.0:
            return False, "deadlock_detected:adversarial_test_timeout"

        if any(c.error for c in collectors) or slow.error or reconnect.error:
            return False, f"client_error:{[c.error for c in collectors]},{slow.error},{reconnect.error}"

        if reconnect.connected < 10:
            return False, f"reconnect_storm_incomplete:{reconnect.connected}/10"

        for c in collectors + [slow]:
            if len(c.update_watermarks) != len(set(c.update_watermarks)):
                return False, f"duplicate_watermark_detected:{c.name}"

        if any(len(c.update_watermarks) < 1 for c in collectors):
            return False, "event_loss_detected:one_or_more_clients_received_no_updates"

        return True, "adversarial_sse_ok"

    def execute_single_run(self) -> RunResult:
        results: list[CheckResult] = []
        server: subprocess.Popen[str] | None = None
        boot_probe: dict[str, Any] | None = None

        try:
            self._kill_listeners_on_ports([8000, 8010, self.port])
            time.sleep(0.6)

            server = self._start_server()
            boot_probe = self._wait_ready()
            self._add_result(results, "boot_server_ready", True, 200, "server ready")

            required_probe_keys = [
                "database",
                "identity_repo",
                "household_repo",
                "auth_middleware",
                "broadcaster",
                "repository_fresh_transaction",
                "sse_internal_probe",
            ]
            key_ok = all(str(boot_probe.get(k)) == "ok" for k in required_probe_keys)
            self._add_result(results, "state_boot_probe_components", key_ok, 200, json.dumps(boot_probe, sort_keys=True))

            founder_email = f"boot-smoke-{uuid.uuid4().hex[:8]}@example.com"
            create_body = {
                "name": f"Boot Smoke {uuid.uuid4().hex[:6]}",
                "timezone": "UTC",
                "founder_user_name": "Boot Founder",
                "founder_email": founder_email,
            }
            status, created = self._json_request(
                "POST",
                "/v1/identity/household/create",
                body=create_body,
                headers={"x-idempotency-key": self._idem()},
            )
            ok = status == 200 and isinstance(created, dict) and "household" in created
            self._add_result(results, "contract_household_create_unique", ok, status, str(created))
            if not ok:
                raise SmokeFailure("CONTRACT_FAILURE", f"household_create_failed:{status}")

            household_id = created["household"]["household_id"]

            dup_status, dup_body = self._json_request(
                "POST",
                "/v1/identity/household/create",
                body=create_body,
                headers={"x-idempotency-key": self._idem()},
            )
            self._add_result(results, "contract_household_create_duplicate_400", dup_status == 400, dup_status, str(dup_body))

            bootstrap_status, bootstrap = self._json_request(
                "POST",
                "/v1/identity/bootstrap",
                body={"household_id": household_id},
            )
            token = bootstrap.get("session_token") if isinstance(bootstrap, dict) else ""
            is_jwt = isinstance(token, str) and token.count(".") == 2
            self._add_result(results, "contract_bootstrap_jwt", bootstrap_status == 200 and is_jwt, bootstrap_status, str(bootstrap))
            if not (bootstrap_status == 200 and is_jwt):
                raise SmokeFailure("AUTH_FAILURE", "bootstrap token invalid")

            ui_msg = {"family_id": household_id, "message": "boot smoke ping", "session_id": "boot-smoke-session"}
            missing_auth_status, missing_auth_body = self._json_request(
                "POST",
                "/v1/ui/message",
                body=ui_msg,
                headers={"x-idempotency-key": self._idem()},
            )
            self._add_result(results, "auth_missing_token_401", missing_auth_status == 401, missing_auth_status, str(missing_auth_body))

            invalid_auth_status, invalid_auth_body = self._json_request(
                "POST",
                "/v1/ui/message",
                body=ui_msg,
                headers={"Authorization": "Bearer invalid", "x-idempotency-key": self._idem()},
            )
            self._add_result(results, "auth_invalid_token_401", invalid_auth_status == 401, invalid_auth_status, str(invalid_auth_body))

            valid_auth_status, valid_auth_body = self._json_request(
                "POST",
                "/v1/ui/message",
                body=ui_msg,
                headers={
                    "Authorization": f"Bearer {token}",
                    "x-hpal-household-id": household_id,
                    "x-idempotency-key": self._idem(),
                },
            )
            self._add_result(results, "auth_valid_token_200", valid_auth_status == 200, valid_auth_status, str(valid_auth_body))

            sse_headers = {"Authorization": f"Bearer {token}", "x-hpal-household-id": household_id}
            initial_events = self._read_sse_events(
                f"/v1/realtime/stream?household_id={household_id}",
                headers=sse_headers,
                count=1,
            )
            event_type, data = initial_events[0]
            self._add_result(results, "stream_connect_valid_jwt", event_type == "connected", 200, str(data))

            watermark = data.get("watermark") if isinstance(data, dict) else None
            if isinstance(watermark, str) and watermark:
                reconnect_events = self._read_sse_events(
                    f"/v1/realtime/stream?household_id={household_id}&last_watermark={watermark}",
                    headers=sse_headers,
                    count=2,
                )
                reconnect_event, reconnect_data = reconnect_events[0]
                reconnect_second_event = reconnect_events[1][0] if len(reconnect_events) > 1 else None
                self._add_result(
                    results,
                    "stream_reconnect_no_forced_resync_on_zero_sequence",
                    reconnect_event == "connected" and reconnect_second_event != "resync_required",
                    200,
                    f"first={reconnect_data}, second_event={reconnect_second_event}",
                )

            gap_events = self._read_sse_events(
                f"/v1/realtime/stream?household_id={household_id}&last_watermark=1111111111111-999999",
                headers=sse_headers,
                count=2,
            )
            first_event, _first_data = gap_events[0]
            second_event, second_data = gap_events[1] if len(gap_events) > 1 else (None, None)
            self._add_result(
                results,
                "stream_gap_requires_resync",
                first_event == "connected" and second_event == "resync_required",
                200,
                str(second_data),
            )

            adversarial_ok, adversarial_detail = self._run_sse_adversarial_test(
                household_id=household_id,
                token=token,
            )
            self._add_result(
                results,
                "stream_adversarial_mini_test",
                adversarial_ok,
                200 if adversarial_ok else None,
                adversarial_detail,
            )

        except SmokeFailure as exc:
            self._add_result(results, f"{exc.category.lower()}_exception", False, None, exc.message)
        except Exception as exc:
            self._add_result(results, "boot_unhandled_exception", False, None, f"{type(exc).__name__}:{exc}")
        finally:
            if server is not None:
                server.terminate()
                try:
                    server.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    server.kill()

        classification, reason = self._classify(results)
        passed = all(r.passed for r in results)
        return RunResult(
            run_index=0,
            port=self.port,
            classification=classification,
            reason=reason,
            passed=passed,
            boot_probe=boot_probe,
            checks=results,
        )


def _determinism_label(success_rate: float) -> str:
    if success_rate == 1.0:
        return "DETERMINISTIC"
    if success_rate == 0.0:
        return "NON-DETERMINISTIC"
    return "PROBABILISTIC"


def main() -> int:
    parser = argparse.ArgumentParser(description="Statistical boot contract validator")
    parser.add_argument("--runs", type=int, default=10, help="Number of full boot validation runs")
    parser.add_argument(
        "--randomized-port-offset",
        action="store_true",
        help="Apply random port offset per run to simulate contention",
    )
    parser.add_argument(
        "--random-start-delay",
        action="store_true",
        help="Apply 0-500ms jitter before server start per run",
    )
    args = parser.parse_args()

    all_runs: list[RunResult] = []
    for idx in range(args.runs):
        if args.random_start_delay:
            time.sleep(random.uniform(0.0, 0.5))

        port = BASE_PORT + random.randint(0, 60) if args.randomized_port_offset else BASE_PORT
        harness = BootSmokeHarness(port=port)
        result = harness.execute_single_run()
        result.run_index = idx + 1
        all_runs.append(result)

    successes = sum(1 for r in all_runs if r.passed)
    success_rate = (successes / args.runs) if args.runs else 0.0
    failure_counter = Counter(r.classification for r in all_runs if r.classification != "NONE")
    failure_distribution = {cls: int(failure_counter.get(cls, 0)) for cls in FAILURE_CLASSES}

    report = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mode": "statistical_boot_validation",
        "runs": args.runs,
        "randomized_port_offset": bool(args.randomized_port_offset),
        "random_start_delay": bool(args.random_start_delay),
        "boot_invariant_score": {
            "success_rate": success_rate,
            "successful_runs": successes,
            "total_runs": args.runs,
            "failure_class_distribution": failure_distribution,
        },
        "system_classification": _determinism_label(success_rate),
        "deterministic_requirement": "success_rate == 1.0",
        "runs_detail": [
            {
                "run_index": r.run_index,
                "port": r.port,
                "classification": r.classification,
                "reason": r.reason,
                "passed": r.passed,
                "boot_probe": r.boot_probe,
                "checks": [asdict(c) for c in r.checks],
            }
            for r in all_runs
        ],
    }

    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if success_rate == 1.0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
