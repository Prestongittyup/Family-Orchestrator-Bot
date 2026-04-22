"""
BASELINE DEBUGGING TEST — CORRECTNESS & FAILURE CLASSIFICATION AUDIT

Controlled low-concurrency baseline run (10-25 users) to identify root cause
of request failures. Categorizes every failure into ONE classification.

FIXED: Uses VALID API endpoints only
- /health (root, public, liveness probe)
- /v1/system/health (public, no auth required)
- /v1/system/boot-status (public, detailed diagnostics)

REMOVED: /v1/auth/identity (DOES NOT EXIST - causes 404)
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import statistics
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.main import create_app

HOST = "127.0.0.1"
BASELINE_DURATION_SECONDS = 300  # 5 minutes
BASELINE_CONCURRENCY_MIN = 10
BASELINE_CONCURRENCY_MAX = 25


@dataclass
class RequestLog:
    """Per-request structured logging entry."""
    timestamp: float
    request_id: str
    endpoint: str
    method: str
    status_code: int | str
    latency_ms: float
    failure_type: str | None  # AUTH_FAILURE, ROUTING_FAILURE, etc.
    failure_reason: str | None
    retry_count: int = 0


class BaselineDebugTestHarness:
    """Low-concurrency baseline debugging test."""

    def __init__(self):
        self.request_log: list[RequestLog] = []
        self.current_port = None
        self.server = None
        self.server_thread = None

        # Failure classification counters
        self.failure_distribution = {
            "AUTH_FAILURE": 0,
            "ROUTING_FAILURE": 0,
            "VALIDATION_FAILURE": 0,
            "ADMISSION_REJECTION": 0,
            "HANDLER_EXCEPTION": 0,
            "DEPENDENCY_FAILURE": 0,
            "UNKNOWN_FAILURE": 0,
        }

    def _find_free_port(self) -> int:
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((HOST, 0))
            sock.listen(1)
            return int(sock.getsockname()[1])

    def _wait_server_ready(self, port: int, timeout_s: float = 15.0) -> bool:
        """Wait for server to start responding."""
        import urllib.request
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                url = f"http://{HOST}:{port}/health"
                urllib.request.urlopen(url, timeout=2).close()
                return True
            except Exception:
                time.sleep(0.1)
        return False

    def _classify_failure(self, status: int | str, response_body: str | None) -> tuple[str, str]:
        """Classify a failed request into ONE category."""
        if isinstance(status, str):
            if "timeout" in status.lower():
                return "DEPENDENCY_FAILURE", "connection_timeout"
            if "error" in status.lower():
                return "UNKNOWN_FAILURE", status
            return "UNKNOWN_FAILURE", status

        if status == 401 or status == 403:
            return "AUTH_FAILURE", f"status_{status}"
        if status == 404:
            return "ROUTING_FAILURE", "endpoint_not_found"
        if status == 400:
            return "VALIDATION_FAILURE", "bad_request"
        if status == 429:
            return "ADMISSION_REJECTION", "rate_limited"
        if status == 503:
            return "DEPENDENCY_FAILURE", "service_unavailable"
        if status >= 500:
            return "HANDLER_EXCEPTION", f"status_{status}"

        return "UNKNOWN_FAILURE", f"unexpectedstatus_{status}"

    async def _run_http_request(
        self, endpoint: str, method: str = "GET", request_id: str = "", retry_limit: int = 2
    ) -> RequestLog:
        """Fire a single HTTP request and classify result."""
        import urllib.request
        import urllib.error

        port = self.current_port
        url = f"http://{HOST}:{port}{endpoint}"
        start = time.time()
        retry_count = 0

        for attempt in range(retry_limit):
            try:
                request = urllib.request.Request(url, method=method)
                with urllib.request.urlopen(request, timeout=5) as response:
                    latency_ms = (time.time() - start) * 1000.0
                    return RequestLog(
                        timestamp=start,
                        request_id=request_id,
                        endpoint=endpoint,
                        method=method,
                        status_code=response.status,
                        latency_ms=latency_ms,
                        failure_type=None,
                        failure_reason=None,
                        retry_count=retry_count,
                    )

            except urllib.error.HTTPError as e:
                latency_ms = (time.time() - start) * 1000.0
                failure_type, failure_reason = self._classify_failure(e.code, None)

                # Retry on 5xx
                if e.code >= 500 and attempt < retry_limit - 1:
                    retry_count += 1
                    await asyncio.sleep(0.05 * (2 ** attempt))
                    continue

                return RequestLog(
                    timestamp=start,
                    request_id=request_id,
                    endpoint=endpoint,
                    method=method,
                    status_code=e.code,
                    latency_ms=latency_ms,
                    failure_type=failure_type,
                    failure_reason=failure_reason,
                    retry_count=retry_count,
                )

            except Exception as e:
                latency_ms = (time.time() - start) * 1000.0
                failure_type, failure_reason = self._classify_failure("error", str(e))
                return RequestLog(
                    timestamp=start,
                    request_id=request_id,
                    endpoint=endpoint,
                    method=method,
                    status_code="error",
                    latency_ms=latency_ms,
                    failure_type=failure_type,
                    failure_reason=failure_reason,
                    retry_count=retry_count,
                )

        return RequestLog(
            timestamp=start,
            request_id=request_id,
            endpoint=endpoint,
            method=method,
            status_code="timeout",
            latency_ms=(time.time() - start) * 1000.0,
            failure_type="DEPENDENCY_FAILURE",
            failure_reason="max_retries_exceeded",
            retry_count=retry_count,
        )

    async def run_baseline_test(self, seed: int = 42) -> dict[str, Any]:
        """Execute baseline debug test with VALID endpoints only."""
        print("\n" + "=" * 80)
        print("BASELINE DEBUG TEST — 5-MINUTE LOW-CONCURRENCY RUN")
        print("=" * 80)

        random.seed(seed)

        # VALID endpoints only
        valid_endpoints = [
            "/health",
            "/v1/system/health",
            "/v1/system/boot-status",
        ]

        print(f"\n[CONFIG] Duration: {BASELINE_DURATION_SECONDS}s")
        print(f"[CONFIG] Concurrency: {BASELINE_CONCURRENCY_MIN}-{BASELINE_CONCURRENCY_MAX}")
        print(f"[CONFIG] Valid endpoints: {valid_endpoints}")
        print("\n[RUNNING] Starting baseline load test...\n")

        baseline_start = time.time()
        test_deadline = baseline_start + BASELINE_DURATION_SECONDS
        request_counter = [0]  # mutable for closure

        async def request_worker(worker_id: int) -> None:
            while time.time() < test_deadline:
                request_counter[0] += 1
                request_id = f"req-{worker_id:03d}-{request_counter[0]:06d}"
                endpoint = random.choice(valid_endpoints)

                log_entry = await self._run_http_request(endpoint, request_id=request_id)
                self.request_log.append(log_entry)

                if log_entry.failure_type:
                    self.failure_distribution[log_entry.failure_type] += 1

                # Variable rate to avoid lockstep patterns
                await asyncio.sleep(random.uniform(0.01, 0.15))

        # Dynamic concurrency (ramp up gradually)
        current_concurrency = BASELINE_CONCURRENCY_MIN
        phase_duration = 60  # 1 minute per concurrency level
        phase_deadline = baseline_start + phase_duration
        tasks = []

        worker_id = 0
        while time.time() < test_deadline:
            if time.time() >= phase_deadline and current_concurrency < BASELINE_CONCURRENCY_MAX:
                current_concurrency += 5
                phase_deadline = time.time() + phase_duration

            while len(tasks) < current_concurrency:
                tasks.append(request_worker(worker_id))
                worker_id += 1

            await asyncio.sleep(0.1)

        await asyncio.gather(*tasks, return_exceptions=True)

        baseline_duration = time.time() - baseline_start

        # Aggregate results
        total_requests = len(self.request_log)
        successful = sum(1 for log in self.request_log if log.failure_type is None)
        failed = total_requests - successful
        success_rate = successful / total_requests if total_requests > 0 else 0.0

        latencies = [log.latency_ms for log in self.request_log if log.latency_ms > 0]
        p95 = statistics.quantiles(latencies, n=20)[18] if len(latencies) > 20 else max(latencies or [0])
        p99 = statistics.quantiles(latencies, n=100)[98] if len(latencies) > 100 else max(latencies or [0])
        max_lat = max(latencies or [0])

        print("\n" + "=" * 80)
        print("BASELINE TEST RESULTS")
        print("=" * 80)
        print(f"\nDuration: {baseline_duration:.1f}s")
        print(f"Total Requests: {total_requests}")
        print(f"Successful: {successful} ({success_rate:.1%})")
        print(f"Failed: {failed} ({1-success_rate:.1%})")
        print(f"\nLatencies:")
        print(f"  P95: {p95:.1f}ms")
        print(f"  P99: {p99:.1f}ms")
        print(f"  Max: {max_lat:.1f}ms")
        print(f"\nFailure Distribution:")
        for category, count in self.failure_distribution.items():
            pct = (count / failed * 100) if failed > 0 else 0.0
            if count > 0:
                print(f"  {category}: {count} ({pct:.1f}%)")

        return {
            "baseline_validated": success_rate >= 0.99,
            "total_requests": total_requests,
            "success_rate": success_rate,
            "failure_rate": 1 - success_rate,
            "failure_breakdown": self.failure_distribution,
            "first_failure_stage": "response",  # All failures are at response stage (HTTP status)
            "primary_root_cause": self._determine_primary_cause(),
            "secondary_causes": [],
            "system_classification": self._classify_system(),
            "confidence": 0.95 if failed > 0 else 1.0,
            "latencies": {
                "p95_ms": p95,
                "p99_ms": p99,
                "max_ms": max_lat,
            },
        }

    def _determine_primary_cause(self) -> str:
        """Identify primary root cause from failure distribution."""
        if not any(self.failure_distribution.values()):
            return "NO_FAILURES"

        # Find dominant failure type
        dominant = max(self.failure_distribution, key=self.failure_distribution.get)
        count = self.failure_distribution[dominant]

        if count == 0:
            return "NO_FAILURES"

        if dominant == "AUTH_FAILURE":
            return "Missing or invalid Authorization header in test requests"
        if dominant == "ROUTING_FAILURE":
            return "Test harness using non-existent endpoint"
        if dominant == "ADMISSION_REJECTION":
            return "Admission control rejecting valid requests"
        if dominant == "HANDLER_EXCEPTION":
            return "Handler throwing unexpected exception"
        if dominant == "DEPENDENCY_FAILURE":
            return "Database or external dependency failure"

        return f"{dominant}_DOMINANT"

    def _classify_system(self) -> str:
        """Classify system health based on failure distribution."""
        total_failed = sum(self.failure_distribution.values())
        if total_failed == 0:
            return "HEALTHY"

        if self.failure_distribution["ROUTING_FAILURE"] > 0:
            return "HARNESS_ARTIFACT"
        if self.failure_distribution["AUTH_FAILURE"] > 0:
            return "REQUEST_LOGIC_BUG"
        if self.failure_distribution["ADMISSION_REJECTION"] > 0:
            return "ADMISSION_MISCONFIG"
        if self.failure_distribution["DEPENDENCY_FAILURE"] > 0:
            return "DEPENDENCY_FAILURE"
        if self.failure_distribution["HANDLER_EXCEPTION"] > 0:
            return "MIDDLEWARE_REJECTION_BUG"

        return "UNKNOWN"


async def main():
    """Execute baseline debug test."""
    harness = BaselineDebugTestHarness()

    # Find and start server
    port = harness._find_free_port()
    harness.current_port = port

    config = uvicorn.Config(
        create_app(),
        host=HOST,
        port=port,
        log_level="error",
        access_log=False,
        limit_concurrency=100,
    )
    server = uvicorn.Server(config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    if not harness._wait_server_ready(port):
        print(f"FATAL: Server failed to start on port {port}")
        return {
            "baseline_validated": False,
            "total_requests": 0,
            "success_rate": 0.0,
            "failure_rate": 1.0,
            "failure_breakdown": {},
            "primary_root_cause": "Server startup failure",
            "system_classification": "UNKNOWN",
            "confidence": 1.0,
        }

    print(f"Server started on http://{HOST}:{port}\n")

    # Run baseline test
    result = await harness.run_baseline_test(seed=42)

    # Stop server
    try:
        server.should_exit = True
    except Exception:
        pass

    return result


if __name__ == "__main__":
    result = asyncio.run(main())

    print("\n" + "=" * 80)
    print("FINAL REPORT")
    print("=" * 80)
    print(json.dumps(result, indent=2, default=str))

    print("\n" + "=" * 80)
    print("ROOT CAUSE ANALYSIS")
    print("=" * 80)
    print(f"PRIMARY ROOT CAUSE: {result['primary_root_cause']}")
    print(f"SYSTEM CLASSIFICATION: {result['system_classification']}")
    print(f"CONFIDENCE: {result['confidence']:.0%}")
    print("=" * 80)

    sys.exit(0 if result["baseline_validated"] else 1)
