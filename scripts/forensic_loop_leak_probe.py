from __future__ import annotations

import asyncio
import json
import random
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from fastapi import FastAPI
import uvicorn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.core.backpressure_middleware import _get_audit_bootstrap_semaphore
from apps.api.core.backpressure_middleware import install_request_backpressure_middleware
from apps.api.runtime.execution_fairness import fairness_gate
from apps.api.runtime.loop_tracing import (
    clear_context_events,
    clear_violation_events,
    get_violation_events,
    record_violation,
)


HOST = "127.0.0.1"
ATTEMPTS = 8
TOTAL_REQUESTS = 300


def _find_free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


async def _seed_loop_local_resources() -> dict[str, Any]:
    fairness_pool = await fairness_gate._acquire_raw("SHORT")
    fairness_gate._release_raw(fairness_pool)
    semaphore = _get_audit_bootstrap_semaphore()
    async with semaphore:
        await asyncio.sleep(0.001)
    return {"seed_loop_id": id(asyncio.get_running_loop())}


def _seed_once() -> dict[str, Any]:
    return asyncio.run(_seed_loop_local_resources())


def _build_probe_app() -> FastAPI:
    app = FastAPI()
    install_request_backpressure_middleware(app)

    @app.get("/v1/system/loop-probe")
    async def loop_probe() -> dict[str, Any]:
        """Async endpoint — NO asyncio.run(), runs on request loop directly."""
        return await _seed_loop_local_resources()

    return app


def _request(
    url: str,
    headers: dict[str, str] | None = None,
    *,
    method: str = "GET",
    body: bytes | None = None,
) -> dict[str, Any]:
    request = Request(url, headers=headers or {}, data=body, method=method)
    started = time.time()
    try:
        with urlopen(request, timeout=5) as response:
            body = response.read().decode("utf-8", errors="replace")
            return {
                "status": response.status,
                "elapsed_ms": round((time.time() - started) * 1000.0, 3),
                "body": body[:200],
            }
    except HTTPError as exc:
        return {
            "status": exc.code,
            "elapsed_ms": round((time.time() - started) * 1000.0, 3),
            "body": exc.read().decode("utf-8", errors="replace")[:200],
        }
    except URLError as exc:
        return {
            "status": "urlerror",
            "elapsed_ms": round((time.time() - started) * 1000.0, 3),
            "body": str(exc),
        }


def _fire_load(port: int) -> list[dict[str, Any]]:
    probe_url = f"http://{HOST}:{port}/v1/system/loop-probe"

    requests: list[tuple[str, dict[str, str] | None, str, bytes | None]] = [
        (probe_url, None, "GET", None) for _ in range(TOTAL_REQUESTS)
    ]

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=120) as executor:
        futures = []
        for url, headers, method, body in requests:
            futures.append(executor.submit(_request, url, headers, method=method, body=body))
            time.sleep(random.uniform(0.0, 0.01))
        for future in as_completed(futures):
            results.append(future.result())
    return results


def _wait_ready(port: int, timeout_s: float = 15.0) -> None:
    deadline = time.time() + timeout_s
    url = f"http://{HOST}:{port}/v1/system/loop-probe"
    while time.time() < deadline:
        result = _request(url)
        if result.get("status") in {200, 500}:
            return
        time.sleep(0.1)
    raise RuntimeError("server_not_ready")


def run_probe() -> dict[str, Any]:
    clear_violation_events()
    forensic: dict[str, Any] = {
        "leak_detected": False,
        "resource_type": None,
        "creation_site": None,
        "usage_site": None,
        "root_cause_category": None,
        "call_path": [],
        "fix_applied": None,
        "verification_passed": False,
        "attempts": [],
    }

    for attempt in range(1, ATTEMPTS + 1):
        clear_context_events()
        port = _find_free_port()
        seed_info = _seed_once()
        config = uvicorn.Config(
            _build_probe_app(),
            host=HOST,
            port=port,
            log_level="warning",
            access_log=False,
            limit_concurrency=50,
            timeout_keep_alive=0,
        )
        server = uvicorn.Server(config)
        server_thread = threading.Thread(target=server.run, daemon=True)
        server_thread.start()

        attempt_record: dict[str, Any] = {
            "attempt": attempt,
            "port": port,
            "seed_loop_id": seed_info.get("seed_loop_id"),
        }

        try:
            _wait_ready(port)
            results = _fire_load(port)
            attempt_record["response_status_counts"] = {
                str(status): sum(1 for row in results if row.get("status") == status)
                for status in sorted({row.get("status") for row in results}, key=lambda value: str(value))
            }
            violations = get_violation_events()
            if violations:
                violation = violations[-1]
                forensic.update(
                    {
                        "leak_detected": True,
                        "resource_type": violation.get("resource_type"),
                        "creation_site": violation.get("creation_label"),
                        "usage_site": violation.get("label"),
                        "root_cause_category": "GLOBAL STATE LEAK",
                        "call_path": [
                            "scripts/forensic_loop_leak_probe.py:loop_probe",
                            "asyncio.run(_seed_loop_local_resources)",
                            "apps/api/runtime/execution_fairness.py:get_loop_local_resource",
                            "apps/api/core/backpressure_middleware.py:_get_audit_bootstrap_semaphore and apps/api/runtime/execution_fairness.py:_state",
                            "apps/api/core/backpressure_middleware.py:request_backpressure_guard",
                        ],
                        "verification_passed": False,
                    }
                )
                attempt_record["violation"] = violation
                forensic["attempts"].append(attempt_record)
                break
            forensic["attempts"].append(attempt_record)
        except Exception as exc:
            record_violation(
                {
                    "label": "scripts/forensic_loop_leak_probe.py:run_probe",
                    "resource_type": type(exc).__name__,
                    "message": str(exc),
                    "current_stack": traceback.format_exc(),
                }
            )
            attempt_record["exception"] = traceback.format_exc()
            forensic["attempts"].append(attempt_record)
            break
        finally:
            server.should_exit = True
            server_thread.join(timeout=15)

    return forensic


if __name__ == "__main__":
    print(json.dumps(run_probe(), indent=2))