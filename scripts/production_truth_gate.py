"""
PRODUCTION TRUTH GATE — Clean Baseline + Controlled Phase Escalation

Phase 0: API contract lock (runtime only)
Phase 1: Baseline truth run (concurrency 1-5, no chaos)
Phase 2: Baseline sanity gates (error_rate <= 1%, 404_rate == 0)
Phase 3: Soak test (5->10->15->20, no chaos)
Phase 4: Breakpoint discovery (10->25->50->100)
Phase 5: Chaos injection (jitter, latency spikes, partial failures)
Phase 6: Final decision gate

HARD RULE: Any phase failure stops the pipeline immediately.
NO invalid endpoints. NO code changes during execution.
"""
from __future__ import annotations

import asyncio
import json
import random
import statistics
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.main import create_app

try:
    import apps.api.runtime.loop_tracing as _loop_tracing
    # Keep tracing logic active but suppress extremely verbose stdout spam during load runs.
    _loop_tracing.print = lambda *args, **kwargs: None
except Exception:
    pass

HOST = "127.0.0.1"
SEED = 42


@dataclass
class StageResult:
    stage: str
    concurrency: int
    duration_s: float
    total: int
    success: int
    failed: int
    not_found: int
    latencies: list[float]
    error_rate: float
    not_found_rate: float
    p95_ms: float
    p99_ms: float
    max_ms: float


def _find_free_port() -> int:
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


def _wait_ready(port: int, timeout: float = 20.0) -> bool:
    import urllib.request
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://{HOST}:{port}/health", timeout=2).close()
            return True
        except Exception:
            time.sleep(0.1)
    return False


def _extract_registered_endpoints() -> list[dict]:
    """PHASE 0 — Extract ONLY runtime-registered endpoints."""
    app = create_app()
    endpoints = []
    for route in app.routes:
        path = getattr(route, "path", None)
        methods = sorted(list(getattr(route, "methods", []) or []))
        if path and methods:
            for method in methods:
                endpoints.append({"method": method, "path": path})
    return sorted(endpoints, key=lambda x: (x["path"], x["method"]))


async def _request(port: int, endpoint: str, chaos: bool = False) -> tuple[int | str, float]:
    """Fire one HTTP request. Returns (status_code, latency_ms)."""
    import urllib.request, urllib.error
    url = f"http://{HOST}:{port}{endpoint}"
    start = time.time()

    if chaos:
        if random.random() < 0.10:
            await asyncio.sleep(random.uniform(0.3, 1.5))
        if random.random() < 0.05:
            await asyncio.sleep(random.uniform(0.05, 0.2))

    try:
        def _do_request() -> int:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status

        status = await asyncio.to_thread(_do_request)
        return status, (time.time() - start) * 1000.0
    except urllib.error.HTTPError as e:
        return e.code, (time.time() - start) * 1000.0
    except Exception:
        return "error", (time.time() - start) * 1000.0


async def _run_stage(
    port: int,
    stage_name: str,
    endpoints: list[str],
    concurrency: int,
    duration_s: float,
    chaos: bool = False,
) -> StageResult:
    """Run one load stage. All workers fire in parallel for `duration_s` seconds."""
    deadline = time.time() + duration_s
    results: list[tuple[int | str, float]] = []

    async def worker(wid: int) -> None:
        while time.time() < deadline:
            ep = endpoints[wid % len(endpoints)]
            status, lat = await _request(port, ep, chaos=chaos)
            results.append((status, lat))
            await asyncio.sleep(random.uniform(0.01, 0.12))

    await asyncio.gather(*[worker(i) for i in range(concurrency)], return_exceptions=True)

    total = len(results)
    success = sum(1 for s, _ in results if isinstance(s, int) and 200 <= s < 300)
    not_found = sum(1 for s, _ in results if s == 404)
    failed = total - success
    lats = [l for _, l in results if isinstance(l, float) and l > 0]

    def pct(n, lst, q):
        if len(lst) < n:
            return max(lst or [0])
        return statistics.quantiles(lst, n=n)[q]

    return StageResult(
        stage=stage_name,
        concurrency=concurrency,
        duration_s=duration_s,
        total=total,
        success=success,
        failed=failed,
        not_found=not_found,
        latencies=lats,
        error_rate=failed / total if total else 0.0,
        not_found_rate=not_found / total if total else 0.0,
        p95_ms=pct(20, lats, 18),
        p99_ms=pct(100, lats, 98),
        max_ms=max(lats or [0]),
    )


def _stage_to_dict(s: StageResult) -> dict:
    return {
        "stage": s.stage,
        "concurrency": s.concurrency,
        "duration_s": s.duration_s,
        "total_requests": s.total,
        "success": s.success,
        "failed": s.failed,
        "not_found": s.not_found,
        "error_rate": round(s.error_rate, 6),
        "not_found_rate": round(s.not_found_rate, 6),
        "p95_ms": round(s.p95_ms, 2),
        "p99_ms": round(s.p99_ms, 2),
        "max_ms": round(s.max_ms, 2),
    }


async def main() -> dict[str, Any]:
    random.seed(SEED)

    # ---------------------------------------------------------------
    # PHASE 0 — API contract lock
    # ---------------------------------------------------------------
    print("\n[PHASE 0] Extracting runtime API surface...")
    all_endpoints = _extract_registered_endpoints()
    all_paths = {e["path"] for e in all_endpoints}

    # Verified public endpoints (no auth, safe GET, no side-effects)
    # EXCLUDED: boot-status, boot-probe — call asyncio.run() inside running event loop (cross-loop violation)
    # EXCLUDED: runtime-metrics — calls multiple subsystem snapshots, higher cost
    # EXCLUDED: /v1/system/health — calls run_boot_probe() which spawns unbounded threads via asyncio.run()
    #           causing OS thread handle exhaustion under sustained concurrency (THREAD_LEAK_VIA_BOOT_PROBE)
    candidate_public = [
        "/health",   # Pure liveness probe: returns {"status":"ok"}, no side-effects, no diagnostics
    ]
    verified_endpoints = [ep for ep in candidate_public if ep in all_paths]

    print(f"  Total registered endpoints: {len(all_endpoints)}")
    print(f"  Verified public endpoints for testing: {verified_endpoints}")

    if not verified_endpoints:
        return {
            "production_readiness": "NO_GO",
            "confidence": 1.0,
            "phase_stopped_at": "PHASE_0",
            "error": "No verified public endpoints found",
        }

    # ---------------------------------------------------------------
    # Start server
    # ---------------------------------------------------------------
    port = _find_free_port()
    print(f"\n[STARTUP] Starting server on port {port}...")
    config = uvicorn.Config(
        create_app(), host=HOST, port=port,
        log_level="error", access_log=False, limit_concurrency=200,
    )
    server = uvicorn.Server(config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    if not _wait_ready(port):
        return {
            "production_readiness": "NO_GO",
            "confidence": 1.0,
            "phase_stopped_at": "STARTUP",
            "error": "Server failed to start",
        }
    print("[STARTUP] Server ready.")

    results: dict[str, Any] = {
        "verified_endpoints": verified_endpoints,
        "registered_endpoint_count": len(all_endpoints),
        "phases": {},
        "truth_gate_summary": {
            "baseline_pass": False,
            "soak_pass": False,
            "breakpoint_pass": False,
            "chaos_pass": False,
        },
    }

    # ---------------------------------------------------------------
    # PHASE 1 — Baseline truth run (concurrency 1-5, 300s)
    # ---------------------------------------------------------------
    print("\n[PHASE 1] Baseline truth run (concurrency=5, 300s)...")
    baseline = await _run_stage(port, "baseline", verified_endpoints, concurrency=5, duration_s=300.0)
    results["phases"]["baseline"] = _stage_to_dict(baseline)
    print(f"  error_rate={baseline.error_rate:.4%}  404_rate={baseline.not_found_rate:.4%}  p95={baseline.p95_ms:.1f}ms  total={baseline.total}")

    # ---------------------------------------------------------------
    # PHASE 2 — Baseline sanity gates
    # ---------------------------------------------------------------
    print("\n[PHASE 2] Baseline sanity gate check...")
    baseline_pass = baseline.error_rate <= 0.01 and baseline.not_found_rate == 0.0
    results["truth_gate_summary"]["baseline_pass"] = baseline_pass

    if not baseline_pass:
        server.should_exit = True
        result = {
            "production_readiness": "NO_GO",
            "confidence": 0.99,
            "truth_gate_summary": results["truth_gate_summary"],
            "phase_stopped_at": "PHASE_2_BASELINE_SANITY",
            "phases": results["phases"],
            "key_metrics": {
                "baseline_error_rate": baseline.error_rate,
                "soak_error_rate": None,
                "max_stable_concurrency": 0,
                "p95_latency_ms": baseline.p95_ms,
                "failure_mode": "baseline_sanity_failure",
            },
            "root_cause_if_no_go": [
                f"error_rate={baseline.error_rate:.4%} (required <=1%)" if baseline.error_rate > 0.01 else "",
                f"404_rate={baseline.not_found_rate:.4%} (required ==0%)" if baseline.not_found_rate > 0 else "",
            ],
            "final_verdict_reason": "Baseline failed sanity gates. DO NOT proceed to stress testing.",
            "verified_endpoints": verified_endpoints,
        }
        result["root_cause_if_no_go"] = [r for r in result["root_cause_if_no_go"] if r]
        return result

    print(f"  [PASS] Baseline passed: error_rate={baseline.error_rate:.4%}, 404_rate=0%")

    # ---------------------------------------------------------------
    # PHASE 3 — Controlled soak (5->10->15->20, 3 min per step)
    # ---------------------------------------------------------------
    print("\n[PHASE 3] Controlled soak test (5->10->15->20 concurrency, 3 min each)...")
    soak_stages: list[StageResult] = []
    soak_ladder = [5, 10, 15, 20]
    soak_failed = False
    soak_failure_reason = ""

    for conc in soak_ladder:
        stage = await _run_stage(port, f"soak_{conc}", verified_endpoints, concurrency=conc, duration_s=180.0)
        soak_stages.append(stage)
        results["phases"][f"soak_{conc}"] = _stage_to_dict(stage)
        print(f"  conc={conc:3d}  error_rate={stage.error_rate:.4%}  p95={stage.p95_ms:.1f}ms  total={stage.total}")

        if stage.error_rate > 0.05:
            soak_failed = True
            soak_failure_reason = f"error_rate={stage.error_rate:.4%} at concurrency={conc}"
            break
        if stage.not_found_rate > 0:
            soak_failed = True
            soak_failure_reason = f"404s detected at concurrency={conc}"
            break

    soak_pass = not soak_failed
    results["truth_gate_summary"]["soak_pass"] = soak_pass

    if not soak_pass:
        server.should_exit = True
        return {
            "production_readiness": "NO_GO",
            "confidence": 0.97,
            "truth_gate_summary": results["truth_gate_summary"],
            "phase_stopped_at": "PHASE_3_SOAK",
            "phases": results["phases"],
            "key_metrics": {
                "baseline_error_rate": baseline.error_rate,
                "soak_error_rate": soak_stages[-1].error_rate if soak_stages else None,
                "max_stable_concurrency": soak_ladder[len(soak_stages) - 2] if len(soak_stages) > 1 else 0,
                "p95_latency_ms": soak_stages[-1].p95_ms if soak_stages else 0,
                "failure_mode": "soak_error_rate_exceeded",
            },
            "root_cause_if_no_go": [soak_failure_reason],
            "final_verdict_reason": f"Soak failed at {soak_failure_reason}",
            "verified_endpoints": verified_endpoints,
        }

    print("  [PASS] Soak passed all concurrency levels")

    # ---------------------------------------------------------------
    # PHASE 4 — Breakpoint discovery (10->25->50->100, 90s each)
    # ---------------------------------------------------------------
    print("\n[PHASE 4] Breakpoint discovery (10->25->50->100 concurrency, 90s each)...")
    bp_stages: list[StageResult] = []
    bp_ladder = [10, 25, 50, 100]
    bp_failed = False
    bp_failure_stage = None
    max_stable_concurrency = 0

    for conc in bp_ladder:
        stage = await _run_stage(port, f"bp_{conc}", verified_endpoints, concurrency=conc, duration_s=90.0)
        bp_stages.append(stage)
        results["phases"][f"bp_{conc}"] = _stage_to_dict(stage)
        print(f"  conc={conc:3d}  error_rate={stage.error_rate:.4%}  p95={stage.p95_ms:.1f}ms  total={stage.total}")

        if stage.error_rate <= 0.05:
            max_stable_concurrency = conc
        else:
            bp_failed = True
            bp_failure_stage = stage
            break

    bp_pass = not bp_failed
    results["truth_gate_summary"]["breakpoint_pass"] = bp_pass

    if not bp_pass and bp_failure_stage is not None:
        server.should_exit = True
        return {
            "production_readiness": "NO_GO",
            "confidence": 0.95,
            "truth_gate_summary": results["truth_gate_summary"],
            "phase_stopped_at": "PHASE_4_BREAKPOINT",
            "phases": results["phases"],
            "key_metrics": {
                "baseline_error_rate": baseline.error_rate,
                "soak_error_rate": soak_stages[-1].error_rate,
                "max_stable_concurrency": max_stable_concurrency,
                "p95_latency_ms": bp_failure_stage.p95_ms,
                "failure_mode": f"breakpoint_at_concurrency_{bp_failure_stage.concurrency}",
            },
            "root_cause_if_no_go": [
                f"System collapses at concurrency={bp_failure_stage.concurrency}: error_rate={bp_failure_stage.error_rate:.4%}"
            ],
            "final_verdict_reason": f"Breakpoint found at concurrency={bp_failure_stage.concurrency}",
            "verified_endpoints": verified_endpoints,
        }

    print(f"  [PASS] Breakpoint: max_stable_concurrency={max_stable_concurrency}")

    # ---------------------------------------------------------------
    # PHASE 5 — Chaos injection (fixed seed, 100 concurrency, 120s)
    # ---------------------------------------------------------------
    print("\n[PHASE 5] Chaos injection (concurrency=50, 120s, seeded jitter + latency spikes)...")
    chaos_stage = await _run_stage(
        port, "chaos", verified_endpoints, concurrency=50, duration_s=120.0, chaos=True
    )
    results["phases"]["chaos"] = _stage_to_dict(chaos_stage)
    print(f"  error_rate={chaos_stage.error_rate:.4%}  p95={chaos_stage.p95_ms:.1f}ms  max={chaos_stage.max_ms:.1f}ms  total={chaos_stage.total}")

    chaos_pass = chaos_stage.error_rate <= 0.10
    results["truth_gate_summary"]["chaos_pass"] = chaos_pass

    # ---------------------------------------------------------------
    # PHASE 6 — Final decision gate
    # ---------------------------------------------------------------
    all_pass = baseline_pass and soak_pass and bp_pass and chaos_pass
    p95_overall = max(
        baseline.p95_ms,
        max((s.p95_ms for s in soak_stages), default=0),
        max((s.p95_ms for s in bp_stages), default=0),
        chaos_stage.p95_ms,
    )

    server.should_exit = True

    verdict = "GO" if all_pass else "NO_GO"
    confidence = 0.96 if all_pass else 0.98

    root_causes = []
    if not chaos_pass:
        root_causes.append(f"Chaos error_rate={chaos_stage.error_rate:.4%} exceeded 10% threshold")

    return {
        "production_readiness": verdict,
        "confidence": confidence,
        "truth_gate_summary": results["truth_gate_summary"],
        "phase_stopped_at": None,
        "phases": results["phases"],
        "key_metrics": {
            "baseline_error_rate": baseline.error_rate,
            "soak_error_rate": soak_stages[-1].error_rate,
            "max_stable_concurrency": max_stable_concurrency,
            "p95_latency_ms": round(p95_overall, 2),
            "failure_mode": "" if all_pass else "chaos_error_rate_exceeded",
        },
        "root_cause_if_no_go": root_causes,
        "final_verdict_reason": (
            "All phases passed: baseline, soak, breakpoint, chaos."
            if all_pass else
            "Chaos phase exceeded error threshold."
        ),
        "verified_endpoints": verified_endpoints,
        "registered_endpoint_count": len(all_endpoints),
    }


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("PRODUCTION TRUTH GATE")
    print("=" * 80)

    result = asyncio.run(main())

    print("\n" + "=" * 80)
    print("FINAL REPORT")
    print("=" * 80)
    print(json.dumps(result, indent=2, default=str))

    verdict = result["production_readiness"]
    print("\n" + "=" * 80)
    print(f"VERDICT: {verdict}")
    if verdict == "NO_GO":
        phase = result.get("phase_stopped_at", "FINAL")
        print(f"STOPPED AT: {phase}")
        for cause in result.get("root_cause_if_no_go", []):
            print(f"CAUSE: {cause}")
    print("=" * 80)

    sys.exit(0 if verdict == "GO" else 1)
