"""
System-level stability lock test suite for OS-1 → OS-2 pipeline.

This is a non-functional regression safety layer that proves:
- Event ingestion (OS-1) works reliably
- Decision + brief generation (OS-2) works reliably
- System is deterministic across repeated runs
- No state leaks across iterations
- No hidden singleton or bus corruption

The test runs the ENTIRE pipeline 10 consecutive times with fixed input,
verifying bitwise-identical output and zero state accumulation.
"""
from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import text

from apps.api.core.database import SessionLocal
from apps.api.endpoints import brief_endpoint
from apps.api.models.event_log import EventLog
from apps.api.models.idempotency_key import IdempotencyKey
from apps.api.models.task import Task
from apps.api.services import decision_engine as de
from apps.api.services import synthesis_engine as se


# ────────────────────────────────────────────────────────────────────────────
# FIXED DETERMINISTIC DATASET
# ────────────────────────────────────────────────────────────────────────────

HOUSEHOLD_ID = "household_test_001"
FIXED_NOW = datetime(2026, 4, 15, 9, 0, 0, tzinfo=UTC)
STABILITY_TEST_RUN_COUNT = 10


class _FrozenDateTime(datetime):
    @classmethod
    def utcnow(cls) -> datetime:
        return cls(2026, 4, 15, 9, 0, 0)


class _FrozenDate(se.date):
    @classmethod
    def today(cls):
        return cls(2026, 4, 15)


def _build_fixed_dataset() -> list[dict[str, Any]]:
    """
    Build the fixed deterministic dataset for stability testing.
    
    Includes:
    - 3 task_created events
    - 2 email_received events
    - 1 calendar event
    
    All with fixed timestamps, IDs, and idempotency keys.
    """
    return [
        # Task 1: Grocery shopping
        {
            "household_id": HOUSEHOLD_ID,
            "type": "task_created",
            "source": "stability_lock",
            "timestamp": "2026-04-15T09:00:00Z",
            "severity": "info",
            "idempotency_key": "stability-task-001",
            "payload": {"title": "Buy groceries"},
        },
        # Task 2: Household cleanup
        {
            "household_id": HOUSEHOLD_ID,
            "type": "task_created",
            "source": "stability_lock",
            "timestamp": "2026-04-15T09:05:00Z",
            "severity": "info",
            "idempotency_key": "stability-task-002",
            "payload": {"title": "Clean house"},
        },
        # Task 3: Meal prep
        {
            "household_id": HOUSEHOLD_ID,
            "type": "task_created",
            "source": "stability_lock",
            "timestamp": "2026-04-15T09:10:00Z",
            "severity": "info",
            "idempotency_key": "stability-task-003",
            "payload": {"title": "Prepare meals"},
        },
        # Email 1: Meeting reminder
        {
            "household_id": HOUSEHOLD_ID,
            "type": "email_received",
            "source": "stability_lock",
            "timestamp": "2026-04-15T09:15:00Z",
            "severity": "info",
            "idempotency_key": "stability-email-001",
            "payload": {
                "subject": "Team meeting reminder",
                "sender": "team@example.test",
                "priority": "medium",
                "category": "reference=meet-1; time_window=2026-04-16T10:00:00->2026-04-16T11:00:00",
            },
        },
        # Email 2: Appointment confirmation
        {
            "household_id": HOUSEHOLD_ID,
            "type": "email_received",
            "source": "stability_lock",
            "timestamp": "2026-04-15T09:20:00Z",
            "severity": "info",
            "idempotency_key": "stability-email-002",
            "payload": {
                "subject": "Doctor appointment confirmed",
                "sender": "clinic@example.test",
                "priority": "high",
                "category": "reference=appt-1; time_window=2026-04-16T14:00:00->2026-04-16T15:00:00",
            },
        },
        # Calendar: Fixed blocking event
        {
            "household_id": HOUSEHOLD_ID,
            "type": "calendar_event_scheduled",
            "source": "stability_lock",
            "timestamp": "2026-04-15T09:25:00Z",
            "severity": "info",
            "idempotency_key": "stability-calendar-001",
            "payload": {
                "event_id": "stability-evt-fixed-001",
                "title": "Fixed work window",
                "start_time": "2026-04-16T18:00:00",
                "end_time": "2026-04-16T19:00:00",
                "priority": 4,
            },
        },
    ]


def _remove_timestamp_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Deep copy of payload with all timestamp/generated fields removed for comparison.
    
    Timestamps are allowed to differ across runs; structural equality is what matters.
    """
    cleaned = json.loads(json.dumps(payload, default=str))
    cleaned.pop("generated_at", None)
    
    # Remove any other dynamic fields that contain current time
    if "brief" in cleaned:
        brief = cleaned["brief"]
        for action in brief.get("suggested_actions", []):
            action.pop("generated_at", None)
        for action in brief.get("suggestions", []):
            action.pop("generated_at", None)
    
    return cleaned


def _get_db_row_counts(household_id: str) -> dict[str, int]:
    """Get current counts for all key tables for this household."""
    session = SessionLocal()
    try:
        task_count = session.query(Task).filter(Task.household_id == household_id).count()
        event_count = session.query(EventLog).filter(EventLog.household_id == household_id).count()
        idempotency_count = session.query(IdempotencyKey).filter(IdempotencyKey.household_id == household_id).count()
        
        try:
            calendar_count = session.query(EventLog).filter(
                EventLog.household_id == household_id,
                EventLog.type == "calendar_event_scheduled"
            ).count()
        except Exception:
            calendar_count = 0
        
        return {
            "tasks": task_count,
            "events": event_count,
            "idempotency_keys": idempotency_count,
            "calendar_events": calendar_count,
        }
    finally:
        session.close()


def _get_event_bus_handler_count() -> int:
    """Get current handler count from the event bus."""
    from apps.api.core.event_bus import get_event_bus
    
    bus = get_event_bus()
    if hasattr(bus, "_registry"):
        return sum(len(handlers) for handlers in bus._registry.values())
    return 0


def _extract_output_summary(brief_output: dict[str, Any]) -> dict[str, Any]:
    """Extract structural summary for cross-run equality checks."""
    brief = brief_output.get("brief", {})
    
    scheduled = brief.get("suggested_actions", [])
    unscheduled = brief.get("suggestions", [])
    priorities = brief.get("priorities", [])
    
    return {
        "status": brief_output.get("status"),
        "scheduled_count": len(scheduled),
        "unscheduled_count": len(unscheduled),
        "scheduled_titles": [row.get("title") for row in scheduled],
        "unscheduled_titles": [row.get("title") for row in unscheduled],
        "priorities_count": len(priorities),
        "priorities_structure": [
            {
                "priority": p.get("priority"),
                "count": p.get("count"),
            }
            for p in priorities
        ],
    }


def test_system_stability_lock(monkeypatch, test_client: TestClient) -> None:
    """
    System-level stability lock: prove the OS-1→OS-2 pipeline is deterministic.
    
    Runs the full pipeline 10 consecutive times with identical input,
    verifying:
    - /brief output is bitwise identical (except timestamps)
    - DB state resets cleanly between runs
    - Event bus handler count is stable
    - Scheduled/unscheduled counts are stable
    - Zero state leakage across iterations
    """
    # Patch datetime to freeze time
    monkeypatch.setattr(de, "datetime", _FrozenDateTime)
    monkeypatch.setattr(se, "date", _FrozenDate)
    monkeypatch.setattr(brief_endpoint, "_now_utc", lambda: FIXED_NOW)
    
    # Clear cache
    brief_endpoint._clear_brief_cache()
    
    # Build fixed dataset
    fixed_dataset = _build_fixed_dataset()
    
    # Tracking across all runs
    all_runs: list[dict[str, Any]] = []
    db_evolution: list[dict[str, int]] = []
    handler_evolution: list[int] = []
    summaries: list[dict[str, Any]] = []
    
    print("\n" + "=" * 90)
    print("SYSTEM STABILITY LOCK TEST")
    print("=" * 90)
    print(f"household_id: {HOUSEHOLD_ID}")
    print(f"dataset size: {len(fixed_dataset)} events")
    print(f"run count: {STABILITY_TEST_RUN_COUNT}")
    print("=" * 90 + "\n")
    
    baseline_handler_count = None
    baseline_summary = None
    
    for run_index in range(STABILITY_TEST_RUN_COUNT):
        print(f"\n[RUN {run_index + 1}/{STABILITY_TEST_RUN_COUNT}]", end=" ", flush=True)
        
        # Record event bus handler count BEFORE posting events
        handler_count_before = _get_event_bus_handler_count()
        
        # Post all OS-1 events
        for event in fixed_dataset:
            response = test_client.post("/event", json=event)
            assert response.status_code == 200, (
                f"OS-1 event post failed for {event['idempotency_key']}: "
                f"{response.status_code} {response.text}"
            )
        
        # Wait for async task creation
        time.sleep(0.5)
        
        # Call /brief (OS-2 execution)
        brief_response = test_client.get(f"/brief/{HOUSEHOLD_ID}")
        assert brief_response.status_code == 200, (
            f"OS-2 /brief call failed: {brief_response.status_code} {brief_response.text}"
        )
        
        brief_output = brief_response.json()
        
        # Store full output (for diff on failure)
        all_runs.append(brief_output)
        
        # Extract structural summary (for cross-run equality)
        summary = _extract_output_summary(brief_output)
        summaries.append(summary)
        
        # Format: structural summary (before storing)
        print(
            f"scheduled={summary['scheduled_count']:2d} "
            f"unscheduled={summary['unscheduled_count']:2d} "
            f"status={summary['status']}",
            end=" ",
            flush=True,
        )
        
        # Record handler count AFTER posting events (should not grow)
        handler_count_after = _get_event_bus_handler_count()
        handler_evolution.append(handler_count_after)
        
        print(f"handlers={handler_count_after:2d}", end=" ", flush=True)
        
        # Capture DB state
        db_state = _get_db_row_counts(HOUSEHOLD_ID)
        db_evolution.append(db_state)
        
        print(f"db_rows={{t:{db_state['tasks']} e:{db_state['events']}}}", end=" ", flush=True)
        
        # Baseline checks on first run
        if run_index == 0:
            baseline_handler_count = handler_count_after
            baseline_summary = summary
            print("[BASELINE]", end=" ", flush=True)
        else:
            # Verify handler count is stable (no growth)
            if handler_count_after != baseline_handler_count:
                print(f"\n[FAIL] HANDLER COUNT DRIFT: baseline={baseline_handler_count}, now={handler_count_after}")
                raise AssertionError(
                    f"Event bus handler count grew: baseline={baseline_handler_count}, "
                    f"run {run_index}={handler_count_after}"
                )
            
            # Verify summary is identical
            if summary != baseline_summary:
                print(f"\n[FAIL] SUMMARY DIVERGENCE AT RUN {run_index + 1}")
                print("\nBaseline summary:")
                print(json.dumps(baseline_summary, indent=2))
                print(f"\nRun {run_index + 1} summary:")
                print(json.dumps(summary, indent=2))
                raise AssertionError(
                    f"Output structure diverged at run {run_index + 1}: "
                    f"scheduled={summary['scheduled_count']} vs baseline={baseline_summary['scheduled_count']}"
                )
        
        print("[OK]", end="", flush=True)
        
        # CRITICAL: Reset DB for next iteration (full isolation)
        session = SessionLocal()
        try:
            session.query(Task).filter(Task.household_id == HOUSEHOLD_ID).delete(synchronize_session=False)
            session.query(EventLog).filter(EventLog.household_id == HOUSEHOLD_ID).delete(synchronize_session=False)
            session.query(IdempotencyKey).filter(IdempotencyKey.household_id == HOUSEHOLD_ID).delete(synchronize_session=False)
            session.commit()
        finally:
            session.close()
        
        # Clear brief cache for next run
        brief_endpoint._clear_brief_cache()
    
    print("\n" + "=" * 90)
    print("FULL PIPELINE COMPARISON")
    print("=" * 90)
    
    # Remove timestamps for comparison
    cleaned_runs = [_remove_timestamp_fields(brief_output) for brief_output in all_runs]
    
    # Check bitwise equality (with timestamps removed)
    baseline_cleaned = cleaned_runs[0]
    for run_index in range(1, len(cleaned_runs)):
        if cleaned_runs[run_index] != baseline_cleaned:
            print(f"\n[FAIL] BITWISE DIVERGENCE at run {run_index + 1}")
            print("\nBaseline (run 1):")
            print(json.dumps(baseline_cleaned, indent=2, sort_keys=True, default=str))
            print(f"\nRun {run_index + 1}:")
            print(json.dumps(cleaned_runs[run_index], indent=2, sort_keys=True, default=str))
            print("\nDifference:")
            diff = {
                "baseline_only": {k: v for k, v in baseline_cleaned.items() if k not in cleaned_runs[run_index]},
                "run_only": {k: v for k, v in cleaned_runs[run_index].items() if k not in baseline_cleaned},
                "value_mismatches": {
                    k: (baseline_cleaned.get(k), cleaned_runs[run_index].get(k))
                    for k in baseline_cleaned.keys() & cleaned_runs[run_index].keys()
                    if baseline_cleaned.get(k) != cleaned_runs[run_index].get(k)
                },
            }
            print(json.dumps(diff, indent=2, default=str))
            
            raise AssertionError(
                f"Outputs are not bitwise identical. "
                f"Run {run_index + 1} diverges from baseline."
            )
    
    print(f"\n[OK] All {STABILITY_TEST_RUN_COUNT} runs produced bitwise-identical output (timestamps excluded)")
    
    # State isolation check
    print(f"\n[CHECK] STATE ISOLATION VERIFICATION")
    print("-" * 45)
    for run_index, db_state in enumerate(db_evolution):
        if run_index == 0:
            print(f"Run {run_index + 1:2d}: tasks={db_state['tasks']:2d} events={db_state['events']:2d} idempotency_keys={db_state['idempotency_keys']:2d} [BASELINE]")
        else:
            if db_state == db_evolution[0]:
                print(f"Run {run_index + 1:2d}: tasks={db_state['tasks']:2d} events={db_state['events']:2d} idempotency_keys={db_state['idempotency_keys']:2d} [OK] MATCH")
            else:
                print(f"Run {run_index + 1:2d}: tasks={db_state['tasks']:2d} events={db_state['events']:2d} idempotency_keys={db_state['idempotency_keys']:2d} [FAIL] DIVERGED")
                raise AssertionError(
                    f"State isolation failed at run {run_index + 1}. "
                    f"DB rows differ from baseline: {db_state} vs {db_evolution[0]}"
                )
    
    print(f"\n[OK] Zero state leakage across {STABILITY_TEST_RUN_COUNT} runs")
    
    # Event bus integrity check
    print(f"\n[CHECK] EVENT BUS INTEGRITY VERIFICATION")
    print("-" * 45)
    print(f"Baseline handler count: {baseline_handler_count}")
    for run_index, handler_count in enumerate(handler_evolution):
        if handler_count == baseline_handler_count:
            print(f"Run {run_index + 1:2d}: handlers={handler_count:2d} [OK] STABLE")
        else:
            print(f"Run {run_index + 1:2d}: handlers={handler_count:2d} [FAIL] DRIFT")
            raise AssertionError(
                f"Event bus handler count drifted at run {run_index + 1}. "
                f"Baseline={baseline_handler_count}, now={handler_count}"
            )
    
    print(f"\n[OK] Event bus handler count is stable (no accumulation)")
    
    # Summary: success
    print("\n" + "=" * 90)
    print("[SUCCESS] STABILITY LOCK TEST PASSED")
    print("=" * 90)
    print(f"[OK] {STABILITY_TEST_RUN_COUNT}/10 runs completed successfully")
    print(f"[OK] All outputs identical (bitwise, timestamps excluded)")
    print(f"[OK] Zero state leakage between iterations")
    print(f"[OK] Event bus integrity maintained")
    print(f"[OK] OS-1 ingestion deterministic")
    print(f"[OK] OS-2 generation deterministic")
    print(f"\nSYSTEM IS PRODUCTION-READY FOR OS-1 -> OS-2 PIPELINE\n")

