#!/usr/bin/env python
"""
Verification Plan — STRICT PASS/FAIL
Tests A–E: Worker hardening, queue persistence, poison messages,
           backpressure, and stability under mixed load.

Run from workspace root:
    $env:PYTHONPATH='.'; python scripts\verify_worker_hardening.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
from pathlib import Path

# Ensure workspace root is on sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Use sync bus as default; individual tests that need async bus create instances directly.
os.environ.setdefault("EVENT_BUS_TYPE", "sync")

# ── Result tracking ──────────────────────────────────────────────────────────
_results: dict[str, str] = {}
_failures: list[str] = []


def _mark(test: str, passed: bool, detail: str = "") -> None:
    status = "PASS" if passed else "FAIL"
    _results[test] = status
    if not passed:
        _failures.append(f"TEST {test}: {detail}")
    print(f"  {status} | {detail}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _poll_until(predicate, timeout: float = 15.0, interval: float = 0.3) -> bool:
    """Return True if predicate() becomes truthy within timeout, else False."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _fresh_worker():
    """Reset DB worker module globals to a clean state."""
    import apps.api.services.worker as w
    w.WORKER_RUNNING = False
    w.WORKER_HEALTH = True
    w._consecutive_loop_crashes = 0
    w._worker_thread = None


def _flush_stale_queued_tasks() -> int:
    """Set all currently-queued DB tasks to 'completed' to prevent stale rows
    from interfering with worker-based tests.  Returns the count flushed."""
    from apps.api.core.database import SessionLocal
    from apps.api.models.task import Task
    session = SessionLocal()
    try:
        rows = session.query(Task).filter(Task.status == "queued").all()
        for t in rows:
            t.status = "completed"
        session.commit()
        return len(rows)
    finally:
        session.close()


def _task_status(task_id: str) -> str:
    from apps.api.core.database import SessionLocal
    from apps.api.models.task import Task
    session = SessionLocal()
    try:
        t = session.get(Task, task_id)
        return t.status if t else "missing"
    finally:
        session.close()


def _task_retry_count(task_id: str) -> int:
    from apps.api.core.database import SessionLocal
    from apps.api.models.task import Task
    session = SessionLocal()
    try:
        t = session.get(Task, task_id)
        return t.retry_count if t else 0
    finally:
        session.close()


def _count_tasks_in_terminal(household_id: str, force_fail: bool, statuses: tuple) -> int:
    """Count tasks with a given household_id + force_fail flag that are in terminal statuses."""
    from apps.api.core.database import SessionLocal
    from apps.api.models.task import Task
    session = SessionLocal()
    try:
        return (
            session.query(Task)
            .filter(
                Task.household_id == household_id,
                Task.force_fail == force_fail,
                Task.status.in_(list(statuses)),
            )
            .count()
        )
    finally:
        session.close()


# ── TEST A: Crash Recovery ────────────────────────────────────────────────────

def test_a_crash_recovery() -> None:
    print("\nTEST A — Crash Recovery")

    import apps.api.services.queue_store as qs_mod
    import apps.api.core.event_bus_async as bus_async_mod
    from apps.api.core.event_bus_async import AsyncEventBus
    from apps.api.schemas.event import SystemEvent

    # Isolated checkpoint file: monkey-patch the functions imported into
    # event_bus_async's namespace so ALL internal save/load calls use the
    # temp file, not the module-level QUEUE_FILE default.
    tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
    tmp.close()
    temp_path = Path(tmp.name)
    temp_path.unlink(missing_ok=True)

    def _patched_save(queue, **_kw):
        qs_mod.save_queue(queue, file_path=temp_path)

    def _patched_load(**_kw):
        return qs_mod.load_queue(file_path=temp_path)

    original_save = bus_async_mod.save_queue
    original_load = bus_async_mod.load_queue
    bus_async_mod.save_queue = _patched_save
    bus_async_mod.load_queue = _patched_load
    try:
        # Session-1 bus: slow handler so we can stop mid-drain.
        s1_processed: list[str] = []
        gate = threading.Event()  # fires after first event completes

        def slow_handler(event: SystemEvent) -> None:
            s1_processed.append(event.idempotency_key)
            gate.set()
            time.sleep(0.3)  # stay in handler briefly while we signal stop

        bus1 = AsyncEventBus()
        bus1.register("task_created", slow_handler)

        n = 5
        keys = [f"test-a-{i}" for i in range(n)]
        for i, key in enumerate(keys):
            bus1.publish(SystemEvent(
                household_id="test-a",
                type="task_created",
                source="test",
                payload={"title": f"CrashTask-{i}"},
                idempotency_key=key,
            ))

        bus1.start_worker()
        fired = gate.wait(timeout=5.0)
        assert fired, "Session-1 worker never processed first event"

        # "Kill" the worker — stop_worker saves remaining deque to checkpoint
        # (via the patched save_queue → temp_path).
        bus1.stop_worker()
        time.sleep(0.5)  # allow background write-thread to finish

        s1_count = len(s1_processed)
        remaining = n - s1_count

        # Load checkpoint via temp_path directly.
        checkpoint = qs_mod.load_queue(file_path=temp_path)
        checkpoint_keys = [item["idempotency_key"] for item in checkpoint]

        # Session-2 bus: restores from checkpoint automatically on start_worker.
        s2_processed: list[str] = []
        s2_done = threading.Event()
        s2_lock = threading.Lock()

        def handler2(event: SystemEvent) -> None:
            with s2_lock:
                s2_processed.append(event.idempotency_key)
                if len(s2_processed) >= remaining:
                    s2_done.set()

        bus2 = AsyncEventBus()
        bus2.register("task_created", handler2)
        bus2.start_worker()  # <- restores checkpoint here

        drained = s2_done.wait(timeout=8.0)
        bus2.stop_worker()
        time.sleep(0.3)

        all_processed = set(s1_processed) | set(s2_processed)
        no_loss = all_processed == set(keys)
        no_dup = (len(s1_processed) + len(s2_processed)) == n
        checkpoint_correct = len(checkpoint_keys) == remaining
        # FIFO: checkpoint keys must be the tail of the original sequence.
        expected_tail = keys[s1_count:]
        fifo_ok = checkpoint_keys == expected_tail

        passed = no_loss and no_dup and checkpoint_correct and fifo_ok and drained
        _mark("A", passed,
              f"s1={s1_count}, s2={len(s2_processed)}, total={len(all_processed)}/{n}, "
              f"checkpoint_len={len(checkpoint_keys)}, no_loss={no_loss}, "
              f"no_dup={no_dup}, fifo_ok={fifo_ok}, drained={drained}")
    finally:
        bus_async_mod.save_queue = original_save
        bus_async_mod.load_queue = original_load
        temp_path.unlink(missing_ok=True)


# ── TEST B: Poison Message Isolation ─────────────────────────────────────────

def test_b_poison_isolation() -> None:
    print("\nTEST B — Poison Message Isolation")

    from apps.api.services.task_service import create_task
    import apps.api.services.worker as worker_mod

    _fresh_worker()
    flushed = _flush_stale_queued_tasks()
    if flushed:
        print(f"    (flushed {flushed} stale queued task(s) before test)")

    hid = f"test-b-{int(time.time())}"

    # force_fail=True, max_retries=3: fails on every attempt.
    # After 3 retries retry_count==max_retries → should_retry=False → DLQ.
    bad = create_task(hid, "PoisonJob", max_retries=3, force_fail=True)
    # Created AFTER the bad task; worker processes FIFO by created_at.
    good = create_task(hid, "ValidJob", max_retries=1, force_fail=False)

    worker_mod.start_worker_loop()

    reached_terminal = _poll_until(
        lambda: (
            _task_status(bad.id) in ("dead_letter", "poisoned")
            and _task_status(good.id) == "completed"
        ),
        timeout=20.0,
    )
    worker_mod.stop_worker_loop()

    bad_status = _task_status(bad.id)
    good_status = _task_status(good.id)
    bad_retries = _task_retry_count(bad.id)
    worker_healthy = worker_mod.WORKER_HEALTH

    # Use DB status as the source of truth for DLQ evidence — more reliable
    # than checking the in-process in-memory list across thread boundaries.
    dlq_in_db = bad_status in ("dead_letter", "poisoned")

    passed = (
        dlq_in_db
        and good_status == "completed"
        and bad_retries >= 3
        and worker_healthy
    )
    _mark("B", passed,
          f"bad_status={bad_status}, good_status={good_status}, "
          f"bad_retries={bad_retries}/3, dlq_in_db={dlq_in_db}, "
          f"worker_healthy={worker_healthy}, terminal_reached={reached_terminal}")


# ── TEST C: Queue Persistence Integrity ──────────────────────────────────────

def test_c_queue_persistence() -> None:
    print("\nTEST C — Queue Persistence Integrity")

    import apps.api.services.queue_store as qs_mod
    import apps.api.core.event_bus_async as bus_async_mod
    from apps.api.core.event_bus_async import AsyncEventBus
    from apps.api.schemas.event import SystemEvent

    tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
    tmp.close()
    temp_path = Path(tmp.name)
    temp_path.unlink(missing_ok=True)

    def _patched_save(queue, **_kw):
        qs_mod.save_queue(queue, file_path=temp_path)

    def _patched_load(**_kw):
        return qs_mod.load_queue(file_path=temp_path)

    original_save = bus_async_mod.save_queue
    original_load = bus_async_mod.load_queue
    bus_async_mod.save_queue = _patched_save
    bus_async_mod.load_queue = _patched_load
    try:
        bus = AsyncEventBus()
        bus.register("task_created", lambda e: None)

        n = 10
        keys_in = [f"test-c-{i}" for i in range(n)]
        for i, key in enumerate(keys_in):
            bus.publish(SystemEvent(
                household_id="test-c",
                type="task_created",
                source="test",
                payload={"title": f"T{i}"},
                idempotency_key=key,
            ))

        # Stop without starting — saves checkpoint even with no active thread,
        # exercising the "clean shutdown checkpoints everything" path.
        bus.stop_worker()
        time.sleep(0.5)

        checkpoint = qs_mod.load_queue(file_path=temp_path)
        keys_out = [item.get("idempotency_key") for item in checkpoint]

        fifo_ok = keys_out == keys_in
        no_missing = len(keys_out) == n
        no_corruption = all(
            isinstance(item, dict)
            and "type" in item
            and "household_id" in item
            for item in checkpoint
        )

        # Restore test: new bus reads checkpoint and puts items back in FIFO order.
        bus2 = AsyncEventBus()
        restored_keys: list[str] = []
        done = threading.Event()
        lock = threading.Lock()

        def counting_handler(event: SystemEvent) -> None:
            with lock:
                restored_keys.append(event.idempotency_key)
                if len(restored_keys) >= n:
                    done.set()

        bus2.register("task_created", counting_handler)
        bus2.start_worker()
        drained = done.wait(timeout=6.0)
        bus2.stop_worker()

        restore_order_ok = restored_keys == keys_in

        passed = fifo_ok and no_missing and no_corruption and drained and restore_order_ok
        _mark("C", passed,
              f"persisted={len(keys_out)}/{n}, fifo_ok={fifo_ok}, "
              f"no_missing={no_missing}, no_corruption={no_corruption}, "
              f"restore_order_ok={restore_order_ok}, drained={drained}")
    finally:
        bus_async_mod.save_queue = original_save
        bus_async_mod.load_queue = original_load
        temp_path.unlink(missing_ok=True)


# ── TEST D: Backpressure Safety ───────────────────────────────────────────────

def test_d_backpressure() -> None:
    print("\nTEST D — Backpressure Safety")

    from apps.api.core.event_bus_async import AsyncEventBus, MAX_QUEUE_SIZE
    from apps.api.schemas.event import SystemEvent

    # Worker intentionally NOT started so the queue fills up.
    bus = AsyncEventBus()
    bus.register("task_created", lambda e: None)

    total = 120
    accepted = 0
    rejected = 0
    exceptions = 0

    for i in range(total):
        try:
            result = bus.publish(SystemEvent(
                household_id="test-d",
                type="task_created",
                source="test",
                payload={"title": f"D-{i}"},
                idempotency_key=f"test-d-{i}",
            ))
            if isinstance(result, dict) and result.get("status") == "queue_full":
                rejected += 1
            else:
                accepted += 1
        except Exception:
            exceptions += 1

    cap_enforced = accepted == MAX_QUEUE_SIZE
    no_crash = exceptions == 0
    no_silent_drop = (accepted + rejected) == total  # every call accounted for
    queue_intact = len(bus._queue) == MAX_QUEUE_SIZE

    passed = cap_enforced and no_crash and no_silent_drop and queue_intact
    _mark("D", passed,
          f"accepted={accepted}/{MAX_QUEUE_SIZE}, rejected={rejected}, "
          f"exceptions={exceptions}, queue_size={len(bus._queue)}, "
          f"cap_enforced={cap_enforced}, no_silent_drop={no_silent_drop}")


# ── TEST E: Stability Under Mixed Load ───────────────────────────────────────

def test_e_mixed_load() -> None:
    print("\nTEST E — Stability Under Mixed Load")

    from apps.api.core.event_bus import InMemoryEventBus
    from apps.api.core.bootstrap import register_event_handlers
    from apps.api.services.router_service import route_event
    from apps.api.schemas.event import SystemEvent
    import apps.api.core.event_bus as bus_mod
    import apps.api.services.worker as worker_mod

    _fresh_worker()
    flushed = _flush_stale_queued_tasks()
    if flushed:
        print(f"    (flushed {flushed} stale queued task(s) before test)")

    # Inject a fresh, exactly-once-registered sync bus.
    fresh_bus = InMemoryEventBus()
    register_event_handlers(fresh_bus)
    original_instance = bus_mod._event_bus_instance
    bus_mod._event_bus_instance = fresh_bus

    hid = f"test-e-{int(time.time())}"
    unhandled_exceptions = 0
    dup_ignored = 0
    ts = int(time.time() * 1000)

    try:
        worker_mod.start_worker_loop()

        # ── (1) 5 valid task_created events ──────────────────────────────────
        task_keys: list[str] = []
        for i in range(5):
            key = f"e-task-{ts}-{i}"
            task_keys.append(key)
            try:
                route_event(SystemEvent(
                    household_id=hid, type="task_created", source="test",
                    payload={"title": f"ValidTask-{i}"}, idempotency_key=key,
                ))
            except Exception:
                unhandled_exceptions += 1

        # ── (2) 5 valid email_received events ────────────────────────────────
        for i in range(5):
            try:
                route_event(SystemEvent(
                    household_id=hid, type="email_received", source="test",
                    payload={"subject": f"Inbox {i}", "sender": "bot@home.local"},
                    idempotency_key=f"e-email-{ts}-{i}",
                ))
            except Exception:
                unhandled_exceptions += 1

        # ── (3) 5 force_fail email events (max_retries=1 → immediate DLQ) ───
        for i in range(5):
            try:
                route_event(SystemEvent(
                    household_id=hid, type="email_received", source="test",
                    payload={"subject": f"FailMail-{i}", "force_fail": True, "max_retries": 1},
                    idempotency_key=f"e-fail-{ts}-{i}",
                ))
            except Exception:
                unhandled_exceptions += 1

        # ── (4) 5 duplicate key re-submissions ───────────────────────────────
        for key in task_keys:
            try:
                result = route_event(SystemEvent(
                    household_id=hid, type="task_created", source="test",
                    payload={"title": "Duplicate"},
                    idempotency_key=key,
                ))
                if isinstance(result, dict) and result.get("status") == "duplicate_ignored":
                    dup_ignored += 1
            except Exception:
                unhandled_exceptions += 1

        # ── Wait for DB worker to drain force_fail tasks (DB-based check) ────
        TERMINAL = ("dead_letter", "poisoned")
        reached = _poll_until(
            lambda: _count_tasks_in_terminal(hid, force_fail=True, statuses=TERMINAL) >= 5,
            timeout=20.0,
        )

        worker_mod.stop_worker_loop()

        dlq_db_count = _count_tasks_in_terminal(hid, force_fail=True, statuses=TERMINAL)
        worker_healthy = worker_mod.WORKER_HEALTH

        passed = (
            unhandled_exceptions == 0
            and worker_healthy
            and dup_ignored == 5
            and dlq_db_count >= 5
            and reached
        )
        _mark("E", passed,
              f"unhandled_exc={unhandled_exceptions}, worker_healthy={worker_healthy}, "
              f"dups_ignored={dup_ignored}/5, dlq_db_count={dlq_db_count}, "
              f"dlq_reached={reached}")

    finally:
        bus_mod._event_bus_instance = original_instance


# ── Runner ────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 65)
    print("VERIFICATION PLAN — STRICT PASS/FAIL")
    print("Tests: A=CrashRecovery B=Poison C=Persistence D=Backpressure E=Mixed")
    print("=" * 65)

    tests = [
        ("A", test_a_crash_recovery),
        ("B", test_b_poison_isolation),
        ("C", test_c_queue_persistence),
        ("D", test_d_backpressure),
        ("E", test_e_mixed_load),
    ]

    for name, fn in tests:
        try:
            fn()
        except Exception as exc:
            import traceback
            _results[name] = "ERROR"
            _failures.append(f"TEST {name}: unhandled exception — {exc}")
            print(f"  ERROR | {exc}")
            traceback.print_exc()

    print("\n" + "=" * 65)
    overall = "PASS" if not _failures else "FAIL"
    print(f"OVERALL: {overall}")
    for t, r in _results.items():
        print(f"  TEST {t}: {r}")

    if _failures:
        print("\nFailure details:")
        for f in _failures:
            print(f"  ✗ {f}")

    print("=" * 65)
    sys.exit(0 if not _failures else 1)


if __name__ == "__main__":
    main()
