from __future__ import annotations

import os
import sqlite3
import time
import uuid

from fastapi.testclient import TestClient


def db() -> sqlite3.Connection:
    return sqlite3.connect("data/family_orchestration.db")


def count_tasks_by_title(title: str) -> int:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM tasks WHERE title=?", (title,))
    count = cur.fetchone()[0]
    conn.close()
    return int(count)


def fetch_task_status_by_title(title: str) -> tuple[str, int, int, str | None] | None:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT status, retry_count, max_retries, last_error FROM tasks WHERE title=? ORDER BY created_at DESC LIMIT 1",
        (title,),
    )
    row = cur.fetchone()
    conn.close()
    if row is None:
        return None
    return row[0], int(row[1]), int(row[2]), row[3]


def fetch_created_title_sequence(prefix: str) -> list[str]:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT title FROM tasks WHERE title LIKE ? ORDER BY created_at ASC",
        (f"{prefix}%",),
    )
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows


def wait_for_count(title: str, expected: int, timeout_s: float = 5.0) -> bool:
    start = time.time()
    while time.time() - start < timeout_s:
        if count_tasks_by_title(title) >= expected:
            return True
        time.sleep(0.05)
    return False


def wait_queue_empty(bus: object, timeout_s: float = 5.0) -> bool:
    start = time.time()
    while time.time() - start < timeout_s:
        lock = getattr(bus, "_lock")
        queue = getattr(bus, "_queue")
        with lock:
            if len(queue) == 0:
                return True
        time.sleep(0.05)
    return False


def queue_len(bus: object) -> int:
    lock = getattr(bus, "_lock")
    queue = getattr(bus, "_queue")
    with lock:
        return int(len(queue))


def main() -> None:
    os.environ["EVENT_BUS_TYPE"] = "async"

    import apps.api.core.event_bus as event_bus_module

    event_bus_module._event_bus_instance = None

    from apps.api.core.event_bus import get_event_bus
    from apps.api.core.event_bus_async import AsyncEventBus
    from apps.api.main import app

    results: dict[str, tuple[bool, str]] = {}

    with TestClient(app) as client:
        bus = get_event_bus()
        assert isinstance(bus, AsyncEventBus)

        # TEST A - Async Queue Isolation
        title_a = f"Async Boundary Test {uuid.uuid4().hex[:8]}"
        key_a = f"test-a-{uuid.uuid4().hex[:8]}"
        before_a = count_tasks_by_title(title_a)

        t0 = time.perf_counter()
        resp_a = client.post(
            "/event",
            json={
                "household_id": "home-1",
                "type": "task_created",
                "source": "test",
                "idempotency_key": key_a,
                "payload": {"title": title_a},
            },
        )
        elapsed_a = time.perf_counter() - t0

        immediate_a = count_tasks_by_title(title_a)
        eventual_ok_a = wait_for_count(title_a, before_a + 1, timeout_s=5.0)
        after_a = count_tasks_by_title(title_a)

        pass_a = (
            resp_a.status_code == 200
            and elapsed_a < 0.25
            and immediate_a == before_a
            and eventual_ok_a
            and after_a == before_a + 1
        )
        results["TEST A"] = (
            pass_a,
            f"status={resp_a.status_code}, elapsed={elapsed_a:.4f}s, before={before_a}, immediate={immediate_a}, after={after_a}",
        )

        # TEST B - Queue Drain Validation
        prefix_b = f"Queue Drain Validation {uuid.uuid4().hex[:8]}"
        n = 12
        titles_b = [f"{prefix_b}-{i:02d}" for i in range(n)]

        max_q = 0
        for i, title in enumerate(titles_b):
            client.post(
                "/event",
                json={
                    "household_id": "home-1",
                    "type": "task_created",
                    "source": "test",
                    "idempotency_key": f"test-b-{uuid.uuid4().hex}",
                    "payload": {"title": title},
                },
            )
            ql = queue_len(bus)
            if ql > max_q:
                max_q = ql

        queue_drained = wait_queue_empty(bus, timeout_s=6.0)

        counts_ok = all(count_tasks_by_title(t) == 1 for t in titles_b)
        seq = fetch_created_title_sequence(prefix_b)
        expected_seq = titles_b
        order_ok = seq == expected_seq

        pass_b = max_q > 0 and queue_drained and counts_ok and order_ok
        results["TEST B"] = (
            pass_b,
            f"max_queue={max_q}, drained={queue_drained}, counts_ok={counts_ok}, order_ok={order_ok}, created={len(seq)}/{n}",
        )

        # TEST C - Failure Isolation
        def boom_handler(event):
            raise RuntimeError("boom")

        bus.register("boom", boom_handler)

        client.post(
            "/event",
            json={
                "household_id": "home-1",
                "type": "boom",
                "source": "test",
                "idempotency_key": f"test-c-boom-{uuid.uuid4().hex[:8]}",
                "payload": {},
            },
        )

        title_c_followup = f"Failure Isolation Followup {uuid.uuid4().hex[:8]}"
        followup_resp = client.post(
            "/event",
            json={
                "household_id": "home-1",
                "type": "task_created",
                "source": "test",
                "idempotency_key": f"test-c-followup-{uuid.uuid4().hex[:8]}",
                "payload": {"title": title_c_followup},
            },
        )
        followup_ok = wait_for_count(title_c_followup, 1, timeout_s=5.0)

        title_c_dlq = f"Failure Isolation DLQ {uuid.uuid4().hex[:8]}"
        client.post(
            "/event",
            json={
                "household_id": "home-1",
                "type": "email_received",
                "source": "test",
                "idempotency_key": f"test-c-dlq-{uuid.uuid4().hex[:8]}",
                "payload": {
                    "subject": title_c_dlq,
                    "category": "testing",
                    "priority": "medium",
                    "force_fail": True,
                    "max_retries": 0,
                },
            },
        )

        dlq_ready = False
        dlq_state = None
        start = time.time()
        while time.time() - start < 8.0:
            st = fetch_task_status_by_title(title_c_dlq)
            if st and st[0] == "dead_letter":
                dlq_ready = True
                dlq_state = st
                break
            time.sleep(0.1)

        pass_c = followup_resp.status_code == 200 and followup_ok and dlq_ready
        results["TEST C"] = (
            pass_c,
            f"followup_status={followup_resp.status_code}, followup_ok={followup_ok}, dlq_ready={dlq_ready}, dlq_state={dlq_state}",
        )

        # TEST D - Idempotency Regression Check
        key_d = f"test-d-{uuid.uuid4().hex[:8]}"
        title_d = f"Idempotency Regression {uuid.uuid4().hex[:8]}"

        first_d = client.post(
            "/event",
            json={
                "household_id": "home-1",
                "type": "task_created",
                "source": "test",
                "idempotency_key": key_d,
                "payload": {"title": title_d},
            },
        )
        wait_queue_empty(bus, timeout_s=4.0)

        before_dup_q = queue_len(bus)
        second_d = client.post(
            "/event",
            json={
                "household_id": "home-1",
                "type": "task_created",
                "source": "test",
                "idempotency_key": key_d,
                "payload": {"title": title_d},
            },
        )
        after_dup_q = queue_len(bus)

        duplicate_ignored = second_d.status_code == 200 and second_d.json().get("status") == "duplicate_ignored"
        no_dup_enqueue = after_dup_q == before_dup_q
        no_dup_task = count_tasks_by_title(title_d) == 1

        pass_d = first_d.status_code == 200 and duplicate_ignored and no_dup_enqueue and no_dup_task
        results["TEST D"] = (
            pass_d,
            f"first_status={first_d.status_code}, second={second_d.json()}, q_before={before_dup_q}, q_after={after_dup_q}, task_count={count_tasks_by_title(title_d)}",
        )

    overall = all(v[0] for v in results.values())
    print("OVERALL:", "PASS" if overall else "FAIL")
    for name in ("TEST A", "TEST B", "TEST C", "TEST D"):
        ok, detail = results[name]
        print(f"{name}: {'PASS' if ok else 'FAIL'} | {detail}")


if __name__ == "__main__":
    main()
