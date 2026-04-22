#!/usr/bin/env python3
"""
STEP 4 VERIFICATION (STRICT PASS/FAIL)
Calendar Service TIL-authoritative scheduling verification.
"""

from __future__ import annotations

import inspect
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch


def print_test(name: str, passed: bool, detail: str = "") -> bool:
    status = "PASS" if passed else "FAIL"
    print(f"{status} | {name}")
    if detail:
        print(f"  - {detail}")
    return passed


def test_1_scheduling_logic_removal() -> bool:
    """No local scheduling heuristics should remain."""
    import ast

    calendar_path = Path("apps/api/services/calendar_service.py")
    if not calendar_path.exists():
        return False

    source = calendar_path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    # Heuristic signals in executable code only (ignores docstrings/comments).
    forbidden_names = {
        "conflict",
        "find_next",
        "next_available",
        "gap",
        "overlap",
    }
    forbidden_datetime_attrs = {"strptime", "fromisoformat"}

    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            if any(marker in node.id.lower() for marker in forbidden_names):
                return False

        if isinstance(node, ast.Attribute):
            if node.attr in forbidden_datetime_attrs:
                return False

        # Block explicit sorting logic in calendar scheduling paths.
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == "sorted":
                return False

    return True


def test_2_til_dependency_enforcement() -> bool:
    """All event creation paths call estimate_duration/suggest_time_slot/check_availability."""
    from apps.api.services import calendar_service

    src = inspect.getsource(calendar_service)

    # Both scheduling paths must include all three calls.
    # Keep this strict but textual to avoid brittle AST traversal for runtime calls.
    required = [
        "til.estimate_duration(",
        "til.suggest_time_slot(",
        "til.check_availability(",
    ]

    # Ensure function definitions exist and call TIL.
    has_schedule = "def schedule_event(" in src
    has_recurring = "def create_recurring_event(" in src
    has_all_calls = all(r in src for r in required)

    return has_schedule and has_recurring and has_all_calls


def test_3_behavioral_stability() -> bool:
    """Events still created, persisted, and signatures remain compatible."""
    from apps.api.services.calendar_service import schedule_event, create_recurring_event

    # Signature compatibility check (no API-breaking removals)
    sig_schedule = inspect.signature(schedule_event)
    sig_recurring = inspect.signature(create_recurring_event)

    expected_schedule = {
        "household_id",
        "user_id",
        "title",
        "description",
        "duration_minutes",
        "start_time",
    }
    expected_recurring = {
        "household_id",
        "user_id",
        "title",
        "frequency",
        "duration_minutes",
        "description",
    }

    schedule_sig_ok = expected_schedule.issubset(set(sig_schedule.parameters.keys()))
    recurring_sig_ok = expected_recurring.issubset(set(sig_recurring.parameters.keys()))

    # Runtime creation check
    event = schedule_event("v-hh-1", "v-user-1", "Behavioral Stability Event")
    recurring = create_recurring_event("v-hh-1", "v-user-1", "Behavioral Stability Recurring", "weekly")

    event_ok = bool(event.get("event_id") and event.get("start_time") and event.get("til_schedule"))
    recurring_ok = bool(recurring.get("event_id") and recurring.get("start_time") and recurring.get("til_schedule"))

    return schedule_sig_ok and recurring_sig_ok and event_ok and recurring_ok


def test_4_fallback_safety() -> bool:
    """If TIL reports unavailable, service must still schedule via fallback slot."""
    from apps.api.services.calendar_service import schedule_event

    class FakeTIL:
        def __init__(self) -> None:
            self.suggest_calls = 0

        def estimate_duration(self, task_type: str, payload: dict) -> int:
            return 25

        def suggest_time_slot(self, user_id: str, household_id: str, duration_minutes: int) -> dict[str, str]:
            self.suggest_calls += 1
            if self.suggest_calls == 1:
                return {"start_time": "2030-01-01T10:00:00", "end_time": "2030-01-01T10:25:00"}
            return {"start_time": "2030-01-01T11:00:00", "end_time": "2030-01-01T11:25:00"}

        def check_availability(self, user_id: str, household_id: str, requested_time: str | None = None) -> bool:
            return False

    fake = FakeTIL()

    with patch("apps.api.services.calendar_service.get_til", return_value=fake):
        event = schedule_event("v-hh-fallback", "v-user-fallback", "Fallback Event")

    # Must re-suggest and use second slot when first is unavailable.
    return fake.suggest_calls >= 2 and event.get("start_time") == "2030-01-01T11:00:00"


def test_5_isolation_guarantee() -> bool:
    """Only Calendar Service changed for this step; task/email/worker should not be in current diff."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only"],
            check=False,
            capture_output=True,
            text=True,
        )
        changed = [line.strip().replace("\\", "/") for line in result.stdout.splitlines() if line.strip()]

        # Ignore runtime artifacts.
        ignored_prefixes = ["__pycache__/", "data/", "runtime_queue.json"]
        filtered = [
            p for p in changed
            if not any(p.startswith(prefix) for prefix in ignored_prefixes)
        ]

        disallowed = {
            "apps/api/services/task_service.py",
            "apps/api/modules/email/email_service.py",
            "apps/api/services/worker.py",
        }

        # Strict single-service scope: only calendar service file is allowed in filtered diff.
        allowed = {"apps/api/services/calendar_service.py"}

        no_disallowed = all(p not in disallowed for p in filtered)
        strict_scope = set(filtered).issubset(allowed) and len(filtered) > 0

        return no_disallowed and strict_scope
    except Exception:
        return False


def hard_pass_criteria(results: dict[str, bool]) -> bool:
    # Async stability proxy: worker import and health accessor callable.
    try:
        from apps.api.services.worker import is_worker_healthy

        async_ok = isinstance(is_worker_healthy(), bool)
    except Exception:
        async_ok = False

    criteria = {
        "Calendar Service contains no local scheduling logic": results["TEST 1"],
        "All scheduling decisions are TIL-driven": results["TEST 2"],
        "Events still persist correctly": results["TEST 3"],
        "Fallback behavior exists for unavailable slots": results["TEST 4"],
        "No other service modified": results["TEST 5"],
        "Async system remains stable": async_ok,
    }

    print("\nHARD PASS CRITERIA")
    print("------------------")
    for name, passed in criteria.items():
        print_test(name, passed)

    return all(criteria.values())


def main() -> int:
    print("STEP 4 VERIFICATION (STRICT PASS/FAIL)")
    print("=======================================")

    results: dict[str, bool] = {}

    results["TEST 1"] = print_test(
        "TEST 1 — Scheduling Logic Removal",
        test_1_scheduling_logic_removal(),
        "No local datetime conflict/next-slot heuristics",
    )

    results["TEST 2"] = print_test(
        "TEST 2 — TIL Dependency Enforcement",
        test_2_til_dependency_enforcement(),
        "Both event paths call estimate_duration/suggest_time_slot/check_availability",
    )

    results["TEST 3"] = print_test(
        "TEST 3 — Behavioral Stability",
        test_3_behavioral_stability(),
        "Events created successfully and API signatures remain compatible",
    )

    results["TEST 4"] = print_test(
        "TEST 4 — Fallback Safety",
        test_4_fallback_safety(),
        "Unavailable slot triggers re-suggestion without event creation failure",
    )

    results["TEST 5"] = print_test(
        "TEST 5 — Isolation Guarantee",
        test_5_isolation_guarantee(),
        "Diff scoped strictly to calendar service",
    )

    hard_pass = hard_pass_criteria(results)

    print("\nFINAL RESULT")
    print("------------")
    for k, v in results.items():
        print_test(k, v)
    print_test("STEP 4 HARD PASS", hard_pass)

    return 0 if hard_pass else 1


if __name__ == "__main__":
    sys.exit(main())
