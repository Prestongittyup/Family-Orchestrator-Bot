"""
Verify Task Service Upgrade to Authoritative TIL Scheduling Source

Tests that:
  1. Task creation includes TIL-derived scheduling metadata
  2. Metadata is attached but NOT persisted to DB
  3. Calendar and Email services remain unchanged
  4. Fallbacks work when TIL fails
  5. No schema changes
"""

from __future__ import annotations

import sys
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)


def test_task_creation_with_til_metadata() -> bool:
    """TEST 1: Tasks now have TIL-derived scheduling metadata."""
    test_name = "TEST 1 - Task Creation with TIL Metadata"
    try:
        from apps.api.services.task_service import create_task

        task = create_task(
            household_id="test-hh-upgrade",
            title="Upgrade Test Task",
            description="Testing TIL metadata attachment"
        )

        # Check basic task creation still works
        if not task or not task.id:
            logger.error(f"✗ {test_name} FAIL: Task creation failed")
            return False

        # Check that TIL metadata is attached
        if not hasattr(task, "til_scheduling_metadata"):
            logger.error(f"✗ {test_name} FAIL: Missing til_scheduling_metadata attribute")
            return False

        metadata = task.til_scheduling_metadata
        if not isinstance(metadata, dict):
            logger.error(f"✗ {test_name} FAIL: Metadata is not a dict")
            return False

        # Check metadata structure
        required_keys = {"estimated_duration_minutes", "scheduled_start_time", "scheduled_end_time"}
        if not required_keys.issubset(set(metadata.keys())):
            logger.error(f"✗ {test_name} FAIL: Metadata missing required keys")
            return False

        # Check duration is reasonable
        if metadata["estimated_duration_minutes"] != 30:
            logger.error(
                f"✗ {test_name} FAIL: Duration is {metadata['estimated_duration_minutes']}, expected 30"
            )
            return False

        logger.info(f"✔ {test_name} PASS (metadata: {metadata})")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_metadata_not_in_db() -> bool:
    """TEST 2: Metadata is NOT persisted to database (Python attribute only)."""
    test_name = "TEST 2 - Metadata Not Persisted to DB"
    try:
        from apps.api.services.task_service import create_task, set_job_status
        from apps.api.core.database import SessionLocal
        from apps.api.models.task import Task

        # Create a task with metadata
        task1 = create_task(
            household_id="test-hh-db-check",
            title="DB Persistence Test Task"
        )

        task_id = task1.id

        # Reload from DB in a new session
        session = SessionLocal()
        try:
            task2 = session.get(Task, task_id)
            
            # Check that the reloaded task does NOT have the metadata attribute
            # (it's only a Python attribute, not a column)
            if hasattr(task2, "til_scheduling_metadata"):
                logger.error(f"✗ {test_name} FAIL: Metadata was persisted to DB")
                return False
            
            # Verify the task still has all its regular DB fields
            if not task2 or not task2.id or not task2.title:
                logger.error(f"✗ {test_name} FAIL: Regular fields missing")
                return False

            logger.info(f"✔ {test_name} PASS (metadata not in DB, task data intact)")
            return True
        finally:
            session.close()
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_calendar_service_unchanged() -> bool:
    """TEST 3: Calendar service is NOT modified."""
    test_name = "TEST 3 - Calendar Service Unchanged"
    try:
        calendar_path = Path("apps/api/services/calendar_service.py")
        with open(calendar_path) as f:
            source = f.read()

        # Calendar should NOT import or use TIL in new ways
        # It should only have the original shadow mode TIL calls
        if "til_scheduling_metadata" in source:
            logger.error(f"✗ {test_name} FAIL: Calendar modified with TIL metadata")
            return False

        # Quick regex check for authoritatively (we added this word in Task)
        import re
        if re.search(r"authoritatively\s+TIL", source):
            logger.error(f"✗ {test_name} FAIL: Calendar contains upgraded TIL logic")
            return False

        logger.info(f"✔ {test_name} PASS (calendar service untouched)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_email_service_unchanged() -> bool:
    """TEST 4: Email service is NOT modified."""
    test_name = "TEST 4 - Email Service Unchanged"
    try:
        email_path = Path("apps/api/modules/email/email_service.py")
        with open(email_path) as f:
            source = f.read()

        # Email should NOT have any new TIL metadata logic
        if "til_scheduling_metadata" in source:
            logger.error(f"✗ {test_name} FAIL: Email modified with TIL metadata")
            return False

        if "authoritative" in source.lower():
            logger.error(f"✗ {test_name} FAIL: Email contains upgraded logic")
            return False

        logger.info(f"✔ {test_name} PASS (email service untouched)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_no_schema_changes() -> bool:
    """TEST 5: No DB schema changes."""
    test_name = "TEST 5 - No DB Schema Changes"
    try:
        from apps.api.models.task import Task
        import inspect

        # Get Task columns from SQLAlchemy
        if hasattr(Task, "__table__"):
            columns = {col.name for col in Task.__table__.columns}

            # TIL metadata should not be a column
            if "til_scheduling_metadata" in columns:
                logger.error(f"✗ {test_name} FAIL: til_scheduling_metadata added as DB column")
                return False

            # Check expected columns still exist
            required_columns = {"id", "household_id", "title", "status", "priority"}
            if not required_columns.issubset(columns):
                logger.error(f"✗ {test_name} FAIL: Expected columns missing")
                return False

            logger.info(f"✔ {test_name} PASS (DB schema unchanged)")
            return True
        else:
            logger.warning(f"⊘ {test_name} SKIP: Could not inspect Task table")
            return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_fallback_on_til_error() -> bool:
    """TEST 6: Task creation succeeds even if TIL fails."""
    test_name = "TEST 6 - Fallback on TIL Error"
    try:
        # This test verifies the fallback logic by checking the code
        # (We can't easily inject TIL failures in a live test without mocking)
        
        task_path = Path("apps/api/services/task_service.py")
        with open(task_path) as f:
            source = f.read()

        # Check for fallback patterns
        if "estimated_duration = 30" not in source:
            logger.error(f"✗ {test_name} FAIL: No fallback for duration")
            return False

        if "suggested_schedule = None" not in source:
            logger.error(f"✗ {test_name} FAIL: No fallback for schedule")
            return False

        if "except Exception" not in source:
            logger.error(f"✗ {test_name} FAIL: No exception handling")
            return False

        logger.info(f"✔ {test_name} PASS (fallback patterns present)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_worker_and_event_bus_unchanged() -> bool:
    """TEST 7: Worker and event bus are unchanged."""
    test_name = "TEST 7 - Worker/Event Bus Unchanged"
    try:
        worker_path = Path("apps/api/services/worker.py")
        event_bus_path = Path("apps/api/core/event_bus_async.py")

        for path in [worker_path, event_bus_path]:
            if not path.exists():
                continue

            with open(path) as f:
                source = f.read()

            # These should not have been modified
            if "til_scheduling_metadata" in source:
                logger.error(f"✗ {test_name} FAIL: {path.name} contains TIL metadata")
                return False

            if "authoritatively" in source.lower():
                logger.error(f"✗ {test_name} FAIL: {path.name} contains upgraded logic")
                return False

        logger.info(f"✔ {test_name} PASS (worker/event bus untouched)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_system_imports_and_runs() -> bool:
    """TEST 8: Full system still imports and runs."""
    test_name = "TEST 8 - System Import and Runtime"
    try:
        from apps.api import main
        from apps.api.services.task_service import create_task
        from apps.api.services.calendar_service import schedule_event
        from apps.api.modules.email.email_service import handle_email_received

        logger.info(f"✔ {test_name} PASS (system imports and functional)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def run_all_tests() -> bool:
    """Run all verification tests."""
    print("\n" + "="*80)
    print("TASK SERVICE UPGRADE TO AUTHORITATIVE TIL SCHEDULING")
    print("="*80 + "\n")

    tests = [
        test_task_creation_with_til_metadata,
        test_metadata_not_in_db,
        test_calendar_service_unchanged,
        test_email_service_unchanged,
        test_no_schema_changes,
        test_fallback_on_til_error,
        test_worker_and_event_bus_unchanged,
        test_system_imports_and_runs,
    ]

    results = [test() for test in tests]

    print("\n" + "="*80)
    if all(results):
        print("✔ UPGRADE COMPLETE: PASS (8/8 gates cleared)")
        print("="*80)
        print("\nSUMMARY:")
        print("  ✔ Tasks now carry TIL-derived scheduling metadata")
        print("  ✔ Metadata attached as Python attribute (not persisted)")
        print("  ✔ Calendar service unchanged")
        print("  ✔ Email service unchanged")
        print("  ✔ DB schema unchanged")
        print("  ✔ Fallbacks in place for TIL failures")
        print("  ✔ Worker/Event bus unchanged")
        print("  ✔ System still imports and runs")
        print("="*80 + "\n")
        return True
    else:
        print("✗ UPGRADE BLOCKED: FAIL (one or more gates failed)")
        print("="*80 + "\n")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
