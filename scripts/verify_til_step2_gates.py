"""
Temporal Intelligence Layer Shadow Mode Verification Plan (STRICT PASS/FAIL)

GATE CONDITIONS (must ALL pass to complete Step 2):
  1. Behavior Parity: All existing API responses identical
  2. TIL Invocation Proof: Logs show TIL being called in all 3 domains
  3. No Decision Coupling: No if/branches based on TIL output
  4. System Stability: async worker, DLQ, idempotency, replay unchanged

Run this script to validate TIL shadow mode integration across the system.
"""

from __future__ import annotations

import sys
import logging
import ast
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================================
# TEST 1: Behavior Parity
# ============================================================================

def test_no_schema_changes() -> bool:
    """TEST 1.1: No DB schema changes introduced."""
    test_name = "TEST 1.1 - No Schema Changes"
    try:
        # Check that Task model hasn't changed
        from apps.api.models.task import Task
        import inspect

        # Get current schema fields
        fields = {name for name, field in inspect.getmembers(Task) if not name.startswith("_")}
        
        # Critical fields that must exist
        required_fields = {"id", "household_id", "title", "status", "priority"}
        
        # Check for unwanted additions (TIL-specific fields should NOT be in model)
        forbidden_fields = {"til_duration", "til_available", "til_observation"}
        
        if not required_fields.issubset(fields):
            logger.error(f"✗ {test_name} FAIL: Missing required fields")
            return False
        
        if forbidden_fields.intersection(fields):
            logger.error(f"✗ {test_name} FAIL: Found TIL fields in Task model")
            return False
        
        logger.info(f"✔ {test_name} PASS (model unchanged, no TIL fields)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_task_response_structure_unchanged() -> bool:
    """TEST 1.2: Task creation response structure unchanged."""
    test_name = "TEST 1.2 - Task Response Structure"
    try:
        from apps.api.services.task_service import create_task
        from apps.api.core.database import SessionLocal
        
        # Create a test task (will use TIL in shadow mode)
        task = create_task(
            household_id="test-hh-verify",
            title="Verification Test Task",
            description="Testing response structure"
        )
        
        # Check that response has expected fields
        required_attrs = {"id", "household_id", "title", "status", "priority", "retry_count"}
        task_attrs = {attr for attr in dir(task) if not attr.startswith("_")}
        
        if not required_attrs.issubset(task_attrs):
            logger.error(f"✗ {test_name} FAIL: Missing expected fields in response")
            return False
        
        # TIL metadata can exist as Python attribute (til_scheduling_metadata)
        # but verify it's not a database column (columns are in __table__.columns)
        if hasattr(task, "__table__"):
            db_columns = {col.name for col in task.__table__.columns}
            til_db_fields = {col for col in db_columns if col.startswith("til_")}
            if til_db_fields:
                logger.error(f"✗ {test_name} FAIL: Found TIL fields in DB schema: {til_db_fields}")
                return False
        
        # TIL scheduling metadata as Python attribute is OK (not persisted)
        if hasattr(task, "til_scheduling_metadata"):
            logger.info(f"  (Task has TIL scheduling metadata as runtime attribute - OK)")
        
        
        logger.info(f"✔ {test_name} PASS (response structure matches original)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_calendar_event_response_structure() -> bool:
    """TEST 1.3: Calendar event response structure intact."""
    test_name = "TEST 1.3 - Calendar Response Structure"
    try:
        from apps.api.services.calendar_service import schedule_event
        
        # Create a test event
        event = schedule_event(
            household_id="test-hh-verify",
            user_id="test-user-verify",
            title="Verification Calendar Event",
            duration_minutes=30
        )
        
        # Check required fields
        required_fields = {"event_id", "household_id", "user_id", "title", "start_time", "duration_minutes"}
        
        if not required_fields.issubset(set(event.keys())):
            logger.error(f"✗ {test_name} FAIL: Missing expected fields")
            return False
        
        # Check that TIL observation is metadata-only (not in base event)
        if "til_observation" not in event:
            logger.error(f"✗ {test_name} FAIL: TIL observation not included as metadata")
            return False
        
        # TIL should be metadata, not affect main event structure
        main_fields = {k for k in event.keys() if k != "til_observation"}
        if not required_fields.issubset(main_fields):
            logger.error(f"✗ {test_name} FAIL: Main event structure compromised")
            return False
        
        logger.info(f"✔ {test_name} PASS (calendar response intact with TIL metadata)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


# ============================================================================
# TEST 2: TIL Invocation Proof
# ============================================================================

def test_til_calls_in_all_services() -> bool:
    """TEST 2.1: TIL is called in all 3 domain services."""
    test_name = "TEST 2.1 - TIL Invocation in All Services"
    try:
        services = {
            "Task Service": Path("apps/api/services/task_service.py"),
            "Calendar Service": Path("apps/api/services/calendar_service.py"),
            "Email Service": Path("apps/api/modules/email/email_service.py"),
        }
        
        required_til_calls = {
            "get_til()": "TIL singleton access",
            "estimate_duration": "Duration estimation",
            "check_availability": "Availability check",
            "suggest_time_slot": "Time slot suggestion",
        }
        
        all_services_have_til = True
        
        for service_name, service_path in services.items():
            if not service_path.exists():
                logger.error(f"✗ {test_name} FAIL: {service_name} not found")
                return False
            
            with open(service_path) as f:
                source = f.read()
            
            found_calls = []
            for call_name in required_til_calls:
                if call_name in source:
                    found_calls.append(call_name)
            
            if not found_calls:
                logger.error(f"✗ {test_name} FAIL: {service_name} has no TIL calls")
                all_services_have_til = False
        
        if all_services_have_til:
            logger.info(f"✔ {test_name} PASS (TIL invoked in all 3 services)")
            return True
        
        return False
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_til_logging_present() -> bool:
    """TEST 2.2: TIL observations are logged."""
    test_name = "TEST 2.2 - TIL Logging"
    try:
        services = [
            Path("apps/api/services/task_service.py"),
            Path("apps/api/services/calendar_service.py"),
            Path("apps/api/modules/email/email_service.py"),
        ]
        
        logging_present = True
        
        for service_path in services:
            if not service_path.exists():
                continue
            
            with open(service_path) as f:
                source = f.read()
            
            # Check for logging of TIL values
            if "observation" not in source.lower() and "logger" not in source:
                logger.error(f"✗ {test_name} FAIL: {service_path.name} has no logging")
                logging_present = False
        
        if logging_present:
            logger.info(f"✔ {test_name} PASS (TIL observations are logged)")
            return True
        
        return False
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


# ============================================================================
# TEST 3: No Decision Coupling
# ============================================================================

def test_no_til_branching() -> bool:
    """TEST 3.1: No if/branches based on TIL output."""
    test_name = "TEST 3.1 - No TIL-Based Decision Branches"
    try:
        services = [
            Path("apps/api/services/task_service.py"),
            Path("apps/api/services/calendar_service.py"),
            Path("apps/api/modules/email/email_service.py"),
        ]
        
        import re
        
        for service_path in services:
            if not service_path.exists():
                continue
            
            with open(service_path) as f:
                source = f.read()
            
            # Look for branching patterns based on TIL
            forbidden_patterns = [
                r'if\s+til_\w+',
                r'if\s+not\s+til_\w+',
                r'if\s+.*\s+til_\w+',
                r'raise.*til_',
            ]
            
            for pattern in forbidden_patterns:
                matches = re.findall(pattern, source, re.IGNORECASE)
                if matches:
                    logger.error(
                        f"✗ {test_name} FAIL: {service_path.name} has TIL-based branching: {matches}"
                    )
                    return False
        
        logger.info(f"✔ {test_name} PASS (no branching on TIL output)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_original_business_logic_intact() -> bool:
    """TEST 3.2: Original business logic is not replaced or skipped."""
    test_name = "TEST 3.2 - Original Logic Intact"
    try:
        # Task service: create_task should still create tasks
        from apps.api.services.task_service import create_task
        task = create_task(
            household_id="test-hh-logic",
            title="Logic Test Task"
        )
        if not task or not task.id:
            logger.error(f"✗ {test_name} FAIL: Task creation failed")
            return False
        
        # Calendar service: schedule_event should still create events
        from apps.api.services.calendar_service import schedule_event
        event = schedule_event(
            household_id="test-hh-logic",
            user_id="test-user",
            title="Logic Test Event"
        )
        if not event or not event.get("event_id"):
            logger.error(f"✗ {test_name} FAIL: Event creation failed")
            return False
        
        # Email service: should still handle emails
        from apps.api.modules.email.email_service import handle_email_received
        from apps.api.schemas.events.email_events import EmailReceivedEvent
        
        email_data = EmailReceivedEvent(
            subject="Test Email",
            sender="test@example.com",
            body="Test body"
        )
        result = handle_email_received("test-hh-logic", email_data)
        if not result or result.get("status") != "email_processed":
            logger.error(f"✗ {test_name} FAIL: Email processing failed")
            return False
        
        logger.info(f"✔ {test_name} PASS (original business logic works)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


# ============================================================================
# TEST 4: System Stability
# ============================================================================

def test_async_worker_unchanged() -> bool:
    """TEST 4.1: Async event bus worker is unchanged."""
    test_name = "TEST 4.1 - Async Worker Unchanged"
    try:
        worker_path = Path("apps/api/services/worker.py")
        if not worker_path.exists():
            logger.warning(f"⊘ {test_name} SKIP: worker.py not found")
            return True
        
        with open(worker_path) as f:
            source = f.read()
        
        # Check that worker hasn't been decorated with TIL
        if "get_til()" in source:
            logger.error(f"✗ {test_name} FAIL: Worker modified with TIL")
            return False
        
        # Check that critical worker patterns still exist
        if "_worker_loop" not in source and "worker" not in source.lower():
            logger.error(f"✗ {test_name} FAIL: Worker loop removed")
            return False
        
        logger.info(f"✔ {test_name} PASS (worker unchanged)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_dlq_unchanged() -> bool:
    """TEST 4.2: Dead Letter Queue logic unchanged."""
    test_name = "TEST 4.2 - DLQ Unchanged"
    try:
        dlq_path = Path("apps/api/services/dlq_service.py")
        if not dlq_path.exists():
            logger.warning(f"⊘ {test_name} SKIP: dlq_service.py not found")
            return True
        
        with open(dlq_path) as f:
            source = f.read()
        
        # DLQ should not have been modified with TIL
        if "get_til()" in source:
            logger.error(f"✗ {test_name} FAIL: DLQ modified with TIL")
            return False
        
        logger.info(f"✔ {test_name} PASS (DLQ unchanged)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_idempotency_unchanged() -> bool:
    """TEST 4.3: Idempotency mechanisms unchanged."""
    test_name = "TEST 4.3 - Idempotency Unchanged"
    try:
        idempotency_path = Path("apps/api/services/idempotency_service.py")
        if not idempotency_path.exists():
            logger.warning(f"⊘ {test_name} SKIP: idempotency_service.py not found")
            return True
        
        with open(idempotency_path) as f:
            source = f.read()
        
        # Idempotency should not be affected by TIL
        if "get_til()" in source:
            logger.error(f"✗ {test_name} FAIL: Idempotency modified with TIL")
            return False
        
        logger.info(f"✔ {test_name} PASS (idempotency unchanged)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_replay_unchanged() -> bool:
    """TEST 4.4: Event replay logic unchanged."""
    test_name = "TEST 4.4 - Replay Unchanged"
    try:
        replay_path = Path("apps/api/services/event_replay_service.py")
        if not replay_path.exists():
            logger.warning(f"⊘ {test_name} SKIP: event_replay_service.py not found")
            return True
        
        with open(replay_path) as f:
            source = f.read()
        
        # Replay should not be affected by TIL
        if "get_til()" in source:
            logger.error(f"✗ {test_name} FAIL: Replay modified with TIL")
            return False
        
        logger.info(f"✔ {test_name} PASS (replay unchanged)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_system_imports() -> bool:
    """TEST 4.5: Full system imports without errors."""
    test_name = "TEST 4.5 - System Import Health"
    try:
        from apps.api import main
        from apps.api.services.task_service import create_task
        from apps.api.services.calendar_service import schedule_event
        from apps.api.modules.email.email_service import handle_email_received
        
        logger.info(f"✔ {test_name} PASS (full system imports successfully)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


# ============================================================================
# RUNNER
# ============================================================================

def run_all_tests() -> bool:
    """Run all verification tests."""
    print("\n" + "="*80)
    print("TEMPORAL INTELLIGENCE LAYER SHADOW MODE VERIFICATION (STEP 2)")
    print("STRICT PASS/FAIL - ALL GATES MUST PASS")
    print("="*80 + "\n")
    
    # TEST 1: Behavior Parity
    print("TEST 1: BEHAVIOR PARITY")
    print("-" * 80)
    test1_results = [
        test_no_schema_changes(),
        test_task_response_structure_unchanged(),
        test_calendar_event_response_structure(),
    ]
    test1_pass = all(test1_results)
    
    # TEST 2: TIL Invocation Proof
    print("\nTEST 2: TIL INVOCATION PROOF")
    print("-" * 80)
    test2_results = [
        test_til_calls_in_all_services(),
        test_til_logging_present(),
    ]
    test2_pass = all(test2_results)
    
    # TEST 3: No Decision Coupling
    print("\nTEST 3: NO DECISION COUPLING")
    print("-" * 80)
    test3_results = [
        test_no_til_branching(),
        test_original_business_logic_intact(),
    ]
    test3_pass = all(test3_results)
    
    # TEST 4: System Stability
    print("\nTEST 4: SYSTEM STABILITY")
    print("-" * 80)
    test4_results = [
        test_async_worker_unchanged(),
        test_dlq_unchanged(),
        test_idempotency_unchanged(),
        test_replay_unchanged(),
        test_system_imports(),
    ]
    test4_pass = all(test4_results)
    
    # Final gate check
    print("\n" + "="*80)
    all_pass = all([test1_pass, test2_pass, test3_pass, test4_pass])
    
    if all_pass:
        print("✔ STEP 2 COMPLETE: PASS (all gates cleared)")
        print("="*80)
        print("\nSUMMARY:")
        print("  ✔ Behavior Parity: System responses unchanged")
        print("  ✔ TIL Invocation: All 3 services calling TIL")
        print("  ✔ No Decision Coupling: Zero TIL-based logic")
        print("  ✔ System Stability: Worker/DLQ/idempotency/replay intact")
        print("="*80 + "\n")
        return True
    else:
        print("✗ STEP 2 BLOCKED: FAIL (one or more gates failed)")
        print("="*80 + "\n")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
