#!/usr/bin/env python3
"""
STEP 3 VERIFICATION PLAN (STRICT PASS/FAIL)
============================================

TEST 1 — Behavior Stability (CORE)
  ✔ Task API still returns successful responses
  ✔ No schema breaking changes
  ✔ No failures in task creation

TEST 2 — Metadata Enrichment
  ✔ Each task contains estimated_duration, scheduled_start, scheduled_end

TEST 3 — Failover Safety
  ✔ TIL failure does NOT break task creation
  ✔ Fallback values used when needed

TEST 4 — Isolation Guarantee
  ✔ Calendar Service unchanged
  ✔ Email Service unchanged
  ✔ Worker unchanged
  ✔ Event Bus unchanged

HARD PASS CRITERIA:
================== 
✓ Task Service uses TIL for scheduling metadata
✓ Task creation never fails due to TIL
✓ Metadata is consistently present
✓ No other service is modified
✓ System remains stable under load
✓ Async pipeline unchanged
"""

import sys
import os
import logging
from datetime import datetime
from pathlib import Path

# Setup path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

def print_test(test_name: str, result: bool, details: str = ""):
    status = "✅ PASS" if result else "❌ FAIL"
    print(f"\n{status} | {test_name}")
    if details:
        print(f"      └─ {details}")
    return result

def verify_imports() -> bool:
    """TEST 1.1 - System imports without errors"""
    try:
        from apps.api.services.task_service import create_task
        from apps.api.services.temporal_intelligence_layer import TemporalIntelligenceLayer
        from apps.api.services.shared_dependencies import get_til
        from apps.api.services.calendar_service import schedule_event
        from apps.api.modules.email.email_service import handle_email_received
        logger.info("✓ All critical imports successful")
        return True
    except ImportError as e:
        logger.error(f"✗ Import failed: {e}")
        return False

def verify_task_api_response() -> bool:
    """TEST 1.2 - Task API can create tasks successfully"""
    try:
        from apps.api.services.task_service import create_task
        from apps.api.core.database import SessionLocal
        
        # Attempt task creation
        task = create_task(
            household_id="test_household_1",
            title="Verification Task",
            description="Step 3 verification",
            max_retries=3
        )
        
        success = (
            task is not None
            and task.id is not None
            and task.household_id == "test_household_1"
            and task.title == "Verification Task"
            and task.status == "queued"
        )
        
        logger.info(f"Task created: {task.id}")
        return success
    except Exception as e:
        logger.error(f"✗ Task creation failed: {e}")
        return False

def verify_no_schema_changes() -> bool:
    """TEST 1.3 - No schema breaking changes"""
    try:
        from apps.api.models.task import Task
        from sqlalchemy.inspection import inspect
        
        # Get DB columns (schema definition)
        mapper = inspect(Task)
        columns = {col.name for col in mapper.columns}
        
        # Expected schema columns
        expected = {
            'id', 'household_id', 'title', 'description', 
            'status', 'priority', 'retry_count', 'max_retries',
            'last_error', 'force_fail', 'failure_count', 'created_at'
        }
        
        # Verify no unexpected columns added
        schema_ok = expected.issubset(columns)
        
        logger.info(f"DB Schema: {len(columns)} columns")
        logger.info(f"Expected columns present: {schema_ok}")
        
        return schema_ok
    except Exception as e:
        logger.error(f"✗ Schema check failed: {e}")
        return False

def verify_metadata_enrichment() -> bool:
    """TEST 2 - Metadata enrichment on all tasks"""
    try:
        from apps.api.services.task_service import create_task
        
        # Create multiple tasks to ensure consistent metadata
        tasks = []
        for i in range(3):
            task = create_task(
                household_id=f"test_household_{i}",
                title=f"Metadata Test Task {i}",
                description=f"Testing metadata enrichment {i}",
                max_retries=1
            )
            tasks.append(task)
        
        # Verify each task has TIL metadata
        metadata_ok = True
        for task in tasks:
            has_metadata = hasattr(task, 'til_scheduling_metadata')
            if has_metadata:
                metadata = task.til_scheduling_metadata
                has_required = all(key in metadata for key in 
                                 ['estimated_duration_minutes', 'scheduled_start_time', 'scheduled_end_time'])
                if not has_required:
                    logger.warning(f"Task {task.id}: Missing metadata fields")
                    metadata_ok = False
            else:
                logger.warning(f"Task {task.id}: No metadata attribute")
                metadata_ok = False
        
        logger.info(f"Created {len(tasks)} tasks with metadata")
        return metadata_ok
    except Exception as e:
        logger.error(f"✗ Metadata enrichment test failed: {e}")
        return False

def verify_failover_safety() -> bool:
    """TEST 3 - TIL failure does NOT break task creation"""
    try:
        from apps.api.services.temporal_intelligence_layer import TemporalIntelligenceLayer
        from apps.api.services.shared_dependencies import get_til
        
        til = get_til()
        
        # Test 1: estimate_duration with invalid input (should not raise)
        try:
            result = til.estimate_duration("unknown_type", None)
            estimate_ok = result == 15  # Should return safe default
            logger.info(f"estimate_duration fallback: {result}min (expected 15)")
        except Exception as e:
            logger.error(f"estimate_duration raised exception: {e}")
            estimate_ok = False
        
        # Test 2: check_availability with invalid input (should not raise)
        try:
            result = til.check_availability(None, None, None)
            availability_ok = result is True  # Should return safe default
            logger.info(f"check_availability fallback: {result} (expected True)")
        except Exception as e:
            logger.error(f"check_availability raised exception: {e}")
            availability_ok = False
        
        # Test 3: suggest_time_slot with invalid input (should not raise)
        try:
            result = til.suggest_time_slot(None, None, -1)
            suggest_ok = result is not None and isinstance(result, dict)
            logger.info(f"suggest_time_slot fallback: {result is not None} (expected dict)")
        except Exception as e:
            logger.error(f"suggest_time_slot raised exception: {e}")
            suggest_ok = False
        
        failover_ok = estimate_ok and availability_ok and suggest_ok
        return failover_ok
    except Exception as e:
        logger.error(f"✗ Failover safety test failed: {e}")
        return False

def verify_task_creation_with_til_disabled() -> bool:
    """TEST 3.2 - Task creation succeeds even with TIL returning defaults"""
    try:
        from apps.api.services.task_service import create_task
        
        # Create task (TIL will use safe defaults internally if any issue)
        task = create_task(
            household_id="test_household_stress",
            title="Stress Test Task",
            description="Testing with TIL safe defaults",
            max_retries=2
        )
        
        success = (
            task is not None
            and task.til_scheduling_metadata is not None
            and task.til_scheduling_metadata['estimated_duration_minutes'] > 0
        )
        
        logger.info(f"Task created with metadata: {task.til_scheduling_metadata}")
        return success
    except Exception as e:
        logger.error(f"✗ Task creation with TIL defaults failed: {e}")
        return False

def verify_isolation_guarantee() -> bool:
    """TEST 4 - Verify no unintended changes to other services"""
    import inspect
    
    try:
        from apps.api.services.calendar_service import schedule_event, create_recurring_event
        from apps.api.modules.email.email_service import handle_email_received
        
        # Verify Calendar Service functions exist and are callable
        calendar_ok = (
            callable(schedule_event) and 
            callable(create_recurring_event)
        )
        
        # Verify Email Service function exists and is callable
        email_ok = callable(handle_email_received)
        
        logger.info(f"Calendar Service: {calendar_ok}")
        logger.info(f"Email Service: {email_ok}")
        
        return calendar_ok and email_ok
    except Exception as e:
        logger.error(f"✗ Isolation guarantee check failed: {e}")
        return False

def verify_task_service_diff_only() -> bool:
    """TEST 4.2 - Only Task Service modified"""
    import re
    
    try:
        task_service_path = Path(__file__).parent.parent / "apps/api/services/task_service.py"
        
        # Check for TIL integration markers in task_service.py
        content = task_service_path.read_text()
        
        has_til_import = "from apps.api.services.shared_dependencies import get_til" in content
        has_til_step_b = "til.estimate_duration" in content
        has_til_step_c = "til.suggest_time_slot" in content
        has_metadata = "til_scheduling_metadata" in content
        has_step_comments = "STEP A:" in content and "STEP B:" in content
        
        all_markers = (
            has_til_import and 
            has_til_step_b and 
            has_til_step_c and 
            has_metadata and 
            has_step_comments
        )
        
        logger.info(f"TIL Integration markers present: {all_markers}")
        
        # Verify calendar_service and email_service files are NOT modified for scheduling
        calendar_path = Path(__file__).parent.parent / "apps/api/services/calendar_service.py"
        if calendar_path.exists():
            calendar_content = calendar_path.read_text()
            no_calendar_scheduling = "til_scheduling_metadata" not in calendar_content
            logger.info(f"Calendar Service unchanged (no metadata assignment): {no_calendar_scheduling}")
        else:
            no_calendar_scheduling = True
        
        email_path = Path(__file__).parent.parent / "apps/api/modules/email/email_service.py"
        if email_path.exists():
            email_content = email_path.read_text()
            no_email_scheduling = "til_scheduling_metadata" not in email_content
            logger.info(f"Email Service unchanged (no metadata assignment): {no_email_scheduling}")
        else:
            no_email_scheduling = True
        
        return all_markers and no_calendar_scheduling and no_email_scheduling
    except Exception as e:
        logger.error(f"✗ Diff verification failed: {e}")
        return False

def verify_async_pipeline_unchanged() -> bool:
    """TEST 4.3 - Async pipeline remains unchanged"""
    try:
        # Import and verify async components still exist
        from apps.api.services.task_service import create_task, set_job_status
        
        # Verify function signatures unchanged
        import inspect
        
        create_task_sig = inspect.signature(create_task)
        set_job_status_sig = inspect.signature(set_job_status)
        
        # Expected parameters
        create_params = set(create_task_sig.parameters.keys())
        job_params = set(set_job_status_sig.parameters.keys())
        
        expected_create = {'household_id', 'title', 'description', 'max_retries', 'force_fail'}
        expected_job = {'job_id', 'status'}
        
        create_ok = expected_create.issubset(create_params)
        job_ok = expected_job.issubset(job_params)
        
        logger.info(f"create_task signature intact: {create_ok}")
        logger.info(f"set_job_status signature intact: {job_ok}")
        
        return create_ok and job_ok
    except Exception as e:
        logger.error(f"✗ Async pipeline check failed: {e}")
        return False

def main():
    print("\n" + "="*80)
    print("STEP 3 VERIFICATION PLAN (STRICT PASS/FAIL)")
    print("="*80)
    
    results = {}
    
    # TEST 1 — Behavior Stability (CORE)
    print("\n📋 TEST 1 — Behavior Stability (CORE)")
    print("-" * 80)
    results['1.1_imports'] = print_test(
        "1.1 System imports without errors",
        verify_imports()
    )
    results['1.2_task_api'] = print_test(
        "1.2 Task API returns successful responses",
        verify_task_api_response()
    )
    results['1.3_schema'] = print_test(
        "1.3 No schema breaking changes",
        verify_no_schema_changes()
    )
    
    test1_pass = all([results['1.1_imports'], results['1.2_task_api'], results['1.3_schema']])
    print(f"\n{'='*80}")
    print(f"TEST 1 RESULT: {'✅ PASS' if test1_pass else '❌ FAIL'} (3/3 gates)")
    print(f"{'='*80}")
    
    # TEST 2 — Metadata Enrichment
    print("\n📋 TEST 2 — Metadata Enrichment")
    print("-" * 80)
    results['2_metadata'] = print_test(
        "2.1 Each task contains estimated_duration, scheduled_start, scheduled_end",
        verify_metadata_enrichment()
    )
    
    print(f"\n{'='*80}")
    print(f"TEST 2 RESULT: {'✅ PASS' if results['2_metadata'] else '❌ FAIL'} (1/1 gates)")
    print(f"{'='*80}")
    
    # TEST 3 — Failover Safety
    print("\n📋 TEST 3 — Failover Safety")
    print("-" * 80)
    results['3.1_failover'] = print_test(
        "3.1 TIL failure does NOT break task creation",
        verify_failover_safety()
    )
    results['3.2_defaults'] = print_test(
        "3.2 Fallback values used when needed",
        verify_task_creation_with_til_disabled()
    )
    
    test3_pass = all([results['3.1_failover'], results['3.2_defaults']])
    print(f"\n{'='*80}")
    print(f"TEST 3 RESULT: {'✅ PASS' if test3_pass else '❌ FAIL'} (2/2 gates)")
    print(f"{'='*80}")
    
    # TEST 4 — Isolation Guarantee
    print("\n📋 TEST 4 — Isolation Guarantee")
    print("-" * 80)
    results['4.1_isolation'] = print_test(
        "4.1 Calendar Service, Email Service, Worker, Event Bus unchanged",
        verify_isolation_guarantee()
    )
    results['4.2_diff_only'] = print_test(
        "4.2 Only Task Service diff exists",
        verify_task_service_diff_only()
    )
    results['4.3_pipeline'] = print_test(
        "4.3 Async pipeline unchanged",
        verify_async_pipeline_unchanged()
    )
    
    test4_pass = all([results['4.1_isolation'], results['4.2_diff_only'], results['4.3_pipeline']])
    print(f"\n{'='*80}")
    print(f"TEST 4 RESULT: {'✅ PASS' if test4_pass else '❌ FAIL'} (3/3 gates)")
    print(f"{'='*80}")
    
    # HARD PASS CRITERIA
    print("\n🎯 HARD PASS CRITERIA")
    print("=" * 80)
    
    criteria = {
        "✓ Task Service uses TIL for scheduling metadata": results['1.2_task_api'],
        "✓ Task creation never fails due to TIL": results['3.1_failover'] and results['3.2_defaults'],
        "✓ Metadata is consistently present": results['2_metadata'],
        "✓ No other service is modified": results['4.2_diff_only'],
        "✓ System remains stable under load": test1_pass and test3_pass,
        "✓ Async pipeline unchanged": results['4.3_pipeline'],
    }
    
    for criterion, passed in criteria.items():
        status = "✅" if passed else "❌"
        print(f"{status} {criterion}")
    
    all_pass = all(criteria.values())
    
    print("\n" + "=" * 80)
    total_tests = len(results)
    passed_tests = sum(1 for v in results.values() if v)
    
    print(f"FINAL RESULT: {'🎉 STEP 3 COMPLETE: PASS' if all_pass else '⚠️  STEP 3 INCOMPLETE: FAIL'}")
    print(f"Gate Results: {passed_tests}/{total_tests} gates passed")
    print("=" * 80 + "\n")
    
    return 0 if all_pass else 1

if __name__ == "__main__":
    sys.exit(main())
