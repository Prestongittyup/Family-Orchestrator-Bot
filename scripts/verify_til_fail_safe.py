"""
Verify TIL Fail-Safe Behavior

Tests that all TIL functions have fail-safe behavior:
  - Never raise exceptions
  - Return safe defaults on error
"""

from __future__ import annotations

import sys
import logging
from unittest.mock import patch, MagicMock

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)


def test_check_availability_never_raises() -> bool:
    """TEST 1: check_availability never raises exceptions."""
    test_name = "TEST 1 - check_availability Never Raises"
    try:
        from apps.api.services.temporal_intelligence_layer import TemporalIntelligenceLayer

        til = TemporalIntelligenceLayer()

        # Normal call
        result1 = til.check_availability("user-1", "hh-1")
        if result1 is not True:
            logger.error(f"✗ {test_name} FAIL: Expected True, got {result1}")
            return False

        # With requested_time
        result2 = til.check_availability("user-1", "hh-1", "2026-04-15T10:00:00")
        if result2 is not True:
            logger.error(f"✗ {test_name} FAIL: Expected True, got {result2}")
            return False

        logger.info(f"✔ {test_name} PASS (always returns True)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: Exception raised: {e}")
        return False


def test_suggest_time_slot_never_raises() -> bool:
    """TEST 2: suggest_time_slot never raises exceptions."""
    test_name = "TEST 2 - suggest_time_slot Never Raises"
    try:
        from apps.api.services.temporal_intelligence_layer import TemporalIntelligenceLayer

        til = TemporalIntelligenceLayer()

        # Normal call
        result = til.suggest_time_slot("user-1", "hh-1", 30)
        
        if not isinstance(result, dict):
            logger.error(f"✗ {test_name} FAIL: Expected dict, got {type(result)}")
            return False

        if "start_time" not in result or "end_time" not in result:
            logger.error(f"✗ {test_name} FAIL: Missing keys in result")
            return False

        logger.info(
            f"✔ {test_name} PASS (returns dict with start_time and end_time: "
            f"{result['start_time']} to {result['end_time']})"
        )
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: Exception raised: {e}")
        return False


def test_estimate_duration_never_raises() -> bool:
    """TEST 3: estimate_duration never raises exceptions."""
    test_name = "TEST 3 - estimate_duration Never Raises"
    try:
        from apps.api.services.temporal_intelligence_layer import TemporalIntelligenceLayer

        til = TemporalIntelligenceLayer()

        # Test various task types
        test_cases = [
            ("email_received", 10),
            ("task_created", 30),
            ("unknown_type", 15),
            ("", 15),
            (None, 15),  # This might raise or return 15, but should not crash
        ]

        for task_type, expected in test_cases:
            try:
                result = til.estimate_duration(task_type, {})
                if isinstance(result, int) and result >= 0:
                    logger.info(f"  - estimate_duration('{task_type}') = {result}min")
                else:
                    logger.error(f"✗ {test_name} FAIL: Invalid result for '{task_type}': {result}")
                    return False
            except Exception as e:
                logger.error(f"✗ {test_name} FAIL: Exception for '{task_type}': {e}")
                return False

        logger.info(f"✔ {test_name} PASS (handles all inputs, returns valid durations)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: Test itself raised: {e}")
        return False


def test_suggest_time_slot_with_invalid_duration() -> bool:
    """TEST 4: suggest_time_slot handles invalid duration gracefully."""
    test_name = "TEST 4 - suggest_time_slot with Invalid Duration"
    try:
        from apps.api.services.temporal_intelligence_layer import TemporalIntelligenceLayer

        til = TemporalIntelligenceLayer()

        # Try with negative duration (edge case)
        result = til.suggest_time_slot("user-1", "hh-1", -10)
        
        if not isinstance(result, dict) or "start_time" not in result:
            logger.error(f"✗ {test_name} FAIL: Failed to handle negative duration")
            return False

        logger.info(f"✔ {test_name} PASS (handles negative duration gracefully)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: Exception raised: {e}")
        return False


def test_estimate_duration_with_invalid_payload() -> bool:
    """TEST 5: estimate_duration handles invalid payload gracefully."""
    test_name = "TEST 5 - estimate_duration with Invalid Payload"
    try:
        from apps.api.services.temporal_intelligence_layer import TemporalIntelligenceLayer

        til = TemporalIntelligenceLayer()

        # Try with None payload (should not crash)
        result1 = til.estimate_duration("task_created", None)
        if not isinstance(result1, int):
            # Even if it fails, it should return an int or raise, not crash silently
            logger.error(f"✗ {test_name} FAIL: Invalid return type with None payload")
            return False

        # Try with various invalid inputs
        result2 = til.estimate_duration("task_created", "not_a_dict")
        if not isinstance(result2, int):
            logger.error(f"✗ {test_name} FAIL: Invalid return type with string payload")
            return False

        logger.info(f"✔ {test_name} PASS (handles invalid payload gracefully)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: Exception raised: {e}")
        return False


def test_task_service_with_til_fail_safe() -> bool:
    """TEST 6: Task Service benefits from TIL fail-safe behavior."""
    test_name = "TEST 6 - Task Service with TIL Fail-Safe"
    try:
        from apps.api.services.task_service import create_task

        # Create a task - should succeed regardless of TIL behavior
        task = create_task("hh-fail-safe", "Task with TIL Fail-Safe")

        if not task or not task.id:
            logger.error(f"✗ {test_name} FAIL: Task creation failed")
            return False

        if not hasattr(task, "til_scheduling_metadata"):
            logger.error(f"✗ {test_name} FAIL: TIL metadata not attached")
            return False

        metadata = task.til_scheduling_metadata
        if not metadata or "estimated_duration_minutes" not in metadata:
            logger.error(f"✗ {test_name} FAIL: Invalid metadata")
            return False

        logger.info(
            f"✔ {test_name} PASS (task created with TIL metadata: "
            f"duration={metadata['estimated_duration_minutes']}min)"
        )
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def run_all_tests() -> bool:
    """Run all verification tests."""
    print("\n" + "="*80)
    print("TIL FAIL-SAFE BEHAVIOR VERIFICATION")
    print("="*80 + "\n")

    tests = [
        test_check_availability_never_raises,
        test_suggest_time_slot_never_raises,
        test_estimate_duration_never_raises,
        test_suggest_time_slot_with_invalid_duration,
        test_estimate_duration_with_invalid_payload,
        test_task_service_with_til_fail_safe,
    ]

    results = [test() for test in tests]

    print("\n" + "="*80)
    if all(results):
        print("✔ FAIL-SAFE VERIFICATION: PASS (6/6 tests passed)")
        print("="*80)
        print("\nS UMMARY:")
        print("  ✔ check_availability never raises")
        print("  ✔ suggest_time_slot never raises")
        print("  ✔ estimate_duration never raises")
        print("  ✔ All functions handle edge cases gracefully")
        print("  ✔ Task Service benefits from TIL resilience")
        print("="*80 + "\n")
        return True
    else:
        print("✗ FAIL-SAFE VERIFICATION: FAIL (one or more tests failed)")
        print("="*80 + "\n")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
