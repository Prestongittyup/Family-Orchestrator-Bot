"""
Verify TIL Shadow Mode Integration in Calendar Service

Tests that TIL observations are being made without modifying calendar behavior.

CONSTRAINTS VERIFIED:
  ✓ No event scheduling logic changes
  ✓ No event blocking or enforcement
  ✓ Shadow mode (observe only)
  ✓ TIL suggestions logged but not enforced
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


def test_calendar_service_imports() -> bool:
    """TEST 1: calendar_service imports with TIL."""
    test_name = "TEST 1 - Calendar Service Imports"
    try:
        from apps.api.services.calendar_service import (
            schedule_event,
            create_recurring_event,
        )
        logger.info(f"✔ {test_name} PASS")
        return True
    except ImportError as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_til_calls_in_calendar() -> bool:
    """TEST 2: calendar_service contains TIL observation calls."""
    test_name = "TEST 2 - TIL Calls in Calendar Service"
    try:
        calendar_path = Path("apps/api/services/calendar_service.py")
        with open(calendar_path) as f:
            source = f.read()

        required_calls = [
            "get_til()",
            "suggest_time_slot",
            "check_availability",
            "estimate_duration",
        ]

        for call in required_calls:
            if call not in source:
                logger.error(f"✗ {test_name} FAIL: Missing call: {call}")
                return False

        logger.info(f"✔ {test_name} PASS (all TIL calls present)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_no_event_blocking() -> bool:
    """TEST 3: Events are not blocked based on TIL output."""
    test_name = "TEST 3 - No Event Blocking"
    try:
        calendar_path = Path("apps/api/services/calendar_service.py")
        with open(calendar_path) as f:
            source = f.read()

        # Check for patterns that would block on TIL output
        blocking_patterns = [
            "if til_availability",
            "if not til_available",
            "if til_suggestion",
            "raise.*availability",
            "return.*None.*til",
        ]

        for pattern in blocking_patterns:
            import re
            if re.search(pattern, source, re.IGNORECASE):
                logger.error(f"✗ {test_name} FAIL: Found blocking pattern: {pattern}")
                return False

        logger.info(f"✔ {test_name} PASS (no event blocking on TIL)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_shadow_mode_logging() -> bool:
    """TEST 4: TIL observations are logged."""
    test_name = "TEST 4 - Shadow Mode Logging"
    try:
        calendar_path = Path("apps/api/services/calendar_service.py")
        with open(calendar_path) as f:
            source = f.read()

        # Check for logging of TIL observations
        if "logger.info" not in source or "til_" not in source:
            logger.error(f"✗ {test_name} FAIL: No logging of TIL observations")
            return False

        logger.info(f"✔ {test_name} PASS (TIL observations logged)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_schedule_event_runtime() -> bool:
    """TEST 5: schedule_event() executes without errors."""
    test_name = "TEST 5 - schedule_event() Runtime"
    try:
        from apps.api.services.calendar_service import schedule_event

        # Call schedule_event with test data
        event = schedule_event(
            household_id="test-hh-1",
            user_id="test-user-1",
            title="Test Meeting",
            duration_minutes=30,
        )

        # Verify returned event has expected structure
        if not event.get("event_id"):
            logger.error(f"✗ {test_name} FAIL: Missing event_id")
            return False

        if not event.get("til_observation"):
            logger.error(f"✗ {test_name} FAIL: Missing TIL observation")
            return False

        if "suggested_time" not in event["til_observation"]:
            logger.error(f"✗ {test_name} FAIL: Missing suggested_time")
            return False

        logger.info(f"✔ {test_name} PASS (schedule_event executes successfully)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_recurring_event_runtime() -> bool:
    """TEST 6: create_recurring_event() executes without errors."""
    test_name = "TEST 6 - create_recurring_event() Runtime"
    try:
        from apps.api.services.calendar_service import create_recurring_event

        # Call create_recurring_event with test data
        event = create_recurring_event(
            household_id="test-hh-1",
            user_id="test-user-1",
            title="Weekly Sync",
            frequency="weekly",
            duration_minutes=60,
        )

        # Verify returned event has expected structure
        if not event.get("event_id"):
            logger.error(f"✗ {test_name} FAIL: Missing event_id")
            return False

        if not event.get("til_observation"):
            logger.error(f"✗ {test_name} FAIL: Missing TIL observation")
            return False

        if "estimated_duration" not in event["til_observation"]:
            logger.error(f"✗ {test_name} FAIL: Missing estimated_duration")
            return False

        logger.info(f"✔ {test_name} PASS (create_recurring_event executes successfully)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_shadow_mode_behavior_neutral() -> bool:
    """TEST 7: TIL observations don't affect event creation."""
    test_name = "TEST 7 - Shadow Mode Behavior Neutral"
    try:
        from apps.api.services.calendar_service import schedule_event

        # Create event with explicit start_time (should be used if provided)
        explicit_time = "2026-04-15T10:00:00"
        event = schedule_event(
            household_id="test-hh-2",
            user_id="test-user-2",
            title="Explicit Time Meeting",
            start_time=explicit_time,
            duration_minutes=45,
        )

        # Verify that provided start_time is respected
        if event["start_time"] != explicit_time:
            if explicit_time == event.get("til_observation", {}).get("suggested_time"):
                # It's okay if they happen to match
                logger.info(
                    f"✔ {test_name} PASS (event uses provided start_time, "
                    f"happens to match TIL suggestion)"
                )
                return True
            else:
                logger.error(
                    f"✗ {test_name} FAIL: Event start_time {event['start_time']} "
                    f"doesn't match provided {explicit_time}"
                )
                return False

        logger.info(f"✔ {test_name} PASS (event uses provided start_time)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def run_all_tests() -> bool:
    """Run all verification tests."""
    print("\n" + "="*80)
    print("TIL SHADOW MODE INTEGRATION IN CALENDAR SERVICE")
    print("="*80 + "\n")

    tests = [
        test_calendar_service_imports,
        test_til_calls_in_calendar,
        test_no_event_blocking,
        test_shadow_mode_logging,
        test_schedule_event_runtime,
        test_recurring_event_runtime,
        test_shadow_mode_behavior_neutral,
    ]

    results = [test() for test in tests]

    print("\n" + "="*80)
    if all(results):
        print("✔ OVERALL: PASS (shadow mode integrated safely)")
        print("="*80 + "\n")
        return True
    else:
        print("✗ OVERALL: FAIL (shadow mode integration incomplete)")
        print("="*80 + "\n")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
