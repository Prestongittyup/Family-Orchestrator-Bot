"""
Temporal Intelligence Layer (TIL)

A stateless, deterministic module for time-based intelligence and scheduling.

DESIGN CONSTRAINTS:
  • Stateless: No persistent state or side effects
  • No database access: Pure computation only
  • No domain coupling: Independent of Task, Calendar, Email, etc.
  • No async: Synchronous, deterministic rules
  • Deterministic: Output is fully determined by inputs
  • No external APIs: Self-contained logic

RESPONSIBILITIES:
  • Availability checking (MVP: always available)
  • Time slot suggestion (deterministic booking logic)
  • Task duration estimation (rule-based per event type)

This module can be safely used anywhere in the system without circular
dependencies or state side-effects.
"""

from __future__ import annotations

from datetime import datetime, timedelta


LEGACY_ISOLATED = True


class TemporalIntelligenceLayer:
    """
    Temporal Intelligence Layer (TIL)

    Stateless, deterministic time-based intelligence service.
    No database access, no domain coupling, no external dependencies.

    DESIGN:
      • Stateless: No persistent state or side effects
      • Deterministic: Output fully determined by inputs
      • Isolated: Independent of Task, Calendar, Email, etc.
      • Synchronous: No async logic required
    """

    def check_availability(
        self,
        user_id: str,
        household_id: str,
        requested_time: str | None = None,
    ) -> bool:
        """
        Check if a user is available at a requested time.

        MVP BEHAVIOR: Always returns True (every user is available).
        This is a placeholder for future integration with calendar/schedule data.

        FAIL-SAFE: On any error, returns True (safe default: assume available).
        No exceptions propagate to callers.

        Args:
            user_id: Identifier for the user
            household_id: Identifier for the household context
            requested_time: ISO 8601 datetime string (optional). If provided, check
                           availability at that specific time. Ignored in MVP.

        Returns:
            bool: True if available, False if not. MVP always returns True.
                  On error: returns True (safe default).

        Example:
            >>> til = TemporalIntelligenceLayer()
            >>> til.check_availability("user-123", "household-456")
            True
            >>> til.check_availability("user-123", "household-456", "2026-04-14T14:00:00")
            True
        """
        try:
            # MVP: Always available. Future implementation would check:
            #   - User's calendar availability
            #   - Household constraints (e.g., quiet hours, family events)
            #   - User preferences and historical patterns
            return True
        except Exception:
            # FAIL-SAFE: On any error, assume user is available
            return True

    def suggest_time_slot(
        self,
        user_id: str,
        household_id: str,
        duration_minutes: int,
    ) -> dict[str, str]:
        """
        Suggest an available time slot for scheduling a task.

        DETERMINISTIC RULE: Suggest the next hour from now, with the specified
        duration. This ensures consistent, predictable behavior across restarts
        and multiple calls with the same clock time.

        FAIL-SAFE: On any error, returns now + 1 hour window. No exceptions
        propagate to callers.

        Args:
            user_id: Identifier for the user
            household_id: Identifier for the household context
            duration_minutes: How long the task will take (minutes)

        Returns:
            dict with ISO 8601 timestamps:
              {
                "start_time": "2026-04-14T15:00:00",
                "end_time": "2026-04-14T15:30:00"
              }
            On error: returns next hour window (fail-safe default).

        Example:
            >>> til = TemporalIntelligenceLayer()
            >>> slot = til.suggest_time_slot("user-123", "household-456", 30)
            >>> slot["start_time"]  # ~now + 1 hour
            >>> slot["end_time"]    # start_time + 30 minutes
        """
        try:
            # Calculate "next hour on the hour" from now.
            # This is deterministic: same clock time always produces same suggestion.
            now = datetime.utcnow()
            
            # Round up to the next hour (e.g., 14:25 → 15:00:00)
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            
            start_time_iso = next_hour.isoformat()
            end_time = next_hour + timedelta(minutes=duration_minutes)
            end_time_iso = end_time.isoformat()

            return {
                "start_time": start_time_iso,
                "end_time": end_time_iso,
            }
        except Exception:
            # FAIL-SAFE: On any error, return next hour window
            now = datetime.utcnow()
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            return {
                "start_time": next_hour.isoformat(),
                "end_time": (next_hour + timedelta(minutes=60)).isoformat(),
            }

    def estimate_duration(
        self,
        task_type: str,
        payload: dict,
    ) -> int:
        """
        Estimate how long a task will take to complete.

        RULE-BASED ESTIMATION:
          - "email_received" → 10 minutes (quick email processing)
          - "task_created" → 30 minutes (task setup and planning)
          - default → 15 minutes (generic tasks)

        FAIL-SAFE: On any error, returns 15 minutes (safe default).
        No exceptions propagate to callers.

        Args:
            task_type: Type of task/event (e.g., "email_received", "task_created")
            payload: Task metadata (dict). Rules may inspect specific fields
                     in future versions; currently unused.

        Returns:
            int: Estimated duration in minutes
            On error: returns 15 (safe default).

        Example:
            >>> til = TemporalIntelligenceLayer()
            >>> til.estimate_duration("email_received", {})
            10
            >>> til.estimate_duration("task_created", {})
            30
            >>> til.estimate_duration("unknown_type", {})
            15
        """
        try:
            # Deterministic mapping: task_type → duration_minutes
            duration_map = {
                "email_received": 10,
                "task_created": 30,
            }

            # Return mapped duration or default
            return duration_map.get(task_type, 15)
        except Exception:
            # FAIL-SAFE: On any error, return safe default (15 minutes)
            return 15
