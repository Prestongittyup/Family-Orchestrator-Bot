"""
Temporal Intelligence Layer Contract

Strict interface definition for temporal intelligence operations.

This protocol defines the contract that all TIL implementations MUST satisfy.
It is used by domain services (router, planner, etc.) to interact with
temporal intelligence without direct coupling to implementation details.

DESIGN CONSTRAINTS:
  • Interface-only: No logic, no state
  • Type hints only: Full signature documentation
  • No dependencies: Cannot import from services or domain modules
  • Structural typing: Uses Protocol for flexible implementation
"""

from __future__ import annotations

from typing import Protocol


class TILContract(Protocol):
    """
    Temporal Intelligence Layer Contract.

    Defines the interface that all TIL implementations must provide.
    Domain services depend on this contract, not on specific implementations.

    This is a structural protocol: any object with these methods will
    satisfy the contract, regardless of whether it explicitly inherits
    from TILContract.
    """

    def check_availability(
        self,
        user_id: str,
        household_id: str,
        requested_time: str | None = None,
    ) -> bool:
        """
        Check if a user is available at a requested time.

        Args:
            user_id: Identifier for the user
            household_id: Identifier for the household context
            requested_time: ISO 8601 datetime string (optional)

        Returns:
            bool: True if available, False otherwise
        """
        ...

    def suggest_time_slot(
        self,
        user_id: str,
        household_id: str,
        duration_minutes: int,
    ) -> dict[str, str]:
        """
        Suggest an available time slot for scheduling a task.

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
        """
        ...

    def estimate_duration(
        self,
        task_type: str,
        payload: dict,
    ) -> int:
        """
        Estimate how long a task will take to complete.

        Args:
            task_type: Type of task/event (e.g., "email_received", "task_created")
            payload: Task metadata (dict)

        Returns:
            int: Estimated duration in minutes
        """
        ...
