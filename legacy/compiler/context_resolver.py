"""
Context Resolver — Inject household + user context into Intent objects.

Enriches structured Intent objects with household preferences, user settings,
time context, and known recurring patterns. Does NOT silently infer missing
critical constraints; instead marks them as ambiguities for downstream
disambiguation.

Context sources:
  - Household preferences (timezone, language, business hours, budget rules)
  - User settings (notification preferences, availability patterns)
  - Known patterns (existing recurring tasks, schedule templates)
  - System time context (current time, calendar info, availability)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Any

from legacy.compiler.intent_parser import Intent


@dataclass(frozen=True)
class HouseholdContext:
    """Household-level preferences and metadata."""

    household_id: str
    timezone: str = "UTC"
    """IANA timezone identifier (e.g., 'America/New_York')."""

    language: str = "en"
    """Preferred language code."""

    business_hours_start: time = field(default_factory=lambda: time(9, 0))
    """Start of typical household activity window."""

    business_hours_end: time = field(default_factory=lambda: time(18, 0))
    """End of typical household activity window."""

    budget_monthly_limit: float | None = None
    """Monthly budget in household currency."""

    currency: str = "USD"
    """Currency for budget and financial operations."""

    known_members: list[str] = field(default_factory=list)
    """List of household member names (for entity resolution)."""

    recurring_patterns: dict[str, dict[str, Any]] = field(default_factory=dict)
    """
    Known recurring patterns by name.
    {
        "weekly_grocery": {"frequency": "weekly", "day": "Saturday", "duration_minutes": 90},
        "daily_exercise": {"frequency": "daily", "time": "07:00", "duration_minutes": 30},
    }
    """


@dataclass(frozen=True)
class UserContext:
    """User-level settings and preferences."""

    user_id: str
    household_id: str
    name: str = ""
    """User's display name."""

    timezone: str = "UTC"
    """User's personal timezone (may differ from household)."""

    notification_preferences: dict[str, bool] = field(default_factory=dict)
    """
    Notification channel preferences.
    {"email": True, "sms": False, "push": True}
    """

    availability_window_start: time | None = None
    """Preferred start time for tasks (e.g., 08:00)."""

    availability_window_end: time | None = None
    """Preferred end time for tasks (e.g., 17:00)."""

    task_preferences: dict[str, Any] = field(default_factory=dict)
    """
    User's typical task settings.
    {"default_priority": "medium", "typical_duration_minutes": 30}
    """


@dataclass(frozen=True)
class SystemContext:
    """Current system state and time context."""

    now: datetime
    """Current datetime."""

    calendar_events: list[dict[str, Any]] = field(default_factory=list)
    """
    Upcoming calendar events.
    [{"title": "Team Meeting", "start": datetime, "end": datetime, "busy": True}, ...]
    """

    household_is_busy: bool = False
    """True if household is in a busy/coordinated period."""

    available_members: list[str] = field(default_factory=list)
    """Members currently available."""

    metadata: dict[str, Any] = field(default_factory=dict)
    """Additional system context."""


@dataclass(frozen=True)
class EnrichedIntent:
    """
    Intent augmented with resolved context.

    Inherits all fields from Intent and adds context-derived fields.
    """

    origin_intent: Intent
    """Original Intent before enrichment."""

    household_context: HouseholdContext | None = None
    """Resolved household preferences."""

    user_context: UserContext | None = None
    """Resolved user settings."""

    system_context: SystemContext | None = None
    """Current system time and calendar state."""

    resolved_constraints: dict[str, Any] = field(default_factory=dict)
    """
    In-place resolved constraint values (deadline times, budgets, etc.).
    Built from intent.constraints + context.
    """

    resolved_recurrence: dict[str, Any] = field(default_factory=dict)
    """
    Mapped recurrence defaults from known patterns or context.
    If intent says "weekly grocery", maps to pattern if found.
    """

    new_ambiguities: list[str] = field(default_factory=list)
    """
    Ambiguities introduced or discovered during context resolution.
    (e.g., "member_not_found", "budget_limit_unset")
    """

    @property
    def all_ambiguities(self) -> list[str]:
        """Combined origin + new ambiguity flags."""
        return list(set(self.origin_intent.ambiguity_flags) | set(self.new_ambiguities))


class ContextStore(ABC):
    """Abstract interface for retrieving context from storage."""

    @abstractmethod
    def get_household_context(self, household_id: str) -> HouseholdContext | None:
        """Retrieve household preferences and metadata."""
        pass

    @abstractmethod
    def get_user_context(self, user_id: str, household_id: str) -> UserContext | None:
        """Retrieve user settings and preferences."""
        pass

    @abstractmethod
    def get_system_context(self) -> SystemContext:
        """Retrieve current system state."""
        pass


class InMemoryContextStore(ContextStore):
    """Minimal in-memory context store for testing and demos."""

    def __init__(self):
        self._households: dict[str, HouseholdContext] = {}
        self._users: dict[tuple[str, str], UserContext] = {}

    def register_household(self, ctx: HouseholdContext) -> None:
        """Register a household context."""
        self._households[ctx.household_id] = ctx

    def register_user(self, ctx: UserContext) -> None:
        """Register a user context."""
        self._users[(ctx.user_id, ctx.household_id)] = ctx

    def get_household_context(self, household_id: str) -> HouseholdContext | None:
        return self._households.get(household_id)

    def get_user_context(self, user_id: str, household_id: str) -> UserContext | None:
        return self._users.get((user_id, household_id))

    def get_system_context(self) -> SystemContext:
        return SystemContext(now=datetime.now())


class ContextResolver:
    """
    Resolves and injects context into Intent objects.

    Does NOT silently guess missing critical constraints; instead adds
    ambiguity flags for disambiguation.
    """

    def __init__(self, context_store: ContextStore):
        self._store = context_store

    def resolve(self, intent: Intent) -> EnrichedIntent:
        """
        Enrich an Intent with resolved context.

        Args:
            intent: The Intent to enrich.

        Returns:
            An EnrichedIntent with resolved context fields.
        """
        # Fetch context from store
        household_ctx = self._store.get_household_context(intent.household_id)
        user_ctx = self._store.get_user_context(intent.user_id, intent.household_id)
        system_ctx = self._store.get_system_context()

        # Resolve constraints
        resolved_constraints = self._resolve_constraints(
            intent, household_ctx, user_ctx, system_ctx
        )

        # Resolve recurrence patterns
        resolved_recurrence = self._resolve_recurrence(
            intent, household_ctx
        )

        # Collect new ambiguities
        new_ambiguities = self._collect_new_ambiguities(
            intent, household_ctx, user_ctx, resolved_constraints
        )

        return EnrichedIntent(
            origin_intent=intent,
            household_context=household_ctx,
            user_context=user_ctx,
            system_context=system_ctx,
            resolved_constraints=resolved_constraints,
            resolved_recurrence=resolved_recurrence,
            new_ambiguities=new_ambiguities,
        )

    def _resolve_constraints(
        self,
        intent: Intent,
        household_ctx: HouseholdContext | None,
        user_ctx: UserContext | None,
        system_ctx: SystemContext,
    ) -> dict[str, Any]:
        """
        Resolve constraint values from context.

        Fills in missing constraint values where context provides them.
        """
        resolved = dict(intent.constraints)

        # Deadline resolution
        if resolved.get("deadline") is None:
            # Check if intent says "ASAP" or "today"
            if "asap" in intent.raw_input.lower() or "today" in intent.raw_input.lower():
                # Set deadline to end of business hours today
                if household_ctx:
                    resolved["deadline"] = datetime.combine(
                        system_ctx.now.date(),
                        household_ctx.business_hours_end,
                    )

        # Budget limit resolution
        if resolved.get("budget_limit") is None and household_ctx:
            # Auto-set monthly budget if household has one
            if household_ctx.budget_monthly_limit is not None:
                resolved["budget_limit"] = household_ctx.budget_monthly_limit

        # Time slot resolution
        if resolved.get("time_slot") is None and user_ctx:
            # Try user availability window
            if user_ctx.availability_window_start and user_ctx.availability_window_end:
                resolved["time_slot"] = (
                    user_ctx.availability_window_start.isoformat(),
                    user_ctx.availability_window_end.isoformat(),
                )

        return resolved

    def _resolve_recurrence(
        self,
        intent: Intent,
        household_ctx: HouseholdContext | None,
    ) -> dict[str, Any]:
        """
        Resolve recurrence patterns from known household patterns.

        Maps phrase-like patterns to actual recurrence configs.
        """
        resolved = dict(intent.recurrence_hints)

        # If intent mentions a known pattern name, look it up
        if household_ctx and household_ctx.recurring_patterns:
            subject = intent.entities.get("subject", "").lower()
            for pattern_name, pattern_config in household_ctx.recurring_patterns.items():
                if pattern_name.replace("_", " ") in subject:
                    # Merge pattern config with intent hints
                    resolved.update(pattern_config)
                    resolved["is_recurring"] = True
                    break

        return resolved

    def _collect_new_ambiguities(
        self,
        intent: Intent,
        household_ctx: HouseholdContext | None,
        user_ctx: UserContext | None,
        resolved_constraints: dict[str, Any],
    ) -> list[str]:
        """Collect ambiguity flags raised during context resolution."""
        flags: list[str] = []

        # Critical: no household context found
        if household_ctx is None:
            flags.append("household_context_missing")

        # Critical: no user context found
        if user_ctx is None:
            flags.append("user_context_missing")

        # Budget limit required but not set (after resolution)
        if "budget" in intent.raw_input.lower():
            if resolved_constraints.get("budget_limit") is None:
                flags.append("budget_limit_unset")

        # Recipient mentioned but not found in household members
        if household_ctx:
            recipients = intent.entities.get("recipients", [])
            for recipient in recipients:
                if recipient not in household_ctx.known_members:
                    flags.append(f"member_not_found: {recipient}")

        return flags
