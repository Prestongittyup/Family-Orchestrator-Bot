"""
Demo of ContextResolver — Intent enrichment with household context.

Run with:
    PYTHONPATH='.' python compiler/demo_context_resolver.py
"""

from datetime import datetime, time, timedelta

from legacy.compiler.intent_parser import IntentParser
from legacy.compiler.context_resolver import (
    ContextResolver,
    InMemoryContextStore,
    HouseholdContext,
    UserContext,
)


def main():
    # Set up context store
    store = InMemoryContextStore()

    # Register household context
    household_ctx = HouseholdContext(
        household_id="h001",
        timezone="America/New_York",
        language="en",
        business_hours_start=time(8, 0),
        business_hours_end=time(18, 0),
        budget_monthly_limit=5000.0,
        currency="USD",
        known_members=["Alice", "Bob", "Charlie"],
        recurring_patterns={
            "weekly_grocery": {
                "frequency": "weekly",
                "day": "Saturday",
                "duration_minutes": 90,
            },
            "daily_exercise": {
                "frequency": "daily",
                "time": "07:00",
                "duration_minutes": 30,
            },
        },
    )
    store.register_household(household_ctx)

    # Register user context
    user_ctx = UserContext(
        user_id="user_alice",
        household_id="h001",
        name="Alice",
        timezone="America/New_York",
        notification_preferences={"email": True, "sms": False, "push": True},
        availability_window_start=time(9, 0),
        availability_window_end=time(17, 0),
        task_preferences={"default_priority": "medium", "typical_duration_minutes": 45},
    )
    store.register_user(user_ctx)

    # Create resolver
    resolver = ContextResolver(store)

    # Parse and enrich Intent 1: Budget query
    print("=" * 70)
    print("EXAMPLE 1: Budget Query (No Missing Context)")
    print("=" * 70)
    parser = IntentParser()
    intent1 = parser.parse(
        "Check the household budget",
        household_id="h001",
        user_id="user_alice",
    )
    enriched1 = resolver.resolve(intent1)

    print(f"Original ambiguities:  {intent1.ambiguity_flags or 'None'}")
    print(f"New ambiguities:       {enriched1.new_ambiguities or 'None'}")
    print(f"All ambiguities:       {enriched1.all_ambiguities or 'None'}")
    print(f"Resolved constraints:  {enriched1.resolved_constraints}")
    print()

    # Parse and enrich Intent 2: Known recurring pattern
    print("=" * 70)
    print("EXAMPLE 2: Known Recurring Pattern ('weekly_grocery')")
    print("=" * 70)
    intent2 = parser.parse(
        "Schedule the weekly grocery run",
        household_id="h001",
        user_id="user_alice",
    )
    enriched2 = resolver.resolve(intent2)

    print(f"Original recurrence:   {intent2.recurrence_hints}")
    print(f"Resolved recurrence:   {enriched2.resolved_recurrence}")
    print(f"New ambiguities:       {enriched2.new_ambiguities or 'None'}")
    print()

    # Parse and enrich Intent 3: Task with deadline (ASAP)
    print("=" * 70)
    print("EXAMPLE 3: Task with ASAP Deadline (Auto-resolved)")
    print("=" * 70)
    intent3 = parser.parse(
        "Create a task ASAP",
        household_id="h001",
        user_id="user_alice",
    )
    enriched3 = resolver.resolve(intent3)

    original_deadline = intent3.constraints.get("deadline")
    resolved_deadline = enriched3.resolved_constraints.get("deadline")
    print(f"Original deadline:     {original_deadline}")
    print(f"Resolved deadline:     {resolved_deadline}")
    print(f"  (set to business hours end: {household_ctx.business_hours_end})")
    print(f"New ambiguities:       {enriched3.new_ambiguities or 'None'}")
    print()

    # Parse and enrich Intent 4: Member mention with validation
    print("=" * 70)
    print("EXAMPLE 4: Task for Known Member (Alice)")
    print("=" * 70)
    intent4 = parser.parse(
        "Create a task for Alice",
        household_id="h001",
        user_id="user_bob",  # different user
    )
    enriched4 = resolver.resolve(intent4)

    print(f"Mentioned recipients:  {intent4.entities.get('recipients', [])}")
    print(f"Known members:         {household_ctx.known_members}")
    print(f"New ambiguities:       {enriched4.new_ambiguities or 'None'}")
    print()

    # Parse and enrich Intent 5: Unknown member
    print("=" * 70)
    print("EXAMPLE 5: Task for Unknown Member (Diana) - Marked Ambiguous")
    print("=" * 70)
    intent5 = parser.parse(
        "Create a task for Diana",
        household_id="h001",
        user_id="user_bob",
    )
    enriched5 = resolver.resolve(intent5)

    print(f"Mentioned recipients:  {intent5.entities.get('recipients', [])}")
    print(f"Known members:         {household_ctx.known_members}")
    print(f"New ambiguities:       {enriched5.new_ambiguities}")
    print()

    # Parse and enrich Intent 6: Missing household context
    print("=" * 70)
    print("EXAMPLE 6: Unknown Household (h999) - Critical Ambiguity")
    print("=" * 70)
    intent6 = parser.parse(
        "Create a task",
        household_id="h999",  # unknown household
        user_id="user_alice",
    )
    enriched6 = resolver.resolve(intent6)

    print(f"Household found:       {enriched6.household_context is not None}")
    print(f"New ambiguities:       {enriched6.new_ambiguities}")


if __name__ == "__main__":
    main()
