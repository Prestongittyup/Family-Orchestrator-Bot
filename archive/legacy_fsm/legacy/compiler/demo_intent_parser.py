"""
Quick demonstration of IntentParser functionality.

Run with:
    cd /path/to/bot && python -m compiler.demo_intent_parser
"""

from legacy.compiler.intent_parser import IntentParser


def main():
    parser = IntentParser()

    # Example 1: Task creation
    intent1 = parser.parse(
        "Create a weekly grocery shopping task for tomorrow, deadline by 5pm",
        household_id="h001",
        user_id="user_alice",
        context_snapshot={
            "family_members": ["Alice", "Bob", "Charlie"],
            "current_budget": 500.0,
        }
    )

    print("=" * 70)
    print("INTENT 1: Task Creation")
    print("=" * 70)
    print(f"Type:           {intent1.intent_type}")
    print(f"Priority:       {intent1.priority_level}")
    print(f"Is Recurring:   {intent1.recurrence_hints['is_recurring']}")
    print(f"Frequency:      {intent1.recurrence_hints['frequency']}")
    print(f"Ambiguities:    {intent1.ambiguity_flags if intent1.ambiguity_flags else 'None'}")
    print(f"Entities:       {intent1.entities}")
    print()

    # Example 2: Budget query
    intent2 = parser.parse(
        "Check the household budget for March",
        household_id="h001",
        user_id="user_bob",
    )

    print("=" * 70)
    print("INTENT 2: Budget Query")
    print("=" * 70)
    print(f"Type:           {intent2.intent_type}")
    print(f"Priority:       {intent2.priority_level}")
    print(f"Subject:        {intent2.entities['subject']}")
    print()

    # Example 3: Health checkin with urgency
    intent3 = parser.parse(
        "Remind me daily at 7am to exercise for 30 minutes, it's urgent",
        household_id="h001",
        user_id="user_charlie",
    )

    print("=" * 70)
    print("INTENT 3: Health Checkin (Recurring, Urgent)")
    print("=" * 70)
    print(f"Type:           {intent3.intent_type}")
    print(f"Priority:       {intent3.priority_level}")
    print(f"Is Recurring:   {intent3.recurrence_hints['is_recurring']}")
    print(f"Frequency:      {intent3.recurrence_hints['frequency']}")
    print(f"Interval:       {intent3.recurrence_hints['interval']}")
    print(f"Duration:       {intent3.entities['values'].get('duration_amount')} minutes")
    print()


if __name__ == "__main__":
    main()
