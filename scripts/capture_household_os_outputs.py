#!/usr/bin/env python
"""
Capture and print deterministic outputs for three sample household OS queries.

These outputs demonstrate cross-domain reasoning:
1. "I'm overwhelmed this week" → appointment-domain action with conflict reasoning
2. "What should I cook tonight?" → meal-domain action with inventory reasoning
3. "I need to start working out" → fitness-domain action with scheduling reasoning
"""

import sys
import json
from pathlib import Path

# Add parent to path to allow imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient
from apps.api.main import app

client = TestClient(app)

PROMPTS = [
    {
        "id": "overwhelmed",
        "prompt": "I'm overwhelmed this week",
        "household_id": "household-demo-overwhelmed",
    },
    {
        "id": "dinner",
        "prompt": "What should I cook tonight?",
        "household_id": "household-demo-dinner",
    },
    {
        "id": "fitness",
        "prompt": "I need to start working out",
        "household_id": "household-demo-fitness",
    },
]

def main():
    print("=" * 80)
    print("HOUSEHOLD OPERATING SYSTEM - DETERMINISTIC OUTPUT CAPTURE")
    print("=" * 80)
    print()

    for test_case in PROMPTS:
        print(f"\n{'-' * 80}")
        print(f"Query {test_case['id'].upper()}: {test_case['prompt']}")
        print(f"{'-' * 80}")
        
        response = client.post(
            "/assistant/run",
            json={
                "query": test_case["prompt"],
                "household_id": test_case["household_id"],
            }
        )
        
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            print(response.text)
            continue
        
        payload = response.json()
        
        # Print intent interpretation
        intent = payload.get("intent_interpretation", {})
        print(f"\n[Intent]")
        print(f"  Summary: {intent.get('summary', 'N/A')}")
        print(f"  Urgency: {intent.get('urgency', 'N/A')}")
        if intent.get("extracted_signals"):
            print(f"  Signals: {', '.join(intent['extracted_signals'])}")
        
        # Print current state
        state = payload.get("current_state_summary", {})
        print(f"\n[Household State]")
        print(f"  Calendar Events: {state.get('calendar_events', 0)}")
        print(f"  Open Tasks: {state.get('open_tasks', 0)}")
        print(f"  Meals Recorded: {state.get('meals_recorded', 0)}")
        print(f"  Fitness Routines: {state.get('fitness_routines', 0)}")
        if state.get("low_grocery_items"):
            print(f"  Low Inventory: {', '.join(state['low_grocery_items'])}")
        print(f"  Constraints: {state.get('constraints_count', 0)}")
        print(f"  Pending Approvals: {state.get('pending_approvals', 0)}")
        
        # Print recommended action
        action = payload.get("recommended_action", {})
        print(f"\n[Recommended Action]")
        print(f"  Title: {action.get('title', 'N/A')}")
        print(f"  Description: {action.get('description', 'N/A')}")
        print(f"  Urgency: {action.get('urgency', 'N/A')}")
        if action.get("scheduled_for"):
            print(f"  Scheduled For: {action.get('scheduled_for')}")
        print(f"  Approval Required: {action.get('approval_required', False)}")
        print(f"  Approval Status: {action.get('approval_status', 'pending')}")
        
        # Print grouped approval
        approval = payload.get("grouped_approval_payload", {})
        print(f"\n[Approval Payload]")
        print(f"  Label: {approval.get('label', 'N/A')}")
        print(f"  Execution Mode: {approval.get('execution_mode', 'N/A')}")
        print(f"  Approval Status: {approval.get('approval_status', 'pending')}")
        print(f"  Action IDs: {', '.join(approval.get('action_ids', []))}")
        
        # Print follow-ups
        follow_ups = payload.get("follow_ups", [])
        if follow_ups:
            print(f"\n[Follow-Ups (max 3)]")
            for fu in follow_ups:
                print(f"  - {fu}")
        
        # Print reasoning trace
        trace = payload.get("reasoning_trace", [])
        if trace:
            print(f"\n[Reasoning Trace]")
            for i, step in enumerate(trace, 1):
                print(f"  {i}. {step}")
        
        # Print full JSON for reference
        print(f"\n[Full Response JSON]")
        print(json.dumps(payload, indent=2))

    print(f"\n{'=' * 80}")
    print("DETERMINISTIC OUTPUT CAPTURE COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
