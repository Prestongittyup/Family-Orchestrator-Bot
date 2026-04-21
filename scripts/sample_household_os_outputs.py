"""
Generate deterministic outputs from Household OS for sample prompts.
Demonstrates cross-domain reasoning, single-action guarantee, and reasoning trace.
"""

import json
from fastapi.testclient import TestClient

from apps.api.main import app

client = TestClient(app)

# Three prompts demonstrating cross-domain reasoning
SAMPLE_PROMPTS = [
    {
        "query": "I'm overwhelmed this week",
        "household_id": "household-sample-overwhelmed",
        "description": "Multi-domain query expecting calendar/appointment action",
    },
    {
        "query": "What should I cook tonight?",
        "household_id": "household-sample-meal",
        "description": "Meal domain query expecting meal planning action",
    },
    {
        "query": "I need to start working out",
        "household_id": "household-sample-fitness",
        "description": "Fitness domain query expecting fitness action",
    },
]


def print_response_summary(response_json: dict, prompt_info: dict) -> None:
    """Pretty-print a sample response."""
    print("\n" + "=" * 80)
    print(f"PROMPT: {prompt_info['query']}")
    print(f"HOUSEHOLD: {prompt_info['household_id']}")
    print(f"CONTEXT: {prompt_info['description']}")
    print("=" * 80)
    
    # Intent Interpretation
    intent = response_json.get("intent_interpretation", {})
    print(f"\nINTENT INTERPRETATION:")
    print(f"  Summary: {intent.get('summary')}")
    print(f"  Urgency: {intent.get('urgency')}")
    print(f"  Signals: {', '.join(intent.get('extracted_signals', []))}")
    
    # Current State Summary
    state = response_json.get("current_state_summary", {})
    print(f"\nCURRENT STATE:")
    print(f"  Calendar Events: {state.get('calendar_events')}")
    print(f"  Open Tasks: {state.get('open_tasks')}")
    print(f"  Meals Recorded: {state.get('meals_recorded')}")
    print(f"  Low Grocery Items: {', '.join(state.get('low_grocery_items', [])[:3]) or 'None'}")
    print(f"  Fitness Routines: {state.get('fitness_routines')}")
    print(f"  Pending Approvals: {state.get('pending_approvals')}")
    
    # Recommended Action (THE CRITICAL SINGLE ACTION)
    action = response_json.get("recommended_action", {})
    print(f"\nRECOMMENDED ACTION (PRIMARY):")
    print(f"  Action ID: {action.get('action_id')}")
    print(f"  Title: {action.get('title')}")
    print(f"  Description: {action.get('description')}")
    print(f"  Urgency: {action.get('urgency')}")
    print(f"  Approval Status: {action.get('approval_status')}")
    if action.get('scheduled_for'):
        print(f"  Scheduled For: {action.get('scheduled_for')}")
    
    # Follow-ups (max 3)
    follow_ups = response_json.get("follow_ups", [])
    if follow_ups:
        print(f"\nOPTIONAL FOLLOW-UPS ({len(follow_ups)} of max 3):")
        for i, followup in enumerate(follow_ups, 1):
            print(f"  {i}. {followup}")
    
    # Reasoning Trace
    reasoning = response_json.get("reasoning_trace", [])
    if reasoning:
        print(f"\nREASONING TRACE:")
        for i, trace in enumerate(reasoning[:5], 1):  # Show first 5 trace items
            print(f"  {i}. {trace}")
    
    # Contract Validation
    print(f"\nCONTRACT VALIDATION:")
    expected_keys = {
        "request_id", "intent_interpretation", "current_state_summary",
        "recommended_action", "follow_ups", "grouped_approval_payload",
        "reasoning_trace"
    }
    actual_keys = set(response_json.keys())
    print(f"  Expected keys: {expected_keys}")
    print(f"  Actual keys: {actual_keys}")
    print(f"  Match: {expected_keys == actual_keys} ✓" if expected_keys == actual_keys else f"  Match: False ✗")
    
    # Single action guarantee
    print(f"\nSINGLE ACTION GUARANTEE:")
    print(f"  Recommended action is dict: {isinstance(action, dict)} ✓" if isinstance(action, dict) else f"  Recommended action is dict: False ✗")
    print(f"  Recommended action is NOT list: {not isinstance(action, list)} ✓" if not isinstance(action, list) else f"  Recommended action is NOT list: False ✗")
    
    # No module leakage
    response_str = json.dumps(response_json)
    forbidden_keywords = ["proposals", "candidate_schedules", "fallback_options", "planning_engine"]
    leakage_found = [kw for kw in forbidden_keywords if kw in response_str.lower()]
    print(f"\nMODULE LEAKAGE CHECK:")
    print(f"  Forbidden keywords found: {leakage_found if leakage_found else 'None ✓'}")


def main():
    print("\n" + "=" * 80)
    print("HOUSEHOLD OS DETERMINISTIC OUTPUT SAMPLES")
    print("Demonstrating cross-domain reasoning with single-action guarantee")
    print("=" * 80)
    
    for prompt_info in SAMPLE_PROMPTS:
        try:
            # Submit query via /assistant/run endpoint
            response = client.post("/assistant/run", json={
                "query": prompt_info["query"],
                "household_id": prompt_info["household_id"],
            })
            
            if response.status_code == 200:
                response_json = response.json()
                print_response_summary(response_json, prompt_info)
            else:
                print(f"\n✗ ERROR: Status {response.status_code}")
                print(f"  Response: {response.text[:200]}")
        
        except Exception as exc:
            print(f"\n✗ EXCEPTION: {exc}")
    
    print("\n" + "=" * 80)
    print("END OF SAMPLE OUTPUTS")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
