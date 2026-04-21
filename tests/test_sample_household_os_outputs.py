"""
Test to generate deterministic outputs from Household OS for sample prompts.
Demonstrates cross-domain reasoning, single-action guarantee, and reasoning trace.
"""

import json
from fastapi.testclient import TestClient

from apps.api.main import app


def test_sample_household_os_outputs():
    """Demonstrate Household OS outputs for three sample prompts."""
    
    client = TestClient(app)
    
    # Three prompts demonstrating cross-domain reasoning
    samples = [
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
    
    print("\n" + "=" * 100)
    print("HOUSEHOLD OS DETERMINISTIC OUTPUT SAMPLES")
    print("Demonstrating cross-domain reasoning with single-action guarantee")
    print("=" * 100)
    
    for prompt_info in samples:
        print("\n" + "-" * 100)
        print(f"PROMPT: {prompt_info['query']}")
        print(f"HOUSEHOLD: {prompt_info['household_id']}")
        print(f"CONTEXT: {prompt_info['description']}")
        print("-" * 100)
        
        # Submit query via /assistant/run endpoint
        response = client.post("/assistant/run", json={
            "query": prompt_info["query"],
            "household_id": prompt_info["household_id"],
        })
        
        assert response.status_code == 200, f"Failed with status {response.status_code}: {response.text}"
        response_json = response.json()
        
        # Intent Interpretation
        intent = response_json.get("intent_interpretation", {})
        print(f"\nINTENT INTERPRETATION:")
        print(f"  Summary: {intent.get('summary')}")
        print(f"  Urgency: {intent.get('urgency')}")
        print(f"  Signals: {', '.join(intent.get('extracted_signals', [])[:3]) or 'None'}")
        
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
        print(f"\n[PRIMARY ACTION - EXACTLY ONE]:")
        print(f"  Action ID: {action.get('action_id')}")
        print(f"  Title: {action.get('title')}")
        print(f"  Description: {action.get('description')}")
        print(f"  Urgency: {action.get('urgency')}")
        print(f"  Domain: {action.get('title', '').split()[0]}")  # Infer from title
        if action.get('scheduled_for'):
            print(f"  Scheduled For: {action.get('scheduled_for')}")
        
        # Follow-ups (max 3)
        follow_ups = response_json.get("follow_ups", [])
        if follow_ups:
            print(f"\nOPTIONAL FOLLOW-UPS ({len(follow_ups)}/max 3):")
            for i, followup in enumerate(follow_ups, 1):
                print(f"  {i}. {followup}")
        else:
            print(f"\nOPTIONAL FOLLOW-UPS: None")
        
        # Reasoning Trace
        reasoning = response_json.get("reasoning_trace", [])
        if reasoning:
            print(f"\nREASONING TRACE:")
            for i, trace in enumerate(reasoning[:3], 1):
                print(f"  {i}. {trace}")
        
        # Contract Validation
        print(f"\n[CONTRACT VALIDATION]:")
        expected_keys = {
            "request_id", "intent_interpretation", "current_state_summary",
            "recommended_action", "follow_ups", "grouped_approval_payload",
            "reasoning_trace"
        }
        actual_keys = set(response_json.keys())
        validation_pass = expected_keys == actual_keys
        print(f"  Keys match contract: {'PASS' if validation_pass else 'FAIL'}")
        
        # Single action guarantee
        print(f"\n[SINGLE ACTION GUARANTEE]:")
        is_dict = isinstance(action, dict)
        is_not_list = not isinstance(action, list)
        single_action_pass = is_dict and is_not_list
        print(f"  Recommended action is dict (not list): {'PASS' if single_action_pass else 'FAIL'}")
        
        # No module leakage
        response_str = json.dumps(response_json)
        forbidden_keywords = ["proposals", "candidate_schedules", "fallback_options", "planning_engine"]
        leakage_found = [kw for kw in forbidden_keywords if kw in response_str.lower()]
        leakage_pass = len(leakage_found) == 0
        print(f"\n[MODULE LEAKAGE CHECK]:")
        print(f"  No forbidden keywords: {'PASS' if leakage_pass else f'FAIL - Found: {leakage_found}'}")
        
        # Assertions
        assert validation_pass, f"Contract keys mismatch. Expected {expected_keys}, got {actual_keys}"
        assert single_action_pass, "Single action guarantee failed"
        assert leakage_pass, f"Module leakage detected: {leakage_found}"
    
    print("\n" + "=" * 100)
    print("ALL SAMPLE OUTPUTS VALIDATED SUCCESSFULLY")
    print("=" * 100 + "\n")
