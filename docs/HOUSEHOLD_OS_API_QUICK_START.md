# Household OS API Quick Start Guide

## Overview

The Household OS provides a unified interface for household orchestration. Submit a natural-language query and receive a single recommended action with reasoning.

## Endpoints

### 1. Submit Query / Run Decision

**Endpoint**: `POST /assistant/query` or `POST /assistant/run`  
**Response**: `HouseholdOSRunResponse`

```bash
curl -X POST http://localhost:8000/assistant/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "I'\''m overwhelmed this week",
    "household_id": "household-001"
  }'
```

**Request Schema**:
```json
{
  "query": "Natural language query (required)",
  "household_id": "Household identifier (optional, defaults to 'household-001')",
  "repeat_window_days": 10,
  "fitness_goal": null
}
```

**Response Schema**:
```json
{
  "request_id": "assist-e9053c9b35f2-primary",
  "intent_interpretation": {
    "summary": "general query with low priority and 1 signal(s)",
    "urgency": "low",
    "extracted_signals": ["household"]
  },
  "current_state_summary": {
    "household_id": "household-001",
    "reference_time": "2026-04-20T17:53:00Z",
    "calendar_events": 5,
    "open_tasks": 0,
    "meals_recorded": 3,
    "low_grocery_items": [],
    "fitness_routines": 0,
    "constraints_count": 2,
    "pending_approvals": 1,
    "state_version": 42
  },
  "recommended_action": {
    "action_id": "assist-e9053c9b35f2-primary",
    "title": "Schedule appointment for 2026-04-20 06:00-06:45",
    "description": "Reserve 2026-04-20 06:00-06:45 for the requested appointment because it avoids known calendar conflicts.",
    "urgency": "high",
    "scheduled_for": "2026-04-20 06:00-06:45",
    "approval_required": true,
    "approval_status": "pending"
  },
  "follow_ups": [
    "Would you like me to block off evening time?",
    "I can reschedule lower-priority tasks"
  ],
  "grouped_approval_payload": {
    "group_id": "assist-e9053c9b35f2-group",
    "label": "Batch Household Action Execution",
    "action_ids": ["assist-e9053c9b35f2-primary"],
    "execution_mode": "inert_until_approved",
    "approval_status": "pending"
  },
  "reasoning_trace": [
    "Calendar analysis shows 5 near-term commitments.",
    "2026-04-20 06:00-06:45 is the next low-conflict window.",
    "Scheduling protects meal and family time."
  ]
}
```

### 2. Retrieve Previous Response

**Endpoint**: `GET /assistant/suggestions/{request_id}`  
**Response**: `HouseholdOSRunResponse`

```bash
curl http://localhost:8000/assistant/suggestions/assist-e9053c9b35f2-primary
```

**Query Parameters**:
- `request_id` (required): The response ID from a previous `/query` call
- `household_id` (optional): Needed only if request_id is ambiguous

### 3. Record Approval

**Endpoint**: `POST /assistant/approve`  
**Response**: `HouseholdOSRunResponse` (updated with approval_status)

```bash
curl -X POST http://localhost:8000/assistant/approve \
  -H "Content-Type: application/json" \
  -d '{
    "request_id": "assist-e9053c9b35f2-primary",
    "action_ids": ["assist-e9053c9b35f2-primary"]
  }'
```

**Request Schema**:
```json
{
  "request_id": "Request ID from /query response (required)",
  "action_ids": ["List of action IDs to approve (required)"]
}
```

**Response**: Same `HouseholdOSRunResponse` with `approval_status` updated to "approved"

---

## Query Examples

### Example 1: Calendar/Appointment Query

```bash
curl -X POST http://localhost:8000/assistant/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Can you find me a time to meet with Jenny?",
    "household_id": "household-001"
  }'
```

**Expected Response**:
- Domain: appointment
- Action: "Schedule meeting with Jenny for [available time]"
- Urgency: high (if "meeting" keyword detected)

### Example 2: Meal Planning

```bash
curl -X POST http://localhost:8000/assistant/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What should we cook tonight?",
    "household_id": "household-001"
  }'
```

**Expected Response**:
- Domain: meal
- Action: "Cook [recipe name] for [time]"
- Includes grocery gaps (items to acquire)
- Follow-ups: alternative recipes, grocery shopping tips

### Example 3: Fitness Goal

```bash
curl -X POST http://localhost:8000/assistant/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "I want to start working out more consistently",
    "household_id": "household-001",
    "fitness_goal": "3x per week cardio"
  }'
```

**Expected Response**:
- Domain: fitness (or appointment if calendar conflicts)
- Action: "Schedule workout at [time]"
- Follow-ups: gym location suggestions, class recommendations

### Example 4: Multi-Domain (Overwhelmed)

```bash
curl -X POST http://localhost:8000/assistant/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "I'\''m completely overwhelmed with everything this week",
    "household_id": "household-001"
  }'
```

**Expected Response**:
- Decision engine considers all domains
- Selects single highest-urgency action (usually appointment or task delegation)
- Reasoning explains why this action over alternatives

---

## Response Contract Guarantees

### ✓ Always True

1. **Exactly One Action**: `recommended_action` is always a single object (never list, never null)
2. **Valid Urgency**: `urgency` is one of: "low", "medium", "high"
3. **Max 3 Follow-ups**: `follow_ups` list has at most 3 items
4. **Approval Status**: Either "pending" or "approved"
5. **Scheduling**: If action has `scheduled_for`, it's ISO 8601 datetime string
6. **No Module Leakage**: Response never contains "proposals", "candidate_schedules", "fallback_options"

### Request Format

All `/query` and `/run` requests accept:
```python
class AssistantQueryRequest(BaseModel):
    query: str  # Natural language query
    household_id: str = "household-001"  # Optional
    repeat_window_days: int = 10  # Optional
    fitness_goal: Optional[str] = None  # Optional
```

---

## Error Handling

### 400 Bad Request
Missing required field `query`

Response:
```json
{
  "detail": [
    {
      "type": "missing",
      "loc": ["body", "query"],
      "msg": "Field required"
    }
  ]
}
```

### 404 Not Found
Request ID not found in suggestions endpoint

Response:
```json
{
  "detail": "Suggested response not found"
}
```

### 500 Server Error
Unexpected error in decision engine or state management

Response includes error details (development) or generic message (production)

---

## Sample Integration (Python)

```python
import requests
import json

BASE_URL = "http://localhost:8000"

def get_household_recommendation(query: str, household_id: str = "household-001"):
    """Skip to the recommendation without manual request construction."""
    response = requests.post(
        f"{BASE_URL}/assistant/query",
        json={
            "query": query,
            "household_id": household_id,
        }
    )
    response.raise_for_status()
    return response.json()

def approve_action(request_id: str, action_ids: list[str]):
    """Record action approval."""
    response = requests.post(
        f"{BASE_URL}/assistant/approve",
        json={
            "request_id": request_id,
            "action_ids": action_ids,
        }
    )
    response.raise_for_status()
    return response.json()

# Usage
if __name__ == "__main__":
    # Get a recommendation
    result = get_household_recommendation("I'm overwhelmed this week")
    
    print(f"Request ID: {result['request_id']}")
    print(f"Recommended Action: {result['recommended_action']['title']}")
    print(f"Urgency: {result['recommended_action']['urgency']}")
    print(f"Reasoning:")
    for trace in result['reasoning_trace']:
        print(f"  - {trace}")
    
    # Approve the action
    approval = approve_action(
        result['request_id'],
        [result['recommended_action']['action_id']]
    )
    print(f"\nApproval Status: {approval['recommended_action']['approval_status']}")
```

---

## Sample Integration (JavaScript)

```javascript
const BASE_URL = "http://localhost:8000";

async function getHouseholdRecommendation(query, householdId = "household-001") {
  const response = await fetch(`${BASE_URL}/assistant/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, household_id: householdId }),
  });
  return response.json();
}

async function approveAction(requestId, actionIds) {
  const response = await fetch(`${BASE_URL}/assistant/approve`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ request_id: requestId, action_ids: actionIds }),
  });
  return response.json();
}

// Usage
(async () => {
  const result = await getHouseholdRecommendation("What should I cook tonight?");
  
  console.log(`Request ID: ${result.request_id}`);
  console.log(`Recommended: ${result.recommended_action.title}`);
  console.log(`Urgency: ${result.recommended_action.urgency}`);
  
  // Approve
  const approval = await approveAction(
    result.request_id,
    [result.recommended_action.action_id]
  );
  console.log(`Approved: ${approval.recommended_action.approval_status}`);
})();
```

---

## Performance Tips

### 1. Cache Responses
Store recent responses keyed by request_id to avoid duplicate queries

### 2. Batch Approvals
Use `grouped_approval_payload` to approve multiple actions in one request

### 3. Monitor Reasoning
Use `reasoning_trace` to understand decision quality and tune ranking algorithm

### 4. Follow-ups as Refinement
When recommending follow-ups to user, track which ones they select to learn preferences

---

## Testing

```bash
# Run all household OS tests
pytest tests/test_household_os.py -v

# Run sample output demonstration
pytest tests/test_sample_household_os_outputs.py -v -s

# Run full regression suite
pytest tests/ -v
```

---

## Troubleshooting

### Issue: Consistently getting appointment actions

**Cause**: Intent parser default favoring appointments  
**Fix**: Use explicit domain hints in query ("cook", "meal", "workout")

### Issue: Cross-domain conflicts not detected

**Cause**: State graph missing calendar or fitness data  
**Fix**: Verify household_id has fresh calendar sync

### Issue: Follow-ups are empty

**Cause**: Decision engine not generating suggestions  
**Fix**: Check reasoning_trace for conflicts that prevent follow-ups

---

## Further Reading

- [Household OS Architecture Doc](./HOUSEHOLD_OS_REFACTORING_COMPLETE.md)
- [Response Contract Spec](./HOUSEHOLD_OS_REFACTORING_COMPLETE.md#appendix-a-contract-specification)
- [Test Suite](../tests/test_household_os.py)
- [Sample Outputs](../tests/test_sample_household_os_outputs.py)
