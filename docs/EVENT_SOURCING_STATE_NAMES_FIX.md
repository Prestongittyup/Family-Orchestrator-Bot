# State Name Mismatch Fix - Event Sourcing Migration

## Problem
Test `test_daily_cycle` was failing with `assert 0 == 1` - no follow-ups were being queued when actions were executed.

## Root Cause
**Architectural mismatch between FSM terminology and event sourcing terminology:**

- **FSM Layer** (action_pipeline.py): Uses `"executed"` and `"ignored"` as terminal states
- **Event Sourcing Layer** (state_reducer.py & domain_event.py): Uses `"committed"` and `"failed"` as terminal states

The issue manifested in `queue_next_day_follow_ups()` method:
```python
# WRONG - checking against FSM state name
if derived_state != "executed" or action.reviewed_in_evening:
    continue
```

The method was calling `_get_derived_state()` which replays events using `reduce_state()`, returning event sourcing states. But then it was comparing against FSM state name `"executed"`, which would always be `False` because the actual state was `"committed"`.

## Solution
Changed line 409 in action_pipeline.py:
```python
# CORRECT - checking against event sourcing state name
if derived_state != "committed" or action.reviewed_in_evening:
    continue
```

## Mapping Reference

| System | Meaning | Event Type |
|--------|---------|------------|
| FSM: `proposed` | Action proposed | ACTION_PROPOSED |
| FSM: `pending_approval` | Waiting for approval | (internal, no event) |
| FSM: `approved` | Approved for execution | ACTION_APPROVED |  
| **FSM:** **`executed`** | **Execution completed** | **ACTION_COMMITTED** |
| **Event: `committed`** | **Same meaning** | **ACTION_COMMITTED** |
| FSM: `rejected` | Action rejected | ACTION_REJECTED |
| Event: `rejected` | Same meaning | ACTION_REJECTED |
| FSM: `ignored` | Action ignored/failed | ACTION_FAILED |
| Event: `failed` | Same meaning | ACTION_FAILED |

## Why This Mismatch Exists
During migration from FSM to event sourcing, the FSM still owns the action object's current_state field. But the event store returns "committed" (the actual domain event name) rather than "executed" (the FSM field name).

## Migration Phase Implications
During this migration phase:
- **FSM state names persist**: Still used in action.current_state field for backward compatibility
- **Event sourcing state names emerge**: New names appear when replaying events
- **Both coexist**: Code must handle both for transition periods

## Lessons Learned
1. When migrating to event sourcing, watch for **semantic mismatches** between old and new terminology
2. **Event types** (ACTION_COMMITTED) and **state names** (committed vs executed) may diverge
3. Code comments should clearly indicate which state names are expected from each source
4. Tests will reveal these mismatches quickly - the mismatch manifested as empty result sets rather than errors

## Tests Affected
- **Before fix**: test_daily_cycle failed (0 follow-ups instead of 1)
- **After fix**: test_daily_cycle passes (1 follow-up correctly queued)
- **Full test suite**: 91 tests passing (5 runtime + 37 event sourcing + 49 state machine)

## Files Modified
- `household_os/runtime/action_pipeline.py` line 409: Changed state check from "executed" to "committed"
