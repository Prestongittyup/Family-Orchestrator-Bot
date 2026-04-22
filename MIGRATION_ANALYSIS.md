# FSM READ PATH MIGRATION ANALYSIS

## Phase 1: FSM Read Paths Found

### 1. **action_pipeline.py** - PRIMARY TARGET (8 occurrences)

#### Location 1: approve_actions() - Line 156
```python
if action.request_id != request_id or action.current_state not in {"proposed", "pending_approval"}:
    continue
```
**Type**: State validation read
**Current Pattern**: Direct field read
**Migration Target**: Use event-derived state
**Impact**: CRITICAL - approvals depend on correct state

#### Location 2: reject_actions() - Line 198
```python
if action.request_id != request_id or action.current_state not in {"proposed", "pending_approval"}:
    continue
```
**Type**: State validation read (same as above)
**Current Pattern**: Direct field read
**Migration Target**: Use event-derived state
**Impact**: CRITICAL - rejections depend on correct state

#### Location 3: execute_approved_actions() - Line 246
```python
if action.current_state != "pending_approval":
    FIREWALL.log_unexpected_state(action, "execute_approved_actions")
```
**Type**: State guard read
**Current Pattern**: Direct field read
**Migration Target**: Use event-derived state
**Impact**: HIGH - guards execution eligibility

#### Location 4: close_evening_review() - Line 288
```python
if action.current_state != "approved":
    FIREWALL.log_unexpected_state(action, "close_evening_review")
```
**Type**: State guard read
**Current Pattern**: Direct field read
**Migration Target**: Use event-derived state
**Impact**: HIGH - guards evening review logic

#### Location 5: collect_completed_actions() - Line 328
```python
if action.current_state != "executed" or action.reviewed_in_evening:
```
**Type**: State validation for completed actions
**Current Pattern**: Direct field read
**Migration Target**: Use event-derived state  
**Impact**: MEDIUM - post-execution logic

#### Location 6-8: _append_transition() - Lines 449, 455, 466, 478
```python
# Line 449: from_state = action.current_state if action.transitions else None
# Line 455: if not action.transitions and action.current_state == to_state:
# Line 466: state=self._to_machine_state(action.current_state),
# Line 478: f"Illegal transition for {action.action_id}: {action.current_state} -> {to_state}"
```
**Type**: State read for transition building
**Current Pattern**: Direct field read -> FSM conversion
**Migration Target**: Derive from event history
**Impact**: CRITICAL - transition validation depends on correct prior state

### 2. **Tests - Local FSM State Tests** (NOT CRITICAL FOR MIGRATION)

#### test_state_machine.py - Lines 136, 146, 156, 192, 198, 263, 309, 315, 321, 328, 375
- **Purpose**: Unit tests for StateMachine class
- **Can remain as-is**: These test FSM transition logic which is still valid
- **Will not be changed**: FSM continues to validate transitions

#### test_household_os_runtime.py - 5 occurrences
- **Type**: Integration tests checking action state
- **Migration**: Update to check event-derived state instead

### 3. **state_machine_integration_guide.py** - AUXILIARY (4 writes, not reads)
Lines 103, 161, 187, 254: `raw["current_state"] = fsm.state.value`
- **Type**: FSM-backed persistence (SHOULD BE REMOVED)
- **Action**: Delete these lines - don't write FSM state to persistence

## Migration Implementation Plan

### Step 1: Add Event Store Dependency to ActionPipeline
```python
class ActionPipeline:
    def __init__(self, event_store: EventStore = None):
        self.event_store = event_store or get_migration_layer().event_store
```

### Step 2: Add Helper Method for Event-Sourced State Reads
```python
def _get_derived_state(self, action_id: str) -> LifecycleEventState:
    """Get current state from event replay (SINGLE SOURCE OF TRUTH)."""
    try:
        events = self.event_store.get_events(action_id)
        return reduce_state(events)
    except AggregateNotFoundError:
        return None  # Action not yet in event store
```

### Step 3: Create Events During State Transitions
```python
# In _append_transition(), also:
event = DomainEvent.create(
    aggregate_id=action.action_id,
    event_type=LIFECYCLE_EVENT_TYPES[...]
)
self.event_store.append(event)
```

### Step 4: Replace State Reads
Replace all `action.current_state` reads with `self._get_derived_state(action.action_id)`

### Step 5: Update Tests
Modify test assertions to use event-derived state

## Success Criteria
- [ ] All action_pipeline.py state reads use `_get_derived_state()`
- [ ] Events are created for every state transition
- [ ] No FSM state field reads in business logic paths
- [ ] Tests pass with event-sourced state
- [ ] Event store is single source of truth for actions
- [ ] Dual writes to FSM are removed

## Remaining FSM Roles (TEMPORARY)
1. **Transition Validation**: FSM will remain as transition validator
2. **Retry Policy**: Continue using FSM retry logic
3. **Timeout Rules**: Continue using FSM timeout definitions

These can be kept until fully removed in a future cleanup phase.
