# EVENT SOURCING MIGRATION - CRITICAL INSIGHT

## The Problem
The action lifecycle has TWO layers of state:
1. **Pydantic Model State** (`action.current_state`, `action.transitions[]`)
   - Stores all state transitions including "proposed" -> "pending_approval"
   - Updated immediately when actions are created/transitioned

2. **Event Store State** (from replaying events)
   - Only stores "published" state changes (proposed->approved->executed, etc.)
   - Lower-level event stream

These are currently MISMATCHED:
- When action is created with `approval_required=True`:
  - Pydantic transitions: ["proposed", "pending_approval"]
  - Events created: [ACTION_PROPOSED]
  - Derived state from events: "proposed"
  - But action object says: "pending_approval"

When approving:
- We read derived state: "proposed"
- FSM guard sees: requires_approval=True, from=proposed, to=approved
- FSM rejects: "must transition through pending_approval"
- BUG: We never went through pending_approval in event layer!

## The Solution
**Only create events for FINAL state transitions, not intermediate ones.**

Approach:
1. DO NOT create events during register_proposed_action
2. Only create events when action actually transitions to approved/rejected/executed/ignored
3. This means:
   - ACTION_PROPOSED is created when action is first APPROVED (or becomes pending if approval required)
   - OR: track approval status separately (not as a state, but as a flag)

## Alternative Approach (Simpler)
Keep the current model:
- `action.current_state` is the SOURCE OF TRUTH during execution
- Events are created ONLY for external transitions (approved, rejected, executed)
- `action.transitions` list captures all state changes for audit
- When reading state in migrations, prefer `action.current_state` during migration phase
- Event sourcing validation happens AFTER transition, not before

This keeps event_store as eventual-consistency append log, not immediate truth.

## Implementation
For Phase 1, let's keep the action.current_state as primary read source during the migration.
The event_store should only track actual phase transitions, not internal state changes.
