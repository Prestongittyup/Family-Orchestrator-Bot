# Event System Closure: Implementation Roadmap

**Status:** Ready for Execution  
**Created:** April 23, 2026  
**Total Duration:** 9 weeks  
**Risk Level:** LOW (incremental, test-driven)  

---

## Quick Reference

**Primary Deliverables:**
- [x] System Closure Model — defined in SYSTEM_CLOSURE_SPECIFICATION.md
- [x] Gap Closure Action Plan — documented with fix strategies and risks
- [x] Event Contract Specification — schemas/event_contracts.yaml
- [x] CI Enforcement Design — 6 test gates across static and runtime
- [x] Implementation Sequence — 5 phases, 18 PRs total

**Not Included (Out of Scope):**
- Architecture redesign
- Event system replacement
- Big-bang rewrites
- New frameworks or libraries

---

## Phase 1: Eliminate Silent Mutations (Weeks 1-2)

### Overview
Enforce that every `session.commit()` or state mutation has a corresponding event emission or explicit `@internal_only` marker.

### PR 1.1: Add Static Gate — Silent Mutation Detection

**Title:** `test(ci): add pre-commit gate for silent mutations`

**Files to Create:**
- `tests/test_static_silent_mutations.py`

**Files to Modify:**
- `.github/workflows/ci.yml` (add test job)

**Implementation Sketch:**
```python
# tests/test_static_silent_mutations.py
class SilentMutationScanner(ast.NodeVisitor):
    """Detects session.commit() without router.emit() in same scope."""
    # Scan for commits
    # Scan for emits
    # Check: every commit has emit or @internal_only marker
```

**Validation:**
- Runs on pull request
- Failure: silent mutation detected in apps/api/services/ or apps/api/identity/

**Risk:** LOW — gate added but not enforced yet (warning mode)

---

### PR 1.2: Decorate Internal-Only Writes

**Title:** `refactor(identity): mark internal-only repository writes with @internal_only`

**Files to Modify:**
- `apps/api/identity/sqlalchemy_repository.py`

**Changes Required:**

1. Identify lines with internal-only mutations (decision required):
   - Line 150: `_refresh_session_token()` — mark @internal_only
   - Line 208: `_session_active()` — mark @internal_only  
   - Line 243: `_metadata_sync()` — mark @internal_only
   - Line 300: `_audit_log_write()` — mark @internal_only

2. Add decorator and docstring:
```python
from app.decorators import internal_only

@internal_only
def _refresh_session_token(user_id: str):
    """Internal-only: Token refresh not exposed to UI."""
    # existing code...
    session.commit()
```

**Decision Points:**
- Verify these are NOT user-visible actions
- Document reason for each internal-only classification
- Get code review approval from identity module owner

**Risk:** LOW — annotation only, no behavior change

**Estimated Lines Changed:** 15-20

---

### PR 1.3: Add task_metadata_updated Event

**Title:** `feat(events): add task_metadata_updated canonical event`

**Files to Create:**
- (none — add to existing files)

**Files to Modify:**
- `schemas/event.py`
- `apps/api/services/task_service.py`
- `schemas/event_contracts.yaml`

**Changes in schemas/event.py:**

Add new SystemEvent class:
```python
class TaskMetadataUpdated(SystemEvent):
    """Emitted when task metadata is updated."""
    event_type: Literal["task_metadata_updated"] = "task_metadata_updated"
    task_id: str
    old_metadata: dict
    new_metadata: dict
    timestamp: datetime = Field(default_factory=datetime.utcnow)
```

**Changes in task_service.py (lines 55-67):**

Replace:
```python
def update_task_metadata(task_id: str, metadata: dict):
    task = db_session.query(Task).filter_by(id=task_id).first()
    task.metadata = metadata
    db_session.commit()  # ← SILENT
```

With:
```python
def update_task_metadata(task_id: str, metadata: dict):
    task = db_session.query(Task).filter_by(id=task_id).first()
    old_metadata = task.metadata
    task.metadata = metadata
    db_session.commit()
    
    # NEW: Emit canonical event
    router.emit(SystemEvent.TaskMetadataUpdated(
        task_id=task_id,
        old_metadata=old_metadata,
        new_metadata=metadata
    ))
```

**Changes in event_contracts.yaml:**

Update `task_update_metadata` contract:
```yaml
status: "PHASE-1-COMPLETE: Success event added"
```

**Risk:** LOW — new event, no existing consumers

**Estimated Lines Changed:** 20-30

---

### PR 1.4: Activate Static Silent Mutation Gate

**Title:** `ci: activate silent mutation detection gate`

**Files to Modify:**
- `.github/workflows/ci.yml`

**Changes:**
- Update gate from warning to failure mode
- Gate now blocks PRs with silent mutations
- Exceptions automatically allowed for @internal_only marked functions

**Expected Behavior:**
- Gate runs on all PRs
- Fails if session.commit() found without router.emit() or @internal_only
- Passes if all mutations decorated or mapped

**Risk:** LOW — gate already tested in PR 1.1

---

### Phase 1 Outcome
✓ Every session.commit() in services/ is accounted for  
✓ Internal-only repository operations explicitly marked  
✓ New events always emit canonically  
✓ CI enforces no new silent mutations  

**Estimated Total Size:** ~70-80 lines across 4 PRs

---

## Phase 2: Enforce Event Triads (Weeks 3-4)

### Overview
Ensure every user-visible action emits success + failure + [rejection] events.

### PR 2.1: Add Failure Events

**Title:** `feat(events): add failure events for all user actions`

**Multiple PRs (one per service):**

#### PR 2.1a: task_service.py
- Add `TaskCreationFailed` event
- Add `TaskMetadataUpdateFailed` event  
- Wrap `create_task()` in try-except
- Wrap `update_task_metadata()` in try-except

#### PR 2.1b: calendar_service.py
- Add `CalendarEventCreationFailed`
- Add `CalendarEventUpdateFailed`
- Add `CalendarEventDeletionFailed`
- Wrap all calendar operations in try-except

#### PR 2.1c: chat_gateway_service.py
- Add `ChatMessageFailed`
- Add `ChatProposalResolutionFailed`
- Wrap in try-except

#### PR 2.1d: ingestion/service.py
- Add `WebhookProcessingFailed`
- Add `EmailParsingFailed`
- Wrap in try-except

**Template:**

```python
def some_action(...):
    try:
        # Perform action
        result = perform_mutation()
        session.commit()
        
        # Emit success
        router.emit(SystemEvent.ActionSuccess(...))
        return result
        
    except ValueError as e:
        # Emit failure with reason
        router.emit(SystemEvent.ActionFailed(
            reason="validation_error",
            error_message=str(e),
            input=...
        ))
        raise
    except Exception as e:
        # Emit unexpected failure
        router.emit(SystemEvent.ActionFailed(
            reason="internal_error",
            error_message=str(e),
            input=...
        ))
        raise
```

**Risk:** LOW — failure events are additive, not breaking

**Estimated Lines Changed:** ~150-200 across all service files

---

### PR 2.2: Add Event Triad Validation Test

**Title:** `test(ci): add event triad validation gate`

**Files to Create:**
- `tests/test_static_event_triads.py`

**Files to Modify:**
- `.github/workflows/ci.yml`

**Implementation:**
```python
def test_event_triads_are_valid():
    """Validate contracts against legal triad patterns."""
    with open("schemas/event_contracts.yaml") as f:
        contracts = yaml.safe_load(f)
    
    VALID_TRIADS = [
        (True, False, False),   # success only
        (True, True, False),    # success + failure
        (True, True, True),     # success + failure + rejection
    ]
    
    for action, contract in contracts["contracts"].items():
        triad = (
            contract["success_event"]["required"],
            contract["failure_event"]["required"],
            contract["rejection_event"]["required"]
        )
        assert triad in VALID_TRIADS, f"Invalid triad for {action}: {triad}"
```

**Risk:** LOW — validation only

---

### Phase 2 Outcome
✓ Every action emits success + failure (or success only if exception-impossible)  
✓ No failure event emitted without corresponding success attempt  
✓ CI validates triad completeness  

**Estimated Total Size:** ~200-250 lines across 5 PRs

---

## Phase 3: Map Domain Event Plane (Weeks 5-6)

### Overview
Integrate DomainEvent lifecycle plane into canonical SystemEvent stream.

### PR 3.1: Add Domain-to-Canonical Mapper

**Title:** `feat(events): add DomainCanonicalMapper service`

**Files to Create:**
- `apps/api/services/domain_canonical_mapper.py`

**Files to Modify:**
- `household_os/runtime/command_handler.py`

**Implementation:**

```python
# domain_canonical_mapper.py
class DomainCanonicalMapper:
    """Maps DomainEvent lifecycle to canonical SystemEvent."""
    
    MAPPING = {
        "task_proposed": SystemEvent.TaskProposed,
        "task_approved": SystemEvent.TaskApproved,
        "task_rejected": SystemEvent.TaskRejected,
        "task_committed": None,  # Internal-only
        "task_failed": SystemEvent.TaskFailed,
        # ... other lifecycle events
    }
    
    @staticmethod
    def map_domain_to_canonical(domain_event: DomainEvent):
        """Returns mapped SystemEvent or None if internal-only."""
        if domain_event.event_type not in DomainCanonicalMapper.MAPPING:
            logger.warning(f"Unmapped DomainEvent: {domain_event.event_type}")
            return None
        
        mapper_class = DomainCanonicalMapper.MAPPING[domain_event.event_type]
        if not mapper_class:
            return None  # Internal-only event
        
        # Transform DomainEvent data to SystemEvent format
        return mapper_class.from_domain_event(domain_event)
    
    @staticmethod
    def emit_domain_with_canonical(domain_event: DomainEvent):
        """Emit DomainEvent AND route to canonical plane if mapped."""
        # Append to DomainEvent lifecycle stream (internal)
        event_store.append(domain_event)
        
        # Map and emit to canonical plane if applicable
        canonical = DomainCanonicalMapper.map_domain_to_canonical(domain_event)
        if canonical:
            router.emit(canonical)
```

**Integration in command_handler.py:**

```python
from app.services.domain_canonical_mapper import DomainCanonicalMapper

def emit_domain_event(event_type: str, data: dict):
    """Emit DomainEvent and map to canonical if applicable."""
    domain_event = DomainEvent(event_type=event_type, data=data)
    DomainCanonicalMapper.emit_domain_with_canonical(domain_event)

# Replace existing:
# event_store.append(DomainEvent(...))
# With:
# emit_domain_event(...)
```

**Risk:** MED — new emission path but existing logic preserved

**Estimated Lines Changed:** 50-70

---

### PR 3.2: Add Domain Event Annotation Gate

**Title:** `test(ci): add domain event mapping annotation gate`

**Files to Create:**
- `tests/test_static_domain_event_mapping.py`

**Files to Modify:**
- `.github/workflows/ci.yml`

**Implementation:**

```python
def test_all_domain_events_are_annotated():
    """Every DomainEvent emission must have @canonical_target or @internal_only."""
    import ast
    
    command_handler = pathlib.Path("household_os/runtime/command_handler.py")
    tree = ast.parse(command_handler.read_text())
    
    scanner = DomainEventAnnotationScanner()
    scanner.visit(tree)
    
    unmapped = [e for e in scanner.domain_events 
                if not scanner.has_annotation(e["function"])]
    
    assert not unmapped, f"Unannotated DomainEvents:\n" + "\n".join(
        f"  Line {e['line']}: {e['event_type']}" for e in unmapped
    )
```

**Risk:** LOW — validation only

---

### PR 3.3: Annotate All Domain Event Functions

**Title:** `refactor(command-handler): annotate domain event functions`

**Files to Modify:**
- `household_os/runtime/command_handler.py`

**Annotations Required:**

```python
from app.decorators import canonical_target, internal_only

@canonical_target("TaskProposed")
def handle_task_proposal(...):
    # Maps to SystemEvent.TaskProposed
    emit_domain_event("task_proposed", ...)

@canonical_target("TaskApproved")
def handle_task_approval(...):
    emit_domain_event("task_approved", ...)

@canonical_target("TaskRejected")
def handle_task_rejection(...):
    emit_domain_event("task_rejected", ...)

@internal_only  # ← No UI visibility
def _commit_task_to_execution(...):
    # Internal state machine completion
    emit_domain_event("task_committed", ...)

@canonical_target("TaskFailed")
def handle_task_failure(...):
    emit_domain_event("task_failed", ...)
```

**Risk:** LOW — decoration only

**Estimated Lines Changed:** 15-25

---

### Phase 3 Outcome
✓ DomainEvent lifecycle fully routed to canonical SSE stream (when mapped)  
✓ Internal-only lifecycle events explicitly marked  
✓ CI validates all DomainEvents are annotated  
✓ UI can now consume all lifecycle events via canonical stream  

**Estimated Total Size:** ~150-200 lines across 3 PRs

---

## Phase 4: Normalize HPAL Mutation Plane (Weeks 7-8)

### Overview
Ensure HPAL orchestration state writes emit SystemEvent or are marked internal-only.

### PR 4.1: Add HPAL State Change Events

**Title:** `feat(events): add orchestration_plan_updated event for HPAL mutations`

**Files to Create:**
- (none — add to existing schema)

**Files to Modify:**
- `schemas/event.py`
- `apps/api/hpal/auto_reconciliation.py`
- `schemas/event_contracts.yaml`

**New Event in schemas/event.py:**

```python
class OrchestrationPlanUpdated(SystemEvent):
    """Emitted when HPAL orchestration plan is updated."""
    event_type: Literal["orchestration_plan_updated"] = "orchestration_plan_updated"
    plan_id: str
    old_plan: dict
    new_plan: dict
    reconciliation_reason: str  # "auto_reconcile", "user_update", etc.
    timestamp: datetime = Field(default_factory=datetime.utcnow)
```

**Integration in auto_reconciliation.py:**

Location: `apps/api/hpal/auto_reconciliation.py:78`

```python
def reconcile():
    """Auto-reconcile HPAL state and emit update event."""
    old_state = fetch_current_hpal_state()
    new_state = calculate_reconciled_state()
    
    if new_state != old_state:
        # Persist to graph DB
        save_hpal_state(new_state)
        
        # NEW: Emit canonical event
        router.emit(SystemEvent.OrchestrationPlanUpdated(
            plan_id=current_plan_id,
            old_plan=old_state,
            new_plan=new_state,
            reconciliation_reason="auto_reconcile"
        ))
```

**Risk:** HIGH — HPAL is orchestration core; test thoroughly

**Estimated Lines Changed:** 25-35

---

### PR 4.2: Mark Internal HPAL Mutations

**Title:** `refactor(hpal): mark internal orchestrator mutations with @audit_only`

**Files to Modify:**
- `apps/api/hpal/orchestrator.py`

**Annotations Required:**

```python
from app.decorators import audit_only

@audit_only
def _append_event_history(event):
    """Audit trail only. No UI event needed."""
    orchestrator.event_history.append(event)

@audit_only
def _update_approval_actions(actions):
    """Internal approval tracking."""
    orchestrator.approval_actions.extend(actions)

@audit_only
def _record_response(response):
    """Internal orchestration response tracking."""
    orchestrator.responses.append(response)
```

**Risk:** LOW — marking only, preserves behavior

**Estimated Lines Changed:** 15-20

---

### Phase 4 Outcome
✓ HPAL user-visible mutations emit to canonical stream  
✓ HPAL internal state mutations explicitly marked  
✓ No undeclared state writes in orchestration layer  

**Estimated Total Size:** ~50-60 lines across 2 PRs

---

## Phase 5: Finalize CI Enforcement (Week 9)

### Overview
Activate all invariance gates and document closed system state.

### PR 5.1: Activate All Static Gates

**Title:** `ci: activate all event invariance enforcement gates`

**Files to Modify:**
- `.github/workflows/ci.yml`

**Changes:**
- Upgrade all gates from warning to failure mode
- Gate failures now block PR merge
- Document exceptions in `SILENT_MUTATIONS_ALLOWED.txt`

**Risk:** LOW — gates already tested

---

### PR 5.2: Add Runtime Validation Tests

**Title:** `test(ci): add runtime SSE output and canonical emission validation`

**Files to Create:**
- `tests/test_runtime_canonical_emission.py`
- `tests/test_runtime_sse_output.py`

**Files to Modify:**
- `.github/workflows/integration.yml` (new job)

**test_runtime_canonical_emission.py:**

```python
def test_task_create_emits_canonically(router, event_collector):
    """CI GATE: task creation MUST emit via canonical path."""
    from app.services.task_service import create_task
    
    task = create_task({"name": "Test", "description": "Desc"})
    
    assert len(event_collector) == 1
    emitted = event_collector[0]
    assert isinstance(emitted, SystemEvent)
    assert emitted.event_type == "task_created"

def test_no_direct_broadcaster_bypass(router, event_collector):
    """CI GATE: Services must NOT call broadcaster directly."""
    import inspect
    from app.services import task_service
    
    source = inspect.getsource(task_service)
    assert "broadcaster" not in source or "router.emit" in source
```

**test_runtime_sse_output.py:**

```python
@pytest.mark.asyncio
async def test_sse_stream_only_emits_canonical_events():
    """CI GATE: SSE stream MUST only emit canonical SystemEvent types."""
    broadcaster = EventBroadcaster()
    
    events = []
    async for event in broadcaster.subscribe("*", timeout=1.0):
        events.append(event)
        if len(events) >= 5:
            break
    
    for event in events:
        assert event.__class__.__name__.startswith("SystemEvent"), \
            f"Non-canonical event on SSE: {event.__class__.__name__}"
```

**Risk:** LOW — validation only

**Estimated Lines Changed:** 150-180

---

### PR 5.3: Document Closed System State

**Title:** `docs: add event system closure specification`

**Files to Create/Modify:**
- `docs/EVENT_SYSTEM_CLOSURE.md` (already created above)
- `README.md` (link to closure spec)

**Content:**
- System state summary
- Three planes (canonical, lifecycle, orchestration)
- All event contracts
- How to consume events from SSE
- How to add new events (pattern)
- Emergency: how to disable planes if needed

**Risk:** LOW — documentation only

---

### Phase 5 Outcome
✓ All invariance gates active and enforcing  
✓ Runtime validation passing  
✓ System closure fully documented  
✓ Event system is operationally closed  

**Estimated Total Size:** ~50-100 lines across 3 PRs

---

## Execution Checklist

### Pre-Implementation
- [ ] Review SYSTEM_CLOSURE_SPECIFICATION.md with team
- [ ] Confirm event_contracts.yaml classification decisions
- [ ] Identify identity_repository.py UNKNOWN GAP owner
- [ ] Schedule HPAL team review for Phase 4 (HIGH RISK)
- [ ] Plan rollback strategy if Phase 4 causes issues

### Phase 1: Silent Mutations
- [ ] PR 1.1: Create static gate
- [ ] PR 1.2: Decorate internal-only (decision on classification)
- [ ] PR 1.3: Add task_metadata_updated event
- [ ] PR 1.4: Activate static gate in CI

### Phase 2: Failure Events
- [ ] PR 2.1a: task_service failure events
- [ ] PR 2.1b: calendar_service failure events
- [ ] PR 2.1c: chat_gateway_service failure events
- [ ] PR 2.1d: ingestion/service failure events
- [ ] PR 2.2: Add triad validation test

### Phase 3: Domain Mapping
- [ ] PR 3.1: Add DomainCanonicalMapper
- [ ] PR 3.2: Add annotation gate
- [ ] PR 3.3: Annotate all domain events

### Phase 4: HPAL Normalization
- [ ] PR 4.1: Add orchestration_plan_updated event (coordinate with planning team)
- [ ] PR 4.2: Mark internal HPAL mutations

### Phase 5: Finalization
- [ ] PR 5.1: Activate all static gates
- [ ] PR 5.2: Add runtime tests
- [ ] PR 5.3: Document closure specification
- [ ] Verify all invariants passing
- [ ] Team sign-off on closed state

---

## Risk Mitigation

### Phase 1 (LOW) — Silent Mutations
**Risk:** Decorating internal-only may classify something incorrectly
**Mitigation:** Require code review from module owner; mark with TODO if unsure

### Phase 2 (LOW) — Failure Events
**Risk:** Adding try-except may not catch all failure cases
**Mitigation:** Pair with test suite coverage; iterate on patterns

### Phase 3 (MED) — Domain Mapping
**Risk:** New mapper layer may have bugs or performance impact
**Mitigation:** Run integration tests; monitor event throughput; can disable mapping in router if needed

### Phase 4 (HIGH) — HPAL Normalization
**Risk:** Orchestration layer is mission-critical; event emission may affect state machine
**Mitigation:**
- Coordinate with planning_agent team
- Add comprehensive integration tests
- Stage rollout: internal testing → staging → production
- Plan immediate rollback: disable router.emit() in auto_reconciliation temporarily
- Monitor reconciliation latency and correctness

### Phase 5 (LOW) — Finalization
**Risk:** None; gates only block new violations

---

## Success Metrics

### Phase 1 Complete
- Zero new silent mutations merged
- All internal-only writes decorated
- task_metadata_updated event tested

### Phase 2 Complete
- All user actions emit success + failure
- Triad validation gate passing
- Failure event handling tested

### Phase 3 Complete
- All DomainEvents mapped or marked
- DomainCanonicalMapper working (no throughput impact)
- UI can consume all lifecycle events via SSE

### Phase 4 Complete
- HPAL mutations emit or marked
- No undeclared state writes
- Planning team sign-off on changes

### Phase 5 Complete
- All CI gates active and enforcing
- Runtime tests passing
- Event system closure documented
- Team sign-off on closed state

---

## Rollback Plan

If any phase causes production issues:

**Phase 1 Rollback:** Remove event emission; keep @internal_only decorators  
**Phase 2 Rollback:** Disable failure event handling in try-except (keep handlers for now)  
**Phase 3 Rollback:** Disable DomainCanonicalMapper.emit_domain_with_canonical() call in command_handler  
**Phase 4 Rollback:** Disable router.emit() call in auto_reconciliation.py and command_gateway.py  
**Phase 5 Rollback:** Downgrade CI gate warnings; no code changes needed  

---

## Questions & Decisions Needed

1. **Identity Repository Classification (Phase 1, PR 1.2)**
   - Question: Which operations in sqlalchemy_repository.py are USER-VISIBLE vs INTERNAL-ONLY?
   - Decision Owner: Identity module lead
   - Deadline: Before Phase 1, Week 1

2. **HPAL Mutation Risk (Phase 4, PR 4.1)**
   - Question: Will adding event emission to auto_reconciliation.py cause latency or state machine issues?
   - Decision Owner: Planning agent team lead
   - Deadline: Before Phase 4, Week 7
   - Mitigation: Staging tests + monitoring

3. **Event Schema Format (Phases 1-5)**
   - Question: Should new events be Pydantic models or YAML classes?
   - Decision: Existing codebase uses Pydantic; continue with Pydantic

4. **SSE Topic Scoping (Phase 5, PR 5.2)**
   - Question: Should UI subscribe to specific topics (e.g., "task.*") or consume all?
   - Decision: UI can choose; default is all canonical events

---

## Expected Timeline

| Week | Phase | Status | Key Milestone |
|---|---|---|---|
| 1-2 | Phase 1 | Silent Mutations | CI gate active |
| 3-4 | Phase 2 | Failure Events | Triad validation passing |
| 5-6 | Phase 3 | Domain Mapping | All lifecycle events routed |
| 7-8 | Phase 4 | HPAL Normalization | HPAL mutations declared |
| 9 | Phase 5 | CI Enforcement | System closure complete |

---

## Next Steps

1. **NOW:** Review this roadmap with your team
2. **Week 1:** Start Phase 1, PR 1.1 (static gate)
3. **Weekly:** Update phase_status in event_contracts.yaml
4. **Weekly:** Monitor CI gate results and adjust as needed
5. **End of Week 9:** Verify all invariants passing; hand off to ops team

---

**Ready to begin Phase 1?**
