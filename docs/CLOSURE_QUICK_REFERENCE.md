# Event System Closure: Quick Reference Index

**Status:** ✅ COMPLETE — Ready for Phase 1 Implementation  
**Date:** April 23, 2026  
**Duration:** 9 weeks, 18 PRs, 5 phases  

---

## 📋 Artifacts Created

### Primary Specification Documents

1. **[SYSTEM_CLOSURE_SPECIFICATION.md](./SYSTEM_CLOSURE_SPECIFICATION.md)**
   - System closure model (3 planes defined)
   - Gap closure action plan (5 gap categories)
   - Event contract specification (validation rules)
   - CI enforcement design (6 test gates)
   - Implementation sequence (5 phases)
   - **Use When:** Understanding overall architecture and enforcement model

2. **[schemas/event_contracts.yaml](../schemas/event_contracts.yaml)**
   - 18 action→event contracts from audit findings
   - Machine-checkable triad validation
   - Phase progress tracking
   - Decision point markers
   - **Use When:** Verifying contract completeness or checking action status

3. **[IMPLEMENTATION_ROADMAP.md](./IMPLEMENTATION_ROADMAP.md)**
   - Detailed 18-PR execution plan
   - PR-by-PR breakdown with implementation sketches
   - Risk assessments and rollback strategies
   - Execution checklist
   - Timeline and success metrics
   - **Use When:** Planning PRs or tracking phase progress

---

## 🎯 Quick Start

### If you're a developer starting Phase 1:
1. Read: [SYSTEM_CLOSURE_SPECIFICATION.md](./SYSTEM_CLOSURE_SPECIFICATION.md#phase-1-eliminate-silent-mutations-weeks-1-2)
2. Read: [IMPLEMENTATION_ROADMAP.md — Phase 1 section](./IMPLEMENTATION_ROADMAP.md#phase-1-eliminate-silent-mutations-weeks-1-2)
3. Check: [event_contracts.yaml — task_update_metadata contract](../schemas/event_contracts.yaml)
4. Action: Create PR 1.1 (static gate test)

### If you're a reviewer checking PR completeness:
1. Find: Action name in [event_contracts.yaml](../schemas/event_contracts.yaml)
2. Verify: Contract shows success_event + failure_event requirements
3. Check: PR implements both or documents why not
4. Confirm: CI gates pass

### If you're planning Phase 4 (HPAL):
1. Read: [SYSTEM_CLOSURE_SPECIFICATION.md — Orchestration Plane section](./SYSTEM_CLOSURE_SPECIFICATION.md#c-orchestration-plane-hpal)
2. Review: [IMPLEMENTATION_ROADMAP.md — Phase 4 Risk Mitigation](./IMPLEMENTATION_ROADMAP.md#phase-4-hpal-normalization-weeks-7-8)
3. Coordinate: Planning_agent team sign-off required
4. Prepare: Staging test plan for auto_reconciliation.py changes

### If you need to verify closure for compliance:
1. Check: [event_contracts.yaml — phase_status section](../schemas/event_contracts.yaml#phase-progress-tracking)
2. Verify: All phases completed (rows show COMPLETE status)
3. Confirm: All CI gates active and passing
4. Review: [SYSTEM_CLOSURE_SPECIFICATION.md — Success Criteria](./SYSTEM_CLOSURE_SPECIFICATION.md#success-criteria)

---

## 🔍 Three Planes Explained

### Canonical Event Plane (SystemEvent)
- **Purpose:** UI-visible state mutations
- **Emission:** router.emit() → broadcaster → SSE
- **Example:** task_created, calendar_event_updated
- **Status:** ENFORCED via CI gates
- **See:** [SYSTEM_CLOSURE_SPECIFICATION.md#a-canonical-event-plane-systemevent](./SYSTEM_CLOSURE_SPECIFICATION.md#a-canonical-event-plane-systemevent)

### Internal Lifecycle Plane (DomainEvent)
- **Purpose:** Action state machine (proposed → approved → rejected → committed)
- **Emission:** event_store.append() from command_handler.py
- **Status:** DIVERGENT (not routed to UI) → Phase 3 maps to canonical
- **Classification:** @canonical_target OR @internal_only
- **See:** [SYSTEM_CLOSURE_SPECIFICATION.md#b-internal-lifecycle-plane-domainevent](./SYSTEM_CLOSURE_SPECIFICATION.md#b-internal-lifecycle-plane-domainevent)

### Orchestration Plane (HPAL)
- **Purpose:** Graph-level planning and state mutations
- **Emission:** save_hpal_state() in auto_reconciliation.py
- **Status:** NON-CANONICAL (bypasses SystemEvent) → Phase 4 adds events
- **Risk:** HIGH (core orchestration layer)
- **Classification:** Emit SystemEvent OR mark @audit_only
- **See:** [SYSTEM_CLOSURE_SPECIFICATION.md#c-orchestration-plane-hpal](./SYSTEM_CLOSURE_SPECIFICATION.md#c-orchestration-plane-hpal)

---

## 📊 Contract Status Summary

| Category | Count | Status | Notes |
|---|---|---|---|
| **Total Contracts** | 18 | DEFINED | All actions from audit findings |
| **Complete Triads** | 5 | ENFORCED | Success + failure events ready |
| **Partial Triads** | 7 | PHASE-2-PENDING | Missing failure events |
| **Orphan DomainEvents** | 5 | PHASE-3-PENDING | Mapped but not on UI stream |
| **HPAL Mutations** | 2 | PHASE-4-PENDING | Need canonical emission OR @audit_only |
| **Silent Mutations** | 3 | PHASE-1-PENDING | Need event or marker |

**See:** [event_contracts.yaml — Complete listing](../schemas/event_contracts.yaml#contracts)

---

## ⚠️ Decision Points Requiring Owner Sign-Off

### 1. Identity Repository Classification (Phase 1, Week 1)
**Status:** DECISION-NEEDED  
**Problem:** 7 session.commit() calls in sqlalchemy_repository.py with unknown intent  
**Location:** Lines 66, 112, 150, 208, 243, 300, 333+  
**Decision Needed:** Is each one USER-VISIBLE (emit event) or INTERNAL-ONLY (mark with @internal_only)?  
**Owner:** Identity module lead  
**Deadline:** Before Phase 1 completion  
**Reference:** [IMPLEMENTATION_ROADMAP.md — PR 1.2](./IMPLEMENTATION_ROADMAP.md#pr-12-decorate-internal-only-writes)

### 2. HPAL Event Emission Risk (Phase 4, Week 7)
**Status:** DECISION-NEEDED  
**Problem:** Adding router.emit() to auto_reconciliation.py may affect orchestration state machine  
**Risk Level:** HIGH  
**Questions:**
- Will event emission add unacceptable latency?
- Will it interfere with reconciliation logic?
- Can we stage this safely?

**Owner:** Planning agent team lead  
**Deadline:** Before Phase 4 begins  
**Mitigation:** Staging tests + monitoring + ready rollback  
**Reference:** [IMPLEMENTATION_ROADMAP.md — Phase 4 Risk](./IMPLEMENTATION_ROADMAP.md#phase-4-hpal-normalization-weeks-7-8)

---

## 🛠️ Phase Quick Reference

| Phase | Week | Focus | Risk | PRs | Key Files |
|---|---|---|---|---|---|
| P1 | 1-2 | Silent mutations | LOW | 4 | task_service.py, identity_repository.py, ci.yml |
| P2 | 3-4 | Failure events | LOW | 5 | services/*.py, test_static_event_triads.py |
| P3 | 5-6 | Domain mapping | MED | 3 | domain_canonical_mapper.py, command_handler.py |
| P4 | 7-8 | HPAL normalization | HIGH | 2 | auto_reconciliation.py, orchestrator.py |
| P5 | 9 | Finalization | LOW | 3 | ci.yml, test_runtime_*.py, docs |

---

## 🚦 CI Enforcement Gates (Active After Phase 5)

### Static Gates (Pre-Commit)
1. ✅ **Silent Mutation Detection** — Fails if session.commit() without router.emit() or @internal_only
2. ✅ **Event Triad Validation** — Fails if invalid triad pattern in contracts
3. ✅ **Domain Event Annotation** — Fails if DomainEvent without @canonical_target or @internal_only
4. ✅ **Contract Audit** — Fails if action missing contract or event not in schema

### Runtime Gates (Post-Commit)
5. ✅ **Canonical Emission Path** — Fails if event not via router.emit()
6. ✅ **SSE Output Validation** — Fails if non-canonical event on SSE stream

**See:** [SYSTEM_CLOSURE_SPECIFICATION.md#ci-enforcement-design](./SYSTEM_CLOSURE_SPECIFICATION.md#ci-enforcement-design)

---

## 📝 Common Patterns

### Adding a New Event (Template)

```python
# 1. Define event in schemas/event.py
class MyActionFailed(SystemEvent):
    event_type: Literal["my_action_failed"] = "my_action_failed"
    reason: str
    error_message: str

# 2. Emit in action handler with try-except
def my_action(...):
    try:
        # perform action
        session.commit()
        router.emit(SystemEvent.MyActionSuccess(...))
    except Exception as e:
        router.emit(SystemEvent.MyActionFailed(reason="...", error_message=str(e)))
        raise

# 3. Add contract to event_contracts.yaml
my_action:
  success_event:
    type: "my_action_success"
    required: true
  failure_event:
    type: "my_action_failed"
    required: true
  status: "COMPLETE"
```

### Marking Internal-Only (Template)

```python
from app.decorators import internal_only

@internal_only
def _internal_mutation():
    """No UI event needed for this operation."""
    session.commit()  # ← Authorized; gate will not complain
```

### Mapping DomainEvent (Template)

```python
from app.decorators import canonical_target

@canonical_target("MySystemEvent")
def handle_domain_event():
    """This DomainEvent maps to MySystemEvent on canonical plane."""
    emit_domain_event("my_domain_event_type", {...})
```

---

## 🔗 Related Documentation

- Audit findings: See conversation summary in chat
- Event schema: [schemas/event.py](../schemas/event.py)
- Router implementation: [apps/api/services/router.py](../apps/api/services/router.py)
- Broadcaster: [apps/api/product_surface/sse_broadcaster.py](../apps/api/product_surface/sse_broadcaster.py)
- Command handler: [household_os/runtime/command_handler.py](../household_os/runtime/command_handler.py)

---

## ✅ Verification Checklist

### Before Starting Phase 1:
- [ ] Team has read SYSTEM_CLOSURE_SPECIFICATION.md
- [ ] Identity module owner has met for classification discussion
- [ ] event_contracts.yaml decisions are documented
- [ ] HPAL team aware of Phase 4 coming

### After Each Phase:
- [ ] All PRs merged
- [ ] CI gates updated in event_contracts.yaml
- [ ] No regressions in integration tests
- [ ] Documentation updated

### At Project Completion:
- [ ] All 5 phases complete
- [ ] All CI gates active and enforcing
- [ ] No silent mutations in codebase
- [ ] All triads valid
- [ ] DomainEvents mapped or marked
- [ ] HPAL mutations declared
- [ ] Team sign-off on closed state

---

## 🆘 Troubleshooting

### "Silent mutation detected in CI"
→ Either: (1) Add router.emit() to emit event, or (2) Add @internal_only decorator if intentional

### "Invalid event triad"
→ Check [event_contracts.yaml](../schemas/event_contracts.yaml) for your action. Contracts must have (success), (success+failure), or (success+failure+rejection).

### "DomainEvent not annotated"
→ Go to command_handler.py and add @canonical_target("SystemEventType") or @internal_only above the function

### "Non-canonical event on SSE stream"
→ Check that all events emitted via router.emit(), not direct broadcaster calls. Verify DomainCanonicalMapper is routing correctly.

### Phase 4 (HPAL) failing production
→ Rollback: Comment out router.emit() in auto_reconciliation.py temporarily. Reconvene with planning team for next approach.

---

**Status: ✅ COMPLETE — Ready to execute Phase 1**

Start here: [IMPLEMENTATION_ROADMAP.md — Phase 1](./IMPLEMENTATION_ROADMAP.md#phase-1-eliminate-silent-mutations-weeks-1-2)
