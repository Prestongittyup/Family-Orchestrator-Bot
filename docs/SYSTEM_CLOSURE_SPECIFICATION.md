# System Closure Specification: Event-Driven Architecture

**Status:** Ready for Incremental Implementation  
**Created:** April 23, 2026  
**Phase:** Closure Without Breaking Changes  

---

## TABLE OF CONTENTS

1. [System Closure Model](#system-closure-model)
2. [Gap Closure Action Plan](#gap-closure-action-plan)
3. [Event Contract Specification](#event-contract-specification)
4. [CI Enforcement Design](#ci-enforcement-design)
5. [Implementation Sequence (Safe Mode)](#implementation-sequence-safe-mode)

---

## SYSTEM CLOSURE MODEL

### Final System State Definition

#### **A. Canonical Event Plane (SystemEvent)**

**Purpose:** Single source of truth for all UI-visible state mutations and external integrations.

**Allowed Sources:**
- HTTP action handlers in `apps/api/services/*_service.py`
- HTTP action handlers in `apps/api/product_surface/*` (chat, notifications, email)
- Replay service (`apps/api/event_store/replay_service.py`)
- Ingestion service (`apps/api/ingestion/service.py`)

**Required Emission Path:**

```
[Mutation Action]
    ↓
[Service Layer commits to DB]
    ↓
[Create SystemEvent instance (from schema)]
    ↓
[router.emit(SystemEvent)] ← mandatory
    ↓
[Adapter validates + transforms]
    ↓
[Broadcaster queues to Redis + SSE]
    ↓
[External consumers subscribe]
```

**Hard Restrictions:**
- NO direct writes to `event_history` without canonical emission
- NO `session.commit()` without corresponding `router.emit()`
- NO bypass to broadcaster (always through router)
- NO internal-only flags on canonical events

**State:** CLOSED. Every mutation must flow through this path or be explicitly isolated.

---

#### **B. Internal Lifecycle Plane (DomainEvent)**

**Purpose:** Encapsulate action lifecycle state machine (proposed → approved → rejected → committed) for workflow orchestration.

**Current State:** DIVERGENT. Emitted from `household_os/runtime/command_handler.py` but not integrated into canonical SSE stream.

**Classification Required (Per Event Type):**

| DomainEvent Type | Classification | Emission Site | Target |
|---|---|---|---|
| `task_proposed` | **MAP-TO-CANONICAL** | command_handler:180 | Must → `task_created` SystemEvent |
| `task_approved` | **MAP-TO-CANONICAL** | command_handler:200 | Must → `task_approved` SystemEvent |
| `task_rejected` | **MAP-TO-CANONICAL** | command_handler:217 | Must → `task_rejected` SystemEvent |
| `task_committed` | **INTERNAL-ONLY** | command_handler:235 | In-memory orchestration only |
| `task_failed` | **MAP-TO-CANONICAL** | command_handler:255 | Must → `task_failed` SystemEvent |

**Rule:** Every DomainEvent MUST be explicitly annotated as either:
- `@canonical_target('SystemEventType')` — requires mapping to SystemEvent
- `@internal_only` — explicitly excluded from UI stream

**Expected Outcome:** 
- DomainEvent serves as state machine tracer
- Each canonical transition has corresponding SystemEvent
- No orphan lifecycle events reach UI

---

#### **C. Orchestration Plane (HPAL)**

**Purpose:** Graph-level mutations for hierarchical planning and automatic reconciliation.

**Current State:** NON-CANONICAL. Mutations bypass SystemEvent emission entirely.

**Classification:**

| HPAL Mutation | Source | Current Behavior | Required Behavior | Risk |
|---|---|---|---|---|
| `save_hpal_state()` | `auto_reconciliation.py:78`, `command_gateway.py` | No SystemEvent | Emit `hpal_state_updated` OR mark internal-only | HIGH |
| `orchestrator.responses[]` append | `orchestrator.py:730` | In-memory mutation | If user-visible, emit event; else internal-only | MED |
| `orchestrator.event_history[]` append | `orchestrator.py:629` | In-memory mutation | Internal-only (audit trail, no UI) | LOW |
| `orchestrator.approval_actions[]` modify | `orchestrator.py:745` | In-memory mutation | If user-visible, emit event; else internal-only | MED |

**Rule:** Every HPAL graph write MUST be classified as:
- **User-visible mutation:** Emit corresponding SystemEvent via canonical path
- **Audit/internal mutation:** Document with `@audit_only` marker

**Expected Outcome:**
- HPAL remains orchestration layer
- User-facing updates go through canonical plane
- Internal state changes are explicitly marked

---

### System State Summary Table

| Plane | Sources | Emission | UI Visible | Test Gate | Status |
|---|---|---|---|---|---|
| **SystemEvent (Canonical)** | services, ingestion, replay | router.emit() → broadcaster → SSE | YES | Runtime path validation | ENFORCED |
| **DomainEvent (Lifecycle)** | command_handler | append to event_store | NO (orphan) | Contract mapping | MUST MAP |
| **HPAL (Orchestration)** | command_gateway, auto_reconciliation | save_hpal_state() | PARTIAL (undeclared) | Static classification | MUST CLASSIFY |

---

## GAP CLOSURE ACTION PLAN

### Issue Category 1: Silent Mutations

#### Schema: task_service.py:67 — update_task_metadata silent write

**Evidence:**
```python
# apps/api/services/task_service.py:55-67
def update_task_metadata(task_id: str, metadata: dict):
    task = db_session.query(Task).filter_by(id=task_id).first()
    task.metadata = metadata
    db_session.commit()  # ← SILENT: no router.emit()
```

**Fix Strategy:**
- Add `task_metadata_updated` SystemEvent emission
- Create event class in `schemas/event.py`
- Call `router.emit(SystemEvent.task_metadata_updated(...))`

**Minimal Diff:**
```python
def update_task_metadata(task_id: str, metadata: dict):
    task = db_session.query(Task).filter_by(id=task_id).first()
    old_metadata = task.metadata
    task.metadata = metadata
    db_session.commit()
    
    # NEW: Emit canonical event
    router.emit(SystemEvent.task_metadata_updated(
        task_id=task_id,
        old_metadata=old_metadata,
        new_metadata=metadata
    ))
```

**Risk Level:** LOW
- New event type (no existing consumers yet)
- UI can optionally subscribe
- No behavioral regression

**Ordering:** PHASE 1 (Eliminate Silent Mutations)

---

#### Schema: identity_sqlalchemy_repository.py:66+ — 7 commits (UNKNOWN intent)

**Evidence:** 7 session.commit() calls with no visible event emission.

**Proposed Classification (must be verified with module owner):**

| Line | Operation | Classification | Action |
|---|---|---|---|
| 66 | user_account_save | USER-VISIBLE | Emit `user_updated` SystemEvent |
| 112 | permission_update | USER-VISIBLE | Emit `user_permissions_changed` SystemEvent |
| 150 | token_refresh | INTERNAL-ONLY | Mark @internal_only, document |
| 208 | session_active | INTERNAL-ONLY | Mark @internal_only, document |
| 243 | metadata_sync | INTERNAL-ONLY | Mark @internal_only, document |
| 300 | audit_log_write | INTERNAL-ONLY | Mark @internal_only, document |
| 333+ | batch_operations | MIXED | Require per-operation classification |

**Risk Level:** MED
- Requires module owner verification
- May expose internal mutations to UI
- Needs test coverage

**Ordering:** PHASE 1 (after decision on classification)

---

#### Schema: HPAL mutations — auto_reconciliation.py:78 + orchestrator.py

**Evidence:**
```python
# auto_reconciliation.py:78
save_hpal_state(...)  # ← HPAL write, no SystemEvent

# orchestrator.py:629, 730, 745
event_history.append(...)  # ← In-memory, no event
responses.append(...)     # ← In-memory mutation
approval_actions.modify() # ← In-memory mutation
```

**Risk Level:** HIGH
- HPAL is core orchestration layer
- Changes may affect workflow controller logic
- Requires coordination with planning_agent

**Ordering:** PHASE 4 (after canonical plane stabilizes)

---

### Issue Category 2: Missing Failure Events

**Actions Requiring Failure Event:**

| Action | Location | Current Behavior | Required |
|---|---|---|---|
| `create_task` | task_service.py | Emits success, no failure | Emit `task_creation_failed` |
| `update_calendar_event` | calendar_service.py:476 | Emits success, no failure | Emit `calendar_update_failed` |
| `send_chat_message` | chat_gateway_service.py | Emits success, no failure | Emit `chat_message_failed` |
| `parse_email_ingestion` | ingestion/service.py | Emits success, no failure | Emit `email_parse_failed` |
| `update_user_profile` | identity_repository.py | Emits success, no failure | Emit `user_update_failed` |

**Risk Level:** LOW
- New event types (no existing consumers)
- Backward compatible
- Can be added incrementally per action

**Ordering:** PHASE 2 (after silent mutations resolved)

---

### Issue Category 3: Missing Rejection Events

**Actions Requiring Rejection Event:**

| Action | Currently Routes To | Missing Event |
|---|---|---|
| `propose_task` (DomainEvent) | command_handler but not UI | `task_rejected` → SystemEvent |
| `propose_calendar_event` | command_handler but not UI | `calendar_event_rejected` → SystemEvent |
| `propose_chat_resolution` | command_handler but not UI | `chat_proposal_rejected` → SystemEvent |

**Risk Level:** MED
- DomainEvent plane already emitting (no new rejection logic needed)
- Mapping creates canonical visibility
- Requires UI subscription to new channels

**Ordering:** PHASE 3 (after canonical path stabilizes)

---

### Issue Category 4: Parallel Event Plane Divergence

**Evidence:** DomainEvent lifecycle plane has 5 event types not reachable via SSE.

**Fix Strategy — Unified Mapper:**

Create `apps/api/services/domain_canonical_mapper.py`:

```python
class DomainCanonicalMapper:
    """Maps DomainEvent lifecycle to canonical SystemEvent."""
    
    @staticmethod
    def map_domain_to_canonical(domain_event: DomainEvent) -> Optional[SystemEvent]:
        """
        Returns mapped SystemEvent or None if internal-only.
        """
        mapping = {
            "task_proposed": lambda e: SystemEvent.task_created(e.data),
            "task_approved": lambda e: SystemEvent.task_approved(e.data),
            "task_rejected": lambda e: SystemEvent.task_rejected(e.data),
            "task_committed": None,  # Internal-only
            "task_failed": lambda e: SystemEvent.task_failed(e.data),
        }
        
        if domain_event.event_type not in mapping:
            logger.warning(f"Unmapped DomainEvent: {domain_event.event_type}")
            return None
        
        mapper = mapping[domain_event.event_type]
        return mapper(domain_event) if mapper else None

    @staticmethod
    def emit_domain_with_canonical(domain_event: DomainEvent):
        """Emit DomainEvent AND its canonical mapping."""
        event_store.append(domain_event)  # Lifecycle plane
        
        canonical = DomainCanonicalMapper.map_domain_to_canonical(domain_event)
        if canonical:
            router.emit(canonical)  # Canonical plane
```

**Risk Level:** MED
- New mapping layer, but read-only
- No existing plane disruption
- Can be added without removing DomainEvent emission

**Ordering:** PHASE 3 (after canonical path stabilizes, before HPAL)

---

### Issue Category 5: UI Mapping Gaps

**Evidence:** 5 lifecycle events emitted but UI cannot subscribe via SSE.

**Fix Strategy:**

1. Define canonical SSE topics
2. Update SSE broadcaster to scope by topic
3. UI subscribes explicitly

**Risk Level:** LOW
- Additive change (no removal)
- Backward compatible
- UI can opt-in gradually

**Ordering:** PHASE 5 (after all events are emitted canonically)

---

## EVENT CONTRACT SPECIFICATION

Machine-checkable action→event mapping in `schemas/event_contracts.yaml` format.

### Contract Schema Structure

```yaml
contracts:
  action_name:
    action: "HTTP method and path"
    handler: "file path and function name"
    success_event:
      type: "event type string"
      class: "SystemEvent.ClassName"
      required: true/false
      ui_visible: true/false
    failure_event:
      type: "event type string"
      class: "SystemEvent.ClassName"
      required: true/false
      ui_visible: true/false
    rejection_event:
      type: "event type string"
      class: "SystemEvent.ClassName"
      required: true/false
      ui_visible: true/false
    internal_only: true/false
    lifecycle_plane:
      domain_event: "event_type or null"
      mapping: "description"
      annotation: "@canonical_target or @internal_only"
    status: "PHASE-N-CLOSURE or COMPLETE or PENDING-DECISION"
```

### Validation Rules

```yaml
rules:
  silent_mutation_forbidden:
    definition: "Any session.commit() or graph write must have corresponding event emission or @internal_only marker"
    enforcement: "Static AST scan + runtime instrumentation"
    test_gate: "test_no_silent_mutations"

  event_triad_required:
    definition: "User-visible action must emit success OR (success + failure) OR (success + failure + rejection)"
    valid_triads:
      - [success]
      - [success, failure]
      - [success, failure, rejection]
    invalid_triads:
      - [failure]
      - [rejection]
      - [failure, rejection]
    enforcement: "Static contract validation"
    test_gate: "test_event_triad_completeness"

  parallel_plane_mapping_required:
    definition: "Every DomainEvent must be explicitly mapped (@canonical_target) or isolated (@internal_only)"
    enforcement: "Static annotation scan"
    test_gate: "test_domain_event_mapping_complete"

  ui_canonical_only:
    definition: "UI can consume ONLY canonical SystemEvent via SSE; no direct broker or lifecycle plane access"
    enforcement: "Static broadcaster code review + runtime topic validation"
    test_gate: "test_ui_canonical_consumption_only"

  internal_only_marker_required:
    definition: "Mutations with no UI visibility must be marked @internal_only or @audit_only"
    enforcement: "Static decorator scan"
    test_gate: "test_internal_mutations_marked"
```

---

## CI ENFORCEMENT DESIGN

### Test Gate Architecture

#### **A. Static Enforcement Gates (Pre-Commit)**

1. **Silent Mutation Detection** (`test_static_silent_mutations.py`)
   - Detects: session.commit() without router.emit()
   - Exception: Functions marked @internal_only or @audit_only
   - Failure: Silent mutation found

2. **Event Triad Completeness** (`test_static_event_triads.py`)
   - Validates contract triads against legal patterns
   - Failure: Invalid triad pattern detected

3. **Domain Event Mapping Completeness** (`test_static_domain_event_mapping.py`)
   - Scans: Every DomainEvent emission has @canonical_target or @internal_only
   - Failure: Unannotated DomainEvent found

4. **Action Event Triad Mapping Audit** (`test_contract_mapping_audit.py`)
   - Ensures: Every HTTP action has corresponding event contract
   - Ensures: Every contract event exists in SystemEvent schema
   - Failure: Missing or invalid contract detected

#### **B. Runtime Enforcement Gates (Post-Commit)**

5. **Canonical Emission Path Validation** (`test_runtime_canonical_emission.py`)
   - Validates: Events emitted via router.emit()
   - Validates: Event reaches broadcaster/SSE
   - Detects: Direct broadcaster bypass
   - Failure: Event not via canonical path

6. **SSE Output Validation** (`test_runtime_sse_output.py`)
   - Validates: Only canonical SystemEvent on SSE stream
   - Validates: No DomainEvent leakage to UI
   - Validates: Topic filtering works
   - Failure: Non-canonical event on SSE

#### **C. Test Gate Summary**

| Test | File | Type | Enforcement | Failure Condition |
|---|---|---|---|---|
| 1 Silent Mutations | `test_static_silent_mutations.py` | Static AST | Pre-commit | session.commit without router.emit |
| 2 Event Triads | `test_static_event_triads.py` | Static Contract | Pre-commit | Invalid triad pattern |
| 3 Domain Mapping | `test_static_domain_event_mapping.py` | Static Annotation | Pre-commit | Missing @canonical_target or @internal_only |
| 4 Contract Audit | `test_contract_mapping_audit.py` | Static Contract | Pre-commit | Missing or invalid contract |
| 5 Canonical Path | `test_runtime_canonical_emission.py` | Runtime | Post-commit | Event not via router |
| 6 SSE Output | `test_runtime_sse_output.py` | Runtime | Post-commit | Non-canonical on SSE |

---

## IMPLEMENTATION SEQUENCE (SAFE MODE)

### Principle: Closure Without Breaking Changes

Each phase is:
- **Independent:** No blocking dependencies between phases
- **Testable:** New tests added BEFORE fixes
- **Safe:** No removal of existing events or planes
- **Incremental:** PR-sized chunks

---

### PHASE 1: Eliminate Silent Mutations (Weeks 1-2)

**Objective:** Every `session.commit()` has a corresponding event or explicit marker.

#### Step 1.1: Add Static Gate (CI Enforcement)
- **PR Title:** `test(ci): add pre-commit gate for silent mutations`
- **Files:** `tests/test_static_silent_mutations.py`, `.github/workflows/ci.yml`
- **Status:** Gate added; not failing yet (decoration phase)

#### Step 1.2: Decorate Intentional Internal-Only Writes
- **PR Title:** `refactor(identity): mark internal-only repository writes with @internal_only`
- **Files:** `apps/api/identity/sqlalchemy_repository.py`
- **Risk:** LOW
- **Size:** 1 file, ~15 lines

#### Step 1.3: Add Missing Success Event for task_metadata_updated
- **PR Title:** `feat(events): add task_metadata_updated canonical event`
- **Files:** `schemas/event.py`, `apps/api/services/task_service.py`, `schemas/event_contracts.yaml`
- **Risk:** LOW
- **Size:** 3 files, ~30 lines

#### Step 1.4: Activate Static Silent Mutation Gate
- **PR Title:** `ci: activate silent mutation detection gate`
- **Files:** `.github/workflows/ci.yml`, `SILENT_MUTATIONS_ALLOWED.txt`
- **Risk:** LOW
- **Size:** 2 files, ~5 lines

**Phase 1 Outcome:** CI now enforces no new silent mutations.

---

### PHASE 2: Enforce Event Triads (Weeks 3-4)

**Objective:** Every user-visible action emits success + failure + [rejection].

#### Step 2.1: Add Missing Failure Events (Template)
- **PR Title:** `feat(events): add failure events for all user actions`
- **Multiple PRs:** One per service module (task, calendar, chat, etc.)
- **Risk:** LOW (failure events not breaking; additive)
- **Size:** ~30 lines per PR

#### Step 2.2: Add Event Triad Completeness Test
- **PR Title:** `test(ci): add event triad validation gate`
- **Files:** `tests/test_static_event_triads.py`, `.github/workflows/ci.yml`
- **Risk:** LOW
- **Size:** 2 files, ~40 lines

**Phase 2 Outcome:** CI enforces valid event triads.

---

### PHASE 3: Map Domain Event Plane (Weeks 5-6)

**Objective:** Every DomainEvent is mapped to SystemEvent or marked internal-only.

#### Step 3.1: Add Domain-to-Canonical Mapper
- **PR Title:** `feat(events): add DomainCanonicalMapper service`
- **Files:** `apps/api/services/domain_canonical_mapper.py`, `household_os/runtime/command_handler.py`
- **Risk:** MED (new emission path; existing logic preserved)
- **Size:** 2 files, ~50 lines

#### Step 3.2: Add Domain Event Annotation Requirement
- **PR Title:** `test(ci): add domain event mapping annotation gate`
- **Files:** `tests/test_static_domain_event_mapping.py`, `.github/workflows/ci.yml`
- **Risk:** LOW
- **Size:** 2 files, ~60 lines

#### Step 3.3: Annotate All Domain Event Functions
- **PR Title:** `refactor(command-handler): annotate domain event functions with @canonical_target or @internal_only`
- **Files:** `household_os/runtime/command_handler.py`
- **Risk:** LOW
- **Size:** 1 file, ~20 lines

**Phase 3 Outcome:** All DomainEvents now mapped or isolated.

---

### PHASE 4: Normalize HPAL Mutation Plane (Weeks 7-8)

**Objective:** HPAL state writes emit SystemEvent or marked internal-only.

#### Step 4.1: Add HPAL State Change Events
- **PR Title:** `feat(events): add orchestration_plan_updated event for HPAL mutations`
- **Files:** `schemas/event.py`, `apps/api/hpal/auto_reconciliation.py`
- **Risk:** MED (HPAL is orchestration core)
- **Size:** 2 files, ~30 lines

#### Step 4.2: Mark Internal HPAL Mutations
- **PR Title:** `refactor(hpal): mark internal orchestrator mutations with @audit_only`
- **Files:** `apps/api/hpal/orchestrator.py`
- **Risk:** LOW
- **Size:** 1 file, ~20 lines

**Phase 4 Outcome:** HPAL mutations declared or annotated.

---

### PHASE 5: Finalize CI Enforcement (Week 9)

**Objective:** All invariance gates active; no breaking changes.

#### Step 5.1: Activate All Static Gates
- **PR Title:** `ci: activate all event invariance enforcement gates`
- **Files:** `.github/workflows/ci.yml`
- **Risk:** LOW (gates already added; only activation)
- **Size:** 1 file, ~10 lines

#### Step 5.2: Add Runtime SSE Validation Tests
- **PR Title:** `test(ci): add runtime SSE output and canonical emission validation`
- **Files:** `tests/test_runtime_canonical_emission.py`, `tests/test_runtime_sse_output.py`, `.github/workflows/integration.yml`
- **Risk:** LOW
- **Size:** 3 files, ~150 lines

#### Step 5.3: Document Closed System State
- **PR Title:** `docs: add event system closure specification`
- **Files:** `docs/EVENT_SYSTEM_CLOSURE.md` (this file)
- **Risk:** LOW (documentation)
- **Size:** 1 file, ~300 lines

**Phase 5 Outcome:** Event system operationally closed; all mutations accounted for.

---

### Phase Timeline & Dependencies

```
P1.1: Add Silent Mutation Gate
  ↓
P1.2: Decorate Internal-Only
  ↓
P1.3: Add task_metadata Event
  ↓
P1.4: Activate Silent Gate
  ↓
P2.1: Add Failure Events
  ↓
P2.2: Add Triad Test Gate
  ↓
P3.1: Add Domain Mapper
  ↓
P3.2: Add Annotation Gate
  ↓
P3.3: Annotate Functions
  ↓
P4.1: Add HPAL Events
  ↓
P4.2: Mark Internal Mutations
  ↓
P5.1: Activate Static Gates
  ↓
P5.2: Add Runtime Tests
  ↓
P5.3: Document Closure
```

Each phase builds on the previous but does not block parallel work within phases.

---

### Risk & Rollback Strategy

| Phase | Risk Level | Rollback Path |
|---|---|---|
| P1 (Silent Mutations) | LOW | Remove @internal_only; events are additive |
| P2 (Failure Events) | LOW | Events are additive; remove failure handling |
| P3 (Domain Mapping) | MED | Keep DomainCanonicalMapper; disable mapping in router |
| P4 (HPAL Events) | HIGH | HPAL mutations are idempotent; disable event emission |
| P5 (CI Enforcement) | LOW | Downgrade gate warnings; no code rollback needed |

**Rollback Strategy:** Each phase is independently disableable. If HPAL event emission causes orchestration issues, disable mapping without rolling back P1-P3.

---

### Success Criteria

**Phase 1 Complete:** ✓
- No new silent mutations merged
- Existing intentional silent writes decorated

**Phase 2 Complete:** ✓
- Every action emits success + failure
- Event triad validation passing

**Phase 3 Complete:** ✓
- All DomainEvents mapped or isolated
- DomainCanonicalMapper working

**Phase 4 Complete:** ✓
- HPAL mutations emit or marked internal-only
- No undeclared state writes

**Phase 5 Complete:** ✓
- All CI gates active and enforcing
- Runtime validation passing
- System closure documented

---

## READY FOR EXECUTION

This specification is **executable as-is**. Each PR can be created and reviewed independently. No "big bang" rewrite. No event system replacement. Just consistent closure through incremental, test-driven changes.

**Next Step:** Begin Phase 1 implementation.
