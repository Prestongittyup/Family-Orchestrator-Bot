# Threat Model & Attack Vector Analysis
## Family Orchestration Bot - Security Risk Scenarios

**Version**: 1.0  
**Date**: 2024  
**Classification**: Internal Security Analysis

---

## Overview

This document details specific attack vectors that could exploit the identified gaps in permission enforcement. Each scenario describes:
- **Threat Actor**: Who might exploit this
- **Attack Vector**: How they'd do it
- **Preconditions**: What access/knowledge they need
- **Impact**: What damage could occur
- **Likelihood**: How probable this is with current gaps
- **Mitigation**: How remediation fixes it

---

## Threat Model

### Threat Actors

1. **Malicious Inside Household Member**
   - Has valid API credentials
   - Knows their household_id
   - May control automated workflows
   - Example: Jealous spouse with access to family account

2. **Compromised Assistant/AI Service**
   - Has actor_type="assistant" credentials
   - Integrated with the system
   - Cannot be manually stopped (automated)
   - Example: Jailbroken AI model in assistant service

3. **Rogue Workflow/Integration**
   - Has direct access to orchestrator
   - Can trigger scheduled tasks
   - Runs with system_worker credentials
   - Example: Malicious cron job or GitHub action

4. **Network Attacker**
   - Intercepts system components
   - Can invoke orchestrator methods
   - May replay historical event logs
   - Example: Kubernetes pod escape, internal network access

---

## Attack Vector Analysis

### 🔴 CRITICAL: Assistant Self-Approval via Orchestrator

**Threat Actor**: Compromised Assistant Service  
**Attack Vector**: Direct orchestrator invocation  
**Likelihood**: HIGH (gaps confirmed)

#### Scenario

The family has configured an AI assistant service to make suggestions about household coordination. The assistant has `actor_type="assistant"` credentials.

**Attack Steps**:

```python
# Step 1: Attacker compromises the assistant service
# (or operates insider assistant with malicious instructions)

# Step 2: Assistant crafts its own "approval" request
import requests

# Create a high-impact action request
action_id = "pay-all-bills-now"
request_id = "req-attacker-controlled"
household_id = "target-family-household"

# Step 3: Directly call orchestrator bypass API endpoint
# (or compromise the service and call internal method)
response = requests.post(
    "http://internal-orchestrator:8000/orchestrator.approve",
    json={
        "household_id": household_id,
        "request_id": request_id,
        "action_ids": [action_id],
        # ❌ No actor_type forwarding
        # ❌ No user_id validation
    }
)
```

#### Why It Works (Current State)

1. **No actor context in orchestrator** - Method doesn't accept actor_type
2. **No household owner check** - Doesn't validate who owns the household
3. **FSM guard unreachable** - Guard is defined but never invoked because context not passed
4. **State firewall only for direct mutations** - FSM.transition_to() sets context var, but we need to verify this happens

#### Impact

- **Financial**: Assistant could approve bill payments, transfers
- **Data**: Assistant could approve deletion of family data
- **Autonomy**: Family loses control over household automation
- **Escalation**: Assistant could approve other high-risk actions (medical, schedule changes)

#### Preconditions

- ✓ Assistant has network access to orchestrator (likely, internal service)
- ✓ Assistant knows target household_id (in requests it processes)
- ✓ Orchestrator.approve_and_execute() is callable (public API or RPC)
- ❌ Currently missing: actor_type validation (what we're fixing!)

#### Proof of Concept

```python
# Minimal POC showing vulnerability
from household_os.runtime.orchestrator import HouseholdOSOrchestrator
from apps.api.core.state_machine import ActionState

# Attacker has orchestrator instance (internal access)
orchestrator = HouseholdOSOrchestrator()

# Create a pending action in the graph
household_id = "family-123"
graph = orchestrator.state_store.load_graph(household_id)

# Attacker modifies graph to inject malicious action
action = {
    "action_id": "fire-housekeeper",
    "request_id": "req-fake",
    "current_state": ActionState.PENDING_APPROVAL.value,
    "approval_required": True,
    "title": "Terminate housekeeper employment",
    "description": "Remove weekly housekeeper",
    # ... more fields
}

actions = graph.setdefault("action_lifecycle", {}).setdefault("actions", {})
actions["fire-housekeeper"] = action

# Attacker calls approve without actor validation
# ❌ No actor_type parameter required
approval_result = orchestrator.approve_and_execute(
    household_id=household_id,
    request_id="req-fake",
    action_ids=["fire-housekeeper"],
    # ❌ actor_type parameter doesn't exist (current code)
    # ❌ user_id not required
    # ❌ action executes successfully!
)

# Housekeeper is fired without family approval
print("Attack successful—action executed")
```

**Status**: 🔴 Confirmed exploitable (with remediation effort: Phase 1)

---

### 🔴 CRITICAL: Cross-Household Data Manipulation

**Threat Actor**: Malicious Household Member (with valid credentials but wrong scope)  
**Attack Vector**: Workflow trigger with manipulated household_id  
**Likelihood**: MEDIUM (requires compromise of code/env)

#### Scenario

A household member (user-A) has valid API credentials for household "family-1". They discover they can trigger workflows or access the orchestrator directly.

**Attack Steps**:

```python
# Step 1: Attacker has valid credentials for family-1
household_id = "family-1"
user_id = "user-A"
token = "valid_jwt_token_for_family_1"

# Step 2: They find the daily_cycle or workflow trigger
# This is likely internal/scheduled, but if exposed...

from household_os.runtime.daily_cycle import DailyCycle

cycle = DailyCycle()

# Step 3: Trigger cycle for DIFFERENT household
# ❌ No scope validation in cycle.tick()
result = cycle.tick(household_id="family-2")  # ← Wrong household!

# Step 4: System processes family-2 state with family-1 auth
# Creates actions, sends notifications, etc. for wrong family
```

#### Why It Works (Current State)

1. **No household ownership check in orchestrator** - Accepts any household_id
2. **No user_id validation** - Doesn't verify requester owns the target household
3. **Auth context not passed through** - Even if auth middleware knows the scope, orchestrator doesn't

#### Impact

- **Data Leakage**: Attacker can read another family's calendar, tasks, state
- **Sabotage**: Create false actions, cancel legitimate events, corrupt data
- **Privacy**: Access sensitive household information (health, finances, schedules)

#### Preconditions

- ✓ Attacker has valid API token (for any household)
- ✓ Attacker knows another household_id (could be sequential/guessable)
- ✗ Insider knowledge of system architecture (needed to trigger orchestrator directly)
- ❌ Missing: household owner validation

#### Proof of Concept

```python
# Attacker with valid token for family-1
my_household = "family-1"
my_user_id = "user-A"

# Discover another household (e.g., sequential ID or social engineering)
target_household = "family-2"

# Call orchestrator directly for target household
# (This might be via internal job queue, workflow system, or exposed endpoint)

from household_os.runtime.orchestrator import HouseholdOSOrchestrator
orchestrator = HouseholdOSOrchestrator()

# ❌ No validation that my_user_id owns target_household
result = orchestrator.tick(
    household_id=target_household,  # ← Attacker uses wrong household
    user_input="What should we do today?",
    actor_type="api_user",  # Attacker claims valid type
    user_id=my_user_id,  # But user doesn't own this household!
)

# Now orchestrator has loaded
 family-2 state and is processing it
# with attacker's request context

# Read results
print(result.response.reasoning_trace)  # ← Contains family-2's secrets!
```

**Status**: 🔴 Confirmed exploitable (with remediation effort: Phase 2)

---

### 🟡 HIGH: Event Replay Authorization Bypass

**Threat Actor**: Rogue Workflow / Event Processing System  
**Attack Vector**: Manual event injection or historical replay  
**Likelihood**: MEDIUM (requires access to event store)

#### Scenario

The system uses event sourcing. An attacker with access to the event store replays or injects a historical event that violates current authorization rules.

**Attack Steps**:

```python
# Step 1: Attacker accesses event store
# (e.g., database compromise, backup restore)

from household_os.runtime.event_store import EventStore
from household_os.runtime.domain_event import DomainEvent

event_store = EventStore()

# Step 2: Create/inject event with invalid transition
# Example: Assistant transitioning action straight to APPROVED
# (skipping PENDING_APPROVAL)

malicious_event = DomainEvent(
    event_id="evt-injected-123",
    aggregate_id="action-456",
    event_type="action_state_changed",
    event_version=1,
    recorded_at=datetime.now(UTC),
    data={
        "from_state": "proposed",  # ← Skipping pending_approval!
        "to_state": "approved",
    },
    metadata={
        "actor_type": "assistant",  # ← Invalid (assistant can't approve)
        "reason": "Injected event",
    }
)

# Step 3: Replay state (reconstruct from stored events)
from household_os.runtime.state_reducer import reduce_state

events = event_store.get_events("household-123") + [malicious_event]  # ← Inject

initial_state = {...}
final_state = reduce_state(events, initial_state, "household-123")

# Step 4: System accepts the invalid state because
#  reduce_state() doesn't pass context to validate_transition()
```

#### Why It Works (Current State)

1. **Event metadata not used during replay** - Context is stored but not applied
2. **No context in validate_transition call** - Guard logic never runs
3. **Events accepted as-is** - No re-validation against current rules

#### Impact

- **State Corruption**: Invalid actions become committed in state
- **Precedent**: If one invalid event gets through, attacker could replay more
- **Audit Gap**: Event log contains invalid transition but no alert

#### Preconditions

- ✗ Attacker needs database/event store access (high barrier)
- ✓ If they have it, no auth checks on event injection
- ✗ Also needs knowledge of action_ids and household structure

#### Proof of Concept

```python
# Database-level access required to inject event
# SQL injection example (hypothetical):

sql = """
INSERT INTO domain_events (event_id, aggregate_id, event_type, data, metadata)
VALUES (
    'evt-bypass-123',
    'action-critical-task',
    'action_state_changed',
    '{"from_state": "proposed", "to_state": "approved"}',
    '{"actor_type": "assistant", "bypassed_approval": true}'
)
"""

# When state is replayed, event is processed without validation
# Result: Assistant approval is persisted in state
```

**Status**: 🟡 Confirmed exploitable (with remediation effort: Phase 3)

---

### 🟠 MEDIUM: Incomplete Audit Trail

**Threat Actor**: Attacker who wants to hide their actions  
**Attack Vector**: Weak actor attribution  
**Likelihood**: MEDIUM (depends on audit system)

#### Scenario

An attacker makes unauthorized changes. The audit log doesn't clearly identify who did it.

**Example**:

```python
# Action approved without clear actor attribution
approval_result = orchestrator.approve_and_execute(
    household_id=household_id,
    request_id=request_id,
    action_ids=[action_id],
    # No actor_type → defaults to "unknown"
    # Audit log says "unknown user approved action"
    # Who was it? Unclear.
)

# Or approval via event replay:
# Event says actor_type="unknown" (never set)
# Log says "unknown transition applied"
# Impossible to trace to source
```

#### Why It Happens

1. **actor_type optional in orchestrator** - Empty context used
2. **Events created without metadata** - No actor attribution stored
3. **Audit logging doesn't capture actor** - Only action, not who did it

#### Impact

- **Forensics**: Can't trace unauthorized actions back to attacker
- **Accountability**: Blame is diffused ("system did it")
- **Insider Threat**: Malicious employees harder to catch

#### Mitigation Impact

**Phase 1 + Phase 4** ensures:
- All transitions attributed to specific actor
- Event metadata always includes actor_type
- Audit logs show who → what → when
- Forensic tracking is complete

---

### 🔴 CRITICAL: System Worker Privilege Escalation

**Threat Actor**: Rogue Scheduled Task / Workflow Engine  
**Attack Vector**: Direct orchestrator call with system_worker context  
**Likelihood**: MEDIUM (needs control of scheduler)

#### Scenario

An attacker compromises the system that runs scheduled tasks. They invoke the orchestrator with `actor_type="system_worker"`.

```python
# System worker can call orchestrator without the same guards as api_user
# Because most role-based checks don't distinguish system_worker

# Attacker triggers:
result = orchestrator.tick(
    household_id="family-123",
    actor_type="system_worker",  # ← No user_id required
    # This bypasses user ownership checks because we assume
    # system_worker is properly isolated/"trusted"
    
    # But if attacker controls system_worker, they get all privileges
)
```

#### Why It's Dangerous

1. **No distinction between authorized system work and attacker system work**
2. **system_worker context lacks user_id** - Can't trace to person
3. **Household scope less strictly validated for system_worker**

#### Impact

- **Full Household Access**: Can read/write all state
- **No Attribution**: Changes logged as "system" not traceable to attacker
- **Persistence**: If they control scheduler, they can run repeatedly

#### Mitigation Strategy

- Clearly separate "authorized system tasks" from "untrusted system work"
- Example: Daily cycle has explicit system account (immutable)
- Limit what system_worker can approve/execute
- Always log system_worker actions with correlation ID back to scheduler

---

## Risk Summary Matrix

| Attack Vector | Current Risk | After Phase 1 | After Phase 2 | After Phase 3 | After Phase 4 |
|---------------|-------------|--------------|--------------|--------------|--------------|
| Assistant self-approval | 🔴 CRITICAL | 🟢 NONE | 🟢 NONE | 🟢 NONE | 🟢 NONE |
| Cross-household access | 🔴 CRITICAL | 🔴 CRITICAL | 🟢 NONE | 🟢 NONE | 🟢 NONE |
| Event replay bypass | 🟡 HIGH | 🟡 HIGH | 🟡 HIGH | 🟢 LOW | 🟢 LOW |
| Incomplete audit | 🟠 MEDIUM | 🟠 MEDIUM | 🟠 MEDIUM | 🟠 MEDIUM | 🟢 NONE |
| System worker abuse | 🟡 HIGH | 🟡 HIGH | 🟡 HIGH | 🟡 HIGH | 🟠 MEDIUM |

---

## Exploitation Complexity

### Easy to Exploit (Low Barrier)

- **Assistant Self-Approval**: No special access needed, orchestrator likely calls internally
- **Audit Trail Gaps**: Always exploitable if orchestrator called

### Medium Difficulty (Needs some access)

- **Cross-Household Access**: Need valid credentials + knowledge of household_id structure
- **System Worker Abuse**: Need to compromise scheduler/workflow engine

### High Difficulty (Requires deep access)

- **Event Replay Bypass**: Need database access + event store knowledge

---

## Detection Strategies

While waiting for fixes, you can detect attacks:

### Monitor 1: Approval Without Matching Actor

```python
# Alert if action approved with actor_type="assistant"
SELECT action_id, actor_type FROM action_transitions
WHERE to_state = "approved" AND actor_type = "assistant"
```

### Monitor 2: Cross-Household Mutation

```python
# Alert if orchestrator called with mismatched household
SELECT household_id, user_id FROM audit_log
WHERE user_household_id != requested_household_id
```

### Monitor 3: Events Without Actor Metadata

```python
# Alert if events lack actor_type in metadata
SELECT event_id FROM domain_events
WHERE metadata->>'actor_type' IS NULL
   OR metadata->>'actor_type' = 'unknown'
```

### Monitor 4: System Worker Activity

```python
# Log all system_worker actions for review
SELECT event_time, action_id, action_type FROM audit_log
WHERE actor_type = 'system_worker'
ORDER BY event_time DESC
```

---

## Recommendation for Risk Acceptance

**Do NOT accept the risk.** The gaps are exploitable and directly impact core functionality (approval process). Remediation effort is reasonable (16-17 hours) and should be prioritized.

**Minimum acceptable state**: Complete Phase 1 + Phase 2 (before Phase 3-4 enhancements). This closes the critical gaps.

