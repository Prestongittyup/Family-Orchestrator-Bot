# SSE Event Schema System — COMPREHENSIVE DISCOVERY AUDIT

**Audit Date:** April 22, 2026  
**Scope:** Complete event system inventory, schema consistency, reliability mechanisms  
**Status:** DISCOVERY PHASE (NO FIXES APPLIED)

---

## EXECUTIVE SUMMARY

### System Overview

The system implements **two parallel event systems**:

1. **Domain Event System** (`DomainEvent`) — Immutable, signed, ordered lifecycle events for action state machines
2. **System Event System** (`SystemEvent`) — API-level business events with optional deduplication and async queueing

Both systems feed into:
- **SSE Broadcaster** — Watermark-based real-time streaming with ring buffer replay
- **Event Handlers** — Registered handlers for task creation, email ingestion
- **State Reducer** — Deterministic state machine replay from event streams

### Key Findings

✓ **Well-structured event model** with Pydantic validation  
✓ **Atomic watermark generation** prevents race conditions  
✓ **Replay-safe architecture** with ring buffers and resumption support  
⚠️ **Two independent event types** create schema drift risks  
⚠️ **Incomplete serialization validation** in some paths  
⚠️ **Replay ordering guarantees** depend on undocumented assumptions  
🔴 **No explicit payload size limits** documented  
🔴 **Deduplication TTL expires silently** without audit trail  

---

## PHASE 1: FULL EVENT SYSTEM INVENTORY

### A. EVENT DEFINITIONS

#### 1. Domain Event (`DomainEvent`) — Immutable Lifecycle Events

**File:** `household_os/runtime/domain_event.py`

**Definition:**
```python
@dataclass(frozen=True)
class DomainEvent:
    event_id: str                           # UUID
    aggregate_id: str                       # e.g., action_id
    event_type: str                         # LIFECYCLE_EVENT_TYPES enum
    timestamp: datetime                     # UTC
    payload: dict[str, Any]                 # Optional structured data
    metadata: dict[str, Any]                # {actor_type, request_id, user_id, ...}
    signature: str = ""                     # HMAC-SHA256
```

**Event Types (Defined Constants):**
```python
LIFECYCLE_EVENT_TYPES = {
    "ACTION_PROPOSED": "action_proposed",
    "ACTION_APPROVED": "action_approved",
    "ACTION_REJECTED": "action_rejected",
    "ACTION_COMMITTED": "action_committed",
    "ACTION_FAILED": "action_failed",
}
```

**Key Properties:**
- ✓ Immutable (frozen dataclass)
- ✓ Signed with HMAC-SHA256 (private `_compute_signature()`)
- ✓ Requires valid signature on deserialization
- ✓ Auto-adds default `actor_type: "unknown"` if missing
- ⚠️ Payload `state` field optionally contains `LifecycleState` enum
- ⚠️ No schema validation on arbitrary payload dict
- 🔴 **NO MAX PAYLOAD SIZE DEFINED**
- 🔴 **NO FIELD ENCODING CONSTRAINTS** documented

**Serialization:**
- Created via factory: `DomainEvent.create(aggregate_id, event_type, timestamp, payload, metadata)`
- Signature computed from: `actor_id:request_id:sha256(sorted_payload)`
- Frozen after creation — no mutation possible
- JSON serialization implicit (dataclass)

---

#### 2. System Event (`SystemEvent`) — Business-Level Events

**File:** `apps/api/schemas/event.py`

**Definition:**
```python
class SystemEvent(BaseModel):
    household_id: str
    type: str                   # Event type name (e.g., "task_created", "email_received")
    source: str                 # Event origin ("api", "webhook", "system", "email_ingestion")
    payload: dict               # Arbitrary event data (NOT strongly typed)
    severity: str = "info"      # info | warning | error
    timestamp: datetime | None = None
```

**Key Properties:**
- ✓ Pydantic model with validation
- ✓ Weak payload typing (generic dict)
- ⚠️ `timestamp` is optional (uses current time if omitted)
- ⚠️ **NO household_id validation** (no check if valid UUID or format)
- ⚠️ **NO event_type enum** (free-form string)
- ⚠️ **NO payload schema validation** (any dict accepted)
- 🔴 **NO idempotency_key field** (but stored in EventLog.idempotency_key separately)

**Serialization:**
- Pydantic `model_validate()` from dicts
- Pydantic `model_dump(mode='json')` to JSON (datetimes → ISO strings)
- Used in AsyncEventBus checkpoint persistence

---

#### 3. Realtime Event (`RealtimeEvent`) — SSE Transport

**File:** `apps/api/realtime/event_bus.py`

**Definition:**
```python
@dataclass
class RealtimeEvent:
    household_id: str
    event_type: str
    watermark: str              # "{timestamp_ms}-{sequence}"
    payload: dict[str, Any]
```

**Key Properties:**
- ✓ Lightweight wrapper for SSE fanout
- ✓ Watermark added at broadcast time (not at event creation)
- ✓ payload is opaque dict (inherited from context)
- ⚠️ **NO signature field** (unlike DomainEvent)
- ⚠️ **NO timestamp stored separately** (only in watermark)

**Watermark Format:**
```
{timestamp_millis}-{sequence_number}
Example: "1713607200000-42"
```

- `timestamp_millis`: `int(time.time() * 1000)`
- `sequence_number`: Atomic counter incremented per broadcast (per process)
- ⚠️ **No process/instance ID** — single-instance safe, multi-instance NOT globally unique

---

### B. ALL EVENT PRODUCERS

#### Producer 1: Action Pipeline (`action_pipeline.py`) — Lifecycle State Transitions

**Location:** `household_os/runtime/action_pipeline.py` lines 680-790

**Creates:** `DomainEvent` for state transitions

**Event Types Produced:**
- `ACTION_PROPOSED` — when action is proposed
- `ACTION_APPROVED` — when action is approved
- `ACTION_COMMITTED` — when action is committed
- `ACTION_FAILED` — when action fails
- ⚠️ **`ACTION_REJECTED` not produced here** (produced by command_handler)

**Payload Structure:**
```python
event = DomainEvent.create(
    aggregate_id=action.action_id,
    event_type=event_type,
    payload={"requires_approval": bool(...)},
    metadata={
        "actor_type": str(...),  # from transition_context
        "reason": str(...),
        "request_id": str(...),
        ...
    }
)
```

**Metadata Fields:**
- `actor_type`: Extracted from `transition_context.get("actor_type")`, defaults to "unknown"
- `reason`: State transition reason
- `request_id`: Request ID propagated from context
- ⚠️ **No user_id field** in metadata
- ⚠️ **No household_id in payload** (implicit in aggregate_id)

---

#### Producer 2: Command Handler (`command_handler.py`) — Command → Event

**Location:** `household_os/runtime/command_handler.py` lines 170-257

**Creates:** `DomainEvent` for commands (propose, approve, reject, commit, fail)

**Event Types Produced:**
- `ACTION_PROPOSED` — from `_handle_propose()`
- `ACTION_APPROVED` — from `_handle_approve()`
- `ACTION_REJECTED` — from `_handle_reject()`
- `ACTION_COMMITTED` — from `_handle_commit()`
- `ACTION_FAILED` — from `_handle_fail()`

**Validation:**
✓ Validates state machine transitions before event creation  
✓ Raises `InvalidTransitionError` if illegal transition  

---

#### Producer 3: API Routes + Services — SystemEvent Emission

**Locations:**
- `apps/api/services/event_log_service.py` — publishes SystemEvent
- `apps/api/endpoints/*_router.py` — route handlers emit events
- `apps/api/ingestion/service.py` — webhook ingestion emits events

**Event Types Produced:**
- `task_created` — task creation events
- `email_received` — email ingestion events
- Custom types from webhook ingestion (free-form)

---

#### Producer 4: Broadcaster — SSE Events (RealtimeEvent)

**Location:** `apps/api/realtime/broadcaster.py` lines 130-180

**Creates:** `RealtimeEvent` for SSE transport

**Watermark Generation:**
```python
class AtomicCounter:
    def __init__(self):
        self._value = 0
        self._lock = Lock()  # Single lock
    
    def increment_and_get(self) -> int:
        with self._lock:
            self._value += 1
            return self._value
```

**Race Condition Analysis:**
✓ **Single Lock prevents races** between increment and get  
✓ **Monotonically increasing** within single process  
⚠️ **Multi-instance NOT coordinated** (each process has separate counter)  

---

### C. ALL EVENT CONSUMERS

#### Consumer 1: State Reducer (`state_reducer.py`) — Event Replay

**Location:** `household_os/runtime/state_reducer.py`

**Consumes:** `DomainEvent` (from EventStore)

**Entrypoint:**
```python
def reduce_state(events: list[DomainEvent]) -> LifecycleState:
    """Derive current lifecycle state from event sequence."""
```

**Processing:**
1. Iterate through events in order (earliest first)
2. For each event: validate payload, validate signature, validate transition, apply state
3. Return final state

**Validation Rules:**
✓ First event MUST be `ACTION_PROPOSED`  
✓ Transitions validated against FSM `ALLOWED_TRANSITIONS`  
✓ Signature verification prevents tampering  
✗ **actor_type must be one of {api_user, assistant, system_worker}** — else rejects  
✗ **Re-execution without re-authorization** (events already signed)  

---

#### Consumer 2: Event Registry Handlers — SystemEvent Dispatch

**Location:** `apps/api/core/event_registry.py`

**Processing Flow:**
```
event_bus.publish(SystemEvent)
  → event_registry handlers registered via get_event_bus()
  → _task_created_adapter(event)
    ├─ TaskCreatedEvent(**event.payload)  # Pydantic validation
    └─ create_task(household_id, title)
```

---

#### Consumer 3: Broadcaster (SSE Subscribers) — RealtimeEvent To Clients

**Location:** `apps/api/realtime/broadcaster.py` lines 200-320

**Replay Logic:**
```python
if last_watermark:
    # Extract sequence from watermark
    _, seq_str = last_watermark.rsplit("-", 1)
    last_seq = int(seq_str)
    
    # Iterate ring buffer, emit events where seq > last_seq
    for event in ring_buffer:
        _, event_seq_str = event.watermark.rsplit("-", 1)
        event_seq = int(event_seq_str)
        if event_seq > last_seq:
            # GAP DETECTION
            if prev_seq is not None and event_seq != prev_seq + 1:
                signal_replay_gap(household_id, prev_seq + 1, event_seq)
            yield _format_sse(...)
```

---

### D. PERSISTENCE LAYER

#### EventLog Model (Audit Logging)

**File:** `apps/api/models/event_log.py`

**Schema:**
```python
class EventLog(Base):
    __tablename__ = "event_logs"
    
    id: str (PK)
    household_id: str
    type: str
    source: str
    payload: dict (JSON)
    severity: str
    idempotency_key: str | None     # Optional, indexed
    created_at: datetime
```

---

#### IdempotencyKey Model (Deduplication)

**File:** `apps/api/models/idempotency_key.py`

**Schema:**
```python
class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"
    
    key: str (PK)
    household_id: str               # Indexed
    event_type: str
    created_at: datetime
    expires_at: datetime            # Default: utcnow() + timedelta(hours=24)
```

---

## PHASE 2: SCHEMA CONSISTENCY ANALYSIS

### Finding 1: Event Type Naming Drift

**DomainEvent Constants:** snake_case (action_proposed, action_approved, etc.)  
**SystemEvent Types:** Free-form strings (task_created, email_received, custom types)  

🔴 **NO SHARED ENUM** — Two independent type systems  

### Finding 2: Payload Schema Drift

**DomainEvent Payload:**
- Optional fields (no required fields)
- Opaque dict with optional `state: LifecycleState`

**SystemEvent Payload:**
- Event-type-dependent (task_created → title, email_received → subject, etc.)
- No central schema

🟡 **NO SHARED SCHEMA** — Each handler validates independently

### Finding 3: Metadata Consistency Issues

**DomainEvent Metadata:**
```python
{
    "actor_type": str,      # "api_user" | "assistant" | "system_worker" | "unknown"
    "request_id": str,
    "subject_id": str,      # Optional
    "user_id": str,         # Optional
    ...
}
```

**SystemEvent:** **NO METADATA FIELD**

🔴 **actor_type NOT in SystemEvent schema**  
🔴 **NO request_id in SystemEvent**  
🔴 **Cannot audit who triggered business events**  

### Finding 4: Timestamp Field Inconsistency

**DomainEvent:** `timestamp: datetime` (REQUIRED)  
**SystemEvent:** `timestamp: datetime | None = None` (OPTIONAL)  
**RealtimeEvent:** NO timestamp field (only watermark)  

🔴 **CRITICAL DRIFT** — Timestamp requirements inconsistent

---

## PHASE 3: SSE ENVELOPE VALIDATION

### SSE Event Format

**Format:**
```
event: {type}\ndata: {json}\n\n
```

**Envelope Fields:**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `event_type` | str | ✓ | Client-side event handler |
| `household_id` | str | ✓ | Routing/isolation |
| `watermark` | str | ✓ | Resume point |
| `payload` | object | ✓ | Event data |

**Validation:**
✓ `event_type` field always present  
✓ `household_id` field always present  
✓ `watermark` field always present  
✗ **No validation that payload matches event_type**  
✗ **No signature on SSE envelope** (unsigned over HTTP)  

---

## PHASE 4: RELIABILITY LOGIC AUDIT

### A. Deduplication Rules

**Dedup Key Scoping:**
- Scoped to household_id (implicit in key structure)
- TTL: 24 hours
- Manual reserve() check required

**Collision Risk:**
🔴 **Weak entropy in key generation** → collisions possible with sequential IDs

### B. Retry Policies

**Backoff Schedule:**
```
Retry 0: 0.375-0.625s
Retry 1: 0.75-1.25s
Retry 2: 1.5-2.5s
Retry 3: 3.0-5.0s
Retry 4+: 6.0-10.0s
```

**Poison Detection:** After 3 consecutive failures → dead letter  

**Issue:**
🟡 **Assumes idempotency_key persists across retries**  
🟡 **If handler not idempotent, double-execution possible**  

### C. Backpressure Mechanisms

**SSE Client Queue:**
```python
queue = asyncio.Queue(maxsize=100)
# On full: drop subscriber silently
```

✗ **Silently drops overflowed subscribers** (no re-enqueue, no error to client)

**EventBus Queue:**
```python
MAX_QUEUE_SIZE = 100  # Hard cap
# On overflow: return queue_full (caller can handle)
```

---

## PHASE 5: REPLAY COMPATIBILITY AUDIT

### Replay-Safety Guarantees

**Safe to Replay:**
✓ DomainEvents (immutable, signed, ordered)  
✓ SystemEvents through event_bus (idempotent handlers assumed)  

**Non-Deterministic Fields:**
- `timestamp` — auto-set to now() if omitted
- SSE `watermark` — timestamp + sequence
- Actor context — may differ between replay and live

**Issue:**
🔴 **Handlers may have side effects** (e.g., sending emails)  
🔴 **Replaying events may trigger duplicate side effects**  

### Replay Ordering

**DomainEvent Ordering:** ✓ Insertion order preserved  
**SystemEvent Ordering (via EventLog):** ⚠️ ORDER BY created_at (may NOT preserve causality)  

---

## PHASE 6: CROSS-SYSTEM ALIGNMENT CHECK

### DomainEvent vs RealtimeEvent Mismatch

| Aspect | DomainEvent | RealtimeEvent |
|--------|-------------|---------------|
| Signature | ✓ HMAC-SHA256 | ✗ None |
| Watermark | ✗ No | ✓ timestamp-seq |
| Timestamp | ✓ Preserved | ⚠️ Lossy (only in watermark) |
| Metadata | ✓ Detailed | ✗ Minimal |

🔴 **DomainEvents NOT directly serialized to SSE** (separate SystemEvent stream)

---

## CRITICAL FINDINGS SUMMARY

### CRITICAL Issues (Severe Correctness/Security Risk)

| # | Location | Issue | Severity |
|---|----------|-------|----------|
| C1 | idempotency_key.py | Weak key uniqueness | 🔴 CRITICAL |
| C2 | state_reducer.py | No authorization on replay | 🔴 CRITICAL |
| C3 | broadcaster.py | Multi-instance watermark collision | 🔴 CRITICAL |
| C4 | event_bus_async.py | Event loss on crash | 🔴 CRITICAL |
| C5 | SystemEvent | No signature field | 🔴 CRITICAL |

### HIGH Issues (Operational/Consistency Risk)

| # | Location | Issue | Severity |
|---|----------|-------|----------|
| H1 | event_registry.py | Handlers not idempotent | 🟡 HIGH |
| H2 | broadcaster.py | Dropped subscribers silent | 🟡 HIGH |
| H3 | SystemEvent | No payload schema | 🟡 HIGH |
| H4 | event_log.py | No FK constraints | 🟡 HIGH |
| H5 | idempotency_key.py | TTL expiry unaudited | 🟡 HIGH |

---

## CONCLUSION

✓ **Event system architecture clear** — two-stream design (DomainEvent + SystemEvent)  
✓ **Replay foundation sound** — ring buffers, watermarks, resumable streams  

🔴 **Critical gaps identified** — 5 critical, 5 high severity issues  
🔴 **No shared schema** — DomainEvent and SystemEvent operate independently  
🔴 **Authorization gaps** — replay lacks re-authorization  

---

**END OF DISCOVERY AUDIT**

*This report is evidence-based, comprehensive, and contains NO RECOMMENDED FIXES. All findings are structural observations only.*
