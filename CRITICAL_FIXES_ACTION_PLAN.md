# CRITICAL FIXES ACTION PLAN
## Family Orchestration Bot — April 2026

---

## 🚨 MUST FIX BEFORE ANY USER ACCESS

### FIX #1: Implement Token Revocation on Logout

**Current Problem**:
```typescript
// authProvider.ts
logout() {
  localStorage.removeItem("hpal-auth-token");
  // Missing: Backend revocation call
}
```

**Impact**: Logged-out users can still be impersonated

**Fix Steps**:

1. **Backend: Add revocation endpoint**
```python
# apps/api/endpoints/auth_router.py
@router.post("/logout")
def logout(request: LogoutRequest, current_user = Depends(get_current_user)):
    """Revoke current token and all related tokens."""
    token_service.revoke_token(
        token=request.token,
        user_id=current_user.user_id,
        household_id=current_user.household_id
    )
    return {"status": "logged_out"}
```

2. **Backend: Check revocation on EVERY request**
```python
# apps/api/core/auth_middleware.py (modify existing validate_token)
def validate_token(token: str, household_id: str) -> dict:
    claims = jwt.decode(token, SECRET)
    
    # NEW: Check revocation list
    if token_service.is_revoked(token):
        raise UnauthorizedError("Token revoked")
    
    return claims
```

3. **Frontend: Call logout endpoint**
```typescript
// authProvider.ts
async logout() {
  const token = localStorage.getItem("hpal-auth-token");
  
  // Call backend to revoke
  await fetch(`${API_BASE}/logout`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}` },
    body: JSON.stringify({ token })
  });
  
  // Clear local storage
  localStorage.removeItem("hpal-auth-token");
}
```

**Time to implement**: 4 hours  
**Testing**: 2 hours  
**Risk**: LOW (additive change)

---

### FIX #2: Validate Household in Auth Middleware

**Current Problem**:
```typescript
// Frontend can send any household_id
const params = new URLSearchParams({ family_id: familyId });
// No backend validation that user has access
```

**Impact**: Cross-household data leakage

**Fix Steps**:

1. **Backend: Extract household from token**
```python
# apps/api/core/auth_middleware.py
async def verify_household_access(
    household_id: str,
    current_user = Depends(get_current_user)
):
    """Verify user has access to this household."""
    if current_user.household_id != household_id:
        raise HTTPException(
            status_code=403, 
            detail=f"Access denied: not member of {household_id}"
        )
```

2. **Apply to all endpoints that take household_id**
```python
@router.get("/families/{family_id}/overview")
def get_overview(
    family_id: str,
    current_user = Depends(verify_household_access)
):
    # household_id already validated
    return {...}
```

3. **Test: Verify cross-household requests fail**
```python
def test_cross_household_access_denied():
    user_in_family1 = get_user_token("family-1")
    response = client.get(
        "/families/family-2/overview",
        headers={"Authorization": f"Bearer {user_in_family1}"}
    )
    assert response.status_code == 403
```

**Time to implement**: 3 hours  
**Testing**: 1 hour  
**Risk**: MEDIUM (might break existing integrations)

---

### FIX #3: Fix Frontend Idempotency Key Generation

**Current Problem**:
```typescript
// ProductSurfaceClient.ts
async executeAction(request: ActionExecutionRequest, identity: RequestIdentityContext) {
    const response = await fetch(..., {
        headers: {
            "x-idempotency-key": request.idempotency_key  // NEW ID every time
        }
    });
}
```

**Impact**: Retries create duplicate tasks

**Fix Steps**:

1. **Generate deterministic key from message content**
```typescript
// runtime/store.ts
async sendMessage(sessionId: string, message: string) {
    // Instead of random ID:
    const idempotencyKey = generateDeterministicKey({
        message,
        household_id: this.familyId,
        user_id: this.active_user?.user_id,
        timestamp_minute: Math.floor(Date.now() / 60000)  // Per-minute granularity
    });
    
    const request = {
        ...payload,
        idempotency_key: idempotencyKey  // Stable across retries
    };
    
    await productSurfaceClient.sendMessage(request, identity);
}

function generateDeterministicKey(data: object): string {
    const hash = crypto
        .subtle
        .digest("SHA-256", JSON.stringify(data));
    return Buffer.from(hash).toString("hex");
}
```

2. **Test: Verify same key on retry**
```typescript
test("retry with same message uses same idempotency key", () => {
    const id1 = generateDeterministicKey(data);
    const id2 = generateDeterministicKey(data);
    expect(id1).toBe(id2);
});
```

**Time to implement**: 2 hours  
**Testing**: 1 hour  
**Risk**: LOW

---

### FIX #4: Add Persistent Event Journal

**Current Problem**:
```python
# InMemoryRealtimeEventBus
events = deque(maxlen=1000)  # Lost on restart
```

**Impact**: Data loss on backend restart

**Fix Steps**:

1. **Add database table for events**
```python
# apps/api/models/event_journal.py
from sqlalchemy import Column, String, Integer, DateTime, Text
from apps.api.core.database import Base

class EventJournal(Base):
    __tablename__ = "event_journal"
    
    id = Column(Integer, primary_key=True)
    event_id = Column(String, unique=True, index=True)
    household_id = Column(String, index=True)
    event_type = Column(String)
    watermark = Column(String)
    payload = Column(Text)  # JSON
    created_at = Column(DateTime, index=True)
```

2. **Modify event bus to persist**
```python
# apps/api/realtime/event_bus.py
class InMemoryRealtimeEventBus:
    async def publish(self, event: RealtimeEvent):
        # Persist to database
        journal_entry = EventJournal(
            event_id=event.event_id,
            household_id=event.household_id,
            event_type=event.event_type,
            watermark=event.watermark,
            payload=json.dumps(event.payload),
            created_at=datetime.now(UTC)
        )
        db.session.add(journal_entry)
        db.session.commit()
        
        # Also publish to in-memory for real-time
        self.events.append(event)
```

3. **Support event replay on reconnect**
```python
async def get_events_since(
    household_id: str, 
    watermark: str
) -> list[RealtimeEvent]:
    """Get all events since watermark point."""
    entries = db.query(EventJournal).filter(
        EventJournal.household_id == household_id,
        EventJournal.watermark > watermark
    ).all()
    
    return [
        RealtimeEvent(
            event_id=e.event_id,
            household_id=e.household_id,
            event_type=e.event_type,
            watermark=e.watermark,
            payload=json.loads(e.payload)
        )
        for e in entries
    ]
```

4. **Clean up old events (retention policy)**
```python
# apps/api/services/maintenance_service.py
def cleanup_old_events(retention_days: int = 30):
    """Delete events older than retention_days."""
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    db.query(EventJournal).filter(
        EventJournal.created_at < cutoff
    ).delete()
    db.session.commit()
```

**Time to implement**: 8 hours  
**Testing**: 2 hours  
**Risk**: MEDIUM (requires migration)

---

### FIX #5: Enforce LLM Hard Timeout

**Current Problem**:
```python
# No timeout on LLM calls
response = llm_provider.resolve_intent(...)  # Can hang forever
```

**Impact**: Hung requests, connection pool exhaustion

**Fix Steps**:

1. **Wrap LLM call with explicit timeout**
```python
# apps/api/llm/gateway.py
import signal
from concurrent.futures import ThreadPoolExecutor, TimeoutError

class LLMGateway:
    HARD_TIMEOUT_SECONDS = 3.0
    
    def __init__(self, provider, ...):
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.provider = provider
    
    def resolve_intent(self, message: str, context: dict) -> LLMIntentResponse:
        """Resolve intent with hard timeout."""
        try:
            future = self.executor.submit(
                self.provider.resolve_intent,
                message,
                context
            )
            result = future.result(timeout=self.HARD_TIMEOUT_SECONDS)
            return result
        except TimeoutError:
            # Log timeout, return fallback
            logger.warning(f"LLM timeout after {self.HARD_TIMEOUT_SECONDS}s")
            return self._fallback_intent(message)
        except Exception as e:
            logger.error(f"LLM error: {e}")
            return self._fallback_intent(message)
    
    def _fallback_intent(self, message: str) -> LLMIntentResponse:
        """Rule-based fallback when LLM fails."""
        if "create" in message.lower() or "add" in message.lower():
            return LLMIntentResponse(intent_type="CREATE_TASK", confidence=0.5)
        elif "schedule" in message.lower() or "event" in message.lower():
            return LLMIntentResponse(intent_type="CREATE_EVENT", confidence=0.5)
        else:
            return LLMIntentResponse(
                intent_type="CLARIFICATION",
                confidence=0.0,
                clarification_request="I didn't understand. Try: create task, schedule event, or mark complete"
            )
```

2. **Test timeout behavior**
```python
def test_llm_timeout_returns_fallback():
    slow_provider = SlowLLMProvider(delay=10.0)
    gateway = LLMGateway(slow_provider, ...)
    
    start = time.time()
    result = gateway.resolve_intent("create task")
    elapsed = time.time() - start
    
    assert elapsed < 4.0  # Timeout + margin
    assert result.resolved_by == "fallback"
```

**Time to implement**: 3 hours  
**Testing**: 1 hour  
**Risk**: LOW

---

## ⚠️ HIGH PRIORITY FIXES (Before Beta Expansion)

### FIX #6: SSE Event Replay on Reconnect

**Current Problem**:
```python
# On SSE reconnect, no event history available
subscribers = []  # Lost on disconnect
```

**Impact**: Missing updates when client reconnects

**Fix Steps**:

1. **Track watermark on SSE subscribe**
```python
# apps/api/endpoints/realtime_router.py
@router.get("/stream")
async def stream_updates(
    household_id: str = Query(...),
    last_watermark: str = Query(default="v1:0")
):
    """SSE stream with watermark-based replay."""
    
    # Send all missed events since watermark
    event_bus = get_event_bus()
    missed_events = event_bus.get_events_since(household_id, last_watermark)
    
    async def event_stream():
        # Replay missed events first
        for event in missed_events:
            yield f"data: {json.dumps(event)}\n\n"
        
        # Then subscribe to live events
        async for event in event_bus.subscribe(household_id):
            yield event
    
    return StreamingResponse(event_stream(), media_type="text/event-stream")
```

2. **Frontend sends watermark on reconnect**
```typescript
// hpal-frontend/src/runtime/store.ts
startRealtimeStream() {
    const lastWatermark = this.colSignals.last_updated_watermark || "v1:0";
    const url = `${API_BASE}/stream?household_id=${...}&last_watermark=${lastWatermark}`;
    
    this.realtimeStream = new EventSource(url);
    this.realtimeStream.onmessage = (event) => {
        const data = JSON.parse(event.data);
        // Process including replayed events
        this.ingestPatches([data]);
    };
}
```

3. **Test: Verify events replayed on reconnect**
```python
def test_sse_replay_on_reconnect():
    # Publish event
    event_bus.publish(event1)
    
    # Get watermark
    watermark = event_bus.get_current_watermark()
    
    # Publish another event
    event_bus.publish(event2)
    
    # Reconnect with watermark
    replayed = event_bus.get_events_since(watermark)
    assert event2 in replayed
    assert event1 not in replayed  # Only events AFTER watermark
```

**Time to implement**: 6 hours  
**Testing**: 2 hours  
**Risk**: MEDIUM

---

### FIX #7: API Versioning for Deployment Safety

**Current Problem**:
```python
# All routes on /v1/ but no versioning strategy
# Backend changes break old frontend caches
```

**Impact**: Every deployment risks breaking active clients

**Fix Steps**:

1. **Add version negotiation**
```python
# apps/api/core/version.py
API_VERSION = "2"  # Increment on breaking changes
SUPPORTED_VERSIONS = ["1", "2"]  # Support both old and new

class VersionMismatchError(HTTPException):
    def __init__(self):
        super().__init__(
            status_code=400,
            detail="API version mismatch. Please refresh your app."
        )
```

2. **Check version on every request**
```python
# apps/api/core/middleware.py
@app.middleware("http")
async def version_check(request: Request, call_next):
    version = request.headers.get("X-API-Version")
    
    if version not in SUPPORTED_VERSIONS:
        raise VersionMismatchError()
    
    response = await call_next(request)
    response.headers["X-API-Version"] = API_VERSION
    return response
```

3. **Frontend sends version on every request**
```typescript
// ProductSurfaceClient.ts
private identityHeaders(identity: RequestIdentityContext) {
    return {
        Authorization: `Bearer ${identity.token}`,
        "X-API-Version": "1",  // Send this
    };
}
```

4. **Support both v1 and v2 during transition**
```python
# Create v2 endpoints, keep v1
@router.post("/v1/message")  # Old format
def message_v1(...):
    return handle_message_v1(...)

@router.post("/v2/message")  # New format
def message_v2(...):
    return handle_message_v2(...)
```

**Time to implement**: 8 hours  
**Testing**: 2 hours  
**Risk**: MEDIUM (requires client update)

---

## 🛠️ TESTING EACH FIX

### Test Suite Template

```python
# tests/test_critical_fixes.py

def test_logout_revokes_token():
    """Verify logout actually revokes token."""
    # 1. Login
    token = login_user("user@example.com")
    
    # 2. Make request - should work
    response = client.get("/overview", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    
    # 3. Logout
    client.post("/logout", headers={"Authorization": f"Bearer {token}"})
    
    # 4. Try same token - should fail
    response = client.get("/overview", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 401  # Unauthorized

def test_household_validation():
    """Verify users can't access other families."""
    # 1. User from family-1
    token = login_user_in_family("user@example.com", "family-1")
    
    # 2. Try to access family-2
    response = client.get(
        "/families/family-2/overview",
        headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 403  # Forbidden

def test_no_duplicate_retry():
    """Verify deterministic idempotency key prevents duplicates."""
    message = "Create grocery task"
    
    # 1. First request
    key1 = generate_idempotency_key(message)
    response1 = client.post(
        "/message",
        json={"message": message},
        headers={"x-idempotency-key": key1}
    )
    task_id = response1.json()["task_id"]
    
    # 2. Retry with same message
    key2 = generate_idempotency_key(message)
    assert key1 == key2  # Keys should match
    
    response2 = client.post(
        "/message",
        json={"message": message},
        headers={"x-idempotency-key": key2}
    )
    
    # 3. Verify no duplicate created
    tasks = get_tasks()
    assert len([t for t in tasks if t.title == "grocery task"]) == 1

def test_backend_restart_preserves_events():
    """Verify events survive backend restart."""
    # 1. Create event
    event = create_event("Task 1")
    watermark = get_current_watermark()
    
    # 2. Simulate restart
    restart_backend()
    
    # 3. Verify event still in journal
    replayed = get_events_since(watermark)
    assert event in replayed

def test_llm_timeout_fallback():
    """Verify LLM timeout uses fallback."""
    slow_provider = SlowProvider(delay=10.0)
    gateway = LLMGateway(slow_provider)
    
    start = time.time()
    result = gateway.resolve_intent("create task")
    elapsed = time.time() - start
    
    assert elapsed < 4.0
    assert result.intent_type in ["CREATE_TASK", "CLARIFICATION"]
```

---

## 🚀 DEPLOYMENT SEQUENCE

### Week 1: Critical Fixes

| Day | Task | Effort | PR Size |
|-----|------|--------|---------|
| Mon | Logout revocation | 4h | 150 lines |
| Tue | Household validation | 3h | 120 lines |
| Wed | Event journal | 8h | 400 lines |
| Thu | LLM timeout | 3h | 200 lines |
| Thu–Fri | Testing & bugfixes | 4h | varies |

**Verification**: All critical tests passing ✅

### Week 2: Remaining Fixes + Testing

| Day | Task | Effort |
|-----|------|--------|
| Mon | Frontend idempotency key | 2h |
| Tue | SSE replay | 6h |
| Wed | API versioning | 8h |
| Thu | Integration testing | 4h |
| Fri | Load testing | 4h |

**Results**: System passes all 6 dimensions at ~70% fitness

---

## BEFORE LAUNCHING BETA

Checklist:

- [ ] All critical fixes merged + tested
- [ ] Code review completed (require 2+ approvals)
- [ ] Integration tests passing (100% success rate)
- [ ] Load tested (50 concurrent users, 5 messages/sec)
- [ ] Monitoring dashboard deployed
- [ ] On-call rotation established
- [ ] Support runbook written
- [ ] 2-3 trusted beta families identified
- [ ] NDA signed with beta families
- [ ] Incident response plan documented

---

## SUCCESS CRITERIA

**Week 1 Beta (3 families)**:
- Zero security breaches
- Zero duplicate tasks
- Zero event loss
- < 2 support issues/day

**Week 2-4 Expansion (25-50 families)**:
- < 0.1% duplicate task rate
- < 0.1% event loss rate
- < 5 support issues/day
- 70%+ feature adoption

---

**This is your roadmap to production.** Follow it precisely, test thoroughly, and beta carefully. You'll have a solid system.
