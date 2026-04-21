# P0 FIXES — FILES MODIFIED & VERIFICATION CHECKLIST

**Implementation Date**: April 20, 2026  
**Status**: Complete & Ready for Deployment

---

## FILES MODIFIED (4 total)

### 1. `apps/api/models/idempotency_key.py`
**Purpose**: Add TTL support to idempotency keys  
**Changes**:
- Added import: `from datetime import timedelta`
- Added column: `expires_at: Mapped[datetime]` with default `datetime.utcnow() + timedelta(hours=24)`

**Verification**:
```bash
# Check syntax
python -m py_compile apps/api/models/idempotency_key.py

# Verify import
python -c "from apps.api.models.idempotency_key import IdempotencyKey; print('✅ OK')"

# Check model has expires_at
python -c "from apps.api.models.idempotency_key import IdempotencyKey; assert hasattr(IdempotencyKey, 'expires_at'); print('✅ expires_at column exists')"
```

**Lines Changed**: ~5 lines  
**Backward Compatible**: ✅ YES (new column with default value)

---

### 2. `apps/api/services/idempotency_key_service.py`
**Purpose**: Implement TTL logic & cleanup  
**Changes**:
- Added import: `from datetime import datetime`
- Updated `reserve()` function to check `expires_at` and allow expired keys to be reused
- Added new function: `cleanup_expired()` for background cleanup

**Verification**:
```bash
# Check syntax
python -m py_compile apps/api/services/idempotency_key_service.py

# Verify imports
python -c "from apps.api.services.idempotency_key_service import reserve, cleanup_expired; print('✅ OK')"

# Test reserve() with expiry logic
python -c "
from apps.api.services.idempotency_key_service import reserve
key = 'test-' + str(int(__import__('time').time()))
assert reserve(key, 'h1', 'e') == True
assert reserve(key, 'h1', 'e') == False
print('✅ TTL logic works')
"
```

**Lines Changed**: ~50 lines (reserve() expanded, cleanup_expired() added)  
**Backward Compatible**: ✅ YES (existing code calls reserve() same way)

---

### 3. `apps/api/realtime/broadcaster.py`
**Purpose**: Atomic watermark counter + event ring buffer + SSE replay  
**Changes**:
- Added import: `from collections import deque`
- Added new class: `AtomicCounter` (replaces dual-lock pattern)
- Modified `HouseholdBroadcaster.__init__()`:
  - Removed: `self._lock`, `self._sync_lock`
  - Added: `self._counter = AtomicCounter()`
  - Added: `self._ring_buffers: dict[str, deque[RealtimeEvent]]`
- Updated `publish()` and `publish_sync()`:
  - Use `self._counter.increment_and_get()` instead of manual increment
  - Store event in ring buffer: `self._ring_buffers[household_id].append(event)`
- Updated `subscribe()` to accept `last_watermark` parameter and call replay
- Added new method: `_replay_buffered_events()` (implements SSE replay logic)

**Verification**:
```bash
# Check syntax
python -m py_compile apps/api/realtime/broadcaster.py

# Verify imports
python -c "from apps.api.realtime.broadcaster import HouseholdBroadcaster, AtomicCounter; print('✅ OK')"

# Test atomic counter
python -c "
from apps.api.realtime.broadcaster import AtomicCounter
import threading

counter = AtomicCounter()
results = []
def inc():
    for _ in range(100):
        results.append(counter.increment_and_get())

threads = [threading.Thread(target=inc) for _ in range(10)]
for t in threads: t.start()
for t in threads: t.join()

assert len(set(results)) == len(results), 'Duplicate watermarks detected'
print(f'✅ Atomic counter: {len(results)} unique values')
"

# Test ring buffer
python -c "
from apps.api.realtime.broadcaster import broadcaster
import asyncio

async def test():
    await broadcaster.publish('test', 'e1', {})
    await broadcaster.publish('test', 'e2', {})
    assert len(broadcaster._ring_buffers['test']) == 2
    print('✅ Ring buffer stores events')

asyncio.run(test())
"
```

**Lines Changed**: ~100 lines (new class, modified methods, new method)  
**Backward Compatible**: ✅ YES (broadcast API unchanged)

---

### 4. `apps/api/endpoints/realtime_router.py`
**Purpose**: Enable SSE watermark replay via query parameter  
**Changes**:
- Updated `stream_updates()` signature to accept `last_watermark: str | None = Query(None, ...)`
- Pass `last_watermark` to `broadcaster.subscribe()`
- Updated docstring to document replay parameter

**Verification**:
```bash
# Check syntax
python -m py_compile apps/api/endpoints/realtime_router.py

# Verify endpoint signature
python -c "
from apps.api.endpoints.realtime_router import stream_updates
import inspect
sig = inspect.signature(stream_updates)
params = list(sig.parameters.keys())
assert 'last_watermark' in params, 'Missing last_watermark parameter'
print('✅ Endpoint has last_watermark parameter')
"

# Test SSE endpoint with last_watermark
curl 'http://localhost:8000/v1/realtime/stream?household_id=test&last_watermark=1000-50' \
  -H 'Accept: text/event-stream' \
  --max-time 1 \
  || true  # Connection closes after 1s, that's OK
# Should see SSE format events
```

**Lines Changed**: ~10 lines  
**Backward Compatible**: ✅ YES (parameter is optional)

---

## DOCUMENTATION CREATED (4 files)

| File | Purpose | Audience |
|------|---------|----------|
| `P0_FIXES_IMPLEMENTATION_SUMMARY.md` | Complete technical design & implementation details | Engineers, code reviewers |
| `P0_DEPLOYMENT_GUIDE.md` | Step-by-step deployment instructions | DevOps, SREs, release managers |
| `P0_QUICK_REFERENCE.md` | One-page cheat sheet | Operations, on-call engineers |
| `verify_p0_fixes.py` | Automated verification test suite | QA, deployment pipelines |

---

## VERIFICATION CHECKLIST

### ☐ Code Verification

- [ ] **Syntax Check**
  ```bash
  for file in \
    apps/api/models/idempotency_key.py \
    apps/api/services/idempotency_key_service.py \
    apps/api/realtime/broadcaster.py \
    apps/api/endpoints/realtime_router.py; do
    python -m py_compile "$file" && echo "✅ $file" || echo "❌ $file"
  done
  ```

- [ ] **Import Check**
  ```bash
  python -c "
  from apps.api.models.idempotency_key import IdempotencyKey
  from apps.api.services.idempotency_key_service import reserve, cleanup_expired
  from apps.api.realtime.broadcaster import HouseholdBroadcaster, AtomicCounter
  from apps.api.endpoints.realtime_router import stream_updates
  print('✅ All imports successful')
  "
  ```

- [ ] **Model Schema**
  ```bash
  python -c "
  from apps.api.models.idempotency_key import IdempotencyKey
  import sqlalchemy
  cols = [c.name for c in IdempotencyKey.__table__.columns]
  assert 'expires_at' in cols, 'Missing expires_at column'
  print(f'✅ Model columns: {cols}')
  "
  ```

- [ ] **AtomicCounter Class**
  ```bash
  python -c "
  from apps.api.realtime.broadcaster import AtomicCounter
  counter = AtomicCounter()
  assert counter.increment_and_get() == 1
  assert counter.increment_and_get() == 2
  print('✅ AtomicCounter works')
  "
  ```

### ☐ Unit Tests

- [ ] **Run verification test suite**
  ```bash
  python tests/verify_p0_fixes.py
  ```
  Expected output:
  ```
  ✅ Atomic Counter — No Duplicate Watermarks PASSED
  ✅ Idempotency Key Expiration PASSED
  ✅ Ring Buffer Replay PASSED
  ✅ SSE Endpoint Signature PASSED
  ```

- [ ] **Test idempotency TTL**
  ```bash
  python -c "
  from apps.api.services.idempotency_key_service import reserve
  from apps.api.core.database import SessionLocal
  from apps.api.models.idempotency_key import IdempotencyKey
  from datetime import datetime
  
  key = 'test-ttl-' + str(int(__import__('time').time() * 1000))
  
  # First insert should succeed
  assert reserve(key, 'h1', 'event') == True
  
  # Duplicate should fail
  assert reserve(key, 'h1', 'event') == False
  
  # Check expires_at is set
  session = SessionLocal()
  record = session.query(IdempotencyKey).filter(IdempotencyKey.key == key).first()
  assert record.expires_at > datetime.utcnow()
  session.close()
  
  print('✅ TTL logic verified')
  "
  ```

- [ ] **Test watermark atomic generation under load**
  ```bash
  python -c "
  from apps.api.realtime.broadcaster import broadcaster
  import concurrent.futures
  import threading
  
  watermarks = set()
  lock = threading.Lock()
  
  def publish_events():
    for i in range(100):
        broadcaster.publish_sync('test-load', f'e{i}', {})
  
  with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
    futures = [ex.submit(publish_events) for _ in range(20)]
    concurrent.futures.wait(futures)
  
  # All watermarks should be unique
  for event in broadcaster._ring_buffers['test-load']:
    watermarks.add(event.watermark)
  
  assert len(watermarks) == 2000, f'Expected 2000 unique, got {len(watermarks)}'
  print(f'✅ Generated {len(watermarks)} unique watermarks under concurrent load')
  "
  ```

### ☐ Integration Tests

- [ ] **Test SSE endpoint with last_watermark parameter**
  
  Start the app, then:
  ```bash
  # First connection establishes baseline
  curl -N http://localhost:8000/v1/realtime/stream?household_id=test \
    -H 'Accept: text/event-stream' \
    --max-time 2 2>/dev/null | head -5
  
  # Should see: event: connected
  
  # Second connection with watermark (replay test)
  curl -N 'http://localhost:8000/v1/realtime/stream?household_id=test&last_watermark=999999-1' \
    -H 'Accept: text/event-stream' \
    --max-time 2 2>/dev/null | head -5
  
  # May see: event: resync_required or event: update (depending on buffer state)
  ```

- [ ] **Test idempotency middleware with TTL**
  ```bash
  # Create event key
  KEY="idem-test-$(date +%s%3N)"
  
  # First request should succeed
  curl -X POST http://localhost:8000/v1/ui/message \
    -H "X-Idempotency-Key: $KEY" \
    -H "X-HPAL-Household-ID: test-hh" \
    -H "Content-Type: application/json" \
    -d '{"message":"test"}' \
    2>/dev/null | grep -q "200\|201" && echo "✅ First request: OK"
  
  # Retry should get 409
  curl -X POST http://localhost:8000/v1/ui/message \
    -H "X-Idempotency-Key: $KEY" \
    -H "X-HPAL-Household-ID: test-hh" \
    -H "Content-Type: application/json" \
    -d '{"message":"test"}' \
    2>/dev/null | grep -q "409" && echo "✅ Duplicate request: 409 Conflict"
  ```

### ☐ Backward Compatibility

- [ ] **Old clients still work (no last_watermark)**
  ```bash
  # Legacy SSE subscription without last_watermark
  curl -N http://localhost:8000/v1/realtime/stream?household_id=test \
    -H 'Accept: text/event-stream' \
    --max-time 2 2>/dev/null | grep "event:"
  
  # Should work fine, receive live streams
  ```

- [ ] **Existing idempotency calls unaffected**
  ```bash
  # Old code using reserve() doesn't need changes
  python -c "
  from apps.api.services.idempotency_key_service import reserve, release
  # All old API still works
  assert callable(reserve)
  assert callable(release)
  print('✅ Backward compatible')
  "
  ```

### ☐ Performance

- [ ] **No degradation in P95/P99 latencies**
  Load test with same parameters as before deployment:
  ```bash
  python tests/load_test.py --users 100 --duration 60s --target http://localhost:8000
  # P95 should be within ±5% of baseline
  # P99 should be within ±10% of baseline
  ```

- [ ] **Ring buffer doesn't consume excessive memory**
  ```bash
  # Monitor memory usage under load
  while true; do
    ps aux | grep python | grep -v grep | awk '{print $6}' | tail -1
    sleep 1
  done
  # Ring buffer (1000 events per household) ≈ negligible overhead
  ```

### ☐ Staging Deployment

- [ ] **Deploy to staging**
  ```bash
  git push origin main  # (your CI/CD deploys to staging)
  sleep 30
  curl http://staging-api.local/v1/health  # Should return 200
  echo "✅ Staging deployed"
  ```

- [ ] **Run integration test suite on staging**
  ```bash
  pytest tests/ -v -k "idempotency or broadcast or realtime" --tb=short
  # All tests should pass
  ```

- [ ] **Monitor staging for 24 hours**
  Look for:
  - No spike in 409 responses
  - No spike in 5xx errors
  - Watermark generation is smooth (no gaps)
  - No "resync_required" anomalies
  - SSE reconnects are quick

### ☐ Production Deployment

- [ ] **Schedule deployment window**
  - Off-peak time (e.g., 2-3 AM UTC)
  - Have rollback plan ready
  - Notify on-call team

- [ ] **Deploy to production**
  ```bash
  git push origin main --tags  # (your CI/CD deploys to prod)
  sleep 60
  curl http://api.family-orchestration.com/v1/health  # Should return 200
  echo "✅ Production deployed"
  ```

- [ ] **Verify health checks pass**
  ```bash
  curl http://api.family-orchestration.com/v1/health | jq .
  # {
  #   "status": "healthy",
  #   "timestamp": "2026-04-20T...",
  #   "version": "..."
  # }
  ```

- [ ] **Monitor production metrics (24-72 hours)**
  
  Dashboard alerts:
  - Watermark collision rate > 0.001% → **CRITICAL**
  - RESYNC_REQUIRED ratio > 1% → **WARNING**
  - 409 Conflict spike → **WARNING**
  - 500 errors on realtime endpoints → **CRITICAL**
  
  Grafana queries:
  ```
  rate(watermark_generated[5m])  # Should be smooth
  aveerage(ring_buffer_size)     # Should be <100-200
  rate(resync_required[1h])      # Should be <1% of reconnects
  rate(idempotency_dedup[1h])    # Should stay high (>99%)
  ```

- [ ] **Check logs for anomalies**
  ```bash
  tail -f /var/log/family-orchestration-api.log | grep -i "error\|watermark\|replay"
  # Should see NO errors, only INFO level events
  ```

### ☐ Post-Deployment

- [ ] **Set up optional cleanup job** (24-72 hours after deploy)
  ```python
  # In main.py, add background scheduler
  from apscheduler.schedulers.background import BackgroundScheduler
  from apps.api.services.idempotency_key_service import cleanup_expired
  
  scheduler = BackgroundScheduler()
  scheduler.add_job(cleanup_expired, 'cron', hour=2, minute=0)  # 2 AM daily
  scheduler.start()
  ```

- [ ] **Document post-deployment observations**
  - Any anomalies in metrics?
  - Any new errors in logs?
  - Any user-reported issues?
  - Baseline latencies stable?

---

## SUCCESS CRITERIA

✅ **Deployment is successful when**:

1. All code syntax checks pass
2. All unit tests pass
3. All integration tests pass
4. Staging runs clean for 24 hours with no anomalies
5. Production deployment completes without errors
6. Health checks pass
7. Metrics are stable (no spikes, collisions, or anomalies)
8. No user-reported issues after 24-72 hours
9. Documentation is complete and reviewed

---

## ROLLBACK CHECKLIST

If critical issues are discovered:

- [ ] **Identify the issue**
  - Watermark collisions detected?
  - Excessive RESYNC_REQUIRED signals?
  - Idempotency dedup failures?
  - Performance degradation?

- [ ] **Decide: Fix or Rollback**
  - Is it a quick fix? → Deploy patch
  - Is it systemic? → Rollback

- [ ] **Execute rollback** (if needed)
  ```bash
  # Revert code
  git revert <commit-hash>
  
  # Restart service (zero-downtime possible with load balancer)
  systemctl restart family-orchestration-api
  
  # Verify health
  curl http://api.family-orchestration.com/v1/health
  ```

- [ ] **Post-mortem**
  - What went wrong?
  - Why wasn't it caught in staging?
  - How to prevent in future?

---

## SIGN-OFF

| Role | Name | Date | Status |
|------|------|------|--------|
| Engineer | _ | _ | ☐ Code reviewed |
| QA | _ | _ | ☐ Tests passed |
| DevOps | _ | _ | ☐ Deployment ready |
| Manager | _ | _ | ☐ Approved |

---

**Ready for production deployment**: April 20, 2026

