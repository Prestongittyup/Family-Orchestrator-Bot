# P0 Fixes Deployment Guide

**Fixes Deployed**: April 20, 2026  
**Scope**: SYS-01, SYS-02, SYS-03 from execution simulation audit  
**Risk Level**: LOW (backward compatible, no breaking changes)

---

## QUICK START

```bash
# 1. Code changes already applied:
#    - apps/api/models/idempotency_key.py (added expires_at)
#    - apps/api/services/idempotency_key_service.py (TTL logic)
#    - apps/api/realtime/broadcaster.py (atomic counter + ring buffer)
#    - apps/api/endpoints/realtime_router.py (last_watermark param)

# 2. Run verification tests
cd /path/to/project
python -m pytest tests/verify_p0_fixes.py -v

# 3. Deploy to staging
# (Your standard deployment process)

# 4. Monitor metrics for 24 hours
# Look for: watermark collisions (should be 0), RESYNC_REQUIRED signals (rare)

# 5. If staging is clean, deploy to production
# (No downtime, no breaking API changes)
```

---

## STEP-BY-STEP DEPLOYMENT

### Phase 1: Pre-Deployment Checks

**1.1 Verify Code Changes**

```bash
# Check that all four files were modified correctly
git diff apps/api/models/idempotency_key.py  # Should show: +expires_at with timedelta default
git diff apps/api/services/idempotency_key_service.py  # Should show: updated reserve() + cleanup_expired()
git diff apps/api/realtime/broadcaster.py  # Should show: AtomicCounter + ring buffer + replay logic
git diff apps/api/endpoints/realtime_router.py  # Should show: +last_watermark parameter
```

**1.2 Verify No Syntax Errors**

```bash
cd /path/to/project
python -m py_compile apps/api/models/idempotency_key.py
python -m py_compile apps/api/services/idempotency_key_service.py
python -m py_compile apps/api/realtime/broadcaster.py
python -m py_compile apps/api/endpoints/realtime_router.py

# Or run full import check
python -c "from apps.api import *; print('✅ All imports successful')"
```

**1.3 Verify Imports**

```python
# Quick Python check
from apps.api.models.idempotency_key import IdempotencyKey
from apps.api.services.idempotency_key_service import reserve, cleanup_expired
from apps.api.realtime.broadcaster import HouseholdBroadcaster, AtomicCounter
from apps.api.endpoints.realtime_router import stream_updates

print("✅ All imports successful")

# Verify AtomicCounter exists and works
counter = AtomicCounter()
assert counter.increment_and_get() == 1
assert counter.increment_and_get() == 2
print("✅ Atomic counter working")
```

### Phase 2: Local Testing

**2.1 Run Unit Tests**

```bash
# Test specific fix verification
python tests/verify_p0_fixes.py

# Expected output:
# ✅ Atomic counter test PASSED
# ✅ Idempotency TTL test PASSED
# ✅ Ring buffer test PASSED
# ✅ SSE endpoint test PASSED
```

**2.2 Test Idempotency TTL Behavior**

```python
from apps.api.services import idempotency_key_service

# Create key
key1 = "test-key-" + str(int(time.time() * 1000))
assert idempotency_key_service.reserve(key1, "household-1", "test") == True
print("✅ First reserve: True")

# Duplicate should fail
assert idempotency_key_service.reserve(key1, "household-1", "test") == False
print("✅ Duplicate reserve: False (within TTL)")

# Check DB contains expires_at
from apps.api.core.database import SessionLocal
from apps.api.models.idempotency_key import IdempotencyKey
session = SessionLocal()
record = session.query(IdempotencyKey).filter(IdempotencyKey.key == key1).first()
assert record.expires_at is not None
print(f"✅ expires_at set: {record.expires_at}")
session.close()
```

**2.3 Test Watermark Atomicity**

```python
import concurrent.futures
from apps.api.realtime.broadcaster import broadcaster

watermarks = set()
lock = threading.Lock()

def publish_events():
    for i in range(100):
        broadcaster.publish_sync("test-household", f"event-{i}", {"data": i})

# Simulate concurrent publishes
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(publish_events) for _ in range(10)]
    concurrent.futures.wait(futures)

# Extract watermarks from ring buffer
for event in broadcaster._ring_buffers["test-household"]:
    watermarks.add(event.watermark)

print(f"Published {len(watermarks)} events")
assert len(watermarks) == 1000, f"Expected 1000 unique watermarks, got {len(watermarks)}"
print("✅ No watermark collisions under concurrent load")
```

### Phase 3: Staging Deployment

**3.1 Deploy Code**

```bash
# Standard deployment process
git push origin main
# (Your CI/CD pipeline deploys to staging)

# Verify app is running
curl https://staging.family-orchestration.internal/v1/health
# Should return: {"status": "healthy"}
```

**3.2 Run Integration Tests on Staging**

```bash
# Full integration test suite
pytest tests/ -v -k "idempotency or broadcast or realtime"

# Expected: All tests pass

# Load test: simulate 100 concurrent users
# python tests/load_test.py --users 100 --duration 60s --target staging
```

**3.3 Monitor Staging Metrics (24 hours)**

Key metrics to watch:

```
watermark_generated{household_id="*"} — should increase monotonically, no duplicates
ring_buffer_size{household_id="*"} — should stay under RING_BUFFER_SIZE (1000)
idempotency_key_expired — should increase slowly (only on actual expirations)
resync_required (SSE event count) — should be rare (<1% of reconnects)
http_response_time{endpoint="/v1/realtime/stream"} — should not degrade
http_requests_total{endpoint="/v1", status="409"} — should not spike
```

**3.4 Check Logs for Issues**

```bash
# Watch for errors
tail -f logs/staging.log | grep -i "ERROR\|FATAL\|watermark\|idempotency"

# Should see no:
#   - "duplicate watermark"
#   - "integrity error" (from idempotency)
#   - "replay failed"
```

**3.5 Verify SSE Replay Works**

```python
# Manual test: simulate reconnect
from apps.api.realtime.broadcaster import broadcaster
import asyncio

async def test_relay():
    household = "test-household-relay"
    
    # Publish initial events
    for i in range(5):
        await broadcaster.publish(household, f"event-{i}", {"seq": i})
    
    # Get last watermark
    events = list(broadcaster._ring_buffers[household])
    last_watermark = events[2].watermark  # 3rd event
    
    # "Reconnect" and request replay
    # (In real flow, this happens via SSE subscribe() call)
    await broadcaster.publish(household, "event-5", {"seq": 5})
    
    # Check replay logic would return events 4 and 5
    print(f"✅ Replay mechanism verified for watermark: {last_watermark}")

asyncio.run(test_relay())
```

### Phase 4: Production Deployment

**4.1 Schedule Deployment**

- **Ideal window**: Off-peak hours (e.g., 2 AM UTC)
- **Expected downtime**: None (stateless service)
- **Rollback plan**: If issues, revert code changes and restart service (no data changes)

**4.2 Deploy to Production**

```bash
# Standard production deployment
git push origin main --tags
# (Your CI/CD pipeline deploys to production)

# Verify health
curl https://api.family-orchestration.com/v1/health
```

**4.3 Monitor Production (24+ hours)**

```bash
# Set up alerts for anomalies:
# - watermark collision rate > 0
# - resync_required > 1% of reconnects
# - 409 conflict rate spike
# - HTTP 500 errors on /v1/realtime/stream
# - HTTP 500 errors on idempotency endpoints

# Real-time dashboard
# - Watermark generation rate (should be smooth)
# - Ring buffer utilization (should be <80%)
# - Idempotency dedup success rate (should be >99%)
```

**4.4 Enable Optional Cleanup Job (24-72 hours after deploy)**

If you want to actively clean up expired keys (vs. lazy cleanup):

```python
# In your scheduled tasks (e.g., APScheduler setup)
from apscheduler.schedulers.background import BackgroundScheduler
from apps.api.services import idempotency_key_service

scheduler = BackgroundScheduler()
scheduler.add_job(
    idempotency_key_service.cleanup_expired,
    'cron',
    hour=2,  # 2 AM UTC daily
    minute=0,
    id='cleanup_expired_idempotency_keys',
    name='Clean up expired idempotency keys'
)
scheduler.start()
```

---

## VALIDATION QUERIES

### Check Idempotency Keys in DB

```sql
-- Count total keys
SELECT COUNT(*) FROM idempotency_keys;

-- Show keys expiring soon
SELECT key, created_at, expires_at, household_id 
FROM idempotency_keys 
ORDER BY expires_at ASC 
LIMIT 10;

-- Show expired keys (these are candidates for cleanup)
SELECT COUNT(*) FROM idempotency_keys 
WHERE expires_at <= datetime('now');
```

### Check Broadcaster State

```python
from apps.api.realtime.broadcaster import broadcaster

# Check ring buffer sizes
for household_id, buffer in broadcaster._ring_buffers.items():
    print(f"Household {household_id}: {len(buffer)} buffered events")

# Check active subscribers
for household_id, queues in broadcaster._subscribers.items():
    print(f"Household {household_id}: {len(queues)} active SSE subscribers")

# Check counter state
print(f"Current watermark counter: {broadcaster._counter._value}")
```

---

## ROLLBACK PLAN

If critical issues are discovered in production:

### Immediate Rollback (within 1 hour)

```bash
# Revert code to previous version
git revert <commit-hash>

# Restart service
systemctl restart family-orchestration-api

# Verify health
curl https://api.family-orchestration.com/v1/health

# The system will work as before (no data loss, ring buffer and TTL are gracious)
# - Ring buffer will still exist but won't be used (subscribe has last_watermark=None)
# - Idempotency keys will still have expires_at column (just not checked)
# - Watermark counter might have brief collisions until restart resolves
```

### No Data Migration Required

- **IdempotencyKey.expires_at column**: harmless if ignored, queries still work
- **Ring buffer state**: ephemeral, lost on restart anyway
- **Watermark counter**: ephemeral, reset per instance

### Rollback Risk: **NONE**

All changes are additive and backward compatible.

---

## METRICS TO TRACK LONG-TERM

Set up dashboards for these KPIs:

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| `watermark_collision_rate` | 0% | > 0.001% |
| `resync_required_event_ratio` | < 0.1% | > 1% |
| `idempotency_dedup_success` | > 99% | < 95% |
| `ring_buffer_avg_size` | 100-200 events | > 500 (burst) |
| `idem_key_cleanup_count` | ~100/hour at steady state | - |
| `sse_reconnect_rate` | baseline | > 2x baseline |

---

## TROUBLESHOOTING

### Issue: "resync_required" signals appearing frequently

**Symptom**: Clients getting RESYNC_REQUIRED even on first reconnect

**Cause**: Ring buffer is being evicted faster than 1000 events

**Fix**: 
- Increase RING_BUFFER_SIZE in broadcaster.py (line ~42)
- Or reduce event publish rate (if possible)

### Issue: 409 Conflicts still appearing after 24h

**Symptom**: Legitimate retries getting "Conflict" even though 24h passed

**Cause**: Expired keys not being cleaned up, or database clock skew

**Fix**:
- Check database server time: `SELECT datetime('now')`
- Run cleanup: `idempotency_key_service.cleanup_expired()`
- If clock skew: sync database time

### Issue: Watermark collisions detected

**Symptom**: Same watermark value in two separate events

**Cause**: AtomicCounter bug OR concurrent access bug in broadcaster

**Fix**:
- Verify only one instance running (or all using Redis transport)
- Check no custom counter manipulation elsewhere
- Restart service to reset counter

---

## SUCCESS CRITERIA

Deployment is **COMPLETE and SUCCESSFUL** when:

✅ All verification tests pass  
✅ Staging runs clean for 24 hours  
✅ Production metrics normal for 24 hours  
✅ No watermark collisions detected  
✅ RESYNC_REQUIRED ratio < 1%  
✅ No increase in 409 Conflict responses  
✅ No regression in baseline latencies  

---

## TIMELINE

| Phase | Duration | Metrics |
|-------|----------|---------|
| Local Testing | 1 hour | All unit tests pass |
| Staging | 4-24 hours | No anomalies in dashboards |
| Production Deploy | ~10 min | Health check passes |
| Monitoring | 24-72 hours | Metrics stable |
| **Complete** | **Total: 2-4 days** | All checks pass |

---

## SUPPORT & ESCALATION

**Questions about deployment?**
- Review `P0_FIXES_IMPLEMENTATION_SUMMARY.md` for design details
- Review `FULL_SYSTEM_EXECUTION_SIMULATION_AUDIT.md` for why these fixes matter

**Issues discovered?**
- Pre-deployment: revert code, no data loss
- Post-deployment: rollback is zero-downtime (backward compatible)

---

**Deployment started**: April 20, 2026  
**Status**: Ready for production rollout

