# P0 TORTURE TEST — QUICK REFERENCE CARD

**File**: `tests/test_p0_torture.py`  
**Run**: `pytest tests/test_p0_torture.py -v -s`  
**Time**: 4 minutes  
**Status**: Ready for CI/CD

---

## TESTS AT A GLANCE

| # | Test | Duration | What It Tests | Failure Mode |
|----|------|----------|---------------|--------------|
| 1 | Reconnect Torture | 2 min | SSE replay after 500 reconnects | Missing watermarks, duplicates, out-of-order |
| 1b | Quick Reconnect | 30 sec | SSE replay after 100 reconnects (fast) | Same as #1 |
| 2 | Replay + Live Overlap | 30 sec | Concurrent writes during replay | Interleaving corruption, boundary events |
| 3 | Concurrent Storm | 45 sec | 50 parallel writes, watermark ordering | Duplicate watermarks, gaps in sequence |
| 4 | Idempotency TTL | 20 sec | Same key => dedup, expires => reuse | Double execution within TTL |
| 4b | Concurrent Idempotency | 10 sec | 10 parallel identical requests | Race on dedup logic |
| 5 | Buffer Overflow | 10 sec | Old watermark => RESYNC_REQUIRED | Silent failure, partial replay |
| 6 | 30s Chaos | 30 sec | Writes + reconnects + delays + retries | Any of the above under realistic load |

---

## WHAT EACH TEST VALIDATES

### TEST 1 — Watermark Continuity Under Reconnects
```
Open connection
  ↓
Receive events (collect watermarks)
  ↓
Disconnect (simulate network blip)
  ↓
Reconnect with last_watermark
  ↓
Validate: no duplicates, no gaps, strictly increasing
  ↓
Repeat 500 times
```

**Detects**: Event loss on reconnect, ring buffer bugs, race on counter  
**Pass**: All watermarks ∈ strictly increasing sequence

---

### TEST 2 — Replay Order During Concurrent Writes
```
Start SSE with oldish watermark (trigger replay)
  ↓
WHILE replaying: fire 15 concurrent writes
  ↓
Validate: replayed events BEFORE new events, no interleaving
```

**Detects**: Ring buffer ordering bug, timestamp-based ordering race  
**Pass**: All replayed events come before live events in sequence

---

### TEST 3 — Atomic Watermark Counter
```
Fire 50 parallel POST requests
  ↓
Monitor SSE for emitted watermarks
  ↓
Validate: no duplicate sequences, no gaps
```

**Detects**: Dual-lock race condition (SYS-03), counter increment race  
**Pass**: All watermark sequences unique and monotonic

---

### TEST 4 — Idempotency Key TTL
```
POST with X-Idempotency-Key: K
  ↓
Immediate retry with same key
  ↓
Expect: 409 Conflict (dedup active)
```

**Detects**: Idempotency dedup broken, TTL not enforced  
**Pass**: First POST = 200, second POST = 409

---

### TEST 5 — Buffer Overflow Detection
```
Request very old watermark (1000000000000-999999)
  ↓
Expected: server responds with event: resync_required
```

**Detects**: Ring buffer doesn't detect overflow, no RESYNC_REQUIRED signal  
**Pass**: Server emits RESYNC_REQUIRED SSE event

---

### TEST 6 — Realistic Chaos (THE MOST IMPORTANT)
```
20 threads: random writes with 50-500ms delays
  ↓
Main thread: random reconnects every 1-10 seconds
  ↓
Monitor SSE for 30 seconds
  ↓
Validate: watermark integrity maintained under stress
```

**Detects**: Any of tests 1-5 under real-world conditions  
**Pass**: All watermarks strictly ordered, no duplicates

---

## FAILURE INTERPRETATION

### ❌ "DUPLICATE_EVENT"
```
AssertionError: DUPLICATE_EVENT: Duplicates found: {'1713607240000-42'}
```
**What broke**: Atomic counter (SYS-03)  
**Root cause**: Two events emitted with same watermark  
**Fix**: Check `AtomicCounter.increment_and_get()` — ensure single lock

---

### ❌ "OUT_OF_ORDER"
```
AssertionError: OUT_OF_ORDER: Out-of-order at index 42: 1000-42 >= 1000-44
```
**What broke**: Watermark generation or SSE delivery  
**Root cause**: Events received in wrong sequence order  
**Fix**: Check broadcaster._transport.publish() ordering

---

### ❌ "MISSING_EVENT"
```
AssertionError: MISSING_EVENT: No events collected during replay
```
**What broke**: SSE replay logic or transport  
**Root cause**: Events not delivered, timeout, or ring buffer empty  
**Fix**: Check `_replay_buffered_events()` or SSE connection

---

### ❌ "IDEMPOTENCY_VIOLATION"
```
AssertionError: IDEMPOTENCY_VIOLATION: Expected 409 for duplicate, got 200
```
**What broke**: Idempotency dedup (SYS-02)  
**Root cause**: Same key executed twice within TTL  
**Fix**: Check `reserve()` logic in `idempotency_key_service.py`

---

## RUN STRATEGIES

### 🎯 Quick Smoke Test (30 sec)
```bash
pytest tests/test_p0_torture.py::TestIdempotencyTTL -v -s
```
Checks if system is running, basic functionality works

### 🎯 Standard Full Suite (4 min)
```bash
pytest tests/test_p0_torture.py -v -s
```
Runs all 10 tests, recommended before deployment

### 🎯 Stress Test Heavy (7 min)
```bash
# Edit test_torture.py, change:
# - test_reconnect_500_iterations → 1000 iterations
# - test_50_parallel_writes → 100 parallel
# - test_30_second_chaos → 60 seconds

pytest tests/test_p0_torture.py::TestReconnectTortureLoop -v -s
pytest tests/test_p0_torture.py::TestConcurrentWriteStorm -v -s
pytest tests/test_p0_torture.py::TestChaosMix -v -s
```
Finds rare race conditions

### 🎯 Flakiness Hunt (10+ min)
```bash
for i in {1..5}; do
  echo "Run $i/5..."
  pytest tests/test_p0_torture.py -v -x || exit 1
done
```
Repeat until you find inconsistencies (indicates race)

---

## PERFORMANCE EXPECTATIONS

```
Server Health Check          0.5 sec
TEST 1  (500x reconnect)     ~2.0 min
TEST 1b (100x quick)         ~0.5 min
TEST 2  (replay overlap)     ~0.5 min
TEST 3  (50 parallel)        ~0.7 min
TEST 4  (TTL boundary)       ~0.3 min
TEST 4b (concurrent idem)    ~0.2 min
TEST 5  (buffer overflow)    ~0.2 min
TEST 6  (30s chaos)          ~0.5 min
─────────────────────────────────────
TOTAL                        ~4-5 min
```

**If it takes much longer**: backend is slow, may indicate thread contention or blocking I/O

---

## READINESS CHECKLIST

Before marking "READY FOR PRODUCTION":

```
□ TEST 1 passes (500x reconnect)
□ TEST 3 passes (50 parallel writes)
□ TEST 6 passes (30s chaos)
□ Full suite passes without timeouts
□ Full suite runs 2+ times in row, both pass
□ No "flaky" behavior (same results each run)
□ Backend logs show zero errors during tests
```

---

## TROUBLESHOOTING IN 30 SECONDS

| Symptom | Debug Command |
|---------|--------------|
| "Connection refused" | `curl http://localhost:8000/v1/health` |
| Multiple 409s in TEST 4 | `pytest ...::TestIdempotencyTTL::test_concurrent_retries_at_boundary -vv -s` |
| TEST 1 timeout | Increase `RING_BUFFER_SIZE` in broadcaster.py |
| TEST 2 out-of-order | Check SSE event ordering in broadcaster._fanout_local() |
| TEST 3 duplicates | Check AtomicCounter incrementing logic |
| TEST 5 no RESYNC | Check _replay_buffered_events() watermark parsing |
| TEST 6 intermittent | Reduce chaos duration to isolate issue |

---

## WHAT NOT TO WORRY ABOUT

✅ **Acceptable**: 
- Occasional timeout on very slow system (just re-run)
- RESYNC_REQUIRED signal in TEST 5 (expected if buffer full)
- Slight latency variance (network dependent)

❌ **NOT acceptable**:
- Duplicate watermarks
- Out-of-order events
- Same request executed twice
- Missing events after reconnect

---

## CI/CD INTEGRATION

Add to pipeline (e.g., GitHub Actions):

```yaml
- name: P0 Torture Tests
  run: |
    python apps/api/main.py &
    sleep 5
    pytest tests/test_p0_torture.py -v
    # Fail if any test fails
```

---

## ADVANCED: CUSTOM CONFIGURATION

Edit top of `test_p0_torture.py`:

```python
SERVER_BASE_URL = "http://your-server:8000"      # Change server
TEST_HOUSEHOLD_ID = "torture-test-household"     # Change household
TEST_TIMEOUT = 30.0                              # Increase for slow server
RANDOM_SEED = 42                                 # Change seed for different chaos patterns
```

Then run:
```bash
pytest tests/test_p0_torture.py -v -s
```

---

## SUCCESS EXAMPLE

```
====== Running P0 Torture Test Suite ======

tests/test_p0_torture.py::test_suite_summary PASSED
tests/test_p0_torture.py::TestReconnectTortureLoop::test_reconnect_500_iterations PASSED
tests/test_p0_torture.py::TestReconnectTortureLoop::test_quick_reconnect_flood PASSED
tests/test_p0_torture.py::TestReplayLiveOverlap::test_replay_during_concurrent_writes PASSED
tests/test_p0_torture.py::TestConcurrentWriteStorm::test_50_parallel_writes PASSED
tests/test_p0_torture.py::TestIdempotencyTTL::test_ttl_boundary_exact PASSED
tests/test_p0_torture.py::TestIdempotencyTTL::test_concurrent_retries_at_boundary PASSED
tests/test_p0_torture.py::TestBufferOverflow::test_old_watermark_triggers_resync PASSED
tests/test_p0_torture.py::TestChaosMix::test_30_second_chaos PASSED

============== 9 passed in 4m23s ==============

✅ READY FOR PRODUCTION
```

---

**Version**: 1.0 | **Date**: April 20, 2026 | **Status**: Ready

