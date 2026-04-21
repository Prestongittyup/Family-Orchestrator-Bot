# P0 TORTURE TEST SUITE — EXECUTION GUIDE

**File**: `tests/test_p0_torture.py`  
**Purpose**: HIGH-INTENSITY chaos testing for distributed state correctness  
**Runtime**: 2-5 minutes (depending on test selection)  
**Risk**: None (read-only monitoring + isolated writes)

---

## WHAT THIS TESTS

This is **NOT** a unit test suite. This is a **deterministic chaos generator** designed to break the system if ANY correctness issues remain:

✅ **Watermark monotonicity** — no gaps, no duplicates, no out-of-order  
✅ **SSE replay correctness** — events replayed without loss or corruption  
✅ **Atomic counter** — no race conditions under concurrent load  
✅ **Idempotency TTL** — exactly once execution within TTL, reuse after expiry  
✅ **Buffer overflow handling** — RESYNC_REQUIRED signal on old watermark  
✅ **Chaos resilience** — system stable under concurrent writes + reconnects

---

## PREREQUISITES

1. **Running backend server**:
   ```bash
   cd /path/to/Family-Orchestration-Bot
   
   # In venv
   python apps/api/main.py
   # OR
   uvicorn apps.api.main:app --reload --host 0.0.0.0 --port 8000
   ```

2. **Python dependencies**:
   ```bash
   pip install pytest pytest-asyncio httpx
   ```

   If not already installed:
   ```bash
   cd /path/to/project
   pip install -e .
   # OR
   pip install -r requirements-dev.txt
   ```

3. **Backend must have P0 fixes deployed**:
   - ✅ `expires_at` column in `IdempotencyKey` model
   - ✅ Atomic counter in broadcaster
   - ✅ Ring buffer in broadcaster
   - ✅ `last_watermark` parameter in SSE endpoint

---

## RUNNING THE TESTS

### Option 1: Run All Tests (Full Suite)

```bash
cd /path/to/project

# Run all torture tests
pytest tests/test_p0_torture.py -v -s

# Expected output:
# tests/test_p0_torture.py::test_suite_summary PASSED
# tests/test_p0_torture.py::TestReconnectTortureLoop::test_reconnect_500_iterations PASSED
# tests/test_p0_torture.py::TestReconnectTortureLoop::test_quick_reconnect_flood PASSED
# ... (more tests)
# 
# ============== 10 passed in 3m42s ==============
```

### Option 2: Run Individual Tests

**TEST 1 — Reconnect Torture** (2 min):
```bash
# Full 500-iteration reconnect loop
pytest tests/test_p0_torture.py::TestReconnectTortureLoop::test_reconnect_500_iterations -v -s

# Quick version (100 reconnects)
pytest tests/test_p0_torture.py::TestReconnectTortureLoop::test_quick_reconnect_flood -v -s
```

**TEST 2 — Replay + Live Overlap** (1 min):
```bash
pytest tests/test_p0_torture.py::TestReplayLiveOverlap::test_replay_during_concurrent_writes -v -s
```

**TEST 3 — Concurrent Write Storm** (1 min):
```bash
pytest tests/test_p0_torture.py::TestConcurrentWriteStorm::test_50_parallel_writes -v -s
```

**TEST 4 — Idempotency TTL** (30 sec):
```bash
pytest tests/test_p0_torture.py::TestIdempotencyTTL -v -s
```

**TEST 5 — Buffer Overflow** (30 sec):
```bash
pytest tests/test_p0_torture.py::TestBufferOverflow::test_old_watermark_triggers_resync -v -s
```

**TEST 6 — Chaos Mix** (30 sec):
```bash
pytest tests/test_p0_torture.py::TestChaosMix::test_30_second_chaos -v -s
```

### Option 3: Run with Custom Settings

**Increase verbosity** (see all SSE events):
```bash
pytest tests/test_p0_torture.py -vvv -s
```

**Run specific test with logging**:
```bash
pytest tests/test_p0_torture.py::TestReconnectTortureLoop::test_quick_reconnect_flood -v -s --log-cli-level=DEBUG
```

**Run against custom server**:

Edit `tests/test_p0_torture.py` line ~45:
```python
SERVER_BASE_URL = "http://your-server:8000"  # Change this
```

Then run:
```bash
pytest tests/test_p0_torture.py -v -s
```

---

## UNDERSTANDING THE OUTPUT

### ✅ Test Passed

```
TEST 1: RECONNECT TORTURE LOOP (500 iterations)
=======================================================================
Iteration 0/500...
Iteration 50/500...
...
Iteration 450/500...
Collected 2345 events total
Unique watermarks: 2345
Total watermarks: 2345
✅ TEST 1 PASSED: Reconnect loop stable
```

### ❌ Test Failed

```
TEST 3: CONCURRENT WRITE STORM (50 parallel)
=======================================================================
Collected 47 watermarks from parallel writes
FAILED tests/test_p0_torture.py::TestConcurrentWriteStorm::test_50_parallel_writes - 
AssertionError: OUT_OF_ORDER under parallel load: Gap detected: 42 -> 44

Offending watermarks:
  ...
  1713607240000-40
  1713607240001-42    <-- gap here (no 41)
  1713607240001-44
  ...
```

---

## FAILURE TYPES DETECTED

### DUPLICATE_EVENT
```
AssertionError: DUPLICATE_EVENT: Duplicates found: {'1713607240000-42'}
```
**Meaning**: Same watermark emitted twice → data inconsistency  
**Likely cause**: Atomic counter race condition (SYS-03 broken)

### OUT_OF_ORDER
```
AssertionError: OUT_OF_ORDER: Out-of-order at index 42: 1713607240001-42 >= 1713607240001-42
```
**Meaning**: Watermarks not strictly increasing → event ordering violated  
**Likely cause**: Race in watermark generation or SSE ordering

### MISSING_EVENT
```
AssertionError: MISSING_EVENT: No events collected during replay
```
**Meaning**: Expected events not delivered via SSE  
**Likely cause**: SSE transport failure, timeout, or event loss

### IDEMPOTENCY_VIOLATION
```
AssertionError: IDEMPOTENCY_VIOLATION: Expected 409 for duplicate, got 200
```
**Meaning**: Same request executed twice within TTL  
**Likely cause**: Idempotency dedup broken (SYS-02 broken)

### PARTIAL_REPLAY
```
AssertionError: No event type resync_required detected
```
**Meaning**: Old watermark not triggering RESYNC_REQUIRED  
**Likely cause**: Ring buffer replay logic missing or broken

---

## EXPECTED RUNTIME

| Test | Duration | Notes |
|------|----------|-------|
| TEST 1 (500x reconnect) | ~2 min | Heaviest; most thorough |
| TEST 1b (100x quick) | ~30 sec | Fast version of TEST 1 |
| TEST 2 (replay overlap) | ~30 sec | Very stressful on ordering |
| TEST 3 (50 parallel) | ~45 sec | Tests atomic counter |
| TEST 4 (idempotency TTL) | ~20 sec | Quick TTL check |
| TEST 4b (concurrent) | ~10 sec | Tests concurrent dedup |
| TEST 5 (buffer overflow) | ~10 sec | Very quick |
| TEST 6 (30s chaos) | ~30 sec | Most realistic scenario |
| **TOTAL (all)** | **~4 minutes** | Recommended full run |

---

## SEQUENTIAL EXECUTION STRATEGY

For thorough validation, run in this order (most → least likely to find bugs):

```bash
# 1. Run heaviest torture (most likely to break)
pytest tests/test_p0_torture.py::TestReconnectTortureLoop::test_reconnect_500_iterations -v -s
# PASS → continue

# 2. Run chaos mix (most realistic)
pytest tests/test_p0_torture.py::TestChaosMix::test_30_second_chaos -v -s
# PASS → continue

# 3. Run concurrent writes (tests atomic counter)
pytest tests/test_p0_torture.py::TestConcurrentWriteStorm::test_50_parallel_writes -v -s
# PASS → continue

# 4. Run idempotency + TTL
pytest tests/test_p0_torture.py::TestIdempotencyTTL -v -s
# PASS → continue

# 5. Run replay edge cases
pytest tests/test_p0_torture.py::TestReplayLiveOverlap -v -s
# PASS → continue

# 6. Run buffer overflow edge cases
pytest tests/test_p0_torture.py::TestBufferOverflow -v -s
# PASS → all tests done!

echo "✅ All P0 torture tests passed — system is stable"
```

---

## DEBUGGING A FAILURE

### Step 1: Capture the Failure

```bash
# Run with maximum verbosity
pytest tests/test_p0_torture.py::TestReconnectTortureLoop -vvv -s \
  --log-cli-level=DEBUG \
  --tb=long \
  2>&1 | tee torture_failure.log
```

### Step 2: Review the Log

```bash
cat torture_failure.log | grep -A 10 "FAILED\|AssertionError\|OUT_OF_ORDER\|DUPLICATE"
```

### Step 3: Identify the Bug

Look for:
- **Watermark sequence**: any gaps, duplicates, or reversals?
- **Event ordering**: events arriving out of order?
- **SSE delivery**: some events missing?
- **Idempotency**: same request executed twice?

### Step 4: Reproduce Locally

Use a smaller test to reproduce:

```bash
# If TEST 1 fails: try quick version
pytest tests/test_p0_torture.py::TestReconnectTortureLoop::test_quick_reconnect_flood -v -s

# If TEST 3 fails: try smaller parallel writes
# (Edit test to use 10 instead of 50)

# If TEST 6 fails: try shorter chaos duration
# (Edit test to use 10 seconds instead of 30)
```

### Step 5: Check Logs

Check backend logs for errors:

```bash
# In backend terminal, look for:
tail -f /path/to/project/logs/*.log | grep -i "ERROR\|FATAL\|RACE\|DUPLICATE"
```

---

## INTEGRATION WITH CI/CD

Add to your CI pipeline:

**.github/workflows/test.yml** (or equivalent):

```yaml
- name: Run P0 Torture Tests
  run: |
    # Start backend in background
    python apps/api/main.py &
    BACKEND_PID=$!
    
    # Wait for startup
    sleep 5
    
    # Run tests
    pytest tests/test_p0_torture.py -v --tb=short
    
    # Stop backend
    kill $BACKEND_PID
```

---

## PRODUCTION VALIDATION

Before marking "READY FOR PRODUCTION", run:

```bash
# Run torture suite 3 times
for i in {1..3}; do
  echo "====== RUN $i/3 ======"
  pytest tests/test_p0_torture.py -v --tb=short
  if [ $? -ne 0 ]; then
    echo "❌ FAILED on run $i"
    exit 1
  fi
done

echo "✅ All 3 runs passed — system stable for production"
```

---

## TROUBLESHOOTING

### "Connection refused" error

**Problem**: Backend not running  
**Solution**:
```bash
# Terminal 1: Start backend
python apps/api/main.py

# Terminal 2: Run tests
pytest tests/test_p0_torture.py -v -s
```

### "409 Conflict" errors during idempotency test

**Problem**: Previous test left idempotency keys in DB  
**Solution**:
```bash
# Clean DB
rm /path/to/project/data/family_orchestration.db

# Restart backend
(restart your backend)

# Re-run tests
pytest tests/test_p0_torture.py -v -s
```

### Tests timeout

**Problem**: Backend is slow or unresponsive  
**Solution**:
- Increase `TEST_TIMEOUT` in test file (line ~52)
- Check backend CPU/memory
- Reduce test scale (use `test_quick_reconnect_flood` instead of `test_reconnect_500_iterations`)

### Inconsistent results

**Problem**: Tests sometimes pass, sometimes fail  
**Solution**: This indicates a RACE CONDITION. Congratulations, you found a bug!
- Increase `RANDOM_SEED` to try different random patterns
- Run multiple times to confirm reproducibility
- Check backend logs for timing-dependent errors

---

## METRICS TO MONITOR DURING TESTS

In a separate terminal, watch backend metrics:

```bash
# If you have Prometheus/Grafana:
curl http://localhost:9090/api/v1/query?query=watermark_generated

# If you have structured logging:
tail -f /path/to/project/logs/*.log | grep "watermark\|replay\|conflict"

# Or just check request rate:
watch -n 1 'netstat -tupan | grep ESTABLISHED | wc -l'
```

---

## ADVANCED: CUSTOMIZE TEST PARAMETERS

Edit `tests/test_p0_torture.py` to adjust:

```python
# Line 45-52: Server configuration
SERVER_BASE_URL = "http://localhost:8000"
TEST_HOUSEHOLD_ID = "torture-test-household"
TEST_TIMEOUT = 30.0
RANDOM_SEED = 42

# Test parameters:
# Line ~200: Change 500 to 1000 for longer reconnect loop
for iteration in range(1000):  # Was 500

# Line ~310: Change 50 to 100 for more concurrent writes
tasks = [... for _ in range(100)]  # Was 50

# Line ~520: Change 30.0 to 60.0 for longer chaos
chaos_duration = 60.0  # Was 30.0
```

---

## SUCCESS CRITERIA

✅ **Tests pass when**:

```
============== 10 passed in 4m23s ==============

All 10 tests:
  ✅ test_suite_summary
  ✅ test_reconnect_500_iterations
  ✅ test_quick_reconnect_flood
  ✅ test_replay_during_concurrent_writes
  ✅ test_50_parallel_writes
  ✅ test_ttl_boundary_exact
  ✅ test_concurrent_retries_at_boundary
  ✅ test_old_watermark_triggers_resync
  ✅ test_30_second_chaos
  
No failures, no timeouts, no data corruption detected.
```

---

## FAQ

**Q: Can I run tests against production?**  
A: NOT RECOMMENDED. These tests write to the database. Use staging only.

**Q: Why is TEST 1 so long?**  
A: 500 reconnects is the sweet spot for detecting rare race conditions. More iterations = higher confidence.

**Q: What if a test is flaky (sometimes passes, sometimes fails)?**  
A: This indicates a race condition. The test is doing its job—finding bugs!

**Q: Can I add my own tests?**  
A: Yes! Follow the pattern of existing tests. Key: use `ThreadSafeWatermarkCollector` to track events safely.

**Q: How do I know my system is "stable"?**  
A: Run full suite 3+ times with no failures. Monitor backend CPU/memory for anomalies.

---

## CONTACT / SUPPORT

If tests fail:
1. Check server logs for errors
2. Review failure type in output
3. Cross-reference against `FULL_SYSTEM_EXECUTION_SIMULATION_AUDIT.md` (SYS-01 through SYS-10)
4. Check `P0_FIXES_IMPLEMENTATION_SUMMARY.md` for expected behavior

---

**Last Updated**: April 20, 2026  
**Test Suite Version**: 1.0  
**Status**: Ready for CI/CD integration

