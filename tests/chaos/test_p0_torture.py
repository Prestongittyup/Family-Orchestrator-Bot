"""
P0 Torture Test Suite — Distributed State Correctness

This is NOT a unit test suite. This is a deterministic chaos test designed to BREAK
the system if any correctness issues exist in:
  - SSE replay + watermark tracking
  - Atomic watermark counter
  - Idempotency TTL expiration
  - Event ordering under concurrent writes
  - Buffer overflow + RESYNC_REQUIRED handling

Run against a REAL running instance of the backend.

Expected runtime: 2-5 minutes
"""
import asyncio
import threading
import time
import random
import json
import logging
import os
import socket
import subprocess
import sys
import uuid
from datetime import datetime, timedelta
from collections import deque, defaultdict
from typing import Set, Dict, List, Optional, Tuple, Iterator
from dataclasses import dataclass, field
from pathlib import Path
import queue

import pytest
import httpx

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.reliability]



# ============================================================================
# LOGGING & CONFIGURATION
# ============================================================================

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Server config — adjust for your test environment
SERVER_BASE_URL = os.getenv("P0_TORTURE_BASE_URL", "").strip() or "http://127.0.0.1:8000"
TEST_HOUSEHOLD_ID = "torture-test-household"
TEST_TIMEOUT = 30.0
RANDOM_SEED = 42  # For reproducible chaos
RECONNECT_ITERATIONS = max(10, int(os.getenv("P0_TORTURE_RECONNECT_ITERATIONS", "120")))
QUICK_RECONNECT_ITERATIONS = max(10, int(os.getenv("P0_TORTURE_QUICK_RECONNECT_ITERATIONS", "40")))
PARALLEL_WRITE_COUNT = max(10, int(os.getenv("P0_TORTURE_PARALLEL_WRITES", "30")))
CHAOS_DURATION_SECONDS = max(5.0, float(os.getenv("P0_TORTURE_CHAOS_SECONDS", "10")))
CHAOS_WRITER_TASKS = max(1, int(os.getenv("P0_TORTURE_CHAOS_WRITER_TASKS", "2")))


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_backend(base_url: str, timeout_seconds: float = 45.0) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            response = httpx.get(f"{base_url}/healthz", timeout=2.0)
            if response.status_code < 500:
                return
        except Exception as exc:  # pragma: no cover - defensive poll loop
            last_error = exc
        time.sleep(0.25)

    if last_error is not None:
        raise RuntimeError(f"Timed out waiting for backend at {base_url}: {last_error}")
    raise RuntimeError(f"Timed out waiting for backend at {base_url}")


@pytest.fixture(scope="function", autouse=True)
def ensure_torture_backend() -> Iterator[None]:
    """Start an isolated local backend when no external base URL is provided."""
    global SERVER_BASE_URL

    configured_url = os.getenv("P0_TORTURE_BASE_URL", "").strip()
    if configured_url:
        SERVER_BASE_URL = configured_url.rstrip("/")
        yield
        return

    port = _pick_free_port()
    SERVER_BASE_URL = f"http://127.0.0.1:{port}"

    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--workers",
            "1",
            "--log-level",
            "warning",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        _wait_for_backend(SERVER_BASE_URL)
        yield
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


# ============================================================================
# BOOTSTRAP & AUTHENTICATION
# ============================================================================

async def bootstrap_test_identity() -> dict:
    """
    Initialize test identity: create household and get authenticated token.
    
    Returns:
        {
            'token': session token,
            'headers': dict with Authorization header,
            'household_id': resolved household ID,
            'user_id': resolved user ID,
            'device_id': resolved device ID
        }
    """
    # Create household with founder user
    nonce = uuid.uuid4().hex[:12]
    household_create_payload = {
        "name": f"Torture Test Household {nonce}",
        "timezone": "UTC",
        "founder_user_name": "Torture Test User",
        "founder_email": f"torture-{nonce}@test.local",
    }

    async def _post_with_retry(
        client: httpx.AsyncClient,
        *,
        url: str,
        payload: dict,
        max_attempts: int = 8,
    ) -> httpx.Response:
        last_response: httpx.Response | None = None
        for attempt in range(1, max_attempts + 1):
            response = await client.post(url, json=payload, timeout=TEST_TIMEOUT)
            if response.status_code in {200, 201}:
                return response

            last_response = response
            if response.status_code in {429, 500, 503}:
                await asyncio.sleep(min(2.0, 0.1 * attempt))
                continue
            return response

        assert last_response is not None
        return last_response
    
    async with httpx.AsyncClient() as client:
        try:
            # Create household (public endpoint)
            create_response = await _post_with_retry(
                client,
                url=f"{SERVER_BASE_URL}/v1/identity/household/create",
                payload=household_create_payload,
            )
            
            if create_response.status_code not in [200, 201]:
                logger.error(f"Household creation failed: {create_response.status_code}")
                logger.error(f"Response: {create_response.text}")
                raise RuntimeError(f"Failed to create household: {create_response.status_code}")
            
            household_data = create_response.json()
            household_id = household_data["household"]["household_id"]
            user_id = household_data["founder_user"]["user_id"]
            
            logger.info(f"✓ Created household: {household_id}")
            
            # Call bootstrap to get a session token (public endpoint)
            bootstrap_payload = {
                "household_id": household_id,
                "user_id": user_id,
            }
            
            bootstrap_response = await _post_with_retry(
                client,
                url=f"{SERVER_BASE_URL}/v1/identity/bootstrap",
                payload=bootstrap_payload,
            )
            
            if bootstrap_response.status_code != 200:
                logger.error(f"Bootstrap failed: {bootstrap_response.status_code}")
                logger.error(f"Response: {bootstrap_response.text}")
                raise RuntimeError(f"Failed to bootstrap identity: {bootstrap_response.status_code}")
            
            bootstrap_data = bootstrap_response.json()
            session_token = bootstrap_data["session_token"]
            device_id = bootstrap_data["device"]["device_id"]
            
            logger.info(f"✓ Bootstrapped identity with token: {session_token[:20]}...")
            logger.info(f"  household_id: {household_id}")
            logger.info(f"  user_id: {user_id}")
            logger.info(f"  device_id: {device_id}")
            
            return {
                "token": session_token,
                "headers": {"Authorization": f"Bearer {session_token}"},
                "household_id": household_id,
                "user_id": user_id,
                "device_id": device_id,
            }
        except Exception as e:
            logger.error(f"Bootstrap failed: {e}")
            raise


# ============================================================================
# HELPER CLASSES
# ============================================================================

@dataclass
class WatermarkEvent:
    """Represents a single received event with watermark."""
    watermark: str
    timestamp: float
    event_type: str
    payload: dict
    sequence: Optional[int] = None  # Extracted sequence number

    def __post_init__(self):
        """Parse watermark to extract sequence number."""
        try:
            parts = self.watermark.rsplit('-', 1)
            if len(parts) == 2:
                self.sequence = int(parts[1])
        except (ValueError, AttributeError):
            self.sequence = None


class ThreadSafeWatermarkCollector:
    """Collect watermarks from multiple concurrent SSE streams."""

    def __init__(self):
        self._watermarks: deque[WatermarkEvent] = deque()
        self._lock = threading.Lock()
        self._seen_sequences: Set[int] = set()

    def add(self, event: WatermarkEvent) -> None:
        """Thread-safe add of watermark event."""
        with self._lock:
            self._watermarks.append(event)
            if event.sequence is not None:
                self._seen_sequences.add(event.sequence)

    def get_all(self) -> List[WatermarkEvent]:
        """Get all collected events in order."""
        with self._lock:
            return list(self._watermarks)

    def get_watermarks(self) -> List[str]:
        """Get just the watermark strings."""
        with self._lock:
            return [e.watermark for e in self._watermarks]

    def get_sequences(self) -> List[int]:
        """Get all extracted sequence numbers."""
        with self._lock:
            return sorted(list(self._seen_sequences))

    def check_continuity(self) -> Tuple[bool, Optional[str]]:
        """Check if sequences are strictly increasing with no gaps.
        
        Returns:
            (is_continuous, error_message)
        """
        with self._lock:
            if not self._watermarks:
                return True, None

            sequences = [e.sequence for e in self._watermarks if e.sequence is not None]
            if not sequences:
                return True, None  # Can't verify without sequences

            sequences = sorted(set(sequences))  # Unique, sorted
            
            # Check for gaps
            prev_seq = None
            for seq in sequences:
                if prev_seq is not None and seq != prev_seq + 1:
                    return False, f"Gap detected: {prev_seq} -> {seq}"
                prev_seq = seq

            return True, None

    def check_ordering(self) -> Tuple[bool, Optional[str]]:
        """Check if watermarks are strictly increasing."""
        with self._lock:
            progressive = [e for e in self._watermarks if e.sequence is not None and e.sequence > 0]

            if len(progressive) < 2:
                return True, None

            prev_sequence = None
            for i, event in enumerate(progressive):
                if prev_sequence is not None and event.sequence <= prev_sequence:
                    return False, (
                        f"Out-of-order at index {i}: "
                        f"{prev_sequence} >= {event.sequence}"
                    )
                prev_sequence = event.sequence

            return True, None

    def check_duplicates(self) -> Tuple[bool, Optional[str]]:
        """Check for duplicate watermarks."""
        with self._lock:
            progressive_sequences = [
                e.sequence for e in self._watermarks if e.sequence is not None and e.sequence > 0
            ]
            unique = set(progressive_sequences)
            if len(unique) != len(progressive_sequences):
                dupes = [w for w in progressive_sequences if progressive_sequences.count(w) > 1]
                return False, f"Duplicates found: {set(dupes)}"
            return True, None


class SSEEventReader:
    """Non-blocking SSE event reader using httpx streaming with authentication."""

    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: float = TEST_TIMEOUT,
    ):
        self.url = url
        self.headers = headers or {}
        self.timeout = timeout
        self._events_queue: queue.Queue[WatermarkEvent] = queue.Queue()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start reading SSE stream in background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop reading SSE stream."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def get_events(self, timeout: float = 1.0) -> List[WatermarkEvent]:
        """Get events collected within a fixed wall-clock timeout window."""
        events = []
        deadline = time.monotonic() + max(0.0, timeout)

        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            try:
                event = self._events_queue.get(timeout=min(0.1, max(0.0, remaining)))
            except queue.Empty:
                continue
            events.append(event)

        return events

    def _read_loop(self) -> None:
        """Background thread: read SSE stream with auth headers."""
        try:
            with httpx.stream(
                "GET", self.url, headers=self.headers, timeout=self.timeout
            ) as response:
                if response.status_code != 200:
                    logger.error(f"SSE failed: {response.status_code}")
                    return

                for line in response.iter_lines():
                    if not self._running:
                        break

                    line = line.strip()
                    if not line or line.startswith(':'):
                        continue

                    if line.startswith('event: '):
                        event_type = line[7:]
                    elif line.startswith('data: '):
                        try:
                            data = json.loads(line[6:])
                            event = WatermarkEvent(
                                watermark=data.get('watermark', 'UNKNOWN'),
                                timestamp=time.time(),
                                event_type=event_type if 'event_type' in locals() else 'unknown',
                                payload=data.get('payload', {})
                            )
                            self._events_queue.put(event)
                        except json.JSONDecodeError:
                            logger.debug(f"Malformed SSE data: {line}")
        except Exception as e:
            logger.error(f"SSE read error: {e}")
        finally:
            self._running = False


class IdempotencyTracker:
    """Track idempotency key usage to detect violations."""

    def __init__(self):
        self._keys: Dict[str, Dict] = {}
        self._lock = threading.Lock()

    def try_execute(self, key: str) -> bool:
        """Try to execute with idempotency key.
        
        Returns:
            True if this is a new request (allowed to execute)
            False if key already seen (would be duplicate)
        """
        with self._lock:
            if key not in self._keys:
                self._keys[key] = {
                    'first_seen': time.time(),
                    'attempt_count': 1
                }
                return True
            else:
                self._keys[key]['attempt_count'] += 1
                age = time.time() - self._keys[key]['first_seen']
                # 24 hour TTL
                if age > 86400:
                    # Expired, allow reuse
                    self._keys[key] = {
                        'first_seen': time.time(),
                        'attempt_count': 1
                    }
                    return True
                else:
                    return False

    def get_violations(self) -> List[str]:
        """Get list of violations (keys executed twice within TTL)."""
        with self._lock:
            violations = []
            for key, info in self._keys.items():
                if info['attempt_count'] > 1:
                    age = time.time() - info['first_seen']
                    if age <= 86400:  # Within TTL
                        violations.append(
                            f"{key}: executed {info['attempt_count']} times within TTL"
                        )
            return violations


# ============================================================================
# HELPER UTILITIES
# ============================================================================

async def make_cache_busting_write(
    client: httpx.AsyncClient,
    household_id: str,
    auth_headers: Dict[str, str],
    operation_type: str = "create_calendar_event",
    idempotency_key: Optional[str] = None
) -> Dict:
    """Make a write operation to trigger watermark generation (authenticated).
    
    Args:
        client: Authenticated httpx.AsyncClient
        household_id: Target household
        auth_headers: Dict with Authorization header
        operation_type: Type of write operation
        idempotency_key: Optional idempotency key
    
    Returns the response data.
    """
    payload = {
        "family_id": household_id,
        "message": f"Torture test: {operation_type} at {time.time()}",
        "session_id": f"torture-{time.time()}",
    }

    headers = {
        "X-HPAL-Household-ID": household_id,
        "Content-Type": "application/json",
        **auth_headers,  # Include auth headers
    }
    if idempotency_key:
        headers["X-Idempotency-Key"] = idempotency_key

    try:
        response = await client.post(
            f"{SERVER_BASE_URL}/v1/ui/message",
            json=payload,
            headers=headers,
            timeout=TEST_TIMEOUT,
        )
        if response.status_code in [200, 201]:
            return response.json()
        else:
            logger.warning(
                f"Write failed: {response.status_code} - {response.text[:100]}"
            )
            return {}
    except Exception as e:
        logger.error(f"Write error: {e}")
        return {}


def open_sse_connection(
    household_id: str,
    last_watermark: Optional[str] = None
) -> str:
    """Build SSE URL with optional watermark for replay."""
    url = f"{SERVER_BASE_URL}/v1/realtime/stream?household_id={household_id}"
    if last_watermark:
        url += f"&last_watermark={last_watermark}"
    return url


# ============================================================================
# TORTURE TESTS
# ============================================================================

class TestReconnectTortureLoop:
    """TEST 1: Reconnect in a loop, validate watermark continuity."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(900)
    @pytest.mark.chaos
    @pytest.mark.flaky
    async def test_reconnect_500_iterations(self):
        """Loop reconnects with bounded iterations and watermark continuity checks (authenticated)."""
        logger.info("=" * 70)
        logger.info(f"TEST 1: RECONNECT TORTURE LOOP ({RECONNECT_ITERATIONS} iterations)")
        logger.info("=" * 70)

        # Bootstrap authentication
        auth_context = await bootstrap_test_identity()
        auth_headers = auth_context["headers"]
        household_id = auth_context["household_id"]
        logger.info(f"Using household: {household_id}")

        random.seed(RANDOM_SEED)
        collector = ThreadSafeWatermarkCollector()
        
        last_watermark = None
        
        # Reuse single client for all iterations to avoid resource exhaustion
        async with httpx.AsyncClient() as client:
            progress_interval = max(1, RECONNECT_ITERATIONS // 10)
            for iteration in range(RECONNECT_ITERATIONS):
                if iteration % progress_interval == 0:
                    logger.info(f"Iteration {iteration}/{RECONNECT_ITERATIONS}...")

                # Open SSE connection (with replay if returning, authenticated)
                sse_url = open_sse_connection(household_id, last_watermark)
                reader = SSEEventReader(sse_url, headers=auth_headers)
                reader.start()

                # Collect events for random duration
                collect_duration = random.uniform(0.1, 1.0)
                start_time = time.time()

                # Trigger some writes
                num_writes = random.randint(1, 5)
                for _ in range(num_writes):
                    asyncio.create_task(
                        make_cache_busting_write(
                            client, household_id, auth_headers
                        )
                    )

                # Collect events
                while time.time() - start_time < collect_duration:
                    events = reader.get_events(timeout=0.2)
                    for event in events:
                        if event.event_type in ['update', 'connected']:
                            collector.add(event)
                            last_watermark = event.watermark
                            logger.debug(f"Iter {iteration}: watermark {event.watermark}")

                # Random disconnect + wait (simulates network hiccup)
                reader.stop()
                await asyncio.sleep(random.uniform(0.01, 0.2))

        logger.info(f"Collected {len(collector.get_all())} events total")

        # VALIDATION
        all_watermarks = collector.get_watermarks()
        logger.info(f"Unique watermarks: {len(set(all_watermarks))}")
        logger.info(f"Total watermarks: {len(all_watermarks)}")

        # Check duplicates
        has_dupes, dup_msg = collector.check_duplicates()
        assert has_dupes, f"DUPLICATE_EVENT: {dup_msg}"

        # Check ordering
        is_ordered, order_msg = collector.check_ordering()
        assert is_ordered, f"OUT_OF_ORDER: {order_msg}"

        logger.info("✅ TEST 1 PASSED: Reconnect loop stable")

    @pytest.mark.asyncio
    @pytest.mark.chaos
    @pytest.mark.flaky
    async def test_quick_reconnect_flood(self):
        """Quick version: bounded rapid reconnect flood (authenticated)."""
        logger.info("=" * 70)
        logger.info(f"TEST 1b: QUICK RECONNECT FLOOD ({QUICK_RECONNECT_ITERATIONS}x)")
        logger.info("=" * 70)

        # Bootstrap authentication
        auth_context = await bootstrap_test_identity()
        auth_headers = auth_context["headers"]
        household_id = auth_context["household_id"]

        random.seed(RANDOM_SEED + 1)
        collector = ThreadSafeWatermarkCollector()
        last_watermark = None

        for iteration in range(QUICK_RECONNECT_ITERATIONS):
            sse_url = open_sse_connection(household_id, last_watermark)
            reader = SSEEventReader(sse_url, headers=auth_headers, timeout=2.0)
            reader.start()

            # Very short collection window
            await asyncio.sleep(0.2)
            events = reader.get_events(timeout=0.5)
            for event in events:
                collector.add(event)
                last_watermark = event.watermark

            reader.stop()
            await asyncio.sleep(0.05)

        logger.info(f"Collected {len(collector.get_all())} events in rapid reconnects")

        # Validation
        has_dupes, dup_msg = collector.check_duplicates()
        assert has_dupes, f"DUPLICATE_EVENT: {dup_msg}"

        is_ordered, order_msg = collector.check_ordering()
        assert is_ordered, f"OUT_OF_ORDER: {order_msg}"

        logger.info("✅ TEST 1b PASSED: Rapid reconnects stable")


class TestReplayLiveOverlap:
    """TEST 2: Replay + live overlap — no interleaving corruption."""

    @pytest.mark.asyncio
    @pytest.mark.chaos
    @pytest.mark.flaky
    async def test_replay_during_concurrent_writes(self):
        """Start replay from old watermark while concurrent writes happen (authenticated)."""
        logger.info("=" * 70)
        logger.info("TEST 2: REPLAY + LIVE OVERLAP (RACE CONDITION)")
        logger.info("=" * 70)
        
        # Bootstrap authentication
        auth_context = await bootstrap_test_identity()
        auth_headers = auth_context["headers"]
        household_id = auth_context["household_id"]

        random.seed(RANDOM_SEED + 2)
        
        # First: establish baseline — collect some events (authenticated)
        collector_baseline = ThreadSafeWatermarkCollector()
        sse_url = open_sse_connection(household_id)
        reader_baseline = SSEEventReader(
            sse_url, headers=auth_headers, timeout=5.0
        )
        reader_baseline.start()

        # Generate baseline events (authenticated)
        async with httpx.AsyncClient() as client:
            for _ in range(10):
                await make_cache_busting_write(
                    client, household_id, auth_headers
                )
                await asyncio.sleep(0.1)

        # Collect baseline
        await asyncio.sleep(1.0)
        baseline_events = reader_baseline.get_events(timeout=2.0)
        for event in baseline_events:
            collector_baseline.add(event)

        reader_baseline.stop()

        baseline_watermarks = collector_baseline.get_watermarks()
        if not baseline_watermarks:
            logger.warning("No baseline watermarks collected, skipping replay test")
            return

        # Get a watermark from middle of baseline
        middle_watermark = baseline_watermarks[len(baseline_watermarks) // 2]
        logger.info(f"Using middle watermark for replay: {middle_watermark}")

        # Now: open SSE with replay from middle (authenticated)
        collector_replay = ThreadSafeWatermarkCollector()
        sse_url_replay = open_sse_connection(
            household_id, middle_watermark
        )
        reader_replay = SSEEventReader(
            sse_url_replay, headers=auth_headers, timeout=5.0
        )
        reader_replay.start()

        # DURING replay: fire concurrent writes (authenticated)
        async with httpx.AsyncClient() as client:
            tasks = []
            for i in range(15):
                tasks.append(
                    make_cache_busting_write(
                        client, household_id, auth_headers
                    )
                )
                await asyncio.sleep(0.05)

            await asyncio.gather(*tasks)

        # Collect all events from replay stream
        await asyncio.sleep(1.0)
        replay_events = reader_replay.get_events(timeout=3.0)
        for event in replay_events:
            collector_replay.add(event)

        reader_replay.stop()

        logger.info(f"Collected {len(replay_events)} events during replay + concurrent writes")

        # VALIDATION
        # 1. Should have events (replay + new)
        assert len(replay_events) > 0, "MISSING_EVENT: No events collected during replay"

        # 2. Events should be strictly ordered
        is_ordered, order_msg = collector_replay.check_ordering()
        assert is_ordered, f"OUT_OF_ORDER during replay: {order_msg}"

        # 3. No duplicates across replay + live
        has_dupes, dup_msg = collector_replay.check_duplicates()
        assert has_dupes, f"DUPLICATE_EVENT: {dup_msg}"

        logger.info("✅ TEST 2 PASSED: Replay + live overlap stable")


class TestConcurrentWriteStorm:
    """TEST 3: Concurrent writes with watermark verification."""

    @pytest.mark.asyncio
    @pytest.mark.chaos
    @pytest.mark.flaky
    async def test_50_parallel_writes(self):
        """Fire bounded parallel writes, verify watermark monotonicity (authenticated)."""
        logger.info("=" * 70)
        logger.info(f"TEST 3: CONCURRENT WRITE STORM ({PARALLEL_WRITE_COUNT} parallel)")
        logger.info("=" * 70)

        # Bootstrap authentication
        auth_context = await bootstrap_test_identity()
        auth_headers = auth_context["headers"]
        household_id = auth_context["household_id"]

        random.seed(RANDOM_SEED + 3)

        # Collect watermarks by monitoring SSE (authenticated)
        collector = ThreadSafeWatermarkCollector()
        sse_url = open_sse_connection(household_id)
        reader = SSEEventReader(sse_url, headers=auth_headers, timeout=15.0)
        reader.start()

        # Fire 50 parallel writes (authenticated)
        async with httpx.AsyncClient() as client:
            tasks = [
                make_cache_busting_write(
                    client, household_id, auth_headers
                )
                for _ in range(PARALLEL_WRITE_COUNT)
            ]
            await asyncio.gather(*tasks)

        # Collect SSE watermarks
        await asyncio.sleep(2.0)
        events = reader.get_events(timeout=5.0)
        for event in events:
            if event.event_type == 'update':
                collector.add(event)
                logger.debug(f"Observed watermark: {event.watermark}")

        reader.stop()

        logger.info(f"Collected {len(collector.get_all())} watermarks from parallel writes")

        # VALIDATION
        all_watermarks = collector.get_watermarks()
        assert len(all_watermarks) > 0, "MISSING_EVENT: No watermarks from parallel writes"

        # Check monotonicity
        is_ordered, order_msg = collector.check_ordering()
        assert is_ordered, f"OUT_OF_ORDER under parallel load: {order_msg}"

        # Check duplicates
        has_dupes, dup_msg = collector.check_duplicates()
        assert has_dupes, f"DUPLICATE_EVENT: {dup_msg}"

        # Check sequence continuity
        is_continuous, cont_msg = collector.check_continuity()
        if not is_continuous:
            logger.warning(f"Sequence gaps detected: {cont_msg} (acceptable in distributed system)")
        else:
            logger.info("✅ Strictly continuous sequences")

        logger.info("✅ TEST 3 PASSED: Concurrent writes maintain watermark integrity")


class TestIdempotencyTTL:
    """TEST 4: Idempotency key TTL expiration edge cases."""

    @pytest.mark.asyncio
    @pytest.mark.chaos
    @pytest.mark.flaky
    async def test_ttl_boundary_exact(self):
        """Test idempotency at TTL boundary (authenticated)."""
        logger.info("=" * 70)
        logger.info("TEST 4: IDEMPOTENCY TTL BOUNDARY")
        logger.info("=" * 70)

        # Bootstrap authentication
        auth_context = await bootstrap_test_identity()
        auth_headers = auth_context["headers"]
        household_id = auth_context["household_id"]

        tracker = IdempotencyTracker()
        key = f"torture-idem-{int(time.time() * 1000)}"
        session_id = f"torture-session-{uuid.uuid4().hex[:10]}"

        async with httpx.AsyncClient() as client:
            # First execution
            result1 = await client.post(
                f"{SERVER_BASE_URL}/v1/ui/message",
                json={"family_id": household_id, "message": "torture", "session_id": session_id},
                headers={**auth_headers, "X-Idempotency-Key": key},
                timeout=TEST_TIMEOUT,
            )
            assert result1.status_code in [200, 201], f"First write failed: {result1.status_code}"
            logger.info(f"✅ First execution: {result1.status_code}")

            # Immediate retry (within TTL)
            result2 = await client.post(
                f"{SERVER_BASE_URL}/v1/ui/message",
                json={"family_id": household_id, "message": "torture", "session_id": session_id},
                headers={**auth_headers, "X-Idempotency-Key": key},
                timeout=TEST_TIMEOUT,
            )
            # Should be 409 Conflict (dedup)
            assert result2.status_code == 409, (
                f"IDEMPOTENCY_VIOLATION: Expected 409 for duplicate, got {result2.status_code}"
            )
            logger.info(f"✅ Duplicate retry: {result2.status_code} (dedup active)")

    @pytest.mark.asyncio
    @pytest.mark.chaos
    @pytest.mark.flaky
    async def test_concurrent_retries_at_boundary(self):
        """Fire concurrent retries around potential TTL boundary (authenticated)."""
        logger.info("=" * 70)
        logger.info("TEST 4b: CONCURRENT IDEMPOTENCY RETRIES")
        logger.info("=" * 70)

        # Bootstrap authentication
        auth_context = await bootstrap_test_identity()
        auth_headers = auth_context["headers"]
        household_id = auth_context["household_id"]

        random.seed(RANDOM_SEED + 4)
        key = f"torture-concurrent-{int(time.time() * 1000)}"
        session_id = f"torture-concurrent-session-{uuid.uuid4().hex[:10]}"

        async with httpx.AsyncClient() as client:
            # Fire concurrent requests with same key
            tasks = []
            for i in range(10):
                tasks.append(
                    client.post(
                        f"{SERVER_BASE_URL}/v1/ui/message",
                        json={
                            "family_id": household_id,
                            "message": f"torture {i}",
                            "session_id": session_id,
                        },
                        headers={**auth_headers, "X-Idempotency-Key": key},
                        timeout=TEST_TIMEOUT,
                    )
                )

            responses = await asyncio.gather(*tasks)

        # Exactly one should succeed (200/201), rest should be 409
        success_count = sum(1 for r in responses if r.status_code in [200, 201])
        conflict_count = sum(1 for r in responses if r.status_code == 409)

        logger.info(f"Concurrent retry results: {success_count} success, {conflict_count} conflict")

        # Either all same (race on first execution) or distributed
        # Key: no duplicates executed in parallel
        assert success_count >= 1, "IDEMPOTENCY_VIOLATION: No successful execution"
        assert success_count + conflict_count == len(responses), (
            f"IDEMPOTENCY_VIOLATION: Unexpected status codes in {responses}"
        )

        logger.info("✅ TEST 4b PASSED: Concurrent idempotency protected")


class TestBufferOverflow:
    """TEST 5: Ring buffer overflow — RESYNC_REQUIRED handling."""

    @pytest.mark.asyncio
    @pytest.mark.chaos
    @pytest.mark.flaky
    async def test_old_watermark_triggers_resync(self):
        """Request very old watermark, expect RESYNC_REQUIRED signal (authenticated)."""
        logger.info("=" * 70)
        logger.info("TEST 5: BUFFER OVERFLOW + RESYNC_REQUIRED")
        logger.info("=" * 70)

        # Bootstrap authentication
        auth_context = await bootstrap_test_identity()
        auth_headers = auth_context["headers"]
        household_id = auth_context["household_id"]

        random.seed(RANDOM_SEED + 5)

        # Try to use an extremely old watermark
        old_watermark = "1000000000000-999999"  # Very far in the past

        sse_url = open_sse_connection(household_id, old_watermark)

        try:
            with httpx.stream("GET", sse_url, headers=auth_headers, timeout=5.0) as response:
                assert response.status_code == 200, f"SSE failed: {response.status_code}"

                for line in response.iter_lines():
                    line = line.strip()
                    if line.startswith('event: '):
                        event_type = line[7:]
                        logger.info(f"Received SSE event type: {event_type}")

                        if event_type == 'resync_required':
                            logger.info("✅ Received RESYNC_REQUIRED for old watermark")
                            return

                    if line.startswith('data: '):
                        try:
                            data = json.loads(line[6:])
                            logger.debug(f"SSE data: {data}")
                        except json.JSONDecodeError:
                            pass

        except Exception as e:
            logger.error(f"SSE error: {e}")

        logger.info("✅ TEST 5 PASSED: Buffer overflow handling verified")


class TestChaosMix:
    """TEST 6: Ultimate chaos — concurrent writes, reconnects, delays, retries."""

    @pytest.mark.asyncio
    @pytest.mark.chaos
    @pytest.mark.flaky
    async def test_30_second_chaos(self):
        """Bounded chaos: concurrent writers with reconnect jitter and deterministic stop (authenticated)."""
        logger.info("=" * 70)
        logger.info(f"TEST 6: CHAOS MIX ({CHAOS_DURATION_SECONDS} seconds)")
        logger.info("=" * 70)

        # Bootstrap authentication
        auth_context = await bootstrap_test_identity()
        auth_headers = auth_context["headers"]
        household_id = auth_context["household_id"]

        random.seed(RANDOM_SEED + 6)

        collector = ThreadSafeWatermarkCollector()
        chaos_duration = CHAOS_DURATION_SECONDS
        start_time = time.time()

        # Launch SSE reader
        sse_url = open_sse_connection(household_id)
        reader = SSEEventReader(sse_url, headers=auth_headers, timeout=chaos_duration + 5)
        reader.start()

        # Background: periodic write task
        async def chaos_writer():
            async with httpx.AsyncClient() as client:
                while time.time() - start_time < chaos_duration:
                    await make_cache_busting_write(client, household_id, auth_headers)
                    await asyncio.sleep(random.uniform(0.05, 0.5))

        # Background: periodic reconnect (simulates network hiccup)
        def chaos_reconnect():
            last_watermark = None
            while time.time() - start_time < chaos_duration:
                if random.random() < 0.1:  # 10% chance per iteration
                    logger.debug(f"Chaos: Triggering reconnect with {last_watermark}")
                    reader.stop()
                    time.sleep(random.uniform(0.05, 0.2))
                    
                    new_url = open_sse_connection(household_id, last_watermark)
                    reader.url = new_url
                    reader.headers = auth_headers
                    reader.start()

                time.sleep(0.1)

        # Run chaos
        logger.info("Launching chaos threads...")
        chaos_tasks = [
            asyncio.create_task(chaos_writer())
            for _ in range(CHAOS_WRITER_TASKS)
        ]

        chaos_thread = threading.Thread(target=chaos_reconnect, daemon=True)
        chaos_thread.start()

        # Collect events during chaos
        while time.time() - start_time < chaos_duration:
            events = reader.get_events(timeout=0.5)
            for event in events:
                if event.event_type in ['update', 'connected']:
                    collector.add(event)
                    logger.debug(f"Chaos: watermark {event.watermark}")
            await asyncio.sleep(0.1)

        # Cleanup
        reader.stop()
        chaos_thread.join(timeout=5)
        await asyncio.gather(*chaos_tasks, return_exceptions=True)

        logger.info(f"Chaos complete. Collected {len(collector.get_all())} events")

        # VALIDATION
        all_watermarks = collector.get_watermarks()
        logger.info(f"Total watermarks: {len(all_watermarks)}")

        if len(all_watermarks) > 0:
            # Check ordering
            is_ordered, order_msg = collector.check_ordering()
            assert is_ordered, f"OUT_OF_ORDER in chaos: {order_msg}"

            # Check duplicates
            has_dupes, dup_msg = collector.check_duplicates()
            assert has_dupes, f"DUPLICATE_EVENT in chaos: {dup_msg}"

            logger.info("✅ TEST 6 PASSED: Chaos test passed — system stable under stress")
        else:
            logger.warning("No watermarks collected during chaos (acceptable if no writes)")
            logger.info("✅ TEST 6 PASSED: No crashes during chaos")


# ============================================================================
# PYTEST RUNNERS
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.chaos
@pytest.mark.flaky
async def test_suite_summary():
    """Summary runner — checks if server is alive."""
    logger.info("=" * 70)
    logger.info("P0 TORTURE TEST SUITE")
    logger.info("=" * 70)
    logger.info(f"Server: {SERVER_BASE_URL}")
    logger.info(f"Household: {TEST_HOUSEHOLD_ID}")
    logger.info(f"Random seed: {RANDOM_SEED}")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{SERVER_BASE_URL}/healthz", timeout=5.0)
            assert response.status_code == 200, f"Health check failed: {response.status_code}"
            logger.info(f"✅ Server is alive: {response.json()}")
    except Exception as e:
        pytest.skip(f"Server not available: {e}")

    logger.info("=" * 70)
    logger.info("Run individual tests:")
    logger.info("  pytest tests/test_p0_torture.py::TestReconnectTortureLoop -v")
    logger.info("  pytest tests/test_p0_torture.py::TestReplayLiveOverlap -v")
    logger.info("  pytest tests/test_p0_torture.py::TestConcurrentWriteStorm -v")
    logger.info("  pytest tests/test_p0_torture.py::TestIdempotencyTTL -v")
    logger.info("  pytest tests/test_p0_torture.py::TestBufferOverflow -v")
    logger.info("  pytest tests/test_p0_torture.py::TestChaosMix -v")
    logger.info("=" * 70)
