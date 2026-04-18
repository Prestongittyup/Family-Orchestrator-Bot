"""
Persistent in-memory worker loop for background job processing.
Runs in a separate thread to avoid blocking the API.
"""

from __future__ import annotations

import json
import logging
import threading
import time

from apps.api.core.database import SessionLocal
from apps.api.models.task import Task
from apps.api.services.dlq_service import move_to_dlq
from apps.api.services.retry_engine import get_backoff_seconds, is_poisoned, should_retry
from apps.api.services.task_service import set_job_status

# Global state
WORKER_RUNNING: bool = False
WORKER_HEALTH: bool = True          # False only after CATASTROPHIC_CRASH_THRESHOLD consecutive loop crashes
_consecutive_loop_crashes: int = 0
_worker_thread: threading.Thread | None = None
_worker_lock: threading.Lock = threading.Lock()

CATASTROPHIC_CRASH_THRESHOLD: int = 10  # consecutive outer-loop exceptions before health degrades

logger = logging.getLogger(__name__)


def get_next_job() -> Task | None:
    """
    Retrieve the next queued job (Task with status='queued').
    
    Returns:
        Task object if a queued job exists, None otherwise
    """
    session = SessionLocal()
    try:
        job = session.query(Task).filter(Task.status == "queued").order_by(Task.created_at).first()
        if job:
            # Detach from session to avoid issues in worker thread
            session.expunge(job)
        return job
    finally:
        session.close()


def _record_job_failure(job_id: str, error: str) -> Task | None:
    """
    Persist failure details on the job and return a detached job snapshot.
    """
    session = SessionLocal()
    try:
        job = session.get(Task, job_id)
        if job is None:
            return None

        job.retry_count = job.retry_count + 1
        job.last_error = error
        job.status = "failed"
        job.failure_count = job.failure_count + 1
        session.commit()
        session.refresh(job)
        session.expunge(job)
        return job
    finally:
        session.close()


def _reset_failure_count(job_id: str) -> None:
    """Reset consecutive failure counter after a successful job execution."""
    session = SessionLocal()
    try:
        job = session.get(Task, job_id)
        if job is not None:
            job.failure_count = 0
            session.commit()
    finally:
        session.close()


def _get_action_payload(job: Task) -> dict[str, object]:
    """
    Decode action payload metadata from job.description when present.
    """
    if not isinstance(job.description, str):
        return {}

    prefix = "ActionPayload: "
    if not job.description.startswith(prefix):
        return {}

    raw_payload = job.description[len(prefix) :]
    try:
        parsed = json.loads(raw_payload)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return {}

    return {}


def _worker_loop() -> None:
    """
    Main worker loop that continuously processes jobs.
    Runs in background thread while WORKER_RUNNING is True.
    """
    global WORKER_HEALTH, _consecutive_loop_crashes

    logger.info("Worker loop started")

    while WORKER_RUNNING:
        # ── Outer isolation ────────────────────────────────────────────────────
        # Catches catastrophic infrastructure failures (DB unavailable, OOM, …).
        # A single job raising inside the inner try/except will NEVER reach here.
        try:
            job = get_next_job()

            if job:
                logger.info("Processing job: %s - %s", job.id, job.title)

                # Transition: queued → running
                set_job_status(job.id, "running")

                # ── Inner isolation ────────────────────────────────────────────
                # Job-level failures are fully contained here; the outer loop
                # continues unaffected regardless of what happens inside.
                try:
                    if job.force_fail:
                        raise Exception("Simulated failure")

                    payload = _get_action_payload(job)
                    if payload.get("force_fail") is True:
                        raise Exception("Simulated failure")

                    # Simulate job execution
                    time.sleep(0.1)

                    # Transition: running → completed
                    set_job_status(job.id, "completed")
                    _reset_failure_count(job.id)
                    logger.info("Job %s completed", job.id)
                except Exception as exc:
                    error = str(exc)
                    failed_job = _record_job_failure(job.id, error)

                    if failed_job is None:
                        logger.warning("Failed job record not found for job_id=%s", job.id)
                    elif is_poisoned(failed_job):
                        move_to_dlq(failed_job, error, status="poisoned")
                        logger.error(
                            "Job %s poisoned after %s consecutive failures. Moved to DLQ. Error=%s",
                            failed_job.id,
                            failed_job.failure_count,
                            error,
                        )
                    elif should_retry(failed_job):
                        backoff_seconds = get_backoff_seconds(max(0, failed_job.retry_count - 1))
                        set_job_status(failed_job.id, "queued")
                        logger.warning(
                            "Job %s failed (retry_count=%s/%s). Retrying in %.2fs. Error=%s",
                            failed_job.id,
                            failed_job.retry_count,
                            failed_job.max_retries,
                            backoff_seconds,
                            error,
                        )
                        time.sleep(backoff_seconds)
                    else:
                        move_to_dlq(failed_job, error)
                        logger.error(
                            "Job %s moved to DLQ after %s retries. Error=%s",
                            failed_job.id,
                            failed_job.retry_count,
                            error,
                        )
            else:
                # No queued jobs — yield CPU with a minimal sleep.
                time.sleep(0.01)

            # Successful loop iteration: reset consecutive crash counter.
            _consecutive_loop_crashes = 0

        except Exception as loop_exc:
            # Unexpected infrastructure-level failure (not a job failure).
            _consecutive_loop_crashes += 1
            logger.error(
                "Worker loop error (crash #%d): %s",
                _consecutive_loop_crashes,
                loop_exc,
                exc_info=True,
            )

            if _consecutive_loop_crashes > CATASTROPHIC_CRASH_THRESHOLD:
                WORKER_HEALTH = False
                logger.critical(
                    "Worker health degraded: %d consecutive loop crashes exceeded threshold of %d.",
                    _consecutive_loop_crashes,
                    CATASTROPHIC_CRASH_THRESHOLD,
                )

            # Back off briefly before retrying the loop to avoid tight crash loops.
            time.sleep(0.2)


def start_worker_loop() -> None:
    """
    Start the persistent worker loop in a background thread.
    Safe to call multiple times (idempotent).
    """
    global WORKER_RUNNING, _worker_thread
    
    with _worker_lock:
        if WORKER_RUNNING:
            logger.warning("Worker loop is already running")
            return
        
        WORKER_RUNNING = True
        _worker_thread = threading.Thread(target=_worker_loop, daemon=True)
        _worker_thread.start()
        logger.info("Worker loop thread started")


def stop_worker_loop() -> None:
    """
    Stop the persistent worker loop gracefully.
    """
    global WORKER_RUNNING, _worker_thread
    
    with _worker_lock:
        if not WORKER_RUNNING:
            logger.warning("Worker loop is not running")
            return
        
        WORKER_RUNNING = False
        logger.info("Worker loop stop signal sent")
        
        if _worker_thread:
            _worker_thread.join(timeout=5.0)
            logger.info("Worker loop thread joined")


def is_worker_running() -> bool:
    """Check if worker loop is currently running."""
    with _worker_lock:
        return WORKER_RUNNING


def is_worker_healthy() -> bool:
    """Return False when the worker has exceeded CATASTROPHIC_CRASH_THRESHOLD consecutive loop crashes."""
    return WORKER_HEALTH
