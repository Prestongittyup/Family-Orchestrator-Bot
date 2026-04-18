from __future__ import annotations

import threading

from apps.api.core.database import SessionLocal
from apps.api.models.task import Task

Job = Task

DEAD_LETTER_QUEUE: list[Job] = []
_dlq_lock = threading.Lock()


def move_to_dlq(job: Job, error: str, status: str = "dead_letter") -> None:
    """
    Move a permanently failed job to the dead letter queue.

    Updates in-memory job state and persists the given status when possible.
    Pass status="poisoned" for jobs that have exceeded the consecutive failure threshold.
    """
    job.status = status
    job.last_error = error

    # Best-effort persistence to DB for tracked jobs.
    if getattr(job, "id", None):
        session = SessionLocal()
        try:
            db_job = session.get(Task, job.id)
            if db_job is not None:
                db_job.status = status
                db_job.last_error = error
                session.commit()
        finally:
            session.close()

    with _dlq_lock:
        DEAD_LETTER_QUEUE.append(job)


def get_dlq() -> list[Job]:
    """Return a snapshot of dead-lettered jobs."""
    with _dlq_lock:
        return list(DEAD_LETTER_QUEUE)
