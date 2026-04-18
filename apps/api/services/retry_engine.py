from __future__ import annotations

from typing import Protocol

POISON_THRESHOLD = 3  # consecutive failures before a job is considered poisoned


class RetryableJob(Protocol):
    retry_count: int
    max_retries: int
    status: str


class PoisonableJob(Protocol):
    failure_count: int


def should_retry(job: RetryableJob) -> bool:
    """
    Determine whether a job is eligible for retry.

    Returns True when the job has retries remaining and is not dead-lettered.
    """
    return job.retry_count < job.max_retries and job.status != "dead_letter"


def is_poisoned(job: PoisonableJob) -> bool:
    """
    Returns True when consecutive failure count exceeds POISON_THRESHOLD.

    Poisoned jobs must be moved directly to DLQ and must not be retried.
    Checked before should_retry so poison always wins over retry eligibility.
    """
    return job.failure_count > POISON_THRESHOLD


def get_backoff_seconds(retry_count: int) -> float:
    """
    Exponential backoff in seconds: 0.5, 1, 2, 4, ...

    The first retry (retry_count=0) waits 0.5s.
    """
    safe_retry_count = max(0, retry_count)
    return 0.5 * (2 ** safe_retry_count)
