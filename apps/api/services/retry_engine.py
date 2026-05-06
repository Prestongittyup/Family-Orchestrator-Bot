from __future__ import annotations

import random
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
    Exponential backoff in seconds with jitter and a hard cap.

    The first retry (retry_count=0) waits 0.5s.
    """
    safe_retry_count = max(0, retry_count)
    base_delay = 0.5 * (2 ** safe_retry_count)
    capped_delay = min(base_delay, 8.0)
    jitter = random.uniform(0.0, capped_delay * 0.25)
    return min(8.0, capped_delay + jitter)
