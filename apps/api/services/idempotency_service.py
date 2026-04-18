from __future__ import annotations

import threading

from apps.api.core.database import SessionLocal
from apps.api.models.event_log import EventLog


processed_keys: set[str] = set()
_processed_keys_lock = threading.Lock()


def is_duplicate(key: str) -> bool:
    with _processed_keys_lock:
        return key in processed_keys


def mark_processed(key: str) -> None:
    with _processed_keys_lock:
        processed_keys.add(key)


def load_processed_keys_from_db(limit: int | None = None) -> int:
    """
    Optionally preload existing idempotency keys from EventLog at startup.

    Args:
        limit: Optional maximum number of newest keys to load.

    Returns:
        Number of keys loaded into memory.
    """
    session = SessionLocal()
    try:
        query = session.query(EventLog.idempotency_key).filter(EventLog.idempotency_key.isnot(None))

        if limit is not None:
            query = query.order_by(EventLog.created_at.desc()).limit(limit)

        rows = query.all()
        keys = [row[0] for row in rows if row[0] is not None]

        with _processed_keys_lock:
            processed_keys.update(keys)

        return len(keys)
    finally:
        session.close()