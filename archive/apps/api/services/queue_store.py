from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any

# Project root: .../Family Orchestration Bot
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
QUEUE_FILE = _PROJECT_ROOT / "runtime_queue.json"
PERSIST_INTERVAL_SECONDS = 2.0

_write_lock = threading.Lock()
_writer_thread: threading.Thread | None = None


def _normalize_queue(queue: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Defensive copy prevents caller mutations during serialization.
    return [dict(item) for item in queue]


def safe_write(queue: list[dict[str, Any]], file_path: Path = QUEUE_FILE) -> None:
    """Atomically write queue data using temp-file then rename."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = file_path.with_suffix(f"{file_path.suffix}.tmp")

    payload = _normalize_queue(queue)
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, separators=(",", ":"))
        f.flush()
        os.fsync(f.fileno())

    os.replace(temp_path, file_path)


def _save_worker(snapshot: list[dict[str, Any]], file_path: Path) -> None:
    try:
        safe_write(snapshot, file_path=file_path)
    except Exception:
        # Persistence is best-effort and should never crash worker processing.
        return


def save_queue(queue: list[dict[str, Any]], file_path: Path = QUEUE_FILE) -> None:
    """Persist queue snapshot in a background thread to avoid worker-loop blocking."""
    global _writer_thread

    snapshot = _normalize_queue(queue)

    with _write_lock:
        if _writer_thread is not None and _writer_thread.is_alive():
            # Skip if a write is already in flight; the next periodic tick can persist again.
            return

        _writer_thread = threading.Thread(
            target=_save_worker,
            args=(snapshot, file_path),
            daemon=True,
            name="queue-store-writer",
        )
        _writer_thread.start()


def load_queue(file_path: Path = QUEUE_FILE) -> list[dict[str, Any]]:
    """Load a persisted queue snapshot, returning [] for missing/corrupt files."""
    if not file_path.exists():
        return []

    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            return []

        normalized: list[dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict):
                normalized.append(dict(item))
        return normalized
    except (json.JSONDecodeError, OSError, ValueError, TypeError):
        try:
            corrupt_name = file_path.with_suffix(
                f"{file_path.suffix}.corrupt.{int(time.time())}"
            )
            file_path.replace(corrupt_name)
        except OSError:
            pass
        return []


def should_persist(last_persist_at: float, interval_seconds: float = PERSIST_INTERVAL_SECONDS) -> bool:
    """Helper for worker loop: call save_queue only on interval, not per message."""
    return (time.monotonic() - last_persist_at) >= interval_seconds
