"""
EIL Storage Layer
-----------------
Pluggable backends for persisting and loading trace sessions.

Backends:
  JSONLStorageBackend  — append-only JSONL file (default, zero dependencies)
  SQLiteStorageBackend — SQLite event table (richer querying, still local)

Usage:
    from apps.api.observability.eil.storage import get_storage_backend
    from apps.api.observability.eil.tracer import set_persist_callback

    backend = get_storage_backend()
    set_persist_callback(backend.persist)

Loading traces for analysis:
    traces = backend.load_traces()
    traces = backend.load_traces(filters={"source": "runtime", "actor_type": "api_user"})
    traces = backend.load_traces(trace_id="trace-abc")
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Protocol

from apps.api.observability.eil.config import EILConfig, get_config
from apps.api.observability.eil.tracer import TraceSession


# ---------------------------------------------------------------------------
# Backend Protocol
# ---------------------------------------------------------------------------
class StorageBackend(Protocol):
    def persist(self, session: TraceSession) -> None: ...

    def load_traces(
        self,
        *,
        trace_id: str | None = None,
        filters: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]: ...

    def clear(self) -> None: ...


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------
def _session_to_dict(session: TraceSession) -> dict[str, Any]:
    return {
        "trace_id": session.trace_id,
        "actor_type": session.actor_type,
        "entrypoint": session.entrypoint,
        "source": session.source,
        "started_at": session.started_at,
        "ended_at": session.ended_at,
        "events": [
            {
                "event_type": event.event_type,
                "module": event.module,
                "function": event.function,
                "timestamp": event.timestamp,
                "depth": event.depth,
                "status": event.status,
                "error_type": event.error_type,
                "error_message": event.error_message,
            }
            for event in session.events
        ],
    }


def _matches_filters(row: dict[str, Any], filters: dict[str, str]) -> bool:
    for key, value in filters.items():
        if str(row.get(key, "")).lower() != value.lower():
            return False
    return True


# ---------------------------------------------------------------------------
# JSONL Backend
# ---------------------------------------------------------------------------
class JSONLStorageBackend:
    """Append-only JSONL file.  Thread-safe via a per-instance lock."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()

    def _ensure_dir(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def persist(self, session: TraceSession) -> None:
        self._ensure_dir()
        payload = json.dumps(_session_to_dict(session), sort_keys=True)
        with self._lock:
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(payload + "\n")

    def load_traces(
        self,
        *,
        trace_id: str | None = None,
        filters: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        if not self._path.exists():
            return []
        results: list[dict[str, Any]] = []
        with self._path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(row, dict):
                    continue
                if trace_id and row.get("trace_id") != trace_id:
                    continue
                if filters and not _matches_filters(row, filters):
                    continue
                results.append(row)
        return results

    def load_source_file(self, path: Path) -> list[dict[str, Any]]:
        """Load a JSON or JSONL file produced by test harnesses / CI."""
        if not path.exists():
            return []
        text = path.read_text(encoding="utf-8")
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return []
        if isinstance(payload, dict):
            return [payload]
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []

    def clear(self) -> None:
        if self._path.exists():
            self._path.unlink()


# ---------------------------------------------------------------------------
# SQLite Backend
# ---------------------------------------------------------------------------
_SCHEMA = """
CREATE TABLE IF NOT EXISTS traces (
    trace_id    TEXT PRIMARY KEY,
    actor_type  TEXT NOT NULL,
    entrypoint  TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'runtime',
    started_at  TEXT NOT NULL,
    ended_at    TEXT
);
CREATE TABLE IF NOT EXISTS trace_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id     TEXT NOT NULL REFERENCES traces(trace_id),
    event_type   TEXT NOT NULL,
    module       TEXT NOT NULL,
    function     TEXT NOT NULL,
    timestamp    TEXT NOT NULL,
    depth        INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'ok',
    error_type   TEXT,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_trace_id ON trace_events(trace_id);
CREATE INDEX IF NOT EXISTS idx_traces_source   ON traces(source);
CREATE INDEX IF NOT EXISTS idx_traces_actor    ON traces(actor_type);
"""


class SQLiteStorageBackend:
    """SQLite event table.  Supports richer querying by trace_id / filters."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(_SCHEMA)
                conn.commit()
            finally:
                conn.close()

    def persist(self, session: TraceSession) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO traces "
                    "(trace_id, actor_type, entrypoint, source, started_at, ended_at) "
                    "VALUES (?,?,?,?,?,?)",
                    (
                        session.trace_id,
                        session.actor_type,
                        session.entrypoint,
                        session.source,
                        session.started_at,
                        session.ended_at,
                    ),
                )
                for event in session.events:
                    conn.execute(
                        "INSERT INTO trace_events "
                        "(trace_id, event_type, module, function, timestamp, depth, "
                        " status, error_type, error_message) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            session.trace_id,
                            event.event_type,
                            event.module,
                            event.function,
                            event.timestamp,
                            event.depth,
                            event.status,
                            event.error_type,
                            event.error_message,
                        ),
                    )
                conn.commit()
            finally:
                conn.close()

    def load_traces(
        self,
        *,
        trace_id: str | None = None,
        filters: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        if not self._path.exists():
            return []
        with self._lock:
            conn = self._connect()
            try:
                where_parts: list[str] = []
                params: list[Any] = []
                if trace_id:
                    where_parts.append("t.trace_id = ?")
                    params.append(trace_id)
                if filters:
                    for col in ("actor_type", "source", "entrypoint"):
                        if col in filters:
                            where_parts.append(f"t.{col} = ?")
                            params.append(filters[col])

                where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
                rows = conn.execute(
                    f"SELECT * FROM traces {where_clause} ORDER BY started_at", params
                ).fetchall()

                results: list[dict[str, Any]] = []
                for row in rows:
                    t_id = row["trace_id"]
                    events = conn.execute(
                        "SELECT * FROM trace_events WHERE trace_id = ? ORDER BY id",
                        (t_id,),
                    ).fetchall()
                    results.append(
                        {
                            "trace_id": t_id,
                            "actor_type": row["actor_type"],
                            "entrypoint": row["entrypoint"],
                            "source": row["source"],
                            "started_at": row["started_at"],
                            "ended_at": row["ended_at"],
                            "events": [dict(e) for e in events],
                        }
                    )
                return results
            finally:
                conn.close()

    def clear(self) -> None:
        if self._path.exists():
            self._path.unlink()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------
_backend_instance: StorageBackend | None = None
_backend_lock = threading.Lock()


def get_storage_backend(config: EILConfig | None = None) -> StorageBackend:
    """Return the singleton storage backend for the current config.

    Backend type is determined by ``config.storage_backend``:
    - "sqlite" → SQLiteStorageBackend
    - anything else → JSONLStorageBackend  (default)
    """
    global _backend_instance
    with _backend_lock:
        if _backend_instance is None:
            cfg = config or get_config()
            if cfg.storage_backend == "sqlite":
                _backend_instance = SQLiteStorageBackend(cfg.sqlite_path)
            else:
                _backend_instance = JSONLStorageBackend(cfg.jsonl_path)
    return _backend_instance


def reset_storage_backend() -> None:
    """Force re-initialisation of the backend singleton (testing / config reload)."""
    global _backend_instance
    with _backend_lock:
        _backend_instance = None
