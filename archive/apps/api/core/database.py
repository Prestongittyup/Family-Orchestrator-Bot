from __future__ import annotations

import logging
import os
from pathlib import Path
import sqlite3
from threading import Lock

from sqlalchemy import create_engine, event
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import declarative_base, sessionmaker

from archive.apps.api.observability.metrics import metrics


logger = logging.getLogger(__name__)


BASE_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "family_orchestration.db"
DEFAULT_DATABASE_URL = f"sqlite:///{DB_PATH}"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL).strip()
IS_SQLITE = DATABASE_URL.lower().startswith("sqlite")

DB_POOL_SIZE = max(1, int(os.getenv("DB_POOL_SIZE", "20")))
DB_POOL_TIMEOUT_SECONDS = max(0.5, float(os.getenv("DB_POOL_TIMEOUT_SECONDS", "5.0")))
SQLITE_BUSY_TIMEOUT_SECONDS = max(1.0, float(os.getenv("SQLITE_BUSY_TIMEOUT_SECONDS", "10.0")))
SQLITE_BUSY_TIMEOUT_MS = int(SQLITE_BUSY_TIMEOUT_SECONDS * 1000)
SQLITE_JOURNAL_MODE = os.getenv("SQLITE_JOURNAL_MODE", "WAL").strip().upper()
SQLITE_SYNCHRONOUS = os.getenv("SQLITE_SYNCHRONOUS", "NORMAL").strip().upper()

_VALID_SQLITE_JOURNAL_MODES = {"DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"}
_VALID_SQLITE_SYNCHRONOUS_MODES = {"OFF", "NORMAL", "FULL", "EXTRA"}

_pool_lock = Lock()
_pool_in_use = 0

engine_kwargs: dict[str, object] = {
    "poolclass": QueuePool,
    "pool_size": DB_POOL_SIZE,
    "max_overflow": 0,
    "pool_timeout": DB_POOL_TIMEOUT_SECONDS,
    "pool_pre_ping": True,
}
if IS_SQLITE:
    engine_kwargs["connect_args"] = {
        "check_same_thread": False,
        "timeout": SQLITE_BUSY_TIMEOUT_SECONDS,
    }

engine = create_engine(DATABASE_URL, **engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


@event.listens_for(engine, "connect")
def _on_connect(dbapi_connection, _connection_record) -> None:
    """Configure SQLite for safer concurrent read/write behavior."""
    if not IS_SQLITE:
        return

    cursor = dbapi_connection.cursor()
    try:
        try:
            cursor.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
            cursor.execute("PRAGMA foreign_keys=ON")
        except sqlite3.OperationalError:
            logger.warning("sqlite pragma init failed; continuing with defaults", exc_info=True)

        if SQLITE_JOURNAL_MODE in _VALID_SQLITE_JOURNAL_MODES:
            try:
                cursor.execute(f"PRAGMA journal_mode={SQLITE_JOURNAL_MODE}")
            except sqlite3.OperationalError:
                logger.warning(
                    "sqlite journal_mode '%s' failed; attempting DELETE fallback",
                    SQLITE_JOURNAL_MODE,
                    exc_info=True,
                )
                try:
                    cursor.execute("PRAGMA journal_mode=DELETE")
                except sqlite3.OperationalError:
                    logger.warning(
                        "sqlite journal_mode DELETE fallback failed; keeping engine default",
                        exc_info=True,
                    )
        else:
            logger.warning("invalid SQLITE_JOURNAL_MODE '%s'; ignoring", SQLITE_JOURNAL_MODE)

        if SQLITE_SYNCHRONOUS in _VALID_SQLITE_SYNCHRONOUS_MODES:
            try:
                cursor.execute(f"PRAGMA synchronous={SQLITE_SYNCHRONOUS}")
            except sqlite3.OperationalError:
                logger.warning(
                    "sqlite synchronous '%s' failed; keeping default",
                    SQLITE_SYNCHRONOUS,
                    exc_info=True,
                )
        else:
            logger.warning("invalid SQLITE_SYNCHRONOUS '%s'; ignoring", SQLITE_SYNCHRONOUS)
    finally:
        cursor.close()


@event.listens_for(engine, "checkout")
def _on_checkout(*_: object) -> None:
    global _pool_in_use
    with _pool_lock:
        _pool_in_use += 1
        metrics.gauge_set("db_pool_in_use", float(_pool_in_use))


@event.listens_for(engine, "checkin")
def _on_checkin(*_: object) -> None:
    global _pool_in_use
    with _pool_lock:
        _pool_in_use = max(0, _pool_in_use - 1)
        metrics.gauge_set("db_pool_in_use", float(_pool_in_use))
