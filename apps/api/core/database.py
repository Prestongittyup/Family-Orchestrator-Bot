from __future__ import annotations

import os
from pathlib import Path
from threading import Lock

from sqlalchemy import create_engine, event
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import declarative_base, sessionmaker

from apps.api.observability.metrics import metrics


BASE_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "family_orchestration.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

DB_POOL_SIZE = max(1, int(os.getenv("DB_POOL_SIZE", "20")))
DB_POOL_TIMEOUT_SECONDS = 0.05
SQLITE_BUSY_TIMEOUT_SECONDS = 0.05

_pool_lock = Lock()
_pool_in_use = 0

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": SQLITE_BUSY_TIMEOUT_SECONDS},
    poolclass=QueuePool,
    pool_size=DB_POOL_SIZE,
    max_overflow=0,
    pool_timeout=DB_POOL_TIMEOUT_SECONDS,
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


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
