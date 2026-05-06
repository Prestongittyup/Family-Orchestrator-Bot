from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class DatabaseClient:
    """Minimal database adapter for centralized query execution."""

    def __init__(self, database_url: str) -> None:
        self._engine: Engine = create_engine(database_url, future=True, pool_pre_ping=True)

    def execute(self, query: str, params: dict[str, Any] | None = None) -> None:
        with self._engine.begin() as connection:
            connection.execute(text(query), params or {})

    def fetch_all(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        with self._engine.connect() as connection:
            rows = connection.execute(text(query), params or {}).mappings().all()
        return [dict(row) for row in rows]

    def fetch_one(self, query: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        with self._engine.connect() as connection:
            row = connection.execute(text(query), params or {}).mappings().first()
        return dict(row) if row is not None else None

    def close(self) -> None:
        self._engine.dispose()
