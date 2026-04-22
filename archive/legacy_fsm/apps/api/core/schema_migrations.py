from __future__ import annotations

from sqlalchemy import text

from apps.api.core.database import engine


def ensure_event_logs_idempotency_column() -> None:
    """
    Backfill-compatible schema guard.

    Ensures event_logs has idempotency_key (nullable) and an index.
    Existing rows are preserved and receive NULL for the new column.
    """
    with engine.begin() as conn:
        columns = conn.execute(text("PRAGMA table_info(event_logs)")).fetchall()
        column_names = {row[1] for row in columns}

        if "idempotency_key" not in column_names:
            conn.execute(text("ALTER TABLE event_logs ADD COLUMN idempotency_key VARCHAR NULL"))

        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_event_logs_idempotency_key "
                "ON event_logs (idempotency_key)"
            )
        )


def ensure_tasks_failure_tracking_columns() -> None:
    """
    Backfill-compatible schema guard.

    Ensures tasks has retry_count, max_retries, and last_error.
    Existing rows are preserved with safe defaults.
    """
    with engine.begin() as conn:
        columns = conn.execute(text("PRAGMA table_info(tasks)")).fetchall()
        column_names = {row[1] for row in columns}

        if "retry_count" not in column_names:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0"))

        if "max_retries" not in column_names:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 3"))

        if "last_error" not in column_names:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN last_error VARCHAR NULL"))

        if "force_fail" not in column_names:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN force_fail BOOLEAN NOT NULL DEFAULT 0"))

        if "failure_count" not in column_names:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0"))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS calendar_events (
                    id TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 3,
                    metadata TEXT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_calendar_events_household_start "
                "ON calendar_events (household_id, start_time)"
            )
        )


def ensure_idempotency_keys_table() -> None:
    """
    Non-destructive initialization for idempotency_keys.

    Safe for existing DBs: creates table/index only if missing.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS idempotency_keys (
                    key VARCHAR PRIMARY KEY,
                    household_id VARCHAR NOT NULL,
                    event_type VARCHAR NOT NULL,
                    created_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_idempotency_keys_household_id "
                "ON idempotency_keys (household_id)"
            )
        )