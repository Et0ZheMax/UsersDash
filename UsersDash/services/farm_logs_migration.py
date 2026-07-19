"""Идемпотентное обновление схемы централизованных логов ферм."""

from __future__ import annotations

from sqlalchemy import inspect, text

from UsersDash.models import db


_COLUMNS: tuple[tuple[str, str], ...] = (
    ("source_id", "VARCHAR(128)"),
    ("source_cursor", "INTEGER"),
    ("event_at_source", "VARCHAR(64)"),
    ("source_timezone", "VARCHAR(16)"),
    ("level", "VARCHAR(16) NOT NULL DEFAULT 'info'"),
    ("event_code", "VARCHAR(64)"),
    ("parser_version", "INTEGER NOT NULL DEFAULT 1"),
)

_INDEXES: tuple[str, ...] = (
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_farm_log_server_source "
    "ON farm_log_entries (server_id, source_id)",
    "CREATE INDEX IF NOT EXISTS idx_farm_logs_account_date_time_id "
    "ON farm_log_entries (account_id, event_date, event_time, id)",
    "CREATE INDEX IF NOT EXISTS idx_farm_logs_server_date_time_id "
    "ON farm_log_entries (server_id, event_date, event_time, id)",
    "CREATE INDEX IF NOT EXISTS idx_farm_logs_date_level_time_id "
    "ON farm_log_entries (event_date, level, event_time, id)",
    "CREATE INDEX IF NOT EXISTS idx_farm_logs_server_cursor "
    "ON farm_log_entries (server_id, source_cursor)",
)


def ensure_farm_logs_schema() -> None:
    """Добавляет новые колонки и индексы без удаления старых записей."""

    engine = db.engine
    inspector = inspect(engine)
    if "farm_log_entries" not in inspector.get_table_names():
        return

    existing_columns = {
        column["name"] for column in inspector.get_columns("farm_log_entries")
    }
    with engine.begin() as connection:
        for column_name, column_ddl in _COLUMNS:
            if column_name in existing_columns:
                continue
            connection.execute(
                text(f"ALTER TABLE farm_log_entries ADD COLUMN {column_name} {column_ddl}")
            )

        for statement in _INDEXES:
            connection.execute(text(statement))

        connection.execute(
            text("UPDATE farm_log_entries SET level='info' WHERE level IS NULL OR level='' ")
        )
        connection.execute(
            text("UPDATE farm_log_entries SET parser_version=1 WHERE parser_version IS NULL")
        )
