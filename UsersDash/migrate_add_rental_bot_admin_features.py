"""Миграция: расширенные настройки и уточнения для rental Telegram-бота."""

from __future__ import annotations

import os
import sqlite3
from typing import Iterable

from UsersDash.config import Config


def _db_path_from_uri(uri: str) -> str:
    """Возвращает путь к SQLite БД из SQLAlchemy URI."""

    prefix = "sqlite:///"
    if not uri.startswith(prefix):
        raise RuntimeError(f"Ожидается SQLite URI, получено: {uri}")
    return uri[len(prefix):]


def _ensure_column(cur: sqlite3.Cursor, table: str, column: str, ddl: str) -> None:
    """Добавляет колонку в таблицу, если она ещё не существует."""

    cur.execute(f"PRAGMA table_info({table})")
    columns = {row[1] for row in cur.fetchall()}
    if column in columns:
        print(f"[MIGRATE] Колонка {table}.{column} уже существует")
        return
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
    print(f"[MIGRATE] Добавлена колонка {table}.{column}")


def _index_columns(cur: sqlite3.Cursor, index_name: str) -> list[str]:
    """Возвращает набор колонок индекса в порядке объявления."""

    cur.execute(f"PRAGMA index_info({index_name})")
    rows = sorted(cur.fetchall(), key=lambda item: item[0])
    return [row[2] for row in rows]


def _has_unique_notification_key(cur: sqlite3.Cursor) -> bool:
    """Проверяет, что rental_notification_logs имеет уникальный ключ account+user+stage+due_on."""

    cur.execute("PRAGMA index_list(rental_notification_logs)")
    unique_indexes: Iterable[tuple] = [row for row in cur.fetchall() if row[2] == 1]
    expected = ["account_id", "user_id", "stage", "due_on"]
    for row in unique_indexes:
        index_name = row[1]
        if _index_columns(cur, index_name) == expected:
            return True
    return False


def _rebuild_rental_notification_logs(cur: sqlite3.Cursor) -> None:
    """Пересобирает rental_notification_logs с корректным уникальным ключом для идемпотентности."""

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='rental_notification_logs'"
    )
    if not cur.fetchone():
        print("[MIGRATE] Таблица rental_notification_logs отсутствует, пересборка не требуется")
        return

    if _has_unique_notification_key(cur):
        print("[MIGRATE] Уникальный ключ rental_notification_logs уже корректный")
        return

    cur.execute("DROP TABLE IF EXISTS rental_notification_logs_new")
    cur.execute(
        """
        CREATE TABLE rental_notification_logs_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            subscriber_id INTEGER,
            stage VARCHAR(32) NOT NULL,
            due_on DATE NOT NULL,
            status VARCHAR(24) NOT NULL DEFAULT 'sent',
            message_id VARCHAR(64),
            payload_json TEXT,
            error_text TEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(account_id, user_id, stage, due_on)
        )
        """
    )
    cur.execute(
        """
        INSERT OR REPLACE INTO rental_notification_logs_new(
            id, account_id, user_id, subscriber_id, stage, due_on, status, message_id, payload_json, error_text, created_at
        )
        SELECT id, account_id, user_id, subscriber_id, stage, due_on, status, message_id, payload_json, error_text, created_at
        FROM rental_notification_logs
        """
    )
    cur.execute("DROP TABLE rental_notification_logs")
    cur.execute("ALTER TABLE rental_notification_logs_new RENAME TO rental_notification_logs")
    cur.execute("CREATE INDEX idx_rental_notification_account_id ON rental_notification_logs(account_id)")
    cur.execute("CREATE INDEX idx_rental_notification_user_id ON rental_notification_logs(user_id)")
    cur.execute("CREATE INDEX idx_rental_notification_subscriber_id ON rental_notification_logs(subscriber_id)")
    cur.execute("CREATE INDEX idx_rental_notification_stage ON rental_notification_logs(stage)")
    cur.execute("CREATE INDEX idx_rental_notification_due_on ON rental_notification_logs(due_on)")
    cur.execute("CREATE INDEX idx_rental_notification_status ON rental_notification_logs(status)")
    cur.execute("CREATE INDEX idx_rental_notification_created_at ON rental_notification_logs(created_at)")
    print("[MIGRATE] Таблица rental_notification_logs пересобрана с UNIQUE(account_id, user_id, stage, due_on)")


def main() -> None:
    """Точка входа миграции."""

    db_path = _db_path_from_uri(Config.SQLALCHEMY_DATABASE_URI)
    if not os.path.exists(db_path):
        print(f"[MIGRATE] База данных не найдена: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    _ensure_column(cur, "telegram_subscribers", "reminders_enabled", "INTEGER NOT NULL DEFAULT 1")
    _ensure_column(cur, "telegram_subscribers", "pause_until", "DATETIME")

    _ensure_column(cur, "telegram_bot_settings", "payment_details_text", "TEXT")
    _ensure_column(cur, "telegram_bot_settings", "payment_instruction_text", "TEXT")
    _ensure_column(cur, "telegram_bot_settings", "support_contact", "VARCHAR(255)")
    _ensure_column(cur, "telegram_bot_settings", "reminder_days", "VARCHAR(64) NOT NULL DEFAULT '3,1,0,-1'")
    _ensure_column(cur, "telegram_bot_settings", "reminders_enabled", "INTEGER NOT NULL DEFAULT 1")

    _ensure_column(cur, "renewal_requests", "request_type", "VARCHAR(24) NOT NULL DEFAULT 'payment'")
    _ensure_column(cur, "renewal_requests", "status_before_needs_info", "VARCHAR(32)")

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='renewal_request_messages'")
    if not cur.fetchone():
        cur.execute(
            """
            CREATE TABLE renewal_request_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                renewal_request_id INTEGER NOT NULL,
                sender_role VARCHAR(16) NOT NULL,
                sender_user_id INTEGER,
                message_text TEXT,
                attachment_file_id VARCHAR(255),
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute("CREATE INDEX idx_renewal_request_messages_request_id ON renewal_request_messages(renewal_request_id)")
        cur.execute("CREATE INDEX idx_renewal_request_messages_sender_role ON renewal_request_messages(sender_role)")
        print("[MIGRATE] Создана таблица renewal_request_messages")
    else:
        print("[MIGRATE] Таблица renewal_request_messages уже существует")

    _rebuild_rental_notification_logs(cur)

    conn.commit()
    conn.close()
    print("[MIGRATE] Миграция расширений rental-бота завершена")


if __name__ == "__main__":
    main()
