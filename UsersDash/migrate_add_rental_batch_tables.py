"""Миграция: batch-таблицы Telegram-бота продления аренды."""

import os
import sqlite3

from UsersDash.config import Config


def get_db_path_from_uri(uri: str) -> str:
    """Преобразует SQLAlchemy URI sqlite:///... в путь к файлу БД."""

    prefix = "sqlite:///"
    if not uri.startswith(prefix):
        raise RuntimeError(f"Ожидается SQLite URI, получено: {uri}")
    return uri[len(prefix):]


def ensure_table(cur: sqlite3.Cursor, ddl: str, table_name: str) -> None:
    """Создаёт таблицу, если она ещё не существует."""

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    if cur.fetchone():
        print(f"[MIGRATE] Таблица {table_name} уже существует")
        return

    cur.execute(ddl)
    print(f"[MIGRATE] Создана таблица {table_name}")


def main() -> None:
    """Точка входа миграции batch-таблиц."""

    db_path = get_db_path_from_uri(Config.SQLALCHEMY_DATABASE_URI)
    if not os.path.exists(db_path):
        print(f"[MIGRATE] База данных не найдена: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    ensure_table(
        cur,
        """
        CREATE TABLE renewal_batch_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_uid VARCHAR(36) NOT NULL UNIQUE,
            user_id INTEGER NOT NULL,
            subscriber_id INTEGER,
            status VARCHAR(32) NOT NULL DEFAULT 'draft',
            mode VARCHAR(32),
            total_amount_rub INTEGER,
            payment_method VARCHAR(64),
            comment TEXT,
            receipt_file_id VARCHAR(255),
            confirmed_by_user_id INTEGER,
            confirmed_at DATETIME,
            rejected_by_user_id INTEGER,
            rejected_at DATETIME,
            rejection_reason TEXT,
            last_admin_reminder_at DATETIME,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "renewal_batch_requests",
    )

    ensure_table(
        cur,
        """
        CREATE TABLE renewal_batch_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_request_id INTEGER NOT NULL,
            account_id INTEGER NOT NULL,
            account_name_snapshot VARCHAR(128) NOT NULL,
            amount_rub_snapshot INTEGER,
            tariff_snapshot INTEGER,
            due_at_snapshot DATETIME,
            is_active_snapshot BOOLEAN,
            blocked_snapshot BOOLEAN,
            selected_for_renewal BOOLEAN NOT NULL DEFAULT 0,
            result_status VARCHAR(24) NOT NULL DEFAULT 'pending',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(batch_request_id, account_id)
        )
        """,
        "renewal_batch_items",
    )

    cur.execute("PRAGMA table_info(renewal_batch_items)")
    batch_item_cols = {row[1] for row in cur.fetchall()}
    if "is_active_snapshot" not in batch_item_cols:
        cur.execute("ALTER TABLE renewal_batch_items ADD COLUMN is_active_snapshot BOOLEAN")
        print("[MIGRATE] Добавлена колонка renewal_batch_items.is_active_snapshot")
    if "blocked_snapshot" not in batch_item_cols:
        cur.execute("ALTER TABLE renewal_batch_items ADD COLUMN blocked_snapshot BOOLEAN")
        print("[MIGRATE] Добавлена колонка renewal_batch_items.blocked_snapshot")
    if "updated_at" not in batch_item_cols:
        cur.execute("ALTER TABLE renewal_batch_items ADD COLUMN updated_at DATETIME")
        print("[MIGRATE] Добавлена колонка renewal_batch_items.updated_at")

    cur.execute(
        """
        UPDATE renewal_batch_items
        SET updated_at = created_at
        WHERE updated_at IS NULL
        """
    )
    print("[MIGRATE] Выполнен backfill renewal_batch_items.updated_at")

    ensure_table(
        cur,
        """
        CREATE TABLE renewal_batch_admin_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_request_id INTEGER NOT NULL,
            actor_user_id INTEGER NOT NULL,
            action_type VARCHAR(32) NOT NULL,
            details_json TEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "renewal_batch_admin_actions",
    )

    conn.commit()
    conn.close()
    print("[MIGRATE] Batch-миграция rental-бота завершена успешно")


if __name__ == "__main__":
    main()
