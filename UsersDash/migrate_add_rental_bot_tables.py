"""Миграция: таблицы Telegram-бота продления аренды."""

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
    """Точка входа миграции."""

    db_path = get_db_path_from_uri(Config.SQLALCHEMY_DATABASE_URI)
    if not os.path.exists(db_path):
        print(f"[MIGRATE] База данных не найдена: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    ensure_table(
        cur,
        """
        CREATE TABLE telegram_link_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            token_hash VARCHAR(128) NOT NULL UNIQUE,
            expires_at DATETIME NOT NULL,
            consumed_at DATETIME,
            created_by_user_id INTEGER,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "telegram_link_tokens",
    )

    ensure_table(
        cur,
        """
        CREATE TABLE telegram_bot_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            singleton_key VARCHAR(32) NOT NULL UNIQUE DEFAULT 'default',
            bot_username VARCHAR(128),
            default_timezone VARCHAR(64) NOT NULL DEFAULT 'Europe/Moscow',
            renew_duration_days INTEGER NOT NULL DEFAULT 30,
            renewal_price_rub INTEGER NOT NULL DEFAULT 0,
            payment_instructions TEXT,
            admin_contact VARCHAR(255),
            template_reminder_3d TEXT,
            template_reminder_1d TEXT,
            template_reminder_0d TEXT,
            template_expired TEXT,
            pending_admin_reminder_hours INTEGER NOT NULL DEFAULT 12,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "telegram_bot_settings",
    )

    ensure_table(
        cur,
        """
        CREATE TABLE rental_notification_logs (
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
            UNIQUE(account_id, stage, due_on)
        )
        """,
        "rental_notification_logs",
    )

    ensure_table(
        cur,
        """
        CREATE TABLE renewal_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_uid VARCHAR(36) NOT NULL UNIQUE,
            user_id INTEGER NOT NULL,
            account_id INTEGER NOT NULL,
            subscriber_id INTEGER,
            status VARCHAR(32) NOT NULL DEFAULT 'payment_pending_confirmation',
            amount_rub INTEGER,
            payment_method VARCHAR(64),
            comment TEXT,
            receipt_file_id VARCHAR(255),
            expected_days INTEGER NOT NULL DEFAULT 30,
            previous_paid_until DATETIME,
            requested_paid_until DATETIME,
            confirmed_paid_until DATETIME,
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
        "renewal_requests",
    )

    ensure_table(
        cur,
        """
        CREATE TABLE renewal_admin_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            renewal_request_id INTEGER NOT NULL,
            actor_user_id INTEGER NOT NULL,
            action_type VARCHAR(32) NOT NULL,
            details_json TEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "renewal_admin_actions",
    )


    cur.execute("PRAGMA table_info(renewal_requests)")
    renewal_cols = {row[1] for row in cur.fetchall()}
    if "last_admin_reminder_at" not in renewal_cols:
        cur.execute("ALTER TABLE renewal_requests ADD COLUMN last_admin_reminder_at DATETIME")
        print("[MIGRATE] Добавлена колонка renewal_requests.last_admin_reminder_at")

    conn.commit()
    conn.close()
    print('[MIGRATE] Миграция rental-бота завершена успешно')


if __name__ == '__main__':
    main()
