"""
Добавляем колонку blocked_for_payment в accounts для разделения блокировки по неоплате.
"""

import os
import sqlite3

from UsersDash.config import Config


def get_db_path_from_uri(uri: str) -> str:
    prefix = "sqlite:///"
    if not uri.startswith(prefix):
        raise RuntimeError(f"Ожидается SQLite URI вида 'sqlite:///...', а пришло: {uri}")
    return uri[len(prefix):]


def main():
    db_uri = Config.SQLALCHEMY_DATABASE_URI
    db_path = get_db_path_from_uri(db_uri)

    if not os.path.exists(db_path):
        print(f"[MIGRATE] Файл БД не найден: {db_path}")
        return

    print(f"[MIGRATE] Открываем БД: {db_path}")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(accounts)")
    cols = [row[1] for row in cur.fetchall()]
    print(f"[MIGRATE] Колонки в accounts: {cols}")

    if "blocked_for_payment" not in cols:
        print("[MIGRATE] Добавляем колонку blocked_for_payment (BOOLEAN, default false)...")
        cur.execute(
            "ALTER TABLE accounts ADD COLUMN blocked_for_payment BOOLEAN DEFAULT 0 NOT NULL"
        )

    conn.commit()
    conn.close()
    print("[MIGRATE] Готово, миграция успешно выполнена.")


if __name__ == "__main__":
    main()
