# migrate_add_client_config_visibility.py
# Одноразовая миграция: создаёт таблицу client_config_visibility
# для управления видимостью настроек в кабинете клиента.

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

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='client_config_visibility'")
    exists = cur.fetchone()

    if exists:
        print("[MIGRATE] Таблица client_config_visibility уже существует — пропускаем создание.")
        conn.close()
        return

    print("[MIGRATE] Создаём таблицу client_config_visibility...")
    cur.execute(
        """
        CREATE TABLE client_config_visibility (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            script_id TEXT NOT NULL,
            config_key TEXT NOT NULL,
            group_key TEXT NULL,
            client_visible INTEGER NOT NULL DEFAULT 1,
            client_label TEXT NULL,
            order_index INTEGER NOT NULL DEFAULT 0,
            scope TEXT NOT NULL DEFAULT 'global'
        )
        """
    )
    cur.execute(
        "CREATE INDEX idx_client_config_visibility_script_scope ON client_config_visibility (script_id, scope)"
    )

    conn.commit()
    conn.close()
    print("[MIGRATE] Готово, миграция успешно выполнена.")


if __name__ == "__main__":
    main()
