# migrate_add_server_api_token.py
# Одноразовая миграция: добавляет колонку api_token в таблицу servers.
# Рекомендуемый запуск из корня репозитория (Windows/Linux):
#   python -m UsersDash.migrate_add_server_api_token

import importlib.util
import os
import sqlite3
import sys
from pathlib import Path


repo_root = Path(__file__).resolve().parent.parent
if importlib.util.find_spec("UsersDash") is None:
    sys.path.insert(0, str(repo_root))

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

    cur.execute("PRAGMA table_info(servers)")
    cols = [row[1] for row in cur.fetchall()]
    print(f"[MIGRATE] Колонки в servers: {cols}")

    if "api_token" not in cols:
        print("[MIGRATE] Добавляем колонку api_token (TEXT)...")
        cur.execute("ALTER TABLE servers ADD COLUMN api_token TEXT")

    conn.commit()
    conn.close()
    print("[MIGRATE] Готово, миграция успешно выполнена.")


if __name__ == "__main__":
    main()
