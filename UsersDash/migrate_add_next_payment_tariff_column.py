"""Добавляем колонку next_payment_tariff в accounts для фиксации выбранного тарифа."""

import os
import sqlite3
import sys
from pathlib import Path

# Позволяем запускать скрипт напрямую из любой директории
CURRENT_FILE = Path(__file__).resolve()
PACKAGE_ROOT = CURRENT_FILE.parent
REPO_ROOT = PACKAGE_ROOT.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from UsersDash.config import Config


def get_db_path_from_uri(uri: str) -> str:
    prefix = "sqlite:///"
    if not uri.startswith(prefix):
        raise RuntimeError(f"Ожидается SQLite URI вида 'sqlite:///...', а пришло: {uri}")
    return uri[len(prefix) :]


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

    if "next_payment_tariff" not in cols:
        print("[MIGRATE] Добавляем колонку next_payment_tariff (INTEGER)...")
        cur.execute("ALTER TABLE accounts ADD COLUMN next_payment_tariff INTEGER")

    conn.commit()
    conn.close()
    print("[MIGRATE] Готово, миграция успешно выполнена.")


if __name__ == "__main__":
    main()
