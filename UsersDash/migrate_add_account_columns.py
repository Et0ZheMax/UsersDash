# migrate_add_account_columns.py
# Одноразовая миграция: добавляет в таблицу accounts поля
# next_payment_at, next_payment_amount, game_world, notes,
# если их ещё нет. Данные НЕ удаляются.

import os
import sqlite3

from config import Config


def get_db_path_from_uri(uri: str) -> str:
    """
    Преобразует SQLALCHEMY_DATABASE_URI вида 'sqlite:///data/app.db'
    в файловый путь 'data/app.db'.
    """
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
    print(f"[MIGRATE] Текущие колонки в accounts: {cols}")

    # Опциональные колонки, которые должны быть
    if "next_payment_at" not in cols:
        print("[MIGRATE] Добавляем колонку next_payment_at (DATETIME)...")
        cur.execute("ALTER TABLE accounts ADD COLUMN next_payment_at DATETIME")

    if "next_payment_amount" not in cols:
        print("[MIGRATE] Добавляем колонку next_payment_amount (INTEGER)...")
        cur.execute("ALTER TABLE accounts ADD COLUMN next_payment_amount INTEGER")

    if "game_world" not in cols:
        print("[MIGRATE] Добавляем колонку game_world (TEXT)...")
        cur.execute("ALTER TABLE accounts ADD COLUMN game_world TEXT")

    if "notes" not in cols:
        print("[MIGRATE] Добавляем колонку notes (TEXT)...")
        cur.execute("ALTER TABLE accounts ADD COLUMN notes TEXT")

    conn.commit()
    conn.close()
    print("[MIGRATE] Готово, миграция успешно выполнена.")


if __name__ == "__main__":
    main()
