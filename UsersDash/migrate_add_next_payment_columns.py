# migrate_add_next_payment_columns.py
# Одноразовая миграция: добавляет в таблицу accounts поля
# next_payment_at и next_payment_amount, если их ещё нет.
# ДАННЫЕ НЕ УДАЛЯЕМ.

import os
import sqlite3

from UsersDash.config import Config


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
        print("Нечего мигрировать. Возможно, БД будет создана заново при старте приложения.")
        return

    print(f"[MIGRATE] Открываем БД: {db_path}")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Смотрим, какие колонки уже есть в accounts
    cur.execute("PRAGMA table_info(accounts)")
    cols = [row[1] for row in cur.fetchall()]
    print(f"[MIGRATE] Текущие колонки в accounts: {cols}")

    # Добавляем next_payment_at, если нет
    if "next_payment_at" not in cols:
        print("[MIGRATE] Добавляем колонку next_payment_at (DATETIME)...")
        cur.execute("ALTER TABLE accounts ADD COLUMN next_payment_at DATETIME")

    # Добавляем next_payment_amount, если нет
    if "next_payment_amount" not in cols:
        print("[MIGRATE] Добавляем колонку next_payment_amount (INTEGER)...")
        cur.execute("ALTER TABLE accounts ADD COLUMN next_payment_amount INTEGER")

    conn.commit()
    conn.close()
    print("[MIGRATE] Готово, миграция успешно выполнена.")


if __name__ == "__main__":
    main()
