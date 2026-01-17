# migrate_add_farmdata_account_id.py
# Одноразовая миграция: добавляет в farm_data колонку account_id, заполняет её
# по совпадению user_id + farm_name с accounts, удаляет дубликаты и включает
# уникальность по account_id.

from __future__ import annotations

import os
import sqlite3
from datetime import datetime

from UsersDash.config import Config


FARMDATA_MERGE_FIELDS = (
    "email",
    "login",
    "password",
    "igg_id",
    "server",
    "telegram_tag",
)


def get_db_path_from_uri(uri: str) -> str:
    """
    Преобразует SQLALCHEMY_DATABASE_URI вида 'sqlite:///data/app.db'
    в файловый путь 'data/app.db'.
    """
    prefix = "sqlite:///"
    if not uri.startswith(prefix):
        raise RuntimeError(f"Ожидается SQLite URI вида 'sqlite:///...', а пришло: {uri}")
    return uri[len(prefix):]


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _sort_rows(rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
    def _key(row: sqlite3.Row) -> tuple:
        updated = _parse_dt(row["updated_at"]) or _parse_dt(row["created_at"]) or datetime.min
        return (updated, row["id"])

    return sorted(rows, key=_key, reverse=True)


def _merge_farmdata_rows(
    cur: sqlite3.Cursor,
    keep_row: sqlite3.Row,
    dup_rows: list[sqlite3.Row],
) -> None:
    updates: dict[str, str | None] = {}
    for field in FARMDATA_MERGE_FIELDS:
        current = keep_row[field]
        if current not in (None, ""):
            continue
        for row in dup_rows:
            candidate = row[field]
            if candidate not in (None, ""):
                updates[field] = candidate
                break

    if updates:
        assignments = ", ".join([f"{field} = ?" for field in updates])
        values = list(updates.values()) + [keep_row["id"]]
        cur.execute(
            f"UPDATE farm_data SET {assignments} WHERE id = ?",
            values,
        )


def _drop_index_if_exists(cur: sqlite3.Cursor, table: str, index_name: str) -> None:
    cur.execute(f"PRAGMA index_list({table})")
    for row in cur.fetchall():
        if row[1] == index_name:
            cur.execute(f"DROP INDEX {index_name}")
            break


def _dedupe_farm_data(cur: sqlite3.Cursor) -> int:
    cur.execute(
        """
        SELECT account_id
        FROM farm_data
        WHERE account_id IS NOT NULL
        GROUP BY account_id
        HAVING COUNT(*) > 1
        """
    )
    duplicates = [row[0] for row in cur.fetchall()]
    removed = 0

    for account_id in duplicates:
        cur.execute(
            """
            SELECT *
            FROM farm_data
            WHERE account_id = ?
            """,
            (account_id,),
        )
        rows = _sort_rows(cur.fetchall())
        keep_row, dup_rows = rows[0], rows[1:]
        _merge_farmdata_rows(cur, keep_row, dup_rows)

        for dup_row in dup_rows:
            cur.execute("DELETE FROM farm_data WHERE id = ?", (dup_row["id"],))
            removed += 1

    return removed


def main() -> None:
    db_uri = Config.SQLALCHEMY_DATABASE_URI
    db_path = get_db_path_from_uri(db_uri)

    if not os.path.exists(db_path):
        print(f"[MIGRATE] Файл БД не найден: {db_path}")
        return

    print(f"[MIGRATE] Открываем БД: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(farm_data)")
    cols = [row[1] for row in cur.fetchall()]
    print(f"[MIGRATE] Текущие колонки в farm_data: {cols}")

    if "account_id" not in cols:
        print("[MIGRATE] Добавляем колонку account_id (INTEGER)...")
        cur.execute("ALTER TABLE farm_data ADD COLUMN account_id INTEGER")

    print("[MIGRATE] Заполняем account_id из accounts по user_id + farm_name...")
    cur.execute(
        """
        UPDATE farm_data
        SET account_id = (
            SELECT accounts.id
            FROM accounts
            WHERE accounts.owner_id = farm_data.user_id
              AND accounts.name = farm_data.farm_name
        )
        WHERE account_id IS NULL
        """
    )

    print("[MIGRATE] Синхронизируем user_id и farm_name с accounts...")
    cur.execute(
        """
        UPDATE farm_data
        SET user_id = (
            SELECT accounts.owner_id
            FROM accounts
            WHERE accounts.id = farm_data.account_id
        )
        WHERE account_id IS NOT NULL
        """
    )
    cur.execute(
        """
        UPDATE farm_data
        SET farm_name = (
            SELECT accounts.name
            FROM accounts
            WHERE accounts.id = farm_data.account_id
        )
        WHERE account_id IS NOT NULL
        """
    )

    removed = _dedupe_farm_data(cur)
    if removed:
        print(f"[MIGRATE] Удалено дубликатов farm_data: {removed}")

    cur.execute("SELECT COUNT(*) FROM farm_data WHERE account_id IS NULL")
    null_count = cur.fetchone()[0]
    if null_count:
        print(f"[MIGRATE] Внимание: записей без account_id осталось: {null_count}")

    _drop_index_if_exists(cur, "farm_data", "uq_farm_data_user_farm")

    print("[MIGRATE] Добавляем уникальный индекс uq_farm_data_account_id...")
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_farm_data_account_id
        ON farm_data (account_id)
        """
    )

    conn.commit()
    conn.close()
    print("[MIGRATE] Готово, миграция успешно выполнена.")


if __name__ == "__main__":
    main()
