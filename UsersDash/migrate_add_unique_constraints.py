# migrate_add_unique_constraints.py
# Одноразовая миграция: устраняет дубликаты и добавляет уникальные ограничения
# для accounts (internal_id, owner_id + name) и farm_data (user_id + farm_name).

from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve()
REPO_ROOT = CURRENT_DIR.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from UsersDash.config import Config


ACCOUNT_MERGE_FIELDS = (
    "internal_id",
    "game_world",
    "notes",
    "next_payment_at",
    "next_payment_amount",
    "next_payment_tariff",
)

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


def _reassign_account_refs(cur: sqlite3.Cursor, old_id: int, new_id: int) -> None:
    for table in ("account_resource_snapshots", "action_logs", "settings_audit_log"):
        cur.execute(
            f"UPDATE {table} SET account_id = ? WHERE account_id = ?",
            (new_id, old_id),
        )


def _merge_account_fields(
    cur: sqlite3.Cursor,
    keep_row: sqlite3.Row,
    dup_rows: list[sqlite3.Row],
) -> None:
    updates: dict[str, str | int | None] = {}
    for field in ACCOUNT_MERGE_FIELDS:
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
            f"UPDATE accounts SET {assignments} WHERE id = ?",
            values,
        )


def _merge_farmdata_fields(
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


def _dedupe_accounts_by_internal_id(cur: sqlite3.Cursor) -> int:
    cur.execute(
        """
        SELECT internal_id
        FROM accounts
        WHERE internal_id IS NOT NULL
        GROUP BY internal_id
        HAVING COUNT(*) > 1
        """
    )
    duplicates = [row[0] for row in cur.fetchall()]
    removed = 0

    for internal_id in duplicates:
        cur.execute(
            """
            SELECT *
            FROM accounts
            WHERE internal_id = ?
            """,
            (internal_id,),
        )
        rows = _sort_rows(cur.fetchall())
        keep_row, dup_rows = rows[0], rows[1:]
        _merge_account_fields(cur, keep_row, dup_rows)

        for dup_row in dup_rows:
            _reassign_account_refs(cur, dup_row["id"], keep_row["id"])
            cur.execute("DELETE FROM accounts WHERE id = ?", (dup_row["id"],))
            removed += 1

    return removed


def _dedupe_accounts_by_owner_name(cur: sqlite3.Cursor) -> int:
    cur.execute(
        """
        SELECT owner_id, name
        FROM accounts
        GROUP BY owner_id, name
        HAVING COUNT(*) > 1
        """
    )
    duplicates = cur.fetchall()
    removed = 0

    for owner_id, name in duplicates:
        cur.execute(
            """
            SELECT *
            FROM accounts
            WHERE owner_id = ? AND name = ?
            """,
            (owner_id, name),
        )
        rows = _sort_rows(cur.fetchall())
        keep_row, dup_rows = rows[0], rows[1:]
        _merge_account_fields(cur, keep_row, dup_rows)

        for dup_row in dup_rows:
            _reassign_account_refs(cur, dup_row["id"], keep_row["id"])
            cur.execute("DELETE FROM accounts WHERE id = ?", (dup_row["id"],))
            removed += 1

    return removed


def _dedupe_farm_data(cur: sqlite3.Cursor) -> int:
    cur.execute(
        """
        SELECT user_id, farm_name
        FROM farm_data
        GROUP BY user_id, farm_name
        HAVING COUNT(*) > 1
        """
    )
    duplicates = cur.fetchall()
    removed = 0

    for user_id, farm_name in duplicates:
        cur.execute(
            """
            SELECT *
            FROM farm_data
            WHERE user_id = ? AND farm_name = ?
            """,
            (user_id, farm_name),
        )
        rows = _sort_rows(cur.fetchall())
        keep_row, dup_rows = rows[0], rows[1:]
        _merge_farmdata_fields(cur, keep_row, dup_rows)

        for dup_row in dup_rows:
            cur.execute("DELETE FROM farm_data WHERE id = ?", (dup_row["id"],))
            removed += 1

    return removed


def _create_unique_indexes(cur: sqlite3.Cursor) -> None:
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_accounts_internal_id
        ON accounts (internal_id)
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_accounts_owner_name
        ON accounts (owner_id, name)
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_farm_data_user_farm
        ON farm_data (user_id, farm_name)
        """
    )


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

    print("[MIGRATE] Нормализуем пустые internal_id...")
    cur.execute(
        """
        UPDATE accounts
        SET internal_id = NULL
        WHERE internal_id IS NOT NULL AND TRIM(internal_id) = ''
        """
    )

    removed_internal = _dedupe_accounts_by_internal_id(cur)
    removed_owner = _dedupe_accounts_by_owner_name(cur)
    removed_farm_data = _dedupe_farm_data(cur)

    print(
        "[MIGRATE] Удалено дубликатов: "
        f"accounts.internal_id={removed_internal}, "
        f"accounts.owner+name={removed_owner}, "
        f"farm_data={removed_farm_data}"
    )

    _create_unique_indexes(cur)
    conn.commit()
    conn.close()
    print("[MIGRATE] Уникальные ограничения добавлены.")


if __name__ == "__main__":
    main()
