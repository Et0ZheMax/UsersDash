"""Веб-восстановление данных ферм из SQLite-бэкапов UsersDash."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from UsersDash.models import Account, FarmData, db
from UsersDash.services.db_backup import BACKUP_DIR, DB_FILE, backup_database, ensure_backup_dir

FARMDATA_FIELDS: dict[str, tuple[str, str]] = {
    "email": ("E-mail", "farm_data"),
    "login": ("Логин", "farm_data"),
    "password": ("Пароль", "farm_data"),
    "igg_id": ("IGG ID", "farm_data"),
    "server": ("Королевство", "farm_data"),
    "telegram_tag": ("Telegram", "farm_data"),
    "next_payment_at": ("След. оплата", "accounts"),
    "next_payment_amount": ("Стоимость, ₽", "accounts"),
    "next_payment_tariff": ("Тариф, ₽", "accounts"),
}

_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")


@dataclass(slots=True)
class FarmDataBackupRow:
    """Снимок строки farm_data/accounts из основной БД или бэкапа."""

    row_id: int
    account_id: int | None
    user_id: int
    owner_name: str | None
    farm_name: str
    values: dict[str, Any]


@dataclass(slots=True)
class FarmDataRestoreCandidate:
    """Строка, в которой значения из бэкапа отличаются от текущих."""

    row_id: int
    account_id: int | None
    user_id: int
    owner_name: str | None
    farm_name: str
    changes: list[dict[str, Any]]


def _normalize_text(value: Any) -> str | None:
    """Приводит текстовое значение SQLite к стабильному виду для сравнения."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_int(value: Any) -> int | None:
    """Приводит числовое значение SQLite к int или None."""

    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _normalize_date(value: Any) -> str | None:
    """Оставляет дату YYYY-MM-DD из DateTime/строки SQLite для сравнения и отображения."""

    text = _normalize_text(value)
    if not text:
        return None
    match = _DATE_RE.match(text)
    return match.group(1) if match else text


def _safe_name(value: Any) -> str:
    """Возвращает очищенное имя фермы."""

    return (str(value or "")).strip()


def _is_sqlite_db(path: Path) -> bool:
    """Проверяет, похож ли файл на допустимый SQLite-бэкап UsersDash."""

    if not path.is_file() or path.suffix.lower() != ".db":
        return False
    return path.name.startswith("app_")


def _resolve_backup_path(backup_name: str) -> Path:
    """Безопасно преобразует имя бэкапа в путь внутри каталога backups."""

    backups_dir = ensure_backup_dir().resolve()
    candidate = (backups_dir / Path(backup_name).name).resolve()
    if candidate.parent != backups_dir or not _is_sqlite_db(candidate):
        raise FileNotFoundError("Бэкап не найден или имеет недопустимое имя.")
    return candidate


def list_farmdata_backups(limit: int = 30) -> list[dict[str, Any]]:
    """Возвращает список SQLite-бэкапов, доступных для просмотра в админке."""

    if not BACKUP_DIR.exists():
        return []

    backups = sorted(
        [path for path in BACKUP_DIR.iterdir() if _is_sqlite_db(path)],
        key=os.path.getmtime,
        reverse=True,
    )
    result: list[dict[str, Any]] = []
    for path in backups[:limit]:
        stat = path.stat()
        created_at = datetime.fromtimestamp(stat.st_mtime)
        result.append(
            {
                "name": path.name,
                "size": stat.st_size,
                "created_at": created_at.isoformat(timespec="seconds"),
                "created_at_label": created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return result


def load_farmdata_backup_rows(db_path: Path) -> list[FarmDataBackupRow]:
    """Читает данные ферм и оплаты из указанного SQLite-файла."""

    if not db_path.exists():
        raise FileNotFoundError(f"Файл БД не найден: {db_path}")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        farm_columns = {row[1] for row in conn.execute("PRAGMA table_info(farm_data)").fetchall()}
        account_columns = {row[1] for row in conn.execute("PRAGMA table_info(accounts)").fetchall()}
        user_columns = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}

        def fd_expr(column: str) -> str:
            return f"fd.{column}" if column in farm_columns else f"NULL AS {column}"

        def acc_expr(column: str) -> str:
            return f"acc.{column}" if column in account_columns else f"NULL AS {column}"

        owner_expr = "COALESCE(u.username, '') AS owner_name" if "username" in user_columns else "'' AS owner_name"
        account_id_expr = fd_expr("account_id")
        rows = conn.execute(
            f"""
            SELECT
                fd.id,
                {account_id_expr},
                fd.user_id,
                {owner_expr},
                fd.farm_name,
                {fd_expr("email")},
                {fd_expr("login")},
                {fd_expr("password")},
                {fd_expr("igg_id")},
                {fd_expr("server")},
                {fd_expr("telegram_tag")},
                {acc_expr("next_payment_at")},
                {acc_expr("next_payment_amount")},
                {acc_expr("next_payment_tariff")}
            FROM farm_data fd
            LEFT JOIN accounts acc ON acc.id = {"fd.account_id" if "account_id" in farm_columns else "NULL"}
            LEFT JOIN users u ON u.id = fd.user_id
            """
        ).fetchall()

    result: list[FarmDataBackupRow] = []
    for row in rows:
        values = {
            "email": _normalize_text(row["email"]),
            "login": _normalize_text(row["login"]),
            "password": _normalize_text(row["password"]),
            "igg_id": _normalize_text(row["igg_id"]),
            "server": _normalize_text(row["server"]),
            "telegram_tag": _normalize_text(row["telegram_tag"]),
            "next_payment_at": _normalize_date(row["next_payment_at"]),
            "next_payment_amount": _normalize_int(row["next_payment_amount"]),
            "next_payment_tariff": _normalize_int(row["next_payment_tariff"]),
        }
        result.append(
            FarmDataBackupRow(
                row_id=int(row["id"]),
                account_id=row["account_id"],
                user_id=int(row["user_id"]),
                owner_name=_normalize_text(row["owner_name"]),
                farm_name=_safe_name(row["farm_name"]),
                values=values,
            )
        )
    return result


def build_farmdata_restore_candidates(
    main_rows: list[FarmDataBackupRow],
    backup_rows: list[FarmDataBackupRow],
) -> list[FarmDataRestoreCandidate]:
    """Сравнивает текущую БД с бэкапом и возвращает только отличающиеся поля."""

    main_by_account = {row.account_id: row for row in main_rows if row.account_id is not None}
    main_by_fallback = {(row.user_id, _safe_name(row.farm_name).lower()): row for row in main_rows}
    candidates: list[FarmDataRestoreCandidate] = []

    for backup_row in backup_rows:
        main_row = None
        if backup_row.account_id is not None:
            main_row = main_by_account.get(backup_row.account_id)
        if main_row is None:
            main_row = main_by_fallback.get((backup_row.user_id, _safe_name(backup_row.farm_name).lower()))
        if main_row is None:
            continue

        changes: list[dict[str, Any]] = []
        for field, (label, source_table) in FARMDATA_FIELDS.items():
            backup_value = backup_row.values.get(field)
            main_value = main_row.values.get(field)
            if backup_value is None or backup_value == main_value:
                continue
            if source_table == "accounts" and main_row.account_id is None:
                continue
            changes.append(
                {
                    "field": field,
                    "label": label,
                    "main": main_value,
                    "backup": backup_value,
                    "source_table": source_table,
                }
            )

        if changes:
            candidates.append(
                FarmDataRestoreCandidate(
                    row_id=main_row.row_id,
                    account_id=main_row.account_id,
                    user_id=main_row.user_id,
                    owner_name=main_row.owner_name,
                    farm_name=main_row.farm_name,
                    changes=changes,
                )
            )

    candidates.sort(key=lambda item: ((item.owner_name or "").lower(), item.farm_name.lower()))
    return candidates


def preview_farmdata_backup(backup_name: str) -> dict[str, Any]:
    """Готовит JSON-предпросмотр восстановления для выбранного бэкапа."""

    backup_path = _resolve_backup_path(backup_name)
    main_rows = load_farmdata_backup_rows(DB_FILE)
    backup_rows = load_farmdata_backup_rows(backup_path)
    candidates = build_farmdata_restore_candidates(main_rows, backup_rows)
    return {
        "backup": backup_path.name,
        "main_rows_count": len(main_rows),
        "backup_rows_count": len(backup_rows),
        "candidates_count": len(candidates),
        "candidates": [
            {
                "row_id": item.row_id,
                "account_id": item.account_id,
                "user_id": item.user_id,
                "owner_name": item.owner_name,
                "farm_name": item.farm_name,
                "changes": item.changes,
            }
            for item in candidates
        ],
    }


def _parse_payment_date(value: Any) -> datetime | None:
    """Парсит дату оплаты из значения бэкапа."""

    text = _normalize_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    normalized = _normalize_date(text)
    if normalized and normalized != text:
        try:
            return datetime.strptime(normalized, "%Y-%m-%d")
        except ValueError:
            return None
    return None


def _write_restore_report(
    source_backup: Path,
    safety_backup: Path,
    changed_items: list[dict[str, Any]],
) -> Path:
    """Сохраняет JSON-отчёт по веб-восстановлению."""

    report_dir = ensure_backup_dir() / "restore_reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = report_dir / f"farmdata_restore_web_{stamp}.json"
    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source_backup": str(source_backup),
        "safety_backup": str(safety_backup),
        "changed_count": len(changed_items),
        "changes": changed_items,
    }
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return report_path


def apply_farmdata_backup_restore(backup_name: str, selected_changes: list[dict[str, Any]]) -> dict[str, Any]:
    """Применяет выбранные поля из бэкапа к текущей БД через SQLAlchemy."""

    backup_path = _resolve_backup_path(backup_name)
    allowed_fields = set(FARMDATA_FIELDS)
    selected_by_row: dict[int, set[str]] = {}
    for change in selected_changes:
        try:
            row_id = int(change.get("row_id"))
        except (TypeError, ValueError):
            continue
        field = str(change.get("field") or "").strip()
        if field in allowed_fields:
            selected_by_row.setdefault(row_id, set()).add(field)

    if not selected_by_row:
        raise ValueError("Не выбрано ни одного поля для восстановления.")

    preview = preview_farmdata_backup(backup_name)
    candidate_by_row = {int(item["row_id"]): item for item in preview["candidates"]}
    safety_backup = backup_database("before_farmdata_restore_web")
    changed_items: list[dict[str, Any]] = []

    try:
        for row_id, fields in selected_by_row.items():
            candidate = candidate_by_row.get(row_id)
            if not candidate:
                continue
            farm_data = db.session.get(FarmData, row_id)
            if farm_data is None:
                continue
            account = db.session.get(Account, candidate.get("account_id")) if candidate.get("account_id") else None
            changes_by_field = {
                change["field"]: change
                for change in candidate.get("changes", [])
                if change.get("field") in fields
            }
            if not changes_by_field:
                continue

            changed_fields: list[dict[str, Any]] = []
            for field, change in changes_by_field.items():
                backup_value = change.get("backup")
                if field in {"email", "login", "password", "igg_id", "server", "telegram_tag"}:
                    setattr(farm_data, field, backup_value)
                elif field == "next_payment_at" and account is not None:
                    account.next_payment_at = _parse_payment_date(backup_value)
                elif field == "next_payment_amount" and account is not None:
                    account.next_payment_amount = _normalize_int(backup_value)
                elif field == "next_payment_tariff" and account is not None:
                    account.next_payment_tariff = _normalize_int(backup_value)
                else:
                    continue
                changed_fields.append(change)

            if changed_fields:
                farm_data.updated_at = datetime.utcnow()
                changed_items.append(
                    {
                        "row_id": row_id,
                        "account_id": candidate.get("account_id"),
                        "owner_name": candidate.get("owner_name"),
                        "farm_name": candidate.get("farm_name"),
                        "fields": changed_fields,
                    }
                )

        if not changed_items:
            db.session.rollback()
            return {
                "updated_rows": 0,
                "updated_fields": 0,
                "safety_backup": safety_backup.name,
                "report": None,
            }

        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

    report_path = _write_restore_report(backup_path, safety_backup, changed_items)
    return {
        "updated_rows": len(changed_items),
        "updated_fields": sum(len(item["fields"]) for item in changed_items),
        "safety_backup": safety_backup.name,
        "report": report_path.name,
    }
