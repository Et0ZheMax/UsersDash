"""GUI-утилита для выборочного восстановления полей farm_data/accounts из бэкапов UsersDash.

Сценарий рассчитан на экстренное восстановление полей:
- `farm_data.email`, `farm_data.login`, `farm_data.password`;
- `accounts.next_payment_at`, `accounts.next_payment_amount`.

Ключевые свойства:
- безопасное применение: перед записью делается страхующий бэкап основной БД;
- выборочное восстановление: можно включать нужные поля отдельно по строкам;
- сравнение "основная БД" vs "выбранный бэкап" в визуальной таблице;
- отчёт по применению сохраняется в JSON для аудита.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tkinter import BOTH, END, LEFT, RIGHT, VERTICAL, W, Y, BooleanVar, StringVar, Tk, messagebox, ttk

from UsersDash.config import Config


@dataclass(slots=True)
class FarmDataRow:
    """Снимок строки `farm_data` из SQLite."""

    id: int
    account_id: int | None
    user_id: int
    farm_name: str
    email: str | None
    login: str | None
    password: str | None
    next_payment_at: str | None
    next_payment_amount: int | None


@dataclass(slots=True)
class RestoreCandidate:
    """Кандидат на восстановление из бэкапа."""

    row_id: int
    account_id: int | None
    user_id: int
    farm_name: str
    main_email: str | None
    backup_email: str | None
    main_login: str | None
    backup_login: str | None
    main_password: str | None
    backup_password: str | None
    main_next_payment_at: str | None
    backup_next_payment_at: str | None
    main_next_payment_amount: int | None
    backup_next_payment_amount: int | None
    can_restore_email: bool
    can_restore_login: bool
    can_restore_password: bool
    can_restore_next_payment_at: bool
    can_restore_next_payment_amount: bool


def _normalize_text(value: str | None) -> str | None:
    """Нормализует текстовое значение БД: пустые строки -> None."""

    if value is None:
        return None
    text = value.strip()
    return text or None


def _safe_farm_name(name: str | None) -> str:
    return (name or "").strip()


def _normalize_int(value: object) -> int | None:
    """Нормализует числовое значение из SQLite (None/'' -> None)."""

    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _match_main_row(
    backup_row: FarmDataRow,
    main_by_account: dict[int, FarmDataRow],
    main_by_fallback: dict[tuple[int, str], FarmDataRow],
) -> FarmDataRow | None:
    """Подбирает строку из основной БД для записи из бэкапа.

    Сначала пробует точное совпадение по `account_id`, затем fallback по
    `(user_id, farm_name_lower)`. Такой порядок сохраняет стабильность
    обновлений, но позволяет находить строки после миграций, где account_id
    изменился между бэкапом и текущей БД.
    """

    if backup_row.account_id is not None:
        matched = main_by_account.get(backup_row.account_id)
        if matched is not None:
            return matched

    fallback_key = (backup_row.user_id, _safe_farm_name(backup_row.farm_name).lower())
    return main_by_fallback.get(fallback_key)


def load_farmdata_rows(db_path: Path) -> list[FarmDataRow]:
    """Читает `farm_data` из указанной SQLite-БД."""

    if not db_path.exists():
        raise FileNotFoundError(f"Файл БД не найден: {db_path}")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                fd.id,
                fd.account_id,
                fd.user_id,
                fd.farm_name,
                fd.email,
                fd.login,
                fd.password,
                acc.next_payment_at,
                acc.next_payment_amount
            FROM farm_data fd
            LEFT JOIN accounts acc ON acc.id = fd.account_id
            """
        ).fetchall()

    result: list[FarmDataRow] = []
    for row in rows:
        result.append(
            FarmDataRow(
                id=int(row["id"]),
                account_id=row["account_id"],
                user_id=int(row["user_id"]),
                farm_name=_safe_farm_name(row["farm_name"]),
                email=_normalize_text(row["email"]),
                login=_normalize_text(row["login"]),
                password=_normalize_text(row["password"]),
                next_payment_at=_normalize_text(row["next_payment_at"]),
                next_payment_amount=_normalize_int(row["next_payment_amount"]),
            )
        )
    return result


def build_restore_candidates(main_rows: list[FarmDataRow], backup_rows: list[FarmDataRow]) -> list[RestoreCandidate]:
    """Строит список строк, где есть что восстанавливать из бэкапа."""

    main_by_account = {row.account_id: row for row in main_rows if row.account_id is not None}
    main_by_fallback = {(row.user_id, _safe_farm_name(row.farm_name).lower()): row for row in main_rows}
    candidates: list[RestoreCandidate] = []

    for backup_row in backup_rows:
        main_row = _match_main_row(backup_row, main_by_account, main_by_fallback)
        if not main_row:
            continue

        email_diff = backup_row.email is not None and backup_row.email != main_row.email
        login_diff = backup_row.login is not None and backup_row.login != main_row.login
        password_diff = backup_row.password is not None and backup_row.password != main_row.password
        can_restore_payment = main_row.account_id is not None
        next_payment_at_diff = (
            can_restore_payment
            and backup_row.next_payment_at is not None
            and backup_row.next_payment_at != main_row.next_payment_at
        )
        next_payment_amount_diff = (
            can_restore_payment
            and backup_row.next_payment_amount is not None
            and backup_row.next_payment_amount != main_row.next_payment_amount
        )

        if not (email_diff or login_diff or password_diff or next_payment_at_diff or next_payment_amount_diff):
            continue

        candidates.append(
            RestoreCandidate(
                row_id=main_row.id,
                account_id=main_row.account_id,
                user_id=main_row.user_id,
                farm_name=main_row.farm_name,
                main_email=main_row.email,
                backup_email=backup_row.email,
                main_login=main_row.login,
                backup_login=backup_row.login,
                main_password=main_row.password,
                backup_password=backup_row.password,
                main_next_payment_at=main_row.next_payment_at,
                backup_next_payment_at=backup_row.next_payment_at,
                main_next_payment_amount=main_row.next_payment_amount,
                backup_next_payment_amount=backup_row.next_payment_amount,
                can_restore_email=email_diff,
                can_restore_login=login_diff,
                can_restore_password=password_diff,
                can_restore_next_payment_at=next_payment_at_diff,
                can_restore_next_payment_amount=next_payment_amount_diff,
            )
        )

    candidates.sort(key=lambda item: (item.user_id, (item.farm_name or "").lower()))
    return candidates


def build_status_text(
    *,
    main_rows_count: int,
    backup_rows_count: int,
    candidates_count: int,
    visible_count: int,
    only_empty_enabled: bool,
    search_text: str,
) -> str:
    """Формирует строку статуса для GUI с пояснением текущего состояния."""

    base = (
        f"Основная БД: {main_rows_count} строк | "
        f"Бэкап: {backup_rows_count} строк | "
        f"Кандидаты: {candidates_count} | "
        f"Показано: {visible_count}"
    )

    hints: list[str] = []
    if only_empty_enabled:
        hints.append("включён фильтр только пустых полей")
    if search_text.strip():
        hints.append(f"поиск: «{search_text.strip()}»")
    if candidates_count > 0 and visible_count == 0:
        hints.append("ничего не видно из-за фильтров")
    if candidates_count == 0:
        hints.append("совпадений для восстановления не найдено")

    if hints:
        return f"{base} — " + "; ".join(hints)
    return base


def create_safety_backup(main_db_path: Path, backups_dir: Path) -> Path:
    """Создаёт страхующий бэкап основной БД перед применением изменений."""

    backups_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backups_dir / f"app_{stamp}_before_farmdata_restore_gui.db"
    shutil.copy2(main_db_path, backup_path)
    return backup_path


def apply_selected_changes(
    main_db_path: Path,
    items: list[RestoreCandidate],
    selected_email_ids: set[int],
    selected_login_ids: set[int],
    selected_password_ids: set[int],
    selected_next_payment_at_ids: set[int],
    selected_next_payment_amount_ids: set[int],
) -> int:
    """Применяет выбранные поля к основной БД и возвращает число обновлённых строк."""

    updated_rows = 0
    with sqlite3.connect(main_db_path) as conn:
        for item in items:
            update_email = item.row_id in selected_email_ids and item.can_restore_email
            update_login = item.row_id in selected_login_ids and item.can_restore_login
            update_password = item.row_id in selected_password_ids and item.can_restore_password
            update_next_payment_at = (
                item.row_id in selected_next_payment_at_ids and item.can_restore_next_payment_at
            )
            update_next_payment_amount = (
                item.row_id in selected_next_payment_amount_ids and item.can_restore_next_payment_amount
            )
            if not (
                update_email
                or update_login
                or update_password
                or update_next_payment_at
                or update_next_payment_amount
            ):
                continue

            current = conn.execute(
                "SELECT email, login, password FROM farm_data WHERE id = ?",
                (item.row_id,),
            ).fetchone()
            if current is None:
                continue

            new_email = item.backup_email if update_email else current[0]
            new_login = item.backup_login if update_login else current[1]
            new_password = item.backup_password if update_password else current[2]

            conn.execute(
                """
                UPDATE farm_data
                SET email = ?,
                    login = ?,
                    password = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (new_email, new_login, new_password, item.row_id),
            )

            if item.account_id is not None and (update_next_payment_at or update_next_payment_amount):
                current_account = conn.execute(
                    "SELECT next_payment_at, next_payment_amount FROM accounts WHERE id = ?",
                    (item.account_id,),
                ).fetchone()
                if current_account is not None:
                    new_next_payment_at = item.backup_next_payment_at if update_next_payment_at else current_account[0]
                    new_next_payment_amount = (
                        item.backup_next_payment_amount if update_next_payment_amount else current_account[1]
                    )
                    conn.execute(
                        """
                        UPDATE accounts
                        SET next_payment_at = ?,
                            next_payment_amount = ?
                        WHERE id = ?
                        """,
                        (new_next_payment_at, new_next_payment_amount, item.account_id),
                    )
            updated_rows += 1

        conn.commit()

    return updated_rows


def dump_restore_report(
    report_dir: Path,
    source_backup: Path,
    safety_backup: Path,
    changed_items: list[dict[str, object]],
) -> Path:
    """Сохраняет отчёт JSON по восстановлению."""

    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = report_dir / f"farmdata_restore_{stamp}.json"

    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source_backup": str(source_backup),
        "safety_backup": str(safety_backup),
        "changed_count": len(changed_items),
        "changes": changed_items,
    }
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return report_path


class RestoreGui:
    """Tkinter-окно для визуального выбора и применения восстановления."""

    columns = (
        "farm_name",
        "user_id",
        "account_id",
        "main_email",
        "backup_email",
        "main_login",
        "backup_login",
        "main_password",
        "backup_password",
        "main_next_payment_at",
        "backup_next_payment_at",
        "main_next_payment_amount",
        "backup_next_payment_amount",
        "restore_email",
        "restore_login",
        "restore_password",
        "restore_next_payment_at",
        "restore_next_payment_amount",
    )

    def __init__(self, root: Tk, main_db_path: Path, backups_dir: Path, initial_backup: Path | None = None) -> None:
        self.root = root
        self.main_db_path = main_db_path
        self.backups_dir = backups_dir
        self.backup_paths: list[Path] = []
        self.candidates: list[RestoreCandidate] = []
        self.selected_email_ids: set[int] = set()
        self.selected_login_ids: set[int] = set()
        self.selected_password_ids: set[int] = set()
        self.selected_next_payment_at_ids: set[int] = set()
        self.selected_next_payment_amount_ids: set[int] = set()
        self.main_rows_count = 0
        self.backup_rows_count = 0

        self.backup_var = StringVar()
        self.search_var = StringVar()
        self.only_empty_var = BooleanVar(value=False)
        self.status_var = StringVar(value="Выберите бэкап и нажмите «Загрузить».")

        root.title("UsersDash • Восстановление email/login/password/оплаты из бэкапа")
        root.geometry("2320x820")

        self._build_ui()
        self._reload_backups(initial_backup)

    def _build_ui(self) -> None:
        top = ttk.Frame(self.root, padding=8)
        top.pack(fill="x")

        ttk.Label(top, text="Бэкап:").pack(side=LEFT)
        self.backup_combo = ttk.Combobox(top, textvariable=self.backup_var, width=95, state="readonly")
        self.backup_combo.pack(side=LEFT, padx=(8, 8))
        self.backup_combo.bind("<<ComboboxSelected>>", lambda _event: self.load_selected_backup())

        ttk.Button(top, text="Обновить список", command=self._reload_backups).pack(side=LEFT)
        ttk.Button(top, text="Загрузить", command=self.load_selected_backup).pack(side=LEFT, padx=(8, 0))

        filters = ttk.Frame(self.root, padding=(8, 0, 8, 8))
        filters.pack(fill="x")
        ttk.Label(filters, text="Поиск (ферма/email/логин):").pack(side=LEFT)
        search_entry = ttk.Entry(filters, textvariable=self.search_var, width=40)
        search_entry.pack(side=LEFT, padx=(8, 16))
        search_entry.bind("<KeyRelease>", lambda _event: self._refresh_tree())

        ttk.Checkbutton(
            filters,
            text="Показывать только строки с пустыми полями в основной БД",
            variable=self.only_empty_var,
            command=self._refresh_tree,
        ).pack(side=LEFT)

        btns = ttk.Frame(self.root, padding=(8, 0, 8, 8))
        btns.pack(fill="x")
        ttk.Button(btns, text="Отметить email у всех", command=self._select_all_email).pack(side=LEFT)
        ttk.Button(btns, text="Отметить login у всех", command=self._select_all_login).pack(side=LEFT, padx=(8, 0))
        ttk.Button(btns, text="Отметить password у всех", command=self._select_all_password).pack(side=LEFT, padx=(8, 0))
        ttk.Button(btns, text="Отметить дату оплаты у всех", command=self._select_all_next_payment_at).pack(
            side=LEFT, padx=(8, 0)
        )
        ttk.Button(btns, text="Отметить тариф у всех", command=self._select_all_next_payment_amount).pack(
            side=LEFT, padx=(8, 0)
        )
        ttk.Button(btns, text="Снять всё", command=self._clear_selection).pack(side=LEFT, padx=(8, 0))
        ttk.Button(btns, text="Применить выбранное", command=self._apply_changes).pack(side=RIGHT)

        status_frame = ttk.Frame(self.root, padding=(8, 0, 8, 8))
        status_frame.pack(fill="x")
        ttk.Label(status_frame, textvariable=self.status_var).pack(side=LEFT)

        body = ttk.Frame(self.root, padding=(8, 0, 8, 8))
        body.pack(fill=BOTH, expand=True)

        self.tree = ttk.Treeview(body, columns=self.columns, show="headings", height=24)
        self.tree.pack(side=LEFT, fill=BOTH, expand=True)

        scroll = ttk.Scrollbar(body, orient=VERTICAL, command=self.tree.yview)
        scroll.pack(side=RIGHT, fill=Y)
        self.tree.configure(yscrollcommand=scroll.set)

        headers = {
            "farm_name": "Ферма",
            "user_id": "User ID",
            "account_id": "Account ID",
            "main_email": "Основной email",
            "backup_email": "Email из бэкапа",
            "main_login": "Основной login",
            "backup_login": "Login из бэкапа",
            "main_password": "Основной password",
            "backup_password": "Password из бэкапа",
            "main_next_payment_at": "Осн. дата оплаты",
            "backup_next_payment_at": "Дата оплаты из бэкапа",
            "main_next_payment_amount": "Осн. тариф, ₽",
            "backup_next_payment_amount": "Тариф из бэкапа, ₽",
            "restore_email": "Вернуть email",
            "restore_login": "Вернуть login",
            "restore_password": "Вернуть password",
            "restore_next_payment_at": "Вернуть дату оплаты",
            "restore_next_payment_amount": "Вернуть тариф, ₽",
        }
        widths = {
            "farm_name": 190,
            "user_id": 90,
            "account_id": 95,
            "main_email": 180,
            "backup_email": 180,
            "main_login": 180,
            "backup_login": 180,
            "main_password": 180,
            "backup_password": 180,
            "main_next_payment_at": 135,
            "backup_next_payment_at": 165,
            "main_next_payment_amount": 120,
            "backup_next_payment_amount": 140,
            "restore_email": 110,
            "restore_login": 110,
            "restore_password": 130,
            "restore_next_payment_at": 150,
            "restore_next_payment_amount": 130,
        }

        for col in self.columns:
            self.tree.heading(col, text=headers[col], anchor=W)
            self.tree.column(col, width=widths[col], anchor=W, stretch=False)

        self.tree.bind("<Double-1>", self._on_double_click)

    def _reload_backups(self, initial_backup: Path | None = None) -> None:
        self.backups_dir.mkdir(parents=True, exist_ok=True)
        self.backup_paths = sorted(
            [p for p in self.backups_dir.glob("*.db") if p.is_file()],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        values = [str(path) for path in self.backup_paths]
        self.backup_combo["values"] = values

        if initial_backup and initial_backup.exists():
            self.backup_var.set(str(initial_backup))
            self.load_selected_backup()
            return

        if values:
            self.backup_var.set(values[0])
            self.load_selected_backup()

    def load_selected_backup(self) -> None:
        selected = self.backup_var.get().strip()
        if not selected:
            messagebox.showwarning("Нет бэкапа", "Сначала выберите файл бэкапа.")
            return

        backup_path = Path(selected)
        try:
            main_rows = load_farmdata_rows(self.main_db_path)
            backup_rows = load_farmdata_rows(backup_path)
        except Exception as exc:  # pragma: no cover - UI error path
            messagebox.showerror("Ошибка чтения БД", str(exc))
            return

        self.main_rows_count = len(main_rows)
        self.backup_rows_count = len(backup_rows)
        self.candidates = build_restore_candidates(main_rows, backup_rows)
        self.selected_email_ids.clear()
        self.selected_login_ids.clear()
        self.selected_password_ids.clear()
        self.selected_next_payment_at_ids.clear()
        self.selected_next_payment_amount_ids.clear()
        self._refresh_tree()

    def _passes_filters(self, item: RestoreCandidate) -> bool:
        text = self.search_var.get().strip().lower()
        if text:
            haystack = " | ".join(
                [
                    item.farm_name or "",
                    item.main_email or "",
                    item.backup_email or "",
                    item.main_login or "",
                    item.backup_login or "",
                ]
            ).lower()
            if text not in haystack:
                return False

        if self.only_empty_var.get():
            email_empty = not _normalize_text(item.main_email)
            login_empty = not _normalize_text(item.main_login)
            password_empty = not _normalize_text(item.main_password)
            next_payment_at_empty = not _normalize_text(item.main_next_payment_at)
            next_payment_amount_empty = item.main_next_payment_amount is None
            if not (email_empty or login_empty or password_empty or next_payment_at_empty or next_payment_amount_empty):
                return False

        return True

    def _refresh_tree(self) -> None:
        self.tree.delete(*self.tree.get_children())
        visible_count = 0

        for item in self.candidates:
            if not self._passes_filters(item):
                continue

            visible_count += 1
            email_mark = "✅" if item.row_id in self.selected_email_ids else "—"
            login_mark = "✅" if item.row_id in self.selected_login_ids else "—"
            password_mark = "✅" if item.row_id in self.selected_password_ids else "—"
            next_payment_at_mark = "✅" if item.row_id in self.selected_next_payment_at_ids else "—"
            next_payment_amount_mark = "✅" if item.row_id in self.selected_next_payment_amount_ids else "—"

            self.tree.insert(
                "",
                END,
                iid=str(item.row_id),
                values=(
                    item.farm_name,
                    item.user_id,
                    item.account_id if item.account_id is not None else "",
                    item.main_email or "",
                    item.backup_email or "",
                    item.main_login or "",
                    item.backup_login or "",
                    item.main_password or "",
                    item.backup_password or "",
                    item.main_next_payment_at or "",
                    item.backup_next_payment_at or "",
                    item.main_next_payment_amount if item.main_next_payment_amount is not None else "",
                    item.backup_next_payment_amount if item.backup_next_payment_amount is not None else "",
                    email_mark if item.can_restore_email else "н/д",
                    login_mark if item.can_restore_login else "н/д",
                    password_mark if item.can_restore_password else "н/д",
                    next_payment_at_mark if item.can_restore_next_payment_at else "н/д",
                    next_payment_amount_mark if item.can_restore_next_payment_amount else "н/д",
                ),
            )

        self.status_var.set(
            build_status_text(
                main_rows_count=self.main_rows_count,
                backup_rows_count=self.backup_rows_count,
                candidates_count=len(self.candidates),
                visible_count=visible_count,
                only_empty_enabled=self.only_empty_var.get(),
                search_text=self.search_var.get(),
            )
        )

    def _on_double_click(self, event) -> None:  # type: ignore[no-untyped-def]
        row_id = self.tree.identify_row(event.y)
        col_id = self.tree.identify_column(event.x)
        if not row_id or col_id not in ("#14", "#15", "#16", "#17", "#18"):
            return

        row_key = int(row_id)
        item = next((x for x in self.candidates if x.row_id == row_key), None)
        if item is None:
            return

        if col_id == "#14" and item.can_restore_email:
            if row_key in self.selected_email_ids:
                self.selected_email_ids.remove(row_key)
            else:
                self.selected_email_ids.add(row_key)

        if col_id == "#15" and item.can_restore_login:
            if row_key in self.selected_login_ids:
                self.selected_login_ids.remove(row_key)
            else:
                self.selected_login_ids.add(row_key)

        if col_id == "#16" and item.can_restore_password:
            if row_key in self.selected_password_ids:
                self.selected_password_ids.remove(row_key)
            else:
                self.selected_password_ids.add(row_key)

        if col_id == "#17" and item.can_restore_next_payment_at:
            if row_key in self.selected_next_payment_at_ids:
                self.selected_next_payment_at_ids.remove(row_key)
            else:
                self.selected_next_payment_at_ids.add(row_key)

        if col_id == "#18" and item.can_restore_next_payment_amount:
            if row_key in self.selected_next_payment_amount_ids:
                self.selected_next_payment_amount_ids.remove(row_key)
            else:
                self.selected_next_payment_amount_ids.add(row_key)

        self._refresh_tree()

    def _select_all_email(self) -> None:
        self.selected_email_ids = {item.row_id for item in self.candidates if item.can_restore_email and self._passes_filters(item)}
        self._refresh_tree()

    def _select_all_login(self) -> None:
        self.selected_login_ids = {item.row_id for item in self.candidates if item.can_restore_login and self._passes_filters(item)}
        self._refresh_tree()

    def _select_all_password(self) -> None:
        self.selected_password_ids = {
            item.row_id for item in self.candidates if item.can_restore_password and self._passes_filters(item)
        }
        self._refresh_tree()

    def _select_all_next_payment_at(self) -> None:
        self.selected_next_payment_at_ids = {
            item.row_id for item in self.candidates if item.can_restore_next_payment_at and self._passes_filters(item)
        }
        self._refresh_tree()

    def _select_all_next_payment_amount(self) -> None:
        self.selected_next_payment_amount_ids = {
            item.row_id for item in self.candidates if item.can_restore_next_payment_amount and self._passes_filters(item)
        }
        self._refresh_tree()

    def _clear_selection(self) -> None:
        self.selected_email_ids.clear()
        self.selected_login_ids.clear()
        self.selected_password_ids.clear()
        self.selected_next_payment_at_ids.clear()
        self.selected_next_payment_amount_ids.clear()
        self._refresh_tree()

    def _apply_changes(self) -> None:
        selected_any = bool(
            self.selected_email_ids
            or self.selected_login_ids
            or self.selected_password_ids
            or self.selected_next_payment_at_ids
            or self.selected_next_payment_amount_ids
        )
        if not selected_any:
            messagebox.showinfo("Нет выбора", "Отметьте хотя бы одно поле для восстановления.")
            return

        current_backup = Path(self.backup_var.get())
        if not current_backup.exists():
            messagebox.showerror("Ошибка", "Выбранный файл бэкапа не найден.")
            return

        confirm = messagebox.askyesno(
            "Подтверждение",
            "Применить выбранные изменения в основную БД?\n"
            "Перед записью автоматически будет создан страхующий бэкап.",
        )
        if not confirm:
            return

        try:
            safety_backup = create_safety_backup(self.main_db_path, self.backups_dir)
            changed_count = apply_selected_changes(
                self.main_db_path,
                self.candidates,
                self.selected_email_ids,
                self.selected_login_ids,
                self.selected_password_ids,
                self.selected_next_payment_at_ids,
                self.selected_next_payment_amount_ids,
            )

            changed_items: list[dict[str, object]] = []
            for item in self.candidates:
                email_selected = item.row_id in self.selected_email_ids and item.can_restore_email
                login_selected = item.row_id in self.selected_login_ids and item.can_restore_login
                password_selected = item.row_id in self.selected_password_ids and item.can_restore_password
                next_payment_at_selected = (
                    item.row_id in self.selected_next_payment_at_ids and item.can_restore_next_payment_at
                )
                next_payment_amount_selected = (
                    item.row_id in self.selected_next_payment_amount_ids and item.can_restore_next_payment_amount
                )
                if not (
                    email_selected
                    or login_selected
                    or password_selected
                    or next_payment_at_selected
                    or next_payment_amount_selected
                ):
                    continue
                changed_items.append(
                    {
                        "row_id": item.row_id,
                        "account_id": item.account_id,
                        "user_id": item.user_id,
                        "farm_name": item.farm_name,
                        "email_restored": email_selected,
                        "login_restored": login_selected,
                        "password_restored": password_selected,
                        "next_payment_at_restored": next_payment_at_selected,
                        "next_payment_amount_restored": next_payment_amount_selected,
                    }
                )

            report_dir = self.backups_dir / "restore_reports"
            report_path = dump_restore_report(report_dir, current_backup, safety_backup, changed_items)
        except Exception as exc:  # pragma: no cover - UI error path
            messagebox.showerror("Ошибка применения", str(exc))
            return

        messagebox.showinfo(
            "Готово",
            "Восстановление завершено.\n"
            f"Обновлено строк: {changed_count}\n"
            f"Страхующий бэкап: {safety_backup}\n"
            f"Отчёт: {report_path}",
        )
        self.load_selected_backup()


def parse_args() -> argparse.Namespace:
    """Парсит аргументы CLI для запуска GUI."""

    parser = argparse.ArgumentParser(
        description="GUI для сравнения и восстановления farm_data/accounts полей из backup SQLite",
    )
    parser.add_argument(
        "--main-db",
        type=Path,
        default=Path(Config.DATA_DIR) / "app.db",
        help="Путь к основной БД UsersDash (по умолчанию UsersDash/data/app.db)",
    )
    parser.add_argument(
        "--backups-dir",
        type=Path,
        default=Path(Config.DATA_DIR) / "backups",
        help="Папка с бэкапами SQLite (по умолчанию UsersDash/data/backups)",
    )
    parser.add_argument(
        "--backup",
        type=Path,
        default=None,
        help="Конкретный файл бэкапа для автозагрузки при старте GUI",
    )
    return parser.parse_args()


def main() -> int:
    """Точка входа GUI-утилиты."""

    args = parse_args()

    if not args.main_db.exists():
        raise SystemExit(f"Основная БД не найдена: {args.main_db}")

    root = Tk()
    RestoreGui(root, main_db_path=args.main_db, backups_dir=args.backups_dir, initial_backup=args.backup)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
