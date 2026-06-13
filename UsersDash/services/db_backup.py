"""
Утилиты для резервного копирования SQLite-БД UsersDash.
"""
from __future__ import annotations

import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Iterator, Optional

from UsersDash.config import Config

DB_FILE = Path(Config.DATA_DIR) / "app.db"
BACKUP_DIR = Path(Config.DATA_DIR) / "backups"
LOCK_FILE = BACKUP_DIR / ".daily_backup.lock"


def sqlite_uri_to_path(uri: str) -> Path:
    """Преобразует sqlite:/// URI SQLAlchemy в путь к файлу БД."""

    prefix = "sqlite:///"
    if not uri.startswith(prefix):
        raise ValueError(f"Ожидается SQLite URI вида sqlite:///..., получено: {uri}")
    return Path(uri.removeprefix(prefix))


@contextmanager
def _backup_lock(lock_file: Path | None = None, timeout_seconds: float = 30.0) -> Iterator[None]:
    """Захватывает межпроцессный lock, чтобы несколько воркеров не делали один daily-бэкап."""

    target_lock = lock_file or LOCK_FILE
    target_lock.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    lock_handle = open(target_lock, "a+b")
    try:
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    lock_handle.seek(0, os.SEEK_END)
                    if lock_handle.tell() == 0:
                        lock_handle.write(b"\0")
                        lock_handle.flush()
                    lock_handle.seek(0)
                    msvcrt.locking(lock_handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Не удалось захватить lock ежедневного бэкапа")
                time.sleep(0.2)

        yield
    finally:
        try:
            lock_handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(lock_handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        lock_handle.close()


def ensure_backup_dir() -> Path:
    """Создаёт папку с бэкапами, если её нет, и возвращает её путь."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    return BACKUP_DIR


def _sqlite_backup(src: Path, dest: Path) -> None:
    """Создаёт консистентный снимок SQLite через backup API вместо прямого copy2."""

    source = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    try:
        target = sqlite3.connect(dest)
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()


def backup_database(
    tag: Optional[str] = None,
    db_file: Path | None = None,
    backup_dir: Path | None = None,
) -> Path:
    """
    Делает копию текущей БД в каталоге data/backups.

    :param tag: необязательный суффикс имени файла (например, "daily" или
        "before_pull_apply").
    :returns: путь к созданному файлу бэкапа.
    :raises FileNotFoundError: если исходный файл БД не найден.
    :raises OSError: при ошибках записи.
    """
    source_db = db_file or DB_FILE
    target_dir = backup_dir or BACKUP_DIR

    if not source_db.exists():
        raise FileNotFoundError(f"DB file not found: {source_db}")

    target_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{tag}" if tag else ""
    dest = target_dir / f"app_{stamp}{suffix}.db"

    _sqlite_backup(source_db, dest)
    return dest


def daily_backup_exists(day: date | None = None, backup_dir: Path | None = None) -> bool:
    """Проверяет, есть ли daily-бэкап за указанную дату (по имени файла)."""

    target_day = day or date.today()
    target_dir = backup_dir or BACKUP_DIR
    prefix = f"app_{target_day.strftime('%Y%m%d')}_"
    if not target_dir.exists():
        return False

    return any(
        path.is_file() and path.name.startswith(prefix) and path.name.endswith("_daily.db")
        for path in target_dir.iterdir()
    )


def ensure_daily_backup(
    day: date | None = None,
    db_file: Path | None = None,
    backup_dir: Path | None = None,
) -> Path | None:
    """Создаёт daily-бэкап один раз в день и возвращает путь или None, если он уже есть."""

    target_dir = backup_dir or BACKUP_DIR
    with _backup_lock(target_dir / LOCK_FILE.name):
        if daily_backup_exists(day, target_dir):
            return None
        return backup_database("daily", db_file=db_file, backup_dir=target_dir)


def list_backups(limit: int = 10) -> list[Path]:
    """Возвращает последние бэкапы (новые сверху)."""
    if not BACKUP_DIR.exists():
        return []

    backups = sorted(
        [p for p in BACKUP_DIR.iterdir() if p.is_file() and p.suffix == ".db"],
        key=os.path.getmtime,
        reverse=True,
    )
    return backups[:limit]
