"""
Утилиты для резервного копирования SQLite-БД UsersDash.
"""
from __future__ import annotations

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import Config

DB_FILE = Path(Config.DATA_DIR) / "app.db"
BACKUP_DIR = Path(Config.DATA_DIR) / "backups"


def ensure_backup_dir() -> Path:
    """Создаёт папку с бэкапами, если её нет, и возвращает её путь."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    return BACKUP_DIR


def backup_database(tag: Optional[str] = None) -> Path:
    """
    Делает копию текущей БД в каталоге data/backups.

    :param tag: необязательный суффикс имени файла (например, "daily" или
        "before_pull_apply").
    :returns: путь к созданному файлу бэкапа.
    :raises FileNotFoundError: если исходный файл БД не найден.
    :raises OSError: при ошибках записи.
    """
    if not DB_FILE.exists():
        raise FileNotFoundError(f"DB file not found: {DB_FILE}")

    ensure_backup_dir()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{tag}" if tag else ""
    dest = BACKUP_DIR / f"app_{stamp}{suffix}.db"

    shutil.copy2(DB_FILE, dest)
    return dest


def list_backups(limit: int = 10) -> list[Path]:
    """Возвращает последние бэкапы (новые сверху)."""
    if not BACKUP_DIR.exists():
        return []

    backups = sorted(
        [p for p in BACKUP_DIR.iterdir() if p.is_file()],
        key=os.path.getmtime,
        reverse=True,
    )
    return backups[:limit]
