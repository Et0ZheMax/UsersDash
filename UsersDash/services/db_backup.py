"""Безопасное резервное копирование и ротация SQLite-БД UsersDash."""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator, Optional

from UsersDash.config import Config

DB_FILE = Path(Config.DATA_DIR) / "app.db"
BACKUP_DIR = Path(Config.DATA_DIR) / "backups"
LOCK_FILE = BACKUP_DIR / ".daily_backup.lock"
ROTATION_LOG = BACKUP_DIR / "backup_rotation.log"

DAILY_KEEP = 7
WEEKLY_KEEP = 4
MONTHLY_KEEP = 3
MANUAL_KEEP_DAYS = 14
MIN_VALID_DAILY = 3
SOFT_LIMIT_BYTES = 100 * 1024**3
MIN_FREE_RESERVE_BYTES = 15 * 1024**3
FREE_SPACE_FACTOR = 1.3

_BACKUP_RE = re.compile(
    r"^app_(?P<day>\d{8})_(?P<clock>\d{6})(?:_(?P<tag>[A-Za-z0-9_-]+))?\.db$"
)
_TAG_RE = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class BackupInfo:
    path: Path
    created_at: datetime
    tag: str
    size: int

    @property
    def is_daily(self) -> bool:
        return self.tag == "daily"


@dataclass(frozen=True)
class RotationPlan:
    keep: tuple[BackupInfo, ...]
    delete: tuple[BackupInfo, ...]
    invalid: tuple[BackupInfo, ...]
    ignored: tuple[Path, ...]
    total_bytes: int
    delete_bytes: int
    bytes_after: int
    soft_limit_bytes: int


def sqlite_uri_to_path(uri: str) -> Path:
    """Преобразует sqlite:/// URI SQLAlchemy в путь к файлу БД."""
    prefix = "sqlite:///"
    if not uri.startswith(prefix):
        raise ValueError(f"Ожидается SQLite URI вида sqlite:///..., получено: {uri}")
    return Path(uri.removeprefix(prefix))


@contextmanager
def _backup_lock(lock_file: Path | None = None, timeout_seconds: float = 30.0) -> Iterator[None]:
    """Не позволяет нескольким процессам одновременно создавать и удалять бэкапы."""
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
                    raise TimeoutError("Не удалось захватить lock резервного копирования")
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
    """Создаёт консистентный снимок SQLite через backup API."""
    source = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    try:
        target = sqlite3.connect(dest)
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()


def validate_backup(path: Path) -> None:
    """Быстро проверяет структуру завершённого SQLite-файла без создания WAL/SHM."""
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"Пустой или отсутствующий backup: {path}")
    connection = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
    try:
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        schema_rows = int(
            connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table', 'index')"
            ).fetchone()[0]
        )
    finally:
        connection.close()
    if page_count <= 0 or page_size <= 0 or schema_rows <= 0:
        raise ValueError(f"Backup не содержит корректную SQLite-схему: {path}")
    expected_size = page_count * page_size
    actual_size = path.stat().st_size
    if actual_size != expected_size:
        raise ValueError(
            f"Незавершённый SQLite backup {path}: размер {actual_size}, "
            f"ожидалось {expected_size}"
        )


def _check_free_space(source_db: Path, target_dir: Path) -> None:
    required = int(source_db.stat().st_size * FREE_SPACE_FACTOR) + MIN_FREE_RESERVE_BYTES
    free = shutil.disk_usage(target_dir).free
    if free < required:
        raise OSError(
            f"Недостаточно места для backup: свободно {free / 1024**3:.2f} ГБ, "
            f"требуется {required / 1024**3:.2f} ГБ"
        )


def _backup_database_unlocked(tag: Optional[str], source_db: Path, target_dir: Path) -> Path:
    if not source_db.exists():
        raise FileNotFoundError(f"DB file not found: {source_db}")
    if tag and not _TAG_RE.fullmatch(tag):
        raise ValueError(f"Недопустимый тег backup: {tag!r}")
    target_dir.mkdir(parents=True, exist_ok=True)
    _check_free_space(source_db, target_dir)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{tag}" if tag else ""
    destination = target_dir / f"app_{stamp}{suffix}.db"
    partial = target_dir / f"{destination.name}.partial"
    if destination.exists() or partial.exists():
        raise FileExistsError(f"Backup с таким именем уже существует: {destination}")
    try:
        _sqlite_backup(source_db, partial)
        validate_backup(partial)
        os.replace(partial, destination)
        return destination
    except Exception:
        try:
            partial.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def backup_database(
    tag: Optional[str] = None,
    db_file: Path | None = None,
    backup_dir: Path | None = None,
) -> Path:
    """Атомарно создаёт и проверяет копию текущей БД в data/backups."""
    source_db = db_file or DB_FILE
    target_dir = backup_dir or BACKUP_DIR
    with _backup_lock(target_dir / LOCK_FILE.name):
        return _backup_database_unlocked(tag, source_db, target_dir)


def _parse_backup(path: Path) -> BackupInfo | None:
    match = _BACKUP_RE.fullmatch(path.name)
    if not match:
        return None
    try:
        created_at = datetime.strptime(
            match.group("day") + match.group("clock"), "%Y%m%d%H%M%S"
        )
    except ValueError:
        return None
    return BackupInfo(
        path=path,
        created_at=created_at,
        tag=match.group("tag") or "manual",
        size=path.stat().st_size,
    )


def daily_backup_exists(day: date | None = None, backup_dir: Path | None = None) -> bool:
    """Проверяет наличие завершённого и структурно корректного daily-backup."""
    target_day = day or date.today()
    target_dir = backup_dir or BACKUP_DIR
    prefix = f"app_{target_day.strftime('%Y%m%d')}_"
    if not target_dir.exists():
        return False
    for path in target_dir.iterdir():
        if not (path.is_file() and path.name.startswith(prefix) and path.name.endswith("_daily.db")):
            continue
        try:
            validate_backup(path)
        except (OSError, sqlite3.DatabaseError, ValueError):
            continue
        return True
    return False


def ensure_daily_backup(
    day: date | None = None,
    db_file: Path | None = None,
    backup_dir: Path | None = None,
) -> Path | None:
    """Создаёт один исправный daily-backup в день или возвращает None."""
    source_db = db_file or DB_FILE
    target_dir = backup_dir or BACKUP_DIR
    with _backup_lock(target_dir / LOCK_FILE.name):
        if daily_backup_exists(day, target_dir):
            return None
        return _backup_database_unlocked("daily", source_db, target_dir)


def build_rotation_plan(
    backup_dir: Path | None = None,
    *,
    now: datetime | None = None,
    soft_limit_bytes: int = SOFT_LIMIT_BYTES,
) -> RotationPlan:
    """Строит консервативный план GFS-ротации, ничего не удаляя."""
    target_dir = backup_dir or BACKUP_DIR
    current = now or datetime.now()
    parsed: list[BackupInfo] = []
    ignored: list[Path] = []
    if not target_dir.exists():
        return RotationPlan((), (), (), (), 0, 0, 0, soft_limit_bytes)
    for path in target_dir.iterdir():
        if not path.is_file() or path.suffix.lower() != ".db":
            continue
        info = _parse_backup(path)
        if info is None:
            ignored.append(path)
        else:
            parsed.append(info)
    parsed.sort(key=lambda item: item.created_at, reverse=True)
    valid: list[BackupInfo] = []
    invalid: list[BackupInfo] = []
    for item in parsed:
        try:
            validate_backup(item.path)
        except (OSError, sqlite3.DatabaseError, ValueError):
            invalid.append(item)
        else:
            valid.append(item)
    daily = [item for item in valid if item.is_daily]
    manual = [item for item in valid if not item.is_daily]
    keep_paths: set[Path] = set()

    for item in daily[: max(DAILY_KEEP, MIN_VALID_DAILY)]:
        keep_paths.add(item.path)

    current_week = current.isocalendar()[:2]
    weekly_groups: dict[tuple[int, int], BackupInfo] = {}
    for item in daily:
        week = item.created_at.isocalendar()[:2]
        if week != current_week:
            weekly_groups.setdefault(week, item)
    for week in sorted(weekly_groups, reverse=True)[:WEEKLY_KEEP]:
        keep_paths.add(weekly_groups[week].path)

    current_month = (current.year, current.month)
    monthly_groups: dict[tuple[int, int], BackupInfo] = {}
    for item in daily:
        month = (item.created_at.year, item.created_at.month)
        if month != current_month:
            monthly_groups.setdefault(month, item)
    for month in sorted(monthly_groups, reverse=True)[:MONTHLY_KEEP]:
        keep_paths.add(monthly_groups[month].path)

    manual_cutoff = current - timedelta(days=MANUAL_KEEP_DAYS)
    latest_by_tag: dict[str, BackupInfo] = {}
    for item in manual:
        latest_by_tag.setdefault(item.tag, item)
        if item.created_at >= manual_cutoff or Path(f"{item.path}.keep").exists():
            keep_paths.add(item.path)
    for item in latest_by_tag.values():
        keep_paths.add(item.path)

    keep = tuple(item for item in valid if item.path in keep_paths)
    delete = tuple(item for item in valid if item.path not in keep_paths) + tuple(invalid)
    total_bytes = sum(item.size for item in parsed) + sum(path.stat().st_size for path in ignored)
    delete_bytes = sum(item.size for item in delete)
    return RotationPlan(
        keep=keep,
        delete=delete,
        invalid=tuple(invalid),
        ignored=tuple(sorted(ignored)),
        total_bytes=total_bytes,
        delete_bytes=delete_bytes,
        bytes_after=total_bytes - delete_bytes,
        soft_limit_bytes=soft_limit_bytes,
    )


def _safe_delete(path: Path, backup_dir: Path) -> None:
    directory = backup_dir.resolve()
    resolved = path.resolve()
    if resolved.parent != directory or not _BACKUP_RE.fullmatch(path.name):
        raise ValueError(f"Отказ от удаления небезопасного пути: {path}")
    path.unlink()


def rotate_backups(
    backup_dir: Path | None = None,
    *,
    dry_run: bool = True,
    now: datetime | None = None,
) -> RotationPlan:
    """Выполняет план ротации; по умолчанию работает только в dry-run."""
    target_dir = backup_dir or BACKUP_DIR
    with _backup_lock(target_dir / LOCK_FILE.name):
        plan = build_rotation_plan(target_dir, now=now)
        if dry_run:
            return plan
        target_dir.mkdir(parents=True, exist_ok=True)
        with open(target_dir / ROTATION_LOG.name, "a", encoding="utf-8") as log:
            for item in plan.delete:
                _safe_delete(item.path, target_dir)
                log.write(
                    f"{datetime.now().isoformat(timespec='seconds')} DELETE "
                    f"{item.path.name} {item.size}\n"
                )
            log.flush()
        return plan


def list_backups(limit: int = 10) -> list[Path]:
    """Возвращает последние завершённые бэкапы (новые сверху)."""
    if not BACKUP_DIR.exists():
        return []
    backups = sorted(
        [p for p in BACKUP_DIR.iterdir() if p.is_file() and p.suffix == ".db"],
        key=os.path.getmtime,
        reverse=True,
    )
    return backups[:limit]


def _format_gb(value: int) -> str:
    return f"{value / 1024**3:.2f} ГБ"


def main() -> int:
    parser = argparse.ArgumentParser(description="Ротация SQLite-бэкапов UsersDash")
    parser.add_argument(
        "--apply", action="store_true", help="реально удалить файлы (по умолчанию dry-run)"
    )
    args = parser.parse_args()
    plan = rotate_backups(dry_run=not args.apply)
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(
        f"[{mode}] всего: {_format_gb(plan.total_bytes)}, "
        f"к удалению: {len(plan.delete)} файлов / {_format_gb(plan.delete_bytes)}, "
        f"останется: {_format_gb(plan.bytes_after)}"
    )
    for item in plan.delete:
        print(f"DELETE {item.path.name} {_format_gb(item.size)}")
    for path in plan.ignored:
        print(f"IGNORE {path.name}")
    for item in plan.invalid:
        print(f"INVALID {item.path.name}")
    if plan.bytes_after > plan.soft_limit_bytes:
        print(
            f"WARNING: после ротации объём выше мягкого лимита "
            f"{_format_gb(plan.soft_limit_bytes)}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
