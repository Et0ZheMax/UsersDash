"""Принудительно обновляет сегодняшний бэкап конфигурации LDPlayer."""

from __future__ import annotations

import argparse
import shutil
import sys
import uuid
from datetime import date
from pathlib import Path


DEFAULT_SOURCE = Path(r"C:\LDPlayer\LDPlayer9\vms\config")
DEFAULT_BACKUP_ROOT = Path(r"C:\LD_backup\configs")


def create_forced_backup(
    source: Path = DEFAULT_SOURCE,
    backup_root: Path = DEFAULT_BACKUP_ROOT,
    backup_date: date | None = None,
) -> Path:
    """Создаёт полный снимок config за дату, заменяя уже существующий бэкап."""

    source = Path(source)
    backup_root = Path(backup_root)
    if not source.is_dir():
        raise FileNotFoundError(f"Папка конфигурации LDPlayer не найдена: {source}")

    stamp = (backup_date or date.today()).strftime("%d__%m__%Y")
    destination = backup_root / stamp
    backup_root.mkdir(parents=True, exist_ok=True)

    unique_suffix = uuid.uuid4().hex
    staged = backup_root / f".{stamp}.new-{unique_suffix}"
    previous = backup_root / f".{stamp}.old-{unique_suffix}"

    try:
        shutil.copytree(source, staged)
        if destination.exists():
            destination.rename(previous)
        staged.rename(destination)
    except Exception:
        if previous.exists() and not destination.exists():
            previous.rename(destination)
        raise
    finally:
        if staged.exists():
            shutil.rmtree(staged, ignore_errors=True)
        if previous.exists():
            shutil.rmtree(previous, ignore_errors=True)

    return destination


def build_parser() -> argparse.ArgumentParser:
    """Создаёт парсер аргументов для ручного запуска и диагностики."""

    parser = argparse.ArgumentParser(
        description="Принудительно заменить сегодняшний бэкап config LDPlayer.",
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE,
        help=f"Исходная папка config (по умолчанию: {DEFAULT_SOURCE})",
    )
    parser.add_argument(
        "--backup-root",
        type=Path,
        default=DEFAULT_BACKUP_ROOT,
        help=f"Корень бэкапов (по умолчанию: {DEFAULT_BACKUP_ROOT})",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Запускает принудительный бэкап и возвращает код завершения процесса."""

    args = build_parser().parse_args(argv)
    try:
        destination = create_forced_backup(args.source, args.backup_root)
    except (OSError, shutil.Error) as exc:
        print(f"[BACKUP] Ошибка: {exc}", file=sys.stderr)
        return 1

    files_count = sum(1 for path in destination.rglob("*") if path.is_file())
    print(f"[BACKUP] Готово: {files_count} файлов сохранено в {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
