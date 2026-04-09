from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


def _iter_parent_dirs(start_file: Path) -> Iterable[Path]:
    current = start_file.resolve().parent
    yield current
    yield from current.parents


def find_git_root(start_file: str | Path) -> Path:
    """Ищет корень git-репозитория, поднимаясь вверх от файла."""
    start_path = Path(start_file)
    for directory in _iter_parent_dirs(start_path):
        if (directory / ".git").exists():
            return directory
    raise RuntimeError(f"Не удалось найти корень git для: {start_path}")


def _parse_env_line(raw_line: str) -> tuple[str, str] | None:
    line = raw_line.strip()
    if not line or line.startswith("#"):
        return None

    if line.startswith("export "):
        line = line[7:].strip()

    if "=" not in line:
        return None

    key, value = line.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key:
        return None

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]

    return key, value


def load_root_env_file(start_file: str | Path, env_filename: str = ".env") -> Path | None:
    """Подгружает переменные из /.env в os.environ, не перезаписывая уже заданные."""
    git_root = find_git_root(start_file)
    env_path = git_root / env_filename
    if not env_path.is_file():
        return None

    with env_path.open("r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            parsed = _parse_env_line(raw_line)
            if not parsed:
                continue
            key, value = parsed
            os.environ.setdefault(key, value)

    return env_path
