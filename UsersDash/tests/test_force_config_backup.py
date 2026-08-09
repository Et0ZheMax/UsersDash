"""Тесты принудительного бэкапа конфигурации LDPlayer."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from datetime import date
from pathlib import Path


SCRIPT_PATH = Path(__file__).parents[2] / "RSSv7" / "force_config_backup.py"
SPEC = importlib.util.spec_from_file_location("force_config_backup", SCRIPT_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Не удалось загрузить модуль: {SCRIPT_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ForceConfigBackupTests(unittest.TestCase):
    """Проверяет создание и полную замену дневного снимка."""

    def test_creates_today_backup_and_replaces_previous_contents(self) -> None:
        """Повторный запуск обновляет файлы и удаляет устаревшие из снимка."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "config"
            backup_root = root / "backups"
            source.mkdir()
            (source / "leidian0.config").write_text("new", encoding="utf-8")
            (source / "nested").mkdir()
            (source / "nested" / "extra.config").write_text("extra", encoding="utf-8")

            destination = backup_root / "09__08__2026"
            destination.mkdir(parents=True)
            (destination / "leidian0.config").write_text("old", encoding="utf-8")
            (destination / "removed.config").write_text("stale", encoding="utf-8")

            result = MODULE.create_forced_backup(source, backup_root, date(2026, 8, 9))

            self.assertEqual(result, destination)
            self.assertEqual((destination / "leidian0.config").read_text(encoding="utf-8"), "new")
            self.assertEqual((destination / "nested" / "extra.config").read_text(encoding="utf-8"), "extra")
            self.assertFalse((destination / "removed.config").exists())

    def test_does_not_touch_existing_backup_when_source_is_missing(self) -> None:
        """При ошибочном исходном пути сохранённый дневной снимок остаётся целым."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backup_root = root / "backups"
            destination = backup_root / "09__08__2026"
            destination.mkdir(parents=True)
            existing = destination / "leidian0.config"
            existing.write_text("saved", encoding="utf-8")

            with self.assertRaises(FileNotFoundError):
                MODULE.create_forced_backup(root / "missing", backup_root, date(2026, 8, 9))

            self.assertEqual(existing.read_text(encoding="utf-8"), "saved")


if __name__ == "__main__":
    unittest.main()
