import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

from UsersDash.services import db_backup


class DbBackupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db_file = self.root / "app.db"
        self.backup_dir = self.root / "backups"
        with sqlite3.connect(self.db_file) as conn:
            conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT)")
            conn.execute("INSERT INTO sample (name) VALUES ('ok')")

        self.patches = [
            mock.patch.object(db_backup, "DB_FILE", self.db_file),
            mock.patch.object(db_backup, "BACKUP_DIR", self.backup_dir),
            mock.patch.object(db_backup, "LOCK_FILE", self.backup_dir / ".daily_backup.lock"),
        ]
        for patcher in self.patches:
            patcher.start()

    def tearDown(self):
        for patcher in reversed(self.patches):
            patcher.stop()
        self.tmp.cleanup()

    def test_backup_database_creates_readable_sqlite_snapshot(self):
        backup_path = db_backup.backup_database("daily")

        self.assertTrue(backup_path.exists())
        with sqlite3.connect(backup_path) as conn:
            rows = conn.execute("SELECT name FROM sample").fetchall()
        self.assertEqual(rows, [("ok",)])

    def test_ensure_daily_backup_runs_only_once_per_day(self):
        first = db_backup.ensure_daily_backup(date.today())
        second = db_backup.ensure_daily_backup(date.today())

        self.assertIsNotNone(first)
        self.assertIsNone(second)
        daily_backups = sorted(self.backup_dir.glob("app_*_daily.db"))
        self.assertEqual(len(daily_backups), 1)


if __name__ == "__main__":
    unittest.main()
