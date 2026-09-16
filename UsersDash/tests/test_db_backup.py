import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest import mock

from UsersDash.services import db_backup


class DbBackupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db_file = self.root / "app.db"
        self.backup_dir = self.root / "backups"
        conn = sqlite3.connect(self.db_file)
        try:
            conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT)")
            conn.execute("INSERT INTO sample (name) VALUES ('ok')")
            conn.commit()
        finally:
            conn.close()

        self.patches = [
            mock.patch.object(db_backup, "DB_FILE", self.db_file),
            mock.patch.object(db_backup, "BACKUP_DIR", self.backup_dir),
            mock.patch.object(db_backup, "LOCK_FILE", self.backup_dir / ".daily_backup.lock"),
            mock.patch.object(db_backup, "MIN_FREE_RESERVE_BYTES", 0),
        ]
        for patcher in self.patches:
            patcher.start()

    def tearDown(self):
        for patcher in reversed(self.patches):
            patcher.stop()
        self.tmp.cleanup()

    def _make_backup(self, when: datetime, tag: str = "daily") -> Path:
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        path = self.backup_dir / f"app_{when:%Y%m%d_%H%M%S}_{tag}.db"
        path.write_bytes(self.db_file.read_bytes())
        return path

    def test_backup_database_creates_readable_atomic_snapshot(self):
        backup_path = db_backup.backup_database("daily")

        self.assertTrue(backup_path.exists())
        self.assertFalse(list(self.backup_dir.glob("*.partial")))
        db_backup.validate_backup(backup_path)
        conn = sqlite3.connect(backup_path)
        try:
            rows = conn.execute("SELECT name FROM sample").fetchall()
        finally:
            conn.close()
        self.assertEqual(rows, [("ok",)])

    def test_failed_validation_removes_partial_file(self):
        with mock.patch.object(db_backup, "validate_backup", side_effect=ValueError("bad")):
            with self.assertRaises(ValueError):
                db_backup.backup_database("daily")
        self.assertFalse(list(self.backup_dir.glob("*.partial")))
        self.assertFalse(list(self.backup_dir.glob("*.db")))

    def test_invalid_existing_daily_does_not_block_retry(self):
        self.backup_dir.mkdir(parents=True)
        broken = self.backup_dir / f"app_{date.today():%Y%m%d}_000500_daily.db"
        broken.write_bytes(b"broken")

        created = db_backup.ensure_daily_backup(date.today())

        self.assertIsNotNone(created)
        self.assertEqual(len(list(self.backup_dir.glob("app_*_daily.db"))), 2)

    def test_ensure_daily_backup_runs_only_once_per_day(self):
        first = db_backup.ensure_daily_backup(date.today())
        second = db_backup.ensure_daily_backup(date.today())

        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertEqual(len(list(self.backup_dir.glob("app_*_daily.db"))), 1)

    def test_rotation_plan_keeps_gfs_and_recent_manual(self):
        now = datetime(2026, 9, 16, 12, 0, 0)
        for days in range(80):
            self._make_backup(now - timedelta(days=days))
        recent_manual = self._make_backup(now - timedelta(days=3), "before_restore")
        old_manual = self._make_backup(now - timedelta(days=30), "before_restore")
        ignored = self.backup_dir / "app-before-legacy.db"
        ignored.write_bytes(b"legacy")

        plan = db_backup.build_rotation_plan(self.backup_dir, now=now)
        kept = {item.path for item in plan.keep}
        deleted = {item.path for item in plan.delete}

        self.assertIn(recent_manual, kept)
        self.assertIn(old_manual, deleted)
        self.assertIn(ignored, plan.ignored)
        self.assertGreaterEqual(
            len([item for item in plan.keep if item.is_daily]), db_backup.DAILY_KEEP
        )

    def test_rotation_is_dry_run_by_default(self):
        now = datetime(2026, 9, 16, 12, 0, 0)
        paths = [self._make_backup(now - timedelta(days=days)) for days in range(40)]

        plan = db_backup.rotate_backups(self.backup_dir, now=now)

        self.assertTrue(plan.delete)
        self.assertTrue(all(path.exists() for path in paths))

    def test_rotation_marks_corrupt_backup_invalid(self):
        now = datetime(2026, 9, 16, 12, 0, 0)
        self._make_backup(now)
        broken = self.backup_dir / "app_20260915_000500_daily.db"
        broken.write_bytes(b"broken")

        plan = db_backup.build_rotation_plan(self.backup_dir, now=now)

        self.assertEqual([item.path for item in plan.invalid], [broken])
        self.assertIn(broken, {item.path for item in plan.delete})

    def test_apply_deletes_only_planned_files_and_writes_log(self):
        now = datetime(2026, 9, 16, 12, 0, 0)
        for days in range(40):
            self._make_backup(now - timedelta(days=days))
        plan = db_backup.rotate_backups(self.backup_dir, dry_run=False, now=now)

        self.assertTrue(plan.delete)
        self.assertTrue(all(not item.path.exists() for item in plan.delete))
        self.assertTrue(all(item.path.exists() for item in plan.keep))
        self.assertTrue((self.backup_dir / db_backup.ROTATION_LOG.name).exists())

    def test_rejects_unsafe_tag(self):
        with self.assertRaises(ValueError):
            db_backup.backup_database("../outside")


if __name__ == "__main__":
    unittest.main()
