"""Тесты проверенных снимков и атомарного восстановления config LDPlayer."""

from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from RSSv7.config_recovery import ConfigRecoveryError, create_snapshot, find_latest_config, restore_config


class ConfigRecoveryTests(unittest.TestCase):
    def test_restore_uses_latest_snapshot_matching_account_and_keeps_rollback(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, snapshots, rollback = root / "source", root / "snapshots", root / "rollback"
            source.mkdir()
            config = source / "leidian7.config"
            config.write_text("name=OLD", encoding="utf-8")
            create_snapshot(source, snapshots, now=datetime(2026, 9, 1, 10))
            config.write_text("name=ANGEL", encoding="utf-8")
            create_snapshot(source, snapshots, now=datetime(2026, 9, 1, 11))
            destination = root / "active" / config.name
            destination.parent.mkdir()
            destination.write_text("broken", encoding="utf-8")

            selected = find_latest_config(snapshots, config.name, required_text="ANGEL")
            saved = restore_config(selected, destination, rollback)

            self.assertEqual(destination.read_text(encoding="utf-8"), "name=ANGEL")
            self.assertEqual(saved.read_text(encoding="utf-8"), "broken")

    def test_tampered_snapshot_is_never_selected(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, snapshots = root / "source", root / "snapshots"
            source.mkdir()
            config = source / "leidian7.config"
            config.write_text("name=ANGEL", encoding="utf-8")
            created = create_snapshot(source, snapshots, now=datetime(2026, 9, 1, 10))
            (created / config.name).write_text("tampered", encoding="utf-8")

            with self.assertRaises(ConfigRecoveryError):
                find_latest_config(snapshots, config.name, required_text="ANGEL")


if __name__ == "__main__":
    unittest.main()
