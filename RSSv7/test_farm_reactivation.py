import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from farm_reactivation import (
    MARKER_KEY,
    ReactivationError,
    iter_pending_ready,
    mark_emulator_ready,
    prepare_reactivation,
)


def _record(record_id: str, name: str, instance_id: int, *, password: str = "old") -> dict:
    return {
        "Id": record_id,
        "Name": name,
        "InstanceId": instance_id,
        "Active": False,
        "Data": '[{"ScriptId":"vikingbot.base.dailies"}]',
        "MenuData": json.dumps(
            {
                "ScriptId": "appmenu",
                "Config": {
                    "Email": "old@example.test",
                    "Password": password,
                    "Custom": "100",
                    "Slot": "igg",
                },
            }
        ),
    }


class FarmReactivationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.profile = self.root / "LDPplayer.json"
        self.backups = self.root / "backups"
        self.rollbacks = self.root / "rollbacks"
        self.profile.write_text("[]", encoding="utf-8")

    def tearDown(self):
        self.tmp.cleanup()

    def _backup(self, folder: str, records: list[dict]) -> None:
        path = self.backups / folder / "LDPplayer.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(records), encoding="utf-8")

    def test_exact_id_wins_over_newer_same_name(self):
        self._backup("14__07__2026", [_record("wanted-id", "ANGEL", 6)])
        self._backup("27__08__2026", [_record("wrong-id", "ANGEL", 10)])

        result = prepare_reactivation(
            self.profile,
            self.backups,
            self.rollbacks,
            account_id="wanted-id",
            farm_name="ANGEL",
            email="new@example.test",
            password="new-secret",
            igg_id="777",
        )

        self.assertEqual(result["source"], "archive")
        records = json.loads(self.profile.read_text(encoding="utf-8"))
        self.assertEqual(len(records), 1)
        restored = records[0]
        self.assertEqual(restored["Id"], "wanted-id")
        self.assertEqual(restored["InstanceId"], -1)
        self.assertFalse(restored["Active"])
        self.assertIn("vikingbot.base.dailies", restored["Data"])
        menu = json.loads(restored["MenuData"])
        self.assertEqual(menu["Config"]["Email"], "new@example.test")
        self.assertEqual(menu["Config"]["Password"], "new-secret")
        self.assertEqual(menu["Config"]["Custom"], "777")
        self.assertEqual(restored[MARKER_KEY]["status"], "prepared")
        self.assertTrue(Path(result["rollback_path"]).is_file())

    def test_name_collision_is_rejected(self):
        self.profile.write_text(
            json.dumps([_record("wrong-id", "ANGEL", 10)]),
            encoding="utf-8",
        )
        self._backup("14__07__2026", [_record("wanted-id", "ANGEL", 6)])

        with self.assertRaisesRegex(ReactivationError, "занято другой записью"):
            prepare_reactivation(
                self.profile,
                self.backups,
                self.rollbacks,
                account_id="wanted-id",
                farm_name="ANGEL",
                email="new@example.test",
                password="secret",
                igg_id="777",
            )

    def test_ready_marker_requires_non_negative_instance(self):
        record = _record("wanted-id", "ANGEL", -1)
        record[MARKER_KEY] = {"status": "prepared"}
        self.assertEqual(list(iter_pending_ready([record])), [])

        record["InstanceId"] = 42
        self.assertEqual(list(iter_pending_ready([record])), [record])
        mark_emulator_ready(record)
        self.assertTrue(record["Active"])
        self.assertEqual(record[MARKER_KEY]["status"], "emulator_ready")


if __name__ == "__main__":
    unittest.main()
