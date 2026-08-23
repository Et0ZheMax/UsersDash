"""Тесты актуальной сводки проблем LD_problems."""

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import ModuleType
import sys

telegram = ModuleType("telegram")
telegram.Bot = object
telegram_error = ModuleType("telegram.error")
telegram_error.TelegramError = Exception
telegram_error.TimedOut = TimeoutError
sys.modules.setdefault("telegram", telegram)
sys.modules.setdefault("telegram.error", telegram_error)

import LD_problems


class CurrentProblemRecordsTests(unittest.TestCase):
    def test_successful_cycle_clears_older_login_and_restart_errors(self) -> None:
        records = [
            {
                "account": "Akhm2",
                "file": "bot.txt",
                "line": "2026-08-22 19:19:38.876 +03:00 |id|Launch: Account expired, try to login.",
            },
            {
                "account": "Akhm2",
                "file": "bot.txt",
                "line": "2026-08-22T19:20:00 | MULTI>4 'Booting timeout. Restarting.' за 25м",
            },
        ]

        result = LD_problems._current_problem_records(
            records,
            {"Akhm2": datetime(2026, 8, 22, 21, 53)},
        )

        self.assertEqual(result, [])

    def test_error_after_successful_cycle_remains_current(self) -> None:
        record = {
            "account": "Akhm2",
            "file": "bot.txt",
            "line": "2026-08-22 22:01:00.000 +03:00 |id|Launch: Account expired, try to login.",
        }

        result = LD_problems._current_problem_records(
            [record],
            {"Akhm2": datetime(2026, 8, 22, 21, 53)},
        )

        self.assertEqual(result, [record])

    def test_duplicate_log_copies_count_as_one_event(self) -> None:
        records = [
            {
                "account": "Akhm2",
                "file": "bot.txt",
                "line": "2026-08-22 22:01:00.100 +03:00 |id|Launch: Account expired, try to login.",
            },
            {
                "account": "Akhm2",
                "file": "script.txt",
                "line": "2026-08-22 22:01:00.101 +03:00 |id|Tools.Log(Launch: Account expired, try to login., 0)",
            },
        ]

        result = LD_problems._current_problem_records(records, {})

        self.assertEqual(len(result), 1)

    def test_real_account_done_is_healthy_but_empty_loop_is_not(self) -> None:
        self.assertTrue(LD_problems._is_healthy_activity("Account Done [00:10:13]"))
        self.assertFalse(LD_problems._is_healthy_activity("Account Done [00:00:08]"))


def _profile(name: str, instance: int, mode: str, position: str) -> dict:
    step = {
        "ScriptId": "vikingbot.base.accountswitch",
        "Config": {
            "mode": {"value": mode, "optons": ["Player", "IGG_ID"]},
            "account": {"value": position, "options": ["1", "2", "3"]},
        },
    }
    return {"Name": name, "InstanceId": instance, "Data": json.dumps([step])}


class AccountSwitchProfileTests(unittest.TestCase):
    def test_autofix_repairs_layer_and_duplicate_numbered_pair(self) -> None:
        profiles = [
            _profile("Filosof7", 22, "layer", "1"),
            _profile("Filosof8", 22, "layer", "1"),
        ]

        problems, changed = LD_problems.analyze_account_switch_profiles(
            profiles, auto_fix=True
        )

        self.assertTrue(changed)
        self.assertEqual(len(problems), 3)
        first = json.loads(profiles[0]["Data"])[0]["Config"]
        second = json.loads(profiles[1]["Data"])[0]["Config"]
        self.assertEqual(first["mode"]["value"], "Player")
        self.assertEqual(second["mode"]["value"], "Player")
        self.assertEqual(first["account"]["value"], "1")
        self.assertEqual(second["account"]["value"], "2")
        self.assertEqual(first["mode"]["optons"], ["Player", "IGG_ID"])
        self.assertNotIn("options", first["mode"])

    def test_ambiguous_duplicate_pair_is_reported_without_changes(self) -> None:
        profiles = [
            _profile("Alpha", 5, "Player", "1"),
            _profile("Beta", 5, "Player", "1"),
        ]

        problems, changed = LD_problems.analyze_account_switch_profiles(
            profiles, auto_fix=True
        )

        self.assertFalse(changed)
        self.assertEqual(len(problems), 1)
        self.assertFalse(problems[0]["fixed"])

    def test_valid_pair_has_no_problems(self) -> None:
        profiles = [
            _profile("Filosof9", 23, "Player", "1"),
            _profile("Filosof10", 23, "Player", "2"),
        ]

        problems, changed = LD_problems.analyze_account_switch_profiles(
            profiles, auto_fix=True
        )

        self.assertFalse(changed)
        self.assertEqual(problems, [])

    def test_account_switch_problem_is_critical_kind(self) -> None:
        kind, _ = LD_problems._classify_problem(
            "Account Switch config: позиции 1 / 1"
        )

        self.assertEqual(kind, "account_switch")

    def test_checker_writes_atomic_fix_and_backup(self) -> None:
        profiles = [
            _profile("Filosof7", 22, "layer", "1"),
            _profile("Filosof8", 22, "layer", "1"),
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            profile_path = Path(tmp_dir) / "LDPplayer.json"
            profile_path.write_text(
                json.dumps(profiles, ensure_ascii=False), encoding="utf-8"
            )
            original_profile_file = LD_problems.PROFILE_FILE
            LD_problems.PROFILE_FILE = str(profile_path)
            try:
                records = LD_problems.check_account_switch_profile(auto_fix=True)
            finally:
                LD_problems.PROFILE_FILE = original_profile_file

            saved = json.loads(profile_path.read_text(encoding="utf-8"))
            configs = [json.loads(profile["Data"])[0]["Config"] for profile in saved]
            backups = list(
                profile_path.parent.glob(
                    "LDPplayer.json.before_accountswitch_autofix_*"
                )
            )

        self.assertEqual(len(records), 3)
        self.assertTrue(all("автоисправлено" in row["line"] for row in records))
        self.assertEqual([cfg["account"]["value"] for cfg in configs], ["1", "2"])
        self.assertEqual([cfg["mode"]["value"] for cfg in configs], ["Player", "Player"])
        self.assertEqual(len(backups), 1)


if __name__ == "__main__":
    unittest.main()
