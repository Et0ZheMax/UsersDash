"""Регрессия ложного простоя при устаревшей БД и обновление кэша без сообщений."""

import ast
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest


def load_functions(file: str, names: set[str], env: dict) -> None:
    tree = ast.parse(Path(__file__).with_name(file).read_text(encoding="utf-8-sig"))
    body = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    exec(compile(ast.Module(body=body, type_ignores=[]), file, "exec"), env)


class RefreshFreshnessTests(unittest.TestCase):
    def test_failed_parse_does_not_recalculate_inactivity_from_old_data(self) -> None:
        calls = []

        def broken() -> None:
            raise RuntimeError("database failure")

        env = {"parse_logs": broken, "inactive_monitor": SimpleNamespace(
            check_inactive_accounts=lambda **kwargs: calls.append(kwargs))}
        load_functions("RssCounterWebV7.py", {"_refresh_logs_and_inactive_cache"}, env)
        with self.assertRaises(RuntimeError):
            env["_refresh_logs_and_inactive_cache"]()
        self.assertEqual(calls, [])

    def test_fresh_resources_clear_false_idle_cache_without_notifications(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            short, full, state = (root / filename for filename in ("short.json", "full.json", "state.json"))
            short.write_text('[{"nickname":"farm","hours":17}]', encoding="utf-8")
            state.write_text('original notification state', encoding="utf-8")
            env = {
                "List": list, "THRESH_HOURS": 6, "CRITICAL_HOURS": 10,
                "TAG_TEXT": "0gain", "datetime": datetime, "timedelta": timedelta,
                "timezone": timezone, "json": json, "ALERT_SHORT": short,
                "ALERT_FULL": full, "STATE_FILE": state,
                "_load_active_ids_from_profile": lambda: {"a"},
                "_load_today_baseline": lambda: {},
                "_query_resources": lambda: [("a", "farm", 1, 2, 3, 4, datetime.now(timezone.utc).isoformat())],
                "_telegram": lambda _: self.fail("cache refresh must not send messages"),
            }
            load_functions("inactive_monitor.py", {"_tz_aware_from_iso", "check_inactive_accounts"}, env)
            self.assertEqual(env["check_inactive_accounts"](notify=False), [])
            self.assertEqual(json.loads(short.read_text(encoding="utf-8")), [])
            self.assertEqual(state.read_text(encoding="utf-8"), 'original notification state')


if __name__ == "__main__":
    unittest.main()
