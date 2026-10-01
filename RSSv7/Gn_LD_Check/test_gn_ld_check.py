"""Тесты безопасной диагностики web-портов и защиты от циклических перезапусков."""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, main
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch


MODULE_PATH = Path(__file__).with_name("Gn_Ld_Check.py")
SPEC = importlib.util.spec_from_file_location("gn_ld_check_under_test", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _listener(port: int, pid: int | None):
    return SimpleNamespace(
        status=MODULE.psutil.CONN_LISTEN,
        laddr=SimpleNamespace(port=port),
        pid=pid,
    )


def _state_db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:", isolation_level=None)
    con.execute("CREATE TABLE meta_kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    return con


def _settings(db_path: str) -> object:
    return MODULE.Settings(
        telegram_token="x" * 40,
        chat_id="1",
        thread_id=None,
        threshold_windows=5,
        gnbots_shortcut=r"C:\GnBots.lnk",
        log_dir=r"C:\logs",
        days_back_scan=1,
        inactivity_minutes=20,
        web_port_start=5508,
        web_port_end=5512,
        restart_cooldown_minutes=60,
        alert_repeat_minutes=360,
        db_path=db_path,
        retention_days=2,
        tail_init_bytes=65536,
    )


def _live_details() -> dict:
    return {
        "fake.log": {
            "last_live_min_ago": 0.1,
            "last_idle_min_ago": 0.1,
            "last_activity_min_ago": 0.1,
            "mtime_min_ago": 0.1,
        }
    }


class WebPortInspectionTests(TestCase):
    def test_all_orphaned_ports_block_restart(self):
        listeners = [_listener(port, 1000 + port) for port in range(5508, 5513)]

        with patch.object(MODULE.psutil, "net_connections", return_value=listeners), patch.object(
            MODULE.psutil,
            "Process",
            side_effect=lambda pid: (_ for _ in ()).throw(MODULE.psutil.NoSuchProcess(pid)),
        ):
            result = MODULE.inspect_gnbots_web_ports(5508, 5512)

        self.assertTrue(result.blocked)
        self.assertEqual(result.free_ports, ())
        self.assertEqual({owner.kind for owner in result.owners}, {"orphaned"})

    def test_foreign_listener_is_reported_without_blocking_free_ports(self):
        process = SimpleNamespace(name=lambda: "UnrelatedService.exe")

        with patch.object(MODULE.psutil, "net_connections", return_value=[_listener(5508, 42)]), patch.object(
            MODULE.psutil, "Process", return_value=process
        ):
            result = MODULE.inspect_gnbots_web_ports(5508, 5512)

        self.assertFalse(result.blocked)
        self.assertEqual(result.free_ports, (5509, 5510, 5511, 5512))
        self.assertEqual(result.owners[0].kind, "foreign")
        self.assertEqual(result.owners[0].process_name, "UnrelatedService.exe")

    def test_live_gnbots_listener_is_not_treated_as_blocker(self):
        listeners = [_listener(port, port) for port in range(5508, 5513)]
        processes = {
            5508: SimpleNamespace(name=lambda: "GnBots.exe"),
            5509: SimpleNamespace(name=lambda: "Other.exe"),
            5510: SimpleNamespace(name=lambda: "Other.exe"),
            5511: SimpleNamespace(name=lambda: "Other.exe"),
            5512: SimpleNamespace(name=lambda: "Other.exe"),
        }

        with patch.object(MODULE.psutil, "net_connections", return_value=listeners), patch.object(
            MODULE.psutil, "Process", side_effect=lambda pid: processes[pid]
        ):
            result = MODULE.inspect_gnbots_web_ports(5508, 5512)

        self.assertFalse(result.blocked)
        self.assertEqual(result.gnbots_ports, (5508,))


class WatchdogStateTests(TestCase):
    def test_identical_alert_is_suppressed_until_repeat_interval(self):
        con = _state_db()

        self.assertTrue(MODULE.should_send_incident_alert(con, "same", 60, current_ts=1000))
        self.assertFalse(MODULE.should_send_incident_alert(con, "same", 60, current_ts=2000))
        self.assertTrue(
            MODULE.should_send_incident_alert(
                con,
                "same",
                60,
                current_ts=2000,
                key_prefix="web_ports",
            )
        )
        self.assertTrue(MODULE.should_send_incident_alert(con, "same", 60, current_ts=4600))
        self.assertTrue(MODULE.should_send_incident_alert(con, "changed", 60, current_ts=4601))

    def test_restart_cooldown_reports_remaining_minutes(self):
        con = _state_db()
        MODULE.db_set_kv(con, "watchdog_last_restart_ts", "1000")

        self.assertAlmostEqual(MODULE.restart_cooldown_remaining(con, 60, current_ts=2200), 40.0)
        self.assertEqual(MODULE.restart_cooldown_remaining(con, 60, current_ts=5000), 0.0)


class WatchdogActionTests(IsolatedAsyncioTestCase):
    async def test_only_blocked_ports_never_restart_processes(self):
        inspection = MODULE.WebPortInspection(
            5508,
            5512,
            (),
            tuple(MODULE.WebPortOwner(port, port, None, "orphaned") for port in range(5508, 5513)),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            cfg = _settings(str(Path(temp_dir) / "state.sqlite3"))
            with patch.object(MODULE, "is_process_running", return_value=True), patch.object(
                MODULE, "count_processes", return_value=5
            ), patch.object(MODULE, "inspect_gnbots_web_ports", return_value=inspection), patch.object(
                MODULE, "_expand_masks", return_value=["fake.log"]
            ), patch.object(
                MODULE, "scan_logs_incremental", return_value=(time.time(), _live_details())
            ), patch.object(MODULE, "kill_process") as kill_process, patch.object(
                MODULE, "build_bot", return_value=object()
            ), patch.object(MODULE, "flush_spool", new=AsyncMock()), patch.object(
                MODULE, "safe_send", new=AsyncMock()
            ):
                await MODULE.check_and_reboot(cfg)

        kill_process.assert_not_called()

    async def test_real_problem_restarts_once_then_obeys_cooldown(self):
        inspection = MODULE.WebPortInspection(
            5508,
            5512,
            (),
            tuple(MODULE.WebPortOwner(port, port, None, "orphaned") for port in range(5508, 5513)),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            cfg = _settings(str(Path(temp_dir) / "state.sqlite3"))
            with patch.object(MODULE, "is_process_running", return_value=True), patch.object(
                MODULE, "count_processes", return_value=0
            ), patch.object(MODULE, "inspect_gnbots_web_ports", return_value=inspection), patch.object(
                MODULE, "_expand_masks", return_value=["fake.log"]
            ), patch.object(
                MODULE, "scan_logs_incremental", return_value=(time.time(), _live_details())
            ), patch.object(MODULE, "kill_process", return_value=[]) as kill_process, patch.object(
                MODULE.time, "sleep"
            ), patch.object(MODULE.os, "startfile", create=True) as startfile, patch.object(
                MODULE, "build_bot", return_value=object()
            ), patch.object(MODULE, "flush_spool", new=AsyncMock()), patch.object(
                MODULE, "safe_send", new=AsyncMock()
            ):
                await MODULE.check_and_reboot(cfg)
                await MODULE.check_and_reboot(cfg)

        self.assertEqual(kill_process.call_count, 3)
        startfile.assert_called_once_with(cfg.gnbots_shortcut)


if __name__ == "__main__":
    main()
