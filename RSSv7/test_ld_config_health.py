"""Регрессии реального инцидента 208 и защит восстановления (только временные VM)."""

import ast
from datetime import datetime
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from config_recovery import create_snapshot
from ld_config_health import (RepairBlocked, config_problems, inventory, console_rows,
                              runtime_inventory, repair_instances, repair_lock, select_source, disk_usage,
                              target_is_disabled)


def config(name="KR"):
    return {"statusSettings.playerName": name, "advancedSettings.resolution": {"width": 640, "height": 480}}


class ConfigHealthTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.vms = self.root / "vms"
        (self.vms / "config").mkdir(parents=True)
        (self.vms / "leidian7").mkdir()
        (self.vms / "leidian10").mkdir()
        self.profile = self.root / "profile.json"
        self.profile.write_text(json.dumps([{"Id": "kr", "Name": "KR", "InstanceId": 7, "Active": False}]))
        self.destination = self.vms / "config/leidian7.config"
        self.destination.write_text(json.dumps(config(None)))
        (self.vms / "config/leidian10.config").write_text(json.dumps(config(None)))
        self.snapshots, self.daily, self.rollback = [self.root / name for name in ("snapshots", "daily", "rollback")]

    def snapshot(self, value, day=15):
        source = self.root / "source"
        source.mkdir(exist_ok=True)
        (source / "leidian7.config").write_text(json.dumps(value))
        return create_snapshot(source, self.snapshots, now=datetime(2026, 10, day))

    def repair(self, indices=None, **kwargs):
        return repair_instances(indices or [7], vms=self.vms, profile=self.profile,
                                snapshot_root=self.snapshots, daily_root=self.daily,
                                rollback_root=self.rollback, assert_stopped=lambda index: None, **kwargs)

    def test_inactive_and_orphan_are_both_detected(self):
        report = inventory(self.vms, self.profile)
        self.assertEqual(report["crashed"], ["leidian7.config", "leidian10.config"])
        self.assertEqual([row["usage_class"] for row in report["instances"]], ["disabled", "unlinked"])

    def test_missing_config_is_detected(self):
        self.destination.unlink()
        self.assertIn("config_missing", inventory(self.vms, self.profile)["instances"][0]["issues"])

    def test_invalid_json_not_healthy(self):
        self.destination.write_text("{")
        self.assertIn("config_unreadable", inventory(self.vms, self.profile)["instances"][0]["issues"])

    def test_bom_supported(self):
        self.destination.write_text(json.dumps(config()), encoding="utf-8-sig")
        self.assertEqual(inventory(self.vms, self.profile)["instances"][0]["issues"], [])

    def test_generic_default_names(self):
        for name in (None, "", "LDPlayer", "LDPlayer-7", "LDPlayer 10", "ldplayer_7"):
            self.assertIn("player_name_lost", config_problems(config(name)))
        self.assertEqual(config_problems(config("KR")), [])

    def test_factory_resolution_can_be_implicit(self):
        self.assertEqual(config_problems({'statusSettings.playerName': 'checkinstal'}), [])
        self.assertIn('resolution_invalid', config_problems(
            {'statusSettings.playerName': 'KR', 'advancedSettings.resolution': {'width': 0}}))

    def test_disabled_target_requires_current_selected_profile(self):
        gn_config = self.root / 'gn-config.json'
        gn_config.write_text(json.dumps({'SelectedProfile': 'profile'}))
        self.assertTrue(target_is_disabled(7, self.profile, gn_config))
        gn_config.write_text(json.dumps({'SelectedProfile': 'another'}))
        self.assertFalse(target_is_disabled(7, self.profile, gn_config))
        gn_config.write_text('{')
        self.assertFalse(target_is_disabled(7, self.profile, gn_config))

    def test_shared_target_with_any_active_account_blocks_gui_exception(self):
        rows = json.loads(self.profile.read_text())
        rows.append({'Id': 'kr2', 'Name': 'KR2', 'InstanceId': 7, 'Active': True})
        self.profile.write_text(json.dumps(rows))
        gn_config = self.root / 'gn-config.json'
        gn_config.write_text(json.dumps({'SelectedProfile': 'profile.json'}))
        self.assertFalse(target_is_disabled(7, self.profile, gn_config))

    def test_bad_profile_not_empty_success(self):
        self.profile.write_text("{")
        self.assertFalse(inventory(self.vms, self.profile)["ok"])
        with self.assertRaises(RepairBlocked):
            self.repair()

    def test_skip_latest_semantically_damaged_snapshot(self):
        good = self.snapshot(config(), 15)
        self.snapshot(config(None), 18)
        path, _ = select_source(7, {"KR"}, self.snapshots, self.daily)
        self.assertEqual(path.parent, good)

    def test_manifest_tamper_rejected(self):
        good = self.snapshot(config(), 14)
        newer = self.snapshot(config(), 15)
        (newer / "leidian7.config").write_text(json.dumps(config()) + " ")
        path, _ = select_source(7, {"KR"}, self.snapshots, self.daily)
        self.assertEqual(path.parent, good)

    def test_wrong_owner_and_substring_are_rejected(self):
        self.snapshot(config("Airat2"))
        for expected in ({"KR"}, {"Airat"}):
            with self.assertRaises(RepairBlocked):
                select_source(7, expected, self.snapshots, self.daily)

    def test_shared_vm_accepts_second_profile_name(self):
        self.snapshot(config("Filosof3/4"))
        self.assertTrue(select_source(7, {"Filosof4"}, self.snapshots, self.daily))

    def test_legacy_dates_sorted_chronologically(self):
        for day, name in (("31__08__2026", "KR"), ("01__10__2026", "KR")):
            folder = self.daily / day
            folder.mkdir(parents=True)
            (folder / "leidian7.config").write_text(json.dumps(config(name)))
        source, _ = select_source(7, {"KR"}, self.snapshots, self.daily)
        self.assertEqual(source.parent.name, "01__10__2026")

    def test_orphan_never_guesses_historical_owner(self):
        self.snapshot(config())
        result = self.repair([10])
        self.assertFalse(result["ok"])
        self.assertIn("владельца", result["results"][0]["message"])

    def test_verified_restore_and_rollback_preserve_profile_and_disk(self):
        self.snapshot(config())
        original = self.destination.read_bytes()
        profile = self.profile.read_bytes()
        disk = self.vms / "leidian7/data.vmdk"
        disk.write_bytes(b"original android data")
        result = self.repair()
        self.assertTrue(result["ok"])
        self.assertEqual(result["results"][0]["status"], "config_restored")
        self.assertEqual(Path(result["results"][0]["rollback"]).read_bytes(), original)
        self.assertEqual(self.profile.read_bytes(), profile)
        self.assertEqual(disk.read_bytes(), b"original android data")
        self.assertEqual(result["results"][0]["android_status"], "not_checked")

    def test_dry_run_writes_no_config(self):
        self.snapshot(config())
        original = self.destination.read_bytes()
        self.assertEqual(self.repair(dry_run=True)["results"][0]["status"], "planned")
        self.assertEqual(self.destination.read_bytes(), original)

    def test_running_guard_blocks_and_no_write(self):
        self.snapshot(config())
        before = self.destination.read_bytes()
        def busy(index):
            raise RepairBlocked("running")
        result = repair_instances([7], vms=self.vms, profile=self.profile, snapshot_root=self.snapshots,
                                  daily_root=self.daily, rollback_root=self.rollback, assert_stopped=busy)
        self.assertFalse(result["ok"])
        self.assertEqual(self.destination.read_bytes(), before)

    def test_guard_rechecked_before_commit(self):
        self.snapshot(config())
        calls = []
        def guard(index):
            calls.append(index)
            if len(calls) == 2:
                raise RepairBlocked("started meanwhile")
        result = repair_instances([7], vms=self.vms, profile=self.profile, snapshot_root=self.snapshots,
                                  daily_root=self.daily, rollback_root=self.rollback, assert_stopped=guard)
        self.assertFalse(result["ok"])
        self.assertEqual(len(calls), 2)
        self.assertEqual(json.loads(self.destination.read_text())["statusSettings.playerName"], None)

    def test_changed_profile_cancels_write(self):
        self.snapshot(config())
        def guard(index):
            self.profile.write_text("[]")
        result = repair_instances([7], vms=self.vms, profile=self.profile, snapshot_root=self.snapshots,
                                  daily_root=self.daily, rollback_root=self.rollback, assert_stopped=guard)
        self.assertFalse(result["ok"])

    def test_partial_batch_not_success(self):
        self.snapshot(config())
        result = self.repair([7, 7, 10])
        self.assertFalse(result["ok"])
        self.assertEqual([r["ok"] for r in result["results"]], [True, False])

    def test_missing_disks_not_repaired_by_config(self):
        self.snapshot(config())
        (self.vms / "leidian7").rmdir()
        self.assertFalse(self.repair()["ok"])

    def test_lock_prevents_second_repair(self):
        self.snapshot(config())
        with repair_lock(self.rollback):
            with self.assertRaises(RepairBlocked):
                self.repair()
        self.assertTrue(self.repair()["ok"])

    def test_failed_atomic_replace_keeps_original(self):
        self.snapshot(config())
        before = self.destination.read_bytes()
        with patch("config_recovery.os.replace", side_effect=OSError("locked")):
            self.assertFalse(self.repair()["ok"])
        self.assertEqual(self.destination.read_bytes(), before)

    def test_invalid_ids(self):
        for value in ("../7", True, -1, 10000, "7.0"):
            with self.assertRaises(RepairBlocked):
                self.repair([value])

    def test_stopped_vm_is_not_android_failure(self):
        with patch("ld_config_health.subprocess.run", return_value=subprocess.CompletedProcess(
            [], 0, b"7,KR,0,0,0,-1,-1,640,480,120\n10,LDPlayer-10,0,0,0,-1,-1,640,480,120")):
            report = runtime_inventory(inventory(self.vms, self.profile), Path("ldconsole"))
        self.assertEqual(report["instances"][0]["runtime_status"], "stopped")
        self.assertEqual(report["instances"][0]["android_status"], "not_checked")

    def test_bad_list2_does_not_become_healthy_empty_list(self):
        for code, output in ((1, b""), (0, b""), (0, b"invalid")):
            with patch("ld_config_health.subprocess.run", return_value=subprocess.CompletedProcess([], code, output)):
                self.assertFalse(runtime_inventory(inventory(self.vms, self.profile), Path("console"))["ok"])

    def test_comma_in_ldplayer_name(self):
        with patch("ld_config_health.subprocess.run", return_value=subprocess.CompletedProcess(
            [], 0, b"7,KR, a,0,0,0,-1,-1,640,480,120")):
            self.assertEqual(console_rows(Path("console"))[7]["title"], "KR, a")

    def test_disk_usage_reads_without_mutating_files(self):
        disk = self.vms / "leidian10/data.vmdk"
        disk.write_bytes(b"x" * 5000)
        result = disk_usage(disk.parent)
        self.assertEqual(result["logical_bytes"], 5000)
        self.assertIsNotNone(result["disk_bytes"])
        self.assertEqual(disk.read_bytes(), b"x" * 5000)


class EndpointTests(unittest.TestCase):
    """Выполняет настоящие route-функции без startup потоков/БД монолита."""

    def setUp(self):
        from flask import Flask, jsonify, request
        source = Path(__file__).with_name("RssCounterWebV7.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        names = {"api_fix_do", "api_fix_config_batch", "api_fix_batch", "api_replace_configs",
                 "api_crashed_emu", "api_fix_diagnosis", "api_fix_instances", "_repair_config_request"}
        module = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef)
                                 and node.name in names], type_ignores=[])
        self.app = Flask(__name__)
        self.ns = {"app": self.app, "jsonify": jsonify, "request": request,
                   "_config_diagnosis": lambda: {"ok": True, "crashed": ["leidian7.config", "leidian10.config"]}}
        exec(compile(module, "routes", "exec"), self.ns)
        self.ns['_repair_config_request'] = lambda data: ({'ok': False, 'results': [{'ok': False}]}, 409)
        self.client = self.app.test_client()

    def test_all_repair_routes_report_failure(self):
        for route in ("do", "config_batch", "batch", "instances"):
            response = self.client.post("/api/fix/"+route, json={"instance_ids": [7]})
            self.assertEqual(response.status_code, 409)
            self.assertFalse(response.json["ok"])

    def test_get_cannot_mutate(self):
        self.assertEqual(self.client.get("/api/fix/do?acc_id=kr").status_code, 405)

    def test_legacy_cache_not_used(self):
        self.assertEqual(self.client.get("/api/crashedEmus").json, ["leidian7.config", "leidian10.config"])

    def test_bad_diagnosis_not_healthy(self):
        self.ns['_config_diagnosis'] = lambda: {'ok': False, 'errors': ['profile unreadable']}
        self.assertEqual(self.client.get('/api/crashedEmus').status_code, 503)
        self.assertEqual(self.client.get('/api/fix/diagnosis').status_code, 503)

    def test_blind_replacement_disabled(self):
        self.assertEqual(self.client.post('/api/fix/replace_configs', json={}).status_code, 409)


if __name__ == "__main__":
    unittest.main()
