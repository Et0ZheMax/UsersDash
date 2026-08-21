"""Модульные тесты безопасной логики VikingRecovery."""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from datetime import datetime
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import Mock

from viking_recovery import (
    OcrWord,
    Farm,
    Instance,
    RecoveryEngine,
    RecoveryError,
    classify_login_screen,
    choose_backup_name,
    is_resource_prompt,
    load_farms,
    parse_wm_size,
    parse_ocr_payload,
    resolve_profile_path,
    required_free_space,
    select_igg_row,
)


class ProfileTests(unittest.TestCase):
    def test_loads_nested_appmenu_without_exposing_values(self) -> None:
        menu = {
            "ScriptId": "appmenu",
            "Config": {"Custom": "203689", "Email": "mail@example", "Password": "secret", "Slot": "igg"},
        }
        payload = [{"Name": "Akhm2", "Active": True, "MenuData": json.dumps(menu)}]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profile.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            farm = load_farms(path)[0]
        self.assertEqual(farm.name, "Akhm2")
        self.assertEqual(farm.custom, "203689")
        self.assertTrue(farm.ready)

    def test_resolves_selected_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "profiles").mkdir()
            (root / "profiles" / "FRESH_NOX.json").write_text("[]", encoding="utf-8")
            (root / "config.json").write_text(
                json.dumps({"SelectedProfile": "FRESH_NOX"}), encoding="utf-8"
            )
            result = resolve_profile_path(root)
        self.assertEqual(result.name, "FRESH_NOX.json")


class RenameTests(unittest.TestCase):
    def test_prefers_plain_old_suffix(self) -> None:
        self.assertEqual(choose_backup_name("Akhm2", {"Akhm2"}), "Akhm2_OLD")

    def test_adds_timestamp_on_collision(self) -> None:
        now = datetime(2026, 8, 21, 20, 0, 1)
        result = choose_backup_name("Akhm2", {"Akhm2", "Akhm2_OLD"}, now)
        self.assertEqual(result, "Akhm2_OLD_20260821_200001")


class CloneTests(unittest.TestCase):
    def make_engine(self) -> RecoveryEngine:
        engine = object.__new__(RecoveryEngine)
        engine.logger = Mock()
        engine.status = Mock()
        engine.cancel_event = threading.Event()
        engine.ldconsole = Path("ldconsole.exe")
        engine.ldplayer_dir = Path("C:/LDPlayer/LDPlayer9")
        engine.runner = Mock()
        engine.new_index = None
        engine._repair_missing_vbox = Mock()
        engine._validate_instance_storage = Mock()
        return engine

    def test_accepts_new_index_as_ldconsole_exit_code(self) -> None:
        engine = self.make_engine()
        engine.list_instances = Mock(
            side_effect=[[Instance(0, "4copy")], [Instance(0, "4copy"), Instance(6, "RECOVERY_Test")]]
        )
        engine.runner.run.return_value = CompletedProcess([], 6, "", "")
        farm = Farm("Test", "mail", "password", "123456", "igg", True)
        self.assertEqual(engine.clone_template(farm), 6)
        engine._repair_missing_vbox.assert_called_once_with(6)
        engine._validate_instance_storage.assert_called_once_with(6)

    def test_reuses_single_recovery_instance(self) -> None:
        engine = self.make_engine()
        engine.list_instances = Mock(return_value=[Instance(6, "RECOVERY_Test_20260821_195429")])
        farm = Farm("Test", "mail", "password", "123456", "igg", True)
        self.assertEqual(engine.clone_template(farm), 6)
        engine.runner.run.assert_not_called()
        engine._repair_missing_vbox.assert_called_once_with(6)

    def test_repairs_missing_vbox_and_data_uuid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            template = root / "vms" / "leidian0"
            clone = root / "vms" / "leidian51"
            template.mkdir(parents=True)
            clone.mkdir(parents=True)
            template.joinpath("leidian.vbox").write_text(
                '<Machine uuid="{20160302-aaaa-aaaa-0eee-000000000000}" name="leidian0">\n'
                '<HardDisk uuid="{20160302-cccc-cccc-0eee-000000000000}" '
                'location="data.vmdk"/>\n'
                '<Adapter MACAddress="00DBFE6A4838"/>\n'
                '<Image uuid="{20160302-cccc-cccc-0eee-000000000000}"/>\n'
                "</Machine>\n",
                encoding="utf-8",
            )
            clone.joinpath("data.vmdk").write_bytes(
                b'ddb.uuid.image = "20160302-cccc-cccc-0eee-000000000000"\n'
            )
            clone.joinpath("sdcard.vmdk").touch()
            clone.joinpath("system.vmdk").touch()
            engine = object.__new__(RecoveryEngine)
            engine.ldplayer_dir = root
            engine.logger = Mock()
            engine.list_instances = Mock(
                return_value=[Instance(51, "RECOVERY_Test_20260821_201014")]
            )

            RecoveryEngine._repair_missing_vbox(engine, 51)

            vbox = clone.joinpath("leidian.vbox").read_text(encoding="utf-8")
            self.assertIn("000000000033", vbox)
            self.assertIn('name="leidian51"', vbox)
            self.assertIn(b"000000000033", clone.joinpath("data.vmdk").read_bytes())
            self.assertTrue(clone.joinpath("player_life").exists())


class OcrSelectionTests(unittest.TestCase):
    def test_decodes_ocr_text_with_quotes_from_base64(self) -> None:
        payload = json.dumps(
            [{"TextBase64": "Vid7JVMi", "X1": 22, "Y1": 10, "X2": 92, "Y2": 51}]
        )
        self.assertEqual(parse_ocr_payload(payload)[0].text, "V'{%S\"")

    def test_waits_on_loading_screen_without_clicking(self) -> None:
        self.assertEqual(classify_login_screen("Downloading resources, please wait"), "wait")

    def test_recognizes_expired_login_and_provider_screen(self) -> None:
        self.assertEqual(classify_login_screen("HELP Confirm"), "confirm_expired")
        self.assertEqual(classify_login_screen("IGG Account Facebook Guest"), "choose_provider")

    def test_selects_row_by_custom_prefix(self) -> None:
        words = [
            OcrWord("2036890958", 168, 77, 229, 85),
            OcrWord("9999999999", 168, 127, 229, 135),
        ]
        self.assertEqual(select_igg_row(words, "203689"), (300, 81))

    def test_requires_custom_when_multiple_ids_exist(self) -> None:
        words = [
            OcrWord("2036890958", 168, 77, 229, 85),
            OcrWord("9999999999", 168, 127, 229, 135),
        ]
        with self.assertRaises(RecoveryError):
            select_igg_row(words, "")


class ParsingTests(unittest.TestCase):
    def test_prefers_override_size(self) -> None:
        output = "Physical size: 1280x720\nOverride size: 640x480\n"
        self.assertEqual(parse_wm_size(output), (640, 480))

    def test_accepts_stylized_resource_title_ocr_error(self) -> None:
        self.assertTrue(is_resource_prompt("RESOLR<E DOWNLOAD"))
        self.assertFalse(is_resource_prompt("Downloading resources: 120/1907 MB"))

    def test_required_space_includes_resource_reserve(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            template = Path(directory)
            (template / "disk.vmdk").write_bytes(b"x" * 1024)
            self.assertGreaterEqual(required_free_space(template), 8 * 1024**3)


if __name__ == "__main__":
    unittest.main()
