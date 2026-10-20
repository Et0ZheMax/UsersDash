"""Модульные тесты безопасной логики VikingRecovery."""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from datetime import datetime
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import Mock, patch

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
    update_profile_instance,
)


class ProfileTests(unittest.TestCase):
    @staticmethod
    def character(record_id, name, index, slot, email='shared@example', custom='100'):
        return {'Id': record_id, 'Name': name, 'InstanceId': index, 'Active': slot == 1,
                'MenuData': json.dumps({'Config': {'Email': email, 'Slot': 'igg', 'Custom': custom}}),
                'Data': json.dumps([{'ScriptId': 'vikingbot.base.accountswitch', 'IsActive': True,
                                     'Config': {'mode': {'value': 'Player'}, 'account': {'value': slot}}}])}

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

    def test_updates_exact_profile_record_instance_id_atomically(self) -> None:
        menu = {"ScriptId": "appmenu", "Config": {"Email": "m", "Password": "p", "Slot": "igg"}}
        payload = [
            {"Id": "farm-a", "Name": "Same", "InstanceId": 7, "MenuData": json.dumps(menu)},
            {"Id": "farm-b", "Name": "Same", "InstanceId": 8, "MenuData": json.dumps(menu)},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profile.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            farm = Farm("Same", "m", "p", "", "igg", True, "farm-b", 8)
            old_index = update_profile_instance(path, farm, 34)
            saved = json.loads(path.read_text(encoding="utf-8"))
            backup = json.loads(
                path.with_name(path.name + ".before_viking_recovery").read_text(encoding="utf-8")
            )

        self.assertEqual(old_index, 8)
        self.assertEqual(saved[0]["InstanceId"], 7)
        self.assertEqual(saved[1]["InstanceId"], 34)
        self.assertEqual(backup[1]["InstanceId"], 8)

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

    def test_moves_all_characters_of_same_vm_without_changing_their_settings(self) -> None:
        payload = [
            self.character('p1', 'Filosof1', 12, 1),
            self.character('p2', 'Filosof2', 12, 2),
            {'Id': 'p3', 'Name': 'Other', 'InstanceId': 19, 'Data': 'other', 'MenuData': 'login b'},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'profile.json'
            path.write_text(json.dumps(payload), encoding='utf-8')
            farm = Farm('Filosof1', 'm', 'p', '', 'igg', True, 'p1', 12)
            self.assertEqual(update_profile_instance(path, farm, 8), 12)
            saved = json.loads(path.read_text(encoding='utf-8'))
            expected = json.loads(json.dumps(payload))
            expected[0]['InstanceId'] = expected[1]['InstanceId'] = 8
            self.assertEqual(saved, expected)
            self.assertEqual(len(list(path.parent.glob('*.before_viking_recovery.*'))), 1)

    def test_reused_old_number_does_not_move_new_customer(self) -> None:
        payload = [self.character('returning', 'Old', 12, 1),
                   self.character('new-customer', 'New', 12, 2, email='new@example')]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'profile.json'
            path.write_text(json.dumps(payload), encoding='utf-8')
            update_profile_instance(path, Farm('Old', 'm', 'p', '', 'igg', True, 'returning', 12), 13)
            saved = json.loads(path.read_text(encoding='utf-8'))
        self.assertEqual(saved[0]['InstanceId'], 13)
        self.assertEqual(saved[1], payload[1])

    def test_same_login_different_game_id_is_not_shared_vm_proof(self) -> None:
        payload = [self.character('a', 'A', 12, 1), self.character('b', 'B', 12, 2, custom='200')]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'profile.json'
            path.write_text(json.dumps(payload), encoding='utf-8')
            update_profile_instance(path, Farm('A', 'm', 'p', '', 'igg', True, 'a', 12), 13)
            saved = json.loads(path.read_text(encoding='utf-8'))
        self.assertEqual(saved[1], payload[1])

    def test_occupied_target_even_if_disabled_blocks_without_writes(self) -> None:
        payload = [self.character('a', 'A', 12, 1), self.character('b', 'B', 13, 2)]
        self.assertFalse(payload[1]['Active'])
        self.assert_plan_blocked(payload, Farm('A', 'm', 'p', '', 'igg', True, 'a', 12), 13, 'другой ферме')

    def test_duplicate_record_id_blocks_without_writes(self) -> None:
        payload = [self.character('a', 'A', 12, 1), self.character('a', 'B', 19, 2)]
        self.assert_plan_blocked(payload, Farm('A', 'm', 'p', '', 'igg', True, 'a', 12), 13, 'однозначно')

    def test_duplicate_character_slots_block_without_writes(self) -> None:
        payload = [self.character('a', 'A', 12, 1), self.character('b', 'B', 12, 1)]
        self.assert_plan_blocked(payload, Farm('A', 'm', 'p', '', 'igg', True, 'a', 12), 13, 'одинаковые слоты')

    def test_unassigned_customers_do_not_form_shared_vm(self) -> None:
        payload = [self.character('a', 'A', -1, 1), self.character('b', 'B', -1, 2)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'profile.json'
            path.write_text(json.dumps(payload), encoding='utf-8')
            farm = Farm('A', 'm', 'p', '', 'igg', True, 'a', None)
            update_profile_instance(path, farm, 13, dry_run=True)
            self.assertEqual(json.loads(path.read_text(encoding='utf-8')), payload)
            update_profile_instance(path, farm, 13)
            saved = json.loads(path.read_text(encoding='utf-8'))
        self.assertEqual(saved[0]['InstanceId'], 13)
        self.assertEqual(saved[1], payload[1])

    def assert_plan_blocked(self, payload, farm, target, message):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'profile.json'
            path.write_text(json.dumps(payload), encoding='utf-8')
            original = path.read_bytes()
            with self.assertRaisesRegex(RecoveryError, message):
                update_profile_instance(path, farm, target)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(list(path.parent.glob('*.before_viking_recovery*')), [])

    def test_stale_farm_binding_is_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'profile.json'
            path.write_text(json.dumps([{'Id': 'p1', 'Name': 'Filosof1', 'InstanceId': 9}]), encoding='utf-8')
            original = path.read_bytes()
            farm = Farm('Filosof1', 'm', 'p', '', 'igg', True, 'p1', 12)
            with self.assertRaises(RecoveryError):
                update_profile_instance(path, farm, 8)
            self.assertEqual(path.read_bytes(), original)

    def test_concurrent_profile_edit_cancels_group_move(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'profile.json'
            path.write_text(json.dumps([{'Id': 'p1', 'Name': 'Filosof1', 'InstanceId': 12}]), encoding='utf-8')
            farm = Farm('Filosof1', 'm', 'p', '', 'igg', True, 'p1', 12)
            real_copy = __import__('shutil').copy2
            edited = json.dumps([{'Id': 'p1', 'Name': 'Filosof1', 'InstanceId': 15}])
            def copying(source, target, *args, **kwargs):
                result = real_copy(source, target, *args, **kwargs)
                path.write_text(edited, encoding='utf-8')
                return result
            with patch('viking_recovery.shutil.copy2', side_effect=copying):
                with self.assertRaises(RecoveryError):
                    update_profile_instance(path, farm, 8)
            self.assertEqual(path.read_text(encoding='utf-8'), edited)


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
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        engine.profile_path = Path(temporary.name) / 'profile.json'
        engine.profile_path.write_text(json.dumps([{'Name': 'Test', 'InstanceId': -1}]), encoding='utf-8')
        engine._complete_missing_disks = Mock()
        engine._complete_missing_config = Mock()
        engine._repair_missing_vbox = Mock()
        engine._validate_instance_storage = Mock()
        return engine

    def test_reusable_clone_with_foreign_profile_owner_is_not_modified(self) -> None:
        engine = self.make_engine()
        engine.profile_path.write_text(json.dumps([
            {'Name': 'Test', 'InstanceId': -1}, {'Name': 'New customer', 'InstanceId': 6},
        ]), encoding='utf-8')
        engine.list_instances = Mock(return_value=[Instance(6, 'RECOVERY_Test_20260821_195429')])
        with self.assertRaisesRegex(RecoveryError, 'другой ферме'):
            engine.clone_template(Farm('Test', 'm', 'p', '', 'igg', True))
        engine.runner.run.assert_not_called()
        engine._complete_missing_disks.assert_not_called()

    def test_rename_does_not_touch_reused_id_owned_by_new_customer(self) -> None:
        engine = self.make_engine()
        engine.new_index = 13
        engine.profile_path.write_text(json.dumps([
            {'Id': 'old', 'Name': 'Old', 'InstanceId': 12},
            {'Id': 'new', 'Name': 'New', 'InstanceId': 12},
        ]), encoding='utf-8')
        engine.list_instances = Mock(return_value=[Instance(12, 'Old'), Instance(13, 'RECOVERY_Old_clone')])
        with self.assertRaisesRegex(RecoveryError, 'владелец не подтверждён'):
            engine.rename_transaction('Old', farm=Farm('Old', 'm', 'p', '', 'igg', True, 'old', 12))
        engine.runner.run.assert_not_called()

    def test_recovery_clone_name_includes_stable_customer_id(self) -> None:
        engine = self.make_engine()
        engine.profile_path.write_text(json.dumps([{'Id': 'new', 'Name': 'Test', 'InstanceId': -1}]),
                                       encoding='utf-8')
        engine.list_instances = Mock(side_effect=[
            [Instance(6, 'RECOVERY_Test_legacy')],
            [Instance(6, 'RECOVERY_Test_legacy'), Instance(13, 'RECOVERY_Test_6e6577_20261019_120000')],
        ])
        engine.runner.run.return_value = CompletedProcess([], 13, '', '')
        engine.clone_template(Farm('Test', 'm', 'p', '', 'igg', True, 'new'))
        self.assertIn('RECOVERY_Test_6e6577_', engine.runner.run.call_args.args[0][3])
        self.assertEqual(engine.new_index, 13)

    def test_accepts_new_index_as_ldconsole_exit_code(self) -> None:
        engine = self.make_engine()
        engine.list_instances = Mock(
            side_effect=[
                [Instance(0, "4copy")],
                [Instance(0, "4copy"), Instance(6, "RECOVERY_Test_20260827_120000")],
            ]
        )
        engine.runner.run.return_value = CompletedProcess([], 6, "", "")
        farm = Farm("Test", "mail", "password", "123456", "igg", True)
        self.assertEqual(engine.clone_template(farm), 6)
        engine._complete_missing_disks.assert_called_once_with(6)
        engine._complete_missing_config.assert_called_once_with(6)
        engine._repair_missing_vbox.assert_called_once_with(6)
        engine._validate_instance_storage.assert_called_once_with(6)

    def test_reuses_single_recovery_instance(self) -> None:
        engine = self.make_engine()
        engine.list_instances = Mock(return_value=[Instance(6, "RECOVERY_Test_20260821_195429")])
        farm = Farm("Test", "mail", "password", "123456", "igg", True)
        self.assertEqual(engine.clone_template(farm), 6)
        engine.runner.run.assert_not_called()
        engine._complete_missing_disks.assert_called_once_with(6)
        engine._complete_missing_config.assert_called_once_with(6)
        engine._repair_missing_vbox.assert_called_once_with(6)

    def test_renames_default_ldplayer_title_before_repair(self) -> None:
        engine = self.make_engine()
        engine.list_instances = Mock(
            side_effect=[
                [Instance(0, "4copy")],
                [Instance(0, "4copy"), Instance(11, "LDPlayer-11")],
                [Instance(0, "4copy"), Instance(11, "RECOVERY_Test_20260827_120000")],
            ]
        )
        engine.runner.run.side_effect = [
            CompletedProcess([], 11, "", ""),
            CompletedProcess([], 0, "", ""),
        ]
        farm = Farm("Test", "mail", "password", "123456", "igg", True)

        with unittest.mock.patch("viking_recovery.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = datetime(2026, 8, 27, 12, 0, 0)
            self.assertEqual(engine.clone_template(farm), 11)

        rename_call = engine.runner.run.call_args_list[1].args[0]
        self.assertEqual(rename_call[1:4], ["rename", "--index", "11"])
        engine._complete_missing_disks.assert_called_once_with(11)
        engine._complete_missing_config.assert_called_once_with(11)

    def test_completes_missing_secondary_disks_from_template(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            template = root / "vms" / "leidian0"
            clone = root / "vms" / "leidian11"
            template.mkdir(parents=True)
            clone.mkdir(parents=True)
            for name in ("data.vmdk", "sdcard.vmdk", "system.vmdk"):
                (template / name).write_bytes(("template-" + name).encode())
            (clone / "data.vmdk").write_bytes(b"existing-data")
            engine = object.__new__(RecoveryEngine)
            engine.ldplayer_dir = root
            engine.logger = Mock()
            engine.list_instances = Mock(return_value=[Instance(11, "RECOVERY_ANGEL_test")])

            RecoveryEngine._complete_missing_disks(engine, 11)

            self.assertEqual((clone / "data.vmdk").read_bytes(), b"existing-data")
            self.assertEqual((clone / "sdcard.vmdk").read_bytes(), b"template-sdcard.vmdk")
            self.assertEqual((clone / "system.vmdk").read_bytes(), b"template-system.vmdk")

    def test_completes_partial_config_without_replacing_clone_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_dir = root / "vms" / "config"
            config_dir.mkdir(parents=True)
            (config_dir / "leidian0.config").write_text(
                json.dumps(
                    {
                        "propertySettings.phoneIMEI": "template-imei",
                        "basicSettings.rootMode": True,
                        "basicSettings.adbDebug": 1,
                        "advancedSettings.resolution": {"width": 640, "height": 480},
                    }
                ),
                encoding="utf-8",
            )
            (config_dir / "leidian11.config").write_text(
                json.dumps(
                    {
                        "propertySettings.phoneIMEI": "clone-imei",
                        "statusSettings.playerName": "RECOVERY_ANGEL_test",
                    }
                ),
                encoding="utf-8",
            )
            engine = object.__new__(RecoveryEngine)
            engine.ldplayer_dir = root
            engine.logger = Mock()
            engine.list_instances = Mock(return_value=[Instance(11, "RECOVERY_ANGEL_test")])

            RecoveryEngine._complete_missing_config(engine, 11)

            completed = json.loads((config_dir / "leidian11.config").read_text(encoding="utf-8"))
            self.assertEqual(completed["propertySettings.phoneIMEI"], "clone-imei")
            self.assertEqual(completed["statusSettings.playerName"], "RECOVERY_ANGEL_test")
            self.assertEqual(completed["basicSettings.adbDebug"], 1)
            self.assertEqual(completed["advancedSettings.resolution"], {"width": 640, "height": 480})

    def test_repairs_stale_vbox_and_data_uuid(self) -> None:
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
            clone.joinpath("leidian.vbox").write_text(
                '<Machine uuid="{20160302-aaaa-aaaa-0eee-000000000033}" name="leidian51">\n'
                '<HardDisk uuid="{20160302-bbbb-bbbb-0eee-bbbb00000000}" '
                'location="C:\\LDPlayer\\LDPlayer9\\system.vmdk"/>\n'
                '<HardDisk uuid="{20160302-cccc-cccc-0eee-000000000033}" '
                'location="data.vmdk"/>\n'
                "</Machine>\n",
                encoding="utf-8",
            )
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
