"""Возврат архивной строки GnBots и смена номера без потери настроек и чужих ферм."""
import json
import importlib.machinery
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from viking_recovery import (
    Farm, RecoveryEngine, RecoveryError, load_archived_farms, load_farms,
    prepare_profile_recovery, update_profile_instance,
)
from viking_tools import recover_profile_farm


def record(record_id='returning', name='Old farm', index=12, custom='123456'):
    return dict(
        Id=record_id, Name=name, AppId='vikingbot', InstanceId=index, Active=True,
        SessionId=99, HealthCheckPassed=True, CustomField={'keep': [1, 2]},
        MenuData=json.dumps({'ScriptId': 'appmenu', 'Config': {
            'Email': 'old@example.test', 'Password': 'secret', 'Slot': 'igg', 'Custom': custom,
        }}),
        Data=json.dumps([{'ScriptId': 'vikingbot.base.accountswitch', 'IsActive': True,
                          'Config': {'mode': {'value': 'Player'}, 'account': {'value': 2}}}]),
    )


class ArchiveSyncTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.profile = self.root / 'LDPplayer.json'
        self.newcomer = record('new-customer', 'New farm', 12, '654321')
        self.profile.write_text(json.dumps([self.newcomer]), encoding='utf-8')
        self.backups = self.root / 'archives'
        self.archived = record()
        self.archive = self.backup('18__10__2026', [self.archived])

    def backup(self, date, records, filename='LDPplayer.json'):
        path = self.backups / date / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(records), encoding='utf-8')
        return path

    def selected(self):
        return load_archived_farms(self.profile, self.backups)[0]

    def test_complete_archive_to_new_id_preserves_full_config_and_new_customer(self):
        farm = prepare_profile_recovery(self.profile, self.selected())
        prepared = json.loads(self.profile.read_text(encoding='utf-8'))
        self.assertEqual(prepared[0], self.newcomer)
        self.assertEqual(prepared[1]['InstanceId'], -1)
        self.assertFalse(prepared[1]['Active'])
        self.assertEqual(prepared[1]['Data'], self.archived['Data'])
        self.assertEqual(prepared[1]['MenuData'], self.archived['MenuData'])
        self.assertIsNone(farm.instance_id)
        self.assertTrue(farm.reactivation_pending)
        update_profile_instance(self.profile, farm, 13, recovery_verified=True)
        saved = json.loads(self.profile.read_text(encoding='utf-8'))
        self.assertEqual(saved[0], self.newcomer)
        self.assertEqual(saved[1]['InstanceId'], 13)
        self.assertTrue(saved[1]['Active'])
        self.assertEqual(saved[1]['VikingRecoveryRestore']['status'], 'completed')
        self.assertNotIn('UsersDashReactivation', saved[1])
        for field in ['Id', 'Name', 'AppId', 'Data', 'MenuData', 'CustomField']:
            self.assertEqual(saved[1][field], self.archived[field])

    def test_preparation_repeat_never_duplicates_or_resets_assigned_id(self):
        selected = self.selected()
        farm = prepare_profile_recovery(self.profile, selected)
        update_profile_instance(self.profile, farm, 13, recovery_verified=True)
        before = self.profile.read_bytes()
        repeated = prepare_profile_recovery(self.profile, selected)
        self.assertEqual(repeated.instance_id, 13)
        self.assertEqual(self.profile.read_bytes(), before)
        self.assertEqual(len(load_farms(self.profile)), 2)

    def test_archive_restore_does_not_replay_an_old_payment_reactivation(self):
        old_marker = {'status': 'prepared', 'account_id': 'returning', 'requested_at': 'old'}
        self.archived['UsersDashReactivation'] = old_marker
        self.archive.write_text(json.dumps([self.archived]), encoding='utf-8')
        farm = prepare_profile_recovery(self.profile, self.selected())
        update_profile_instance(self.profile, farm, 13, recovery_verified=True)
        saved = json.loads(self.profile.read_text(encoding='utf-8'))[1]
        self.assertNotIn('UsersDashReactivation', saved)
        self.assertEqual(saved['VikingRecoveryRestore']['previous_reactivation'], old_marker)

    def test_usersdash_prepared_record_keeps_its_existing_completion_flow(self):
        prepared = record(index=-1)
        prepared['Active'] = False
        prepared['UsersDashReactivation'] = {'status': 'prepared', 'account_id': 'returning'}
        self.profile.write_text(json.dumps([self.newcomer, prepared]), encoding='utf-8')
        farm = next(farm for farm in load_farms(self.profile) if farm.record_id == 'returning')
        update_profile_instance(self.profile, farm, 13, recovery_verified=True)
        saved = json.loads(self.profile.read_text(encoding='utf-8'))
        self.assertTrue(saved[1]['Active'])
        self.assertEqual(saved[1]['UsersDashReactivation']['status'], 'emulator_ready')
        self.assertEqual(saved[0], self.newcomer)

    def test_latest_daily_snapshot_wins_and_other_profiles_are_excluded(self):
        self.backup('19__10__2026', [record(index=27)])
        self.backup('20__10__2026', [record('foreign', 'Other server')], 'FRESH_NOX.json')
        selected = self.selected()
        self.assertIn('19__10__2026', selected.archive_source)
        self.assertIsNone(selected.instance_id)
        self.assertEqual(len(load_archived_farms(self.profile, self.backups)), 1)
        prepare_profile_recovery(self.profile, selected)
        self.assertEqual(load_archived_farms(self.profile, self.backups), [])

    def test_archive_is_read_only_until_explicit_selection(self):
        before = self.profile.read_bytes()
        self.selected()
        self.assertEqual(self.profile.read_bytes(), before)
        self.assertEqual(list(self.root.glob('*.before_viking_recovery*')), [])

    def test_name_collision_with_different_record_id_is_not_overwritten(self):
        selected = self.selected()
        newcomer = record('new-customer', 'Old farm', 12, '654321')
        self.profile.write_text(json.dumps([newcomer]), encoding='utf-8')
        original = self.profile.read_bytes()
        with self.assertRaisesRegex(RecoveryError, 'занято другой записью'):
            prepare_profile_recovery(self.profile, selected)
        self.assertEqual(self.profile.read_bytes(), original)

    def test_changed_archive_credentials_block_creation(self):
        selected = self.selected()
        changed = record()
        menu = json.loads(changed['MenuData'])
        menu['Config']['Email'] = 'different@example.test'
        changed['MenuData'] = json.dumps(menu)
        self.archive.write_text(json.dumps([changed]), encoding='utf-8')
        original = self.profile.read_bytes()
        with self.assertRaisesRegex(RecoveryError, 'изменились'):
            prepare_profile_recovery(self.profile, selected)
        self.assertEqual(self.profile.read_bytes(), original)

    def test_duplicate_archive_guid_blocks_creation(self):
        selected = self.selected()
        self.archive.write_text(json.dumps([self.archived, self.archived]), encoding='utf-8')
        with self.assertRaisesRegex(RecoveryError, 'однозначно'):
            prepare_profile_recovery(self.profile, selected)

    def test_corrupt_actions_are_not_installed_as_full_config(self):
        selected = self.selected()
        changed = record()
        changed['Data'] = '{broken'
        self.archive.write_text(json.dumps([changed]), encoding='utf-8')
        original = self.profile.read_bytes()
        with self.assertRaisesRegex(RecoveryError, 'повреждены настройки'):
            prepare_profile_recovery(self.profile, selected)
        self.assertEqual(self.profile.read_bytes(), original)

    def test_dry_run_does_not_activate_prepared_record(self):
        farm = prepare_profile_recovery(self.profile, self.selected())
        before = self.profile.read_bytes()
        update_profile_instance(self.profile, farm, 13, dry_run=True, recovery_verified=True)
        self.assertEqual(self.profile.read_bytes(), before)

    def test_archive_restore_does_not_steal_occupied_target(self):
        farm = prepare_profile_recovery(self.profile, self.selected())
        before = self.profile.read_bytes()
        with self.assertRaisesRegex(RecoveryError, 'другой ферме'):
            update_profile_instance(self.profile, farm, 12, recovery_verified=True)
        self.assertEqual(self.profile.read_bytes(), before)

    def test_failed_game_login_leaves_new_record_disabled_without_old_id(self):
        engine = object.__new__(RecoveryEngine)
        engine.profile_path = self.profile
        engine.validate = Mock()
        engine.clone_template = Mock(return_value=13)
        engine.new_index = 13
        engine.launch_and_wait = Mock()
        engine.open_login = Mock(side_effect=RecoveryError('IGG unavailable'))
        with self.assertRaisesRegex(RecoveryError, 'IGG unavailable'):
            engine.recover(self.selected())
        saved = json.loads(self.profile.read_text(encoding='utf-8'))
        self.assertEqual(saved[0], self.newcomer)
        self.assertEqual(saved[1]['InstanceId'], -1)
        self.assertFalse(saved[1]['Active'])

    def test_engine_enables_restored_row_only_after_game_and_rename_succeed(self):
        engine = object.__new__(RecoveryEngine)
        engine.profile_path = self.profile
        engine.validate = Mock()
        engine.clone_template = Mock(return_value=13)
        engine.new_index = 13
        engine.launch_and_wait = Mock()
        engine.open_login = Mock()
        engine.submit_credentials = Mock()
        engine.select_game_id = Mock()

        def check_prepared():
            rows = json.loads(self.profile.read_text(encoding='utf-8'))
            self.assertEqual(rows[1]['InstanceId'], -1)
            self.assertFalse(rows[1]['Active'])

        engine.wait_for_game = Mock(side_effect=check_prepared)
        engine.rename_transaction = Mock(return_value=(None, 13))
        engine.logger = Mock()
        engine._step = Mock()
        self.assertEqual(engine.recover(self.selected()), (None, 13))
        saved = json.loads(self.profile.read_text(encoding='utf-8'))
        self.assertEqual(saved[0], self.newcomer)
        self.assertEqual(saved[1]['InstanceId'], 13)
        self.assertTrue(saved[1]['Active'])
        self.assertEqual(saved[1]['Data'], self.archived['Data'])

    def test_concurrent_profile_edit_cancels_archive_creation(self):
        selected = self.selected()
        real_copy = __import__('shutil').copy2
        edited = json.dumps([self.newcomer, record('parallel', 'Parallel', 18)])

        def copying(source, target, *args, **kwargs):
            result = real_copy(source, target, *args, **kwargs)
            self.profile.write_text(edited, encoding='utf-8')
            return result

        with patch('viking_recovery.shutil.copy2', side_effect=copying):
            with self.assertRaisesRegex(RecoveryError, 'изменился'):
                prepare_profile_recovery(self.profile, selected)
        self.assertEqual(self.profile.read_text(encoding='utf-8'), edited)


class RecoveryMaintenanceTests(unittest.TestCase):
    def test_failure_resumes_only_own_pause(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / 'maintenance.json'
            engine = Mock()
            engine.recover.side_effect = RecoveryError('login failed')
            with patch('viking_tools.Maintenance') as constructor:
                maintenance = constructor.return_value
                maintenance.state_path = state
                maintenance.call.return_value = dict(administrator=True, session_id=1)
                maintenance.pause.side_effect = lambda: state.write_text('{"phase":"paused"}', encoding='utf-8')
                with self.assertRaisesRegex(RecoveryError, 'login failed'):
                    recover_profile_farm(engine, Farm('A', 'm', 'p', '', 'igg', True), Path(folder))
                maintenance.resume.assert_called_once()

    def test_previous_unfinished_pause_is_not_resumed_or_overwritten(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / 'maintenance.json'
            state.write_text('{"phase":"paused"}', encoding='utf-8')
            engine = Mock()
            with patch('viking_tools.Maintenance') as constructor:
                maintenance = constructor.return_value
                maintenance.state_path = state
                with self.assertRaisesRegex(RecoveryError, 'незавершённая'):
                    recover_profile_farm(engine, Farm('A', 'm', 'p', '', 'igg', True), Path(folder))
                engine.recover.assert_not_called()
                maintenance.pause.assert_not_called()
                maintenance.resume.assert_not_called()


class ArchiveInterfaceTests(unittest.TestCase):
    @staticmethod
    def gui_class():
        path = Path(__file__).with_name('viking_tools_gui.pyw')
        loader = importlib.machinery.SourceFileLoader('profile_sync_gui_tests', str(path))
        spec = importlib.util.spec_from_loader(loader.name, loader)
        module = importlib.util.module_from_spec(spec)
        loader.exec_module(module)
        return module.VikingToolsApp

    def test_archive_result_is_selectable_without_becoming_login_target(self):
        gui = self.gui_class()
        window = Mock(archive_generation=1, worker=None)
        window.show_archived.get.return_value = True
        window.recovery_page = {'records': {}, 'tree': Mock()}
        farm = Farm('Old', 'm', 'p', '123456', 'igg', False, 'id', None, True, 'daily.json')
        gui.show_archive_result(window, (1, [farm], ''))
        window.recovery_page['tree'].selection.return_value = ('archive-0',)
        self.assertEqual(gui.selected(window, window.recovery_page), (farm, None))
        window.fill.assert_called_once_with(window.recovery_page)

    def test_stale_archive_scan_result_is_ignored(self):
        gui = self.gui_class()
        window = Mock(archive_generation=2, worker=None)
        window.show_archived.get.return_value = True
        gui.show_archive_result(window, (1, [], ''))
        window.fill.assert_not_called()


if __name__ == '__main__':
    unittest.main()
